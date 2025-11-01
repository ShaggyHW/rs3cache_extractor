use anyhow::Result;
use rusqlite::{params, Connection, OptionalExtension};
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet};

use super::config::Config;
use super::db::{with_tx};
use super::neighbor_policy::{MovementPolicy, Offset};

#[derive(Clone, Debug, Default)]
pub struct IntraStats {
    pub clusters_processed: usize,
    pub edges_created: usize,
}

pub fn build_intra_edges(tiles_db: &Connection, out_db: &mut Connection, cfg: &Config) -> Result<IntraStats> {
    // ensure_schema(out_db)?;

    let mut stats = IntraStats::default();

    // Collect clusters that have at least 2 entrances
    let mut clusters: Vec<(i64,i32)> = Vec::new();
    {
        let mut q = out_db.prepare(
            "SELECT c.cluster_id, c.plane, COUNT(ce.entrance_id) AS ecnt
             FROM clusters c
             JOIN cluster_entrances ce ON ce.cluster_id = c.cluster_id
             GROUP BY c.cluster_id, c.plane
             HAVING ecnt >= 2"
        )?;
        let rows = q.query_map([], |row| {
            let cluster_id: i64 = row.get(0)?;
            let plane: i32 = row.get(1)?;
            Ok((cluster_id, plane))
        })?;
        for r in rows { clusters.push(r?); }
    }

    // Filter by cfg (by plane only here; range filtering is applied per-entrance later)
    clusters.retain(|&(_cid, plane)| {
        if let Some(planes) = &cfg.planes { if !planes.contains(&plane) { return false; } }
        true
    });

    // Deterministic order
    clusters.sort_unstable();

    println!("intra: found {} clusters with >=2 entrances", clusters.len());

    // Movement and costs
    let policy = MovementPolicy::default();
    let offsets = policy.neighbor_offsets();
    let (cost_card, cost_diag) = load_movement_costs(out_db)?;
    println!(
        "intra: movement costs loaded -> straight={} diagonal={}",
        cost_card, cost_diag
    );

    // Track clusters we have cleared to avoid repeated deletes
    let mut cleared_clusters: BTreeSet<i64> = BTreeSet::new();

    if cfg.dry_run {
        println!("intra: dry_run enabled - skipping database writes");
    } else {
        println!("intra: intra edge generation will commit after each cluster");
    }

    for (cluster_id, plane) in clusters.iter().copied() {
        println!("intra: processing cluster {} on plane {}", cluster_id, plane);
        if cfg.dry_run {
            continue;
        }

        let processed_edges = with_tx(out_db, |tx| {
            if !cleared_clusters.contains(&cluster_id) {
                println!("intra: clearing existing intraconnections for cluster {}", cluster_id);
                tx.execute(
                    "DELETE FROM cluster_intraconnections \
                     WHERE entrance_from IN (SELECT entrance_id FROM cluster_entrances WHERE cluster_id=?1)",
                    params![cluster_id],
                )?;
                cleared_clusters.insert(cluster_id);
            }

            // Get entrances for this cluster (filter by chunk range if provided)
            let mut es = tx.prepare(
                "SELECT entrance_id, x, y, neighbor_dir FROM cluster_entrances WHERE cluster_id=?1 ORDER BY entrance_id"
            )?;
            let erows = es.query_map(params![cluster_id], |row| {
                let eid: i64 = row.get(0)?;
                let x: i32 = row.get(1)?;
                let y: i32 = row.get(2)?;
                let d: String = row.get(3)?;
                Ok((eid, x, y, d.chars().next().unwrap_or('?')))
            })?;
            let mut entrances: Vec<(i64,i32,i32,char)> = Vec::new();
            for r in erows { entrances.push(r?); }
            println!(
                "intra: cluster {} has {} entrances",
                cluster_id,
                entrances.len()
            );
            if entrances.len() < 2 {
                println!("intra: skipping cluster {} (needs >=2 entrances)", cluster_id);
                return Ok(None);
            }

            // Optional chunk-range filter: keep cluster only if at least one entrance lies within range
            if let Some((xmin,xmax,zmin,zmax)) = cfg.chunk_range {
                if !entrances.iter().any(|&(_eid, x, y, _)| {
                    let cx = x >> 6; let cz = y >> 6; cx >= xmin && cx <= xmax && cz >= zmin && cz <= zmax
                }) {
                    println!(
                        "intra: skipping cluster {} (no entrances within configured chunk range)",
                        cluster_id
                    );
                    return Ok(None);
                }
            }

            // Load cluster tile set
            let tile_set = load_cluster_tiles(&tx, cluster_id, plane)?;
            println!(
                "intra: cluster {} tile set size = {}",
                cluster_id,
                tile_set.len()
            );

            // Quick membership check for entrances
            if !entrances.iter().all(|&(_eid, x, y, _)| tile_set.contains(&(x,y))) {
                println!("intra: skipping cluster {} (entrance outside cluster tiles)", cluster_id);
                return Ok(None);
            }

            // Pre-prepare insert statement
            let mut ins = tx.prepare(
                "INSERT INTO cluster_intraconnections (entrance_from, entrance_to, cost, path_blob)
                 VALUES (?1,?2,?3,?4)
                 ON CONFLICT(entrance_from, entrance_to)
                 DO UPDATE SET cost = MIN(cluster_intraconnections.cost, excluded.cost),
                               path_blob = COALESCE(cluster_intraconnections.path_blob, excluded.path_blob)"
            )?;

            // Precompute external exit cluster per entrance to allow skipping redundant pairs.
            // External means the neighbor tile belongs to a different cluster than current.
            let mut q_exit = tx.prepare(
                "SELECT cluster_id FROM cluster_tiles WHERE x=?1 AND y=?2 AND plane=?3 LIMIT 1"
            )?;
            let mut exit_clusters: Vec<Option<i64>> = Vec::with_capacity(entrances.len());
            for &(_eid, x, y, d) in entrances.iter() {
                let (dx, dy) = match d {
                    'N' => (0, 1),
                    'S' => (0, -1),
                    'E' => (1, 0),
                    'W' => (-1, 0),
                    _ => (0, 0),
                };
                if dx == 0 && dy == 0 {
                    exit_clusters.push(None);
                    continue;
                }
                let nx = x + dx; let ny = y + dy;
                let cid_opt: Option<i64> = q_exit
                    .query_row(params![nx, ny, plane], |r| r.get(0))
                    .optional()?;
                let cid_opt = cid_opt.filter(|&cid| cid != cluster_id);
                exit_clusters.push(cid_opt);
            }

            // Compute all-pairs shortest paths between entrances deterministically
            let mut cluster_edge_count = 0usize;
            for i in 0..entrances.len() {
                for j in 0..entrances.len() {
                    if i == j { continue; }
                    // Skip redundant pairs where both entrances exit to the same external cluster.
                    if let (Some(ci), Some(cj)) = (exit_clusters[i], exit_clusters[j]) {
                        if ci == cj {
                            let (eid_a, _, _, _ ) = entrances[i];
                            let (eid_b, _, _, _ ) = entrances[j];
                            println!(
                                "intra: skipping pair within cluster {} because entrances {} and {} both exit to cluster {}",
                                cluster_id, eid_a, eid_b, ci
                            );
                            continue;
                        }
                    }
                    let (eid_a, ax, ay, _da) = entrances[i];
                    let (eid_b, bx, by, _db) = entrances[j];
                    // Deterministic ordering via indices already
                    println!(
                        "intra: computing path cluster {} entrance {} -> {}",
                        cluster_id,
                        eid_a,
                        eid_b
                    );
                    let (cost, path_opt) = shortest_path_in_set((ax,ay), (bx,by), &tile_set, offsets, cost_card, cost_diag);
                    if let Some(total) = cost {
                        let blob = if cfg.store_paths { path_opt.map(|p| encode_path_blob(p, plane)) } else { None };
                        ins.execute(params![eid_a, eid_b, total as i64, blob])?;
                        println!(
                            "intra: stored intra edge {} -> {} cost={} (blob={})",
                            eid_a,
                            eid_b,
                            total,
                            blob.as_ref().map(|b| b.len()).unwrap_or(0)
                        );
                        cluster_edge_count += 1;
                    } else {
                        println!(
                            "intra: no path found within cluster {} from entrance {} to {}",
                            cluster_id,
                            eid_a,
                            eid_b
                        );
                    }
                }
            }

            println!(
                "intra: completed cluster {} -> {} intra edges inserted",
                cluster_id,
                cluster_edge_count
            );
            Ok(Some(cluster_edge_count))
        })?;

        if let Some(edge_count) = processed_edges {
            stats.clusters_processed += 1;
            stats.edges_created += edge_count;
        }
    }

    println!(
        "intra: build complete (dry_run={}) -> clusters_processed={} edges_created={}",
        cfg.dry_run,
        stats.clusters_processed,
        stats.edges_created
    );
    Ok(stats)
}

fn load_cluster_tiles(tx: &rusqlite::Transaction<'_>, cluster_id: i64, plane: i32) -> Result<HashSet<(i32,i32)>> {
    let mut tiles_stmt = tx.prepare(
        "SELECT x, y FROM cluster_tiles WHERE cluster_id=?1 AND plane=?2",
    )?;
    let rows = tiles_stmt.query_map(params![cluster_id, plane], |row| {
        let x: i32 = row.get(0)?;
        let y: i32 = row.get(1)?;
        Ok((x,y))
    })?;
    let mut set: HashSet<(i32,i32)> = HashSet::new();
    for r in rows { set.insert(r?); }
    Ok(set)
}

fn load_movement_costs(db: &Connection) -> Result<(i64,i64)> {
    use rusqlite::OptionalExtension;
    let straight: Option<String> = db
        .query_row("SELECT value FROM meta WHERE key='movement_cost_straight'", [], |r| r.get(0))
        .optional()?;
    let diagonal: Option<String> = db
        .query_row("SELECT value FROM meta WHERE key='movement_cost_diagonal'", [], |r| r.get(0))
        .optional()?;
    let sc = straight.and_then(|s| s.parse::<i64>().ok()).unwrap_or(600);
    let dc = diagonal.and_then(|s| s.parse::<i64>().ok()).unwrap_or(1000);
    Ok((sc, dc))
}

fn shortest_path_in_set(
    start: (i32,i32),
    goal: (i32,i32),
    tiles: &HashSet<(i32,i32)>,
    offsets: &[Offset],
    cost_card: i64,
    cost_diag: i64,
) -> (Option<i64>, Option<Vec<(i32,i32)>>) {
    if start == goal { return (Some(0), Some(vec![start])); }
    if !tiles.contains(&start) || !tiles.contains(&goal) { return (None, None); }

    // Dijkstra with deterministic tie-breaking using BTreeMap frontier keyed by (cost, x, y)
    let mut frontier: BTreeSet<(i64, i32, i32)> = BTreeSet::new();
    let mut dist: HashMap<(i32,i32), i64> = HashMap::new();
    let mut prev: HashMap<(i32,i32), (i32,i32)> = HashMap::new();

    dist.insert(start, 0);
    frontier.insert((0, start.0, start.1));

    while let Some((d, x, y)) = frontier.iter().next().cloned() {
        frontier.remove(&(d, x, y));
        if (x,y) == goal { break; }
        for &Offset(dx,dy) in offsets.iter() {
            let nx = x + dx; let ny = y + dy;
            let n = (nx,ny);
            if !tiles.contains(&n) { continue; }
            let step = if dx == 0 || dy == 0 { cost_card } else { cost_diag };
            let nd = d + step;
            match dist.get(&n) {
                Some(&old) if old < nd => {},
                Some(&old) if old == nd => {
                    // tie-breaker: prefer lexicographically smaller (nx,ny)
                    let better = (nx,ny).cmp(&(prev.get(&n).cloned().unwrap_or((i32::MAX,i32::MAX))));
                    if better == Ordering::Less {
                        prev.insert(n, (x,y));
                    }
                },
                _ => {
                    // update
                    if let Some(&old) = dist.get(&n) {
                        frontier.remove(&(old, nx, ny));
                    }
                    dist.insert(n, nd);
                    prev.insert(n, (x,y));
                    frontier.insert((nd, nx, ny));
                }
            }
        }
    }

    if let Some(&total) = dist.get(&goal) {
        // reconstruct path
        let mut path: Vec<(i32,i32)> = Vec::new();
        let mut cur = goal;
        path.push(cur);
        while cur != start {
            if let Some(&p) = prev.get(&cur) { cur = p; path.push(cur); } else { break; }
        }
        path.reverse();
        (Some(total), Some(path))
    } else {
        (None, None)
    }
}

fn encode_path_blob(path: Vec<(i32,i32)>, plane: i32) -> Vec<u8> {
    let reduced = reduce_path_to_breakpoints(&path);
    let mut out = Vec::with_capacity(reduced.len() * 12);
    let plane_bytes = plane.to_le_bytes();
    for (x,y) in reduced {
        out.extend_from_slice(&x.to_le_bytes());
        out.extend_from_slice(&y.to_le_bytes());
        out.extend_from_slice(&plane_bytes);
    }
    out
}

fn reduce_path_to_breakpoints(path: &[(i32,i32)]) -> Vec<(i32,i32)> {
    match path.len() {
        0 => Vec::new(),
        1 => path.to_vec(),
        _ => {
            let mut reduced: Vec<(i32,i32)> = Vec::with_capacity(path.len());
            reduced.push(path[0]);
            for window in path.windows(3) {
                let prev = window[0];
                let cur = window[1];
                let next = window[2];
                let dir_in = movement_dir(prev, cur);
                let dir_out = movement_dir(cur, next);
                if dir_in != dir_out {
                    if reduced.last().copied() != Some(cur) {
                        reduced.push(cur);
                    }
                }
            }
            if let Some(&last) = path.last() {
                if reduced.last().copied() != Some(last) {
                    reduced.push(last);
                }
            }
            reduced
        }
    }
}

fn movement_dir(from: (i32,i32), to: (i32,i32)) -> Option<(i32,i32)> {
    let dx = to.0 - from.0;
    let dy = to.1 - from.1;
    if dx == 0 && dy == 0 {
        None
    } else {
        Some((dx.signum(), dy.signum()))
    }
}

// deterministic_cluster_id no longer needed with explicit cluster IDs in DB

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn setup_small_cluster() -> Result<(Connection, Connection, Config, (i64, (i32,i32), (i32,i32)))> {
        let tiles_dbf = NamedTempFile::new().unwrap();
        let out_dbf = NamedTempFile::new().unwrap();
        let mut tiles = Connection::open(tiles_dbf.path())?;
        let mut out = Connection::open(out_dbf.path())?;
        crate::db::create_tables(&mut tiles)?;
        crate::db::create_tables(&mut out)?;

        // A 2-tile line, plane=0
        tiles.execute("INSERT INTO tiles(x,y,plane,flag,blocked,walk_mask,blocked_mask,walk_data) VALUES (0,0,0,0,0,1,0,'')", [])?;
        tiles.execute("INSERT INTO tiles(x,y,plane,flag,blocked,walk_mask,blocked_mask,walk_data) VALUES (1,0,0,0,0,1,0,'')", [])?;

        // Create a cluster with two tiles and two entrances
        let cid: i64 = 1;
        out.execute("INSERT INTO clusters(cluster_id, plane, label, tile_count) VALUES (?1,0,0,2)", params![cid])?;
        out.execute("INSERT INTO cluster_tiles(cluster_id, x, y, plane) VALUES (?1,0,0,0)", params![cid])?;
        out.execute("INSERT INTO cluster_tiles(cluster_id, x, y, plane) VALUES (?1,1,0,0)", params![cid])?;
        out.execute("INSERT INTO cluster_entrances(cluster_id, x, y, plane, neighbor_dir) VALUES (?1,0,0,0,'E')", params![cid])?;
        out.execute("INSERT INTO cluster_entrances(cluster_id, x, y, plane, neighbor_dir) VALUES (?1,1,0,0,'W')", params![cid])?;

        let cfg = Config::default();
        Ok((tiles, out, cfg, (cid, (0,0), (1,0))))
    }

    #[test]
    fn intra_edges_no_blobs_when_flag_false() -> Result<()> {
        let (tiles, mut out, mut cfg, _info) = setup_small_cluster()?;
        cfg.store_paths = false;
        let stats = build_intra_edges(&tiles, &mut out, &cfg)?;
        assert_eq!(stats.clusters_processed, 1);
        // Expect two directed edges (A->B and B->A)
        let cnt: i64 = out.query_row("SELECT COUNT(*) FROM cluster_intraconnections", [], |r| r.get(0))?;
        assert_eq!(cnt, 2);
        let n_blobs: i64 = out.query_row("SELECT COUNT(*) FROM cluster_intraconnections WHERE path_blob IS NOT NULL", [], |r| r.get(0))?;
        assert_eq!(n_blobs, 0);
        Ok(())
    }

    #[test]
    fn intra_edges_with_blobs_when_flag_true() -> Result<()> {
        let (tiles, mut out, mut cfg, _info) = setup_small_cluster()?;
        cfg.store_paths = true;
        let stats = build_intra_edges(&tiles, &mut out, &cfg)?;
        assert_eq!(stats.clusters_processed, 1);
        let cnt: i64 = out.query_row("SELECT COUNT(*) FROM cluster_intraconnections", [], |r| r.get(0))?;
        assert_eq!(cnt, 2);
        let n_blobs: i64 = out.query_row("SELECT COUNT(*) FROM cluster_intraconnections WHERE path_blob IS NOT NULL", [], |r| r.get(0))?;
        assert_eq!(n_blobs, 2);
        Ok(())
    }

    #[test]
    fn encode_path_blob_reduces_straight_segments() {
        let path = vec![(0, 0), (0, 1), (0, 2), (1, 2), (2, 2)];
        let blob = encode_path_blob(path.clone(), 0);
        assert_eq!(blob.len(), 3 * 12);

        let decoded: Vec<(i32, i32)> = blob
            .chunks_exact(12)
            .map(|chunk| {
                let (xb, yb) = chunk.split_at(4);
                let (yb, _) = yb.split_at(4);
                (
                    i32::from_le_bytes(xb.try_into().unwrap()),
                    i32::from_le_bytes(yb.try_into().unwrap()),
                )
            })
            .collect();
        assert_eq!(decoded, vec![(0, 0), (0, 2), (2, 2)]);
    }

    #[test]
    fn encode_path_blob_preserves_diagonal_runs() {
        let path = vec![(0, 0), (1, 1), (2, 2), (3, 3)];
        let blob = encode_path_blob(path.clone(), 0);
        assert_eq!(blob.len(), 2 * 12);

        let decoded: Vec<(i32, i32)> = blob
            .chunks_exact(12)
            .map(|chunk| {
                let (xb, yb) = chunk.split_at(4);
                let (yb, _) = yb.split_at(4);
                (
                    i32::from_le_bytes(xb.try_into().unwrap()),
                    i32::from_le_bytes(yb.try_into().unwrap()),
                )
            })
            .collect();
        assert_eq!(decoded, vec![(0, 0), (3, 3)]);
    }
}
