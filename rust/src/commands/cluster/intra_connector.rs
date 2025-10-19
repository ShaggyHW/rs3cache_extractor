use anyhow::Result;
use rusqlite::{params, Connection};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

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

    // Collect clusters that have at least 2 entrances, joined with chunk coordinates
    let mut clusters: Vec<(i64,i32,i32,i32)> = Vec::new();
    {
        let mut q = out_db.prepare(
            "SELECT cc.cluster_id, cc.chunk_x, cc.chunk_z, cc.plane, COUNT(ce.entrance_id) AS ecnt
             FROM chunk_clusters cc
             JOIN cluster_entrances ce ON ce.cluster_id = cc.cluster_id
             GROUP BY cc.cluster_id, cc.chunk_x, cc.chunk_z, cc.plane
             HAVING ecnt >= 2"
        )?;
        let rows = q.query_map([], |row| {
            let cluster_id: i64 = row.get(0)?;
            let chunk_x: i32 = row.get(1)?;
            let chunk_z: i32 = row.get(2)?;
            let plane: i32 = row.get(3)?;
            Ok((cluster_id, chunk_x, chunk_z, plane))
        })?;
        for r in rows { clusters.push(r?); }
    }

    // Filter by cfg
    clusters.retain(|&(_cid, cx, cz, plane)| {
        if let Some(planes) = &cfg.planes { if !planes.contains(&plane) { return false; } }
        if let Some((xmin,xmax,zmin,zmax)) = cfg.chunk_range { if cx < xmin || cx > xmax || cz < zmin || cz > zmax { return false; } }
        true
    });

    // Deterministic order
    clusters.sort_unstable();

    // Movement and costs
    let policy = MovementPolicy::default();
    let offsets = policy.neighbor_offsets();
    let (cost_card, cost_diag) = load_movement_costs(out_db)?;

    // Track chunks we have cleared to avoid repeated deletes
    let mut cleared_scopes: BTreeSet<(i32,i32,i32)> = BTreeSet::new();

    // Compute and cache label maps per (plane, cx, cz)
    let mut label_cache: BTreeMap<(i32,i32,i32), HashMap<(i32,i32), i64>> = BTreeMap::new();

    if !cfg.dry_run {
        with_tx(out_db, |tx| {
            for (cluster_id, cx, cz, plane) in clusters.iter().copied() {
                if !cleared_scopes.contains(&(cx,cz,plane)) {
                    tx.execute("DELETE FROM cluster_intraconnections WHERE chunk_x_from=?1 AND chunk_z_from=?2 AND plane_from=?3",
                        params![cx, cz, plane])?;
                    cleared_scopes.insert((cx,cz,plane));
                }

                // Get entrances for this cluster
                let mut es = tx.prepare(
                    "SELECT entrance_id, x, y FROM cluster_entrances WHERE cluster_id=?1 ORDER BY entrance_id"
                )?;
                let erows = es.query_map(params![cluster_id], |row| {
                    let eid: i64 = row.get(0)?;
                    let x: i32 = row.get(1)?;
                    let y: i32 = row.get(2)?;
                    Ok((eid, x, y))
                })?;
                let mut entrances: Vec<(i64,i32,i32)> = Vec::new();
                for r in erows { entrances.push(r?); }
                if entrances.len() < 2 { continue; }

                // Get label map for the chunk
                let lbl = ensure_labels(tiles_db, &mut label_cache, plane, cx, cz, &policy)?;

                // Quick membership check for entrances
                if !entrances.iter().all(|&(_eid, x, y)| lbl.get(&(x,y)) == Some(&cluster_id)) {
                    // If any entrance isn't in this cluster's label set, skip
                    continue;
                }

                // Pre-prepare insert statement
                let mut ins = tx.prepare(
                    "INSERT INTO cluster_intraconnections (
                        chunk_x_from, chunk_z_from, plane_from, entrance_from, entrance_to, cost, path_blob
                    ) VALUES (?1,?2,?3,?4,?5,?6,?7)"
                )?;

                // Compute all-pairs shortest paths between entrances deterministically
                for i in 0..entrances.len() {
                    for j in 0..entrances.len() {
                        if i == j { continue; }
                        let (eid_a, ax, ay) = entrances[i];
                        let (eid_b, bx, by) = entrances[j];
                        // Deterministic ordering via indices already
                        let (cost, path_opt) = shortest_path((ax,ay), (bx,by), &lbl, offsets, cost_card, cost_diag);
                        if let Some(total) = cost {
                            let blob = if cfg.store_paths { path_opt.map(encode_path_blob) } else { None };
                            println!("Intra");
                            println!("{} {} {} {} {} {}", cx, cz, plane, eid_a, eid_b, total);
                            ins.execute(params![cx, cz, plane, eid_a, eid_b, total as i64, blob])?;
                            stats.edges_created += 1;
                        }
                    }
                }

                stats.clusters_processed += 1;
            }
            Ok(())
        })?;
    }

    Ok(stats)
}

fn ensure_labels(
    tiles_db: &Connection,
    cache: &mut BTreeMap<(i32,i32,i32), HashMap<(i32,i32), i64>>,
    plane: i32,
    cx: i32,
    cz: i32,
    policy: &MovementPolicy,
) -> Result<HashMap<(i32,i32), i64>> {
    if let Some(m) = cache.get(&(plane,cx,cz)) { return Ok(m.clone()); }
    let m = compute_labels_for_chunk(tiles_db, plane, cx, cz, policy)?;
    cache.insert((plane,cx,cz), m.clone());
    Ok(m)
}

fn compute_labels_for_chunk(
    tiles_db: &Connection,
    plane: i32,
    cx: i32,
    cz: i32,
    policy: &MovementPolicy,
) -> Result<HashMap<(i32,i32), i64>> {
    use rusqlite::params;
    use std::collections::VecDeque;

    // Load walkable tiles in chunk
    let mut tiles_stmt = tiles_db.prepare(
        "SELECT x, y FROM tiles WHERE blocked=0 AND plane=?1 AND chunk_x=?2 AND chunk_z=?3",
    )?;
    let rows = tiles_stmt.query_map(params![plane, cx, cz], |row| {
        let x: i32 = row.get(0)?;
        let y: i32 = row.get(1)?;
        Ok((x,y))
    })?;
    let mut walkable: HashSet<(i32,i32)> = HashSet::new();
    for r in rows { walkable.insert(r?); }

    // BFS components
    let mut comps: Vec<Vec<(i32,i32)>> = Vec::new();
    let mut visited: HashSet<(i32,i32)> = HashSet::new();
    let offsets = policy.neighbor_offsets();
    let mut q: VecDeque<(i32,i32)> = VecDeque::new();
    for &start in walkable.iter() {
        if visited.contains(&start) { continue; }
        let mut comp: Vec<(i32,i32)> = Vec::new();
        visited.insert(start);
        q.push_back(start);
        while let Some((sx,sy)) = q.pop_front() {
            comp.push((sx,sy));
            for &Offset(dx,dy) in offsets.iter() {
                let nx = sx + dx;
                let ny = sy + dy;
                let n = (nx,ny);
                if !visited.contains(&n) && walkable.contains(&n) {
                    visited.insert(n);
                    q.push_back(n);
                }
            }
        }
        comp.sort_unstable();
        comps.push(comp);
    }
    comps.sort_by(|a,b| a.first().cmp(&b.first()).then(a.len().cmp(&b.len())));

    // Build tile->cluster_id map using deterministic id scheme
    let mut map: HashMap<(i32,i32), i64> = HashMap::new();
    for (idx, comp) in comps.iter().enumerate() {
        let local_index = idx as i64;
        let cid = deterministic_cluster_id(plane as i64, cx as i64, cz as i64, local_index);
        for &(x,y) in comp.iter() {
            map.insert((x,y), cid);
        }
    }
    Ok(map)
}

fn load_movement_costs(db: &Connection) -> Result<(i64,i64)> {
    let straight: String = db.query_row("SELECT value FROM meta WHERE key='movement_cost_straight'", [], |r| r.get(0))?;
    let diagonal: String = db.query_row("SELECT value FROM meta WHERE key='movement_cost_diagonal'", [], |r| r.get(0))?;
    let sc = straight.parse::<i64>().unwrap_or(1024);
    let dc = diagonal.parse::<i64>().unwrap_or(1448);
    Ok((sc, dc))
}

fn shortest_path(
    start: (i32,i32),
    goal: (i32,i32),
    label_map: &HashMap<(i32,i32), i64>,
    offsets: &[Offset],
    cost_card: i64,
    cost_diag: i64,
) -> (Option<i64>, Option<Vec<(i32,i32)>>) {
    if start == goal { return (Some(0), Some(vec![start])); }
    let cid = match label_map.get(&start) { Some(c) => *c, None => return (None, None) };
    if label_map.get(&goal) != Some(&cid) { return (None, None); }

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
            if label_map.get(&n) != Some(&cid) { continue; }
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

fn encode_path_blob(path: Vec<(i32,i32)>) -> Vec<u8> {
    let mut out = Vec::with_capacity(path.len() * 8);
    for (x,y) in path {
        out.extend_from_slice(&x.to_le_bytes());
        out.extend_from_slice(&y.to_le_bytes());
    }
    out
}

fn deterministic_cluster_id(plane: i64, chunk_x: i64, chunk_z: i64, local_index: i64) -> i64 {
    let p  = (plane & 0xF) << 60;
    let cx = (chunk_x & 0xFFFFFF) << 36;
    let cz = (chunk_z & 0xFFFFFF) << 12;
    let li = (local_index & 0xFFF);
    p | cx | cz | li
}

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

        tiles.execute("INSERT INTO chunks(chunk_x, chunk_z, chunk_size, tile_count) VALUES (0,0,64,0)", [])?;
        out.execute("INSERT INTO chunks(chunk_x, chunk_z, chunk_size, tile_count) VALUES (0,0,64,0)", [])?;

        // A 2-tile line inside one chunk (0,0), plane=0
        tiles.execute("INSERT INTO tiles(x,y,plane,chunk_x,chunk_z,flag,blocked,walk_mask,blocked_mask,walk_data) VALUES (0,0,0,0,0,0,0,0,0,'')", [])?;
        tiles.execute("INSERT INTO tiles(x,y,plane,chunk_x,chunk_z,flag,blocked,walk_mask,blocked_mask,walk_data) VALUES (1,0,0,0,0,0,0,0,0,'')", [])?;

        // Build clusters to get cluster_id
        let cfg = Config::default();
        // Use a minimal builder to compute cluster_id
        let lbl = super::compute_labels_for_chunk(&tiles, 0, 0, 0, &MovementPolicy::default())?;
        let cid = *lbl.get(&(0,0)).unwrap();

        // Insert two entrances at the two tiles
        out.execute("INSERT INTO chunk_clusters(cluster_id, chunk_x, chunk_z, plane, label, tile_count) VALUES (?1,0,0,0,0,2)", params![cid])?;
        out.execute("INSERT INTO cluster_entrances(cluster_id, x, y, plane, neighbor_dir) VALUES (?1,0,0,0,'E')", params![cid])?;
        out.execute("INSERT INTO cluster_entrances(cluster_id, x, y, plane, neighbor_dir) VALUES (?1,1,0,0,'W')", params![cid])?;

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
}
