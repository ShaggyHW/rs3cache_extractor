use anyhow::Result;
use rusqlite::{params, Connection, OptionalExtension};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

use super::config::Config;
use super::db::{ensure_schema, with_tx};
use super::neighbor_policy::{MovementPolicy, Offset};

#[derive(Clone, Debug, Default)]
pub struct EntrancesStats {
    pub chunks_processed: usize,
    pub entrances_created: usize,
}

pub fn discover_entrances(tiles_db: &Connection, out_db: &mut Connection, cfg: &Config) -> Result<EntrancesStats> {
    ensure_schema(out_db)?;

    // Cache per-plane chunk label maps: (plane) -> ( (cx,cz) -> tile->cluster_id )
    let mut cache: BTreeMap<i32, HashMap<(i32,i32), HashMap<(i32,i32), i64>>> = BTreeMap::new();

    // List candidate chunk+plane scopes from tiles
    let mut stmt = tiles_db.prepare("SELECT DISTINCT chunk_x, chunk_z, plane FROM tiles WHERE blocked = 0")?;
    let rows = stmt.query_map([], |row| {
        let cx: i32 = row.get(0)?;
        let cz: i32 = row.get(1)?;
        let plane: i32 = row.get(2)?;
        Ok((cx, cz, plane))
    })?;

    let mut scopes: Vec<(i32,i32,i32)> = Vec::new();
    for r in rows { let (cx,cz,pl) = r?; scopes.push((cx,cz,pl)); }

    // Apply filters
    scopes.retain(|(cx, cz, plane)| {
        if let Some(planes) = &cfg.planes { if !planes.contains(&plane) { return false; } }
        if let Some((xmin,xmax,zmin,zmax)) = cfg.chunk_range { if *cx < xmin || *cx > xmax || *cz < zmin || *cz > zmax { return false; } }
        true
    });
    scopes.sort_unstable();

    let policy = MovementPolicy::default();
    let card: [Offset; 4] = [Offset(1,0), Offset(-1,0), Offset(0,1), Offset(0,-1)];

    let mut stats = EntrancesStats::default();

    // Collect entrances for entire run; also track which cluster_ids are affected for idempotent delete
    let mut entrances: BTreeSet<(i64,i32,i32,i32,char)> = BTreeSet::new();
    let mut affected_clusters: BTreeSet<i64> = BTreeSet::new();

    for (cx, cz, plane) in scopes.into_iter() {
        let map = compute_chunk_labels(tiles_db, &mut cache, plane, cx, cz, &policy)?;
        if map.is_empty() { continue; }

        // For each walkable tile in this chunk, check 4-neighbors
        for (&(x,y), &cid) in map.iter() {
            for &Offset(dx,dy) in &card {
                let nx = x + dx;
                let ny = y + dy;
                let n_chunk_x = nx >> 6; // 64-based chunks
                let n_chunk_z = ny >> 6;
                let neighbor_map = if n_chunk_x == cx && n_chunk_z == cz {
                    Some(&map)
                } else {
                    // Pull or compute neighbor chunk map lazily
                    ensure_chunk_labels(tiles_db, &mut cache, plane, n_chunk_x, n_chunk_z, &policy)?
                };
                if let Some(nmap) = neighbor_map {
                    if let Some(&ncid) = nmap.get(&(nx,ny)) {
                        if ncid != cid {
                            let dir = dir_from(dx,dy);
                            let opp = opp_dir(dir);
                            entrances.insert((cid, x, y, plane, dir));
                            entrances.insert((ncid, nx, ny, plane, opp));
                            affected_clusters.insert(cid);
                            affected_clusters.insert(ncid);
                        }
                    }
                }
            }
        }

        stats.chunks_processed += 1;
    }

    if !cfg.dry_run {
        with_tx(out_db, |tx| {
            // Clear existing entrances for affected clusters for idempotence
            if !affected_clusters.is_empty() {
                // Build a temporary table of cluster_ids (or delete in batches)
                let mut del = tx.prepare("DELETE FROM cluster_entrances WHERE cluster_id = ?1")?;
                for cid in affected_clusters.iter() {
                    del.execute(params![cid])?;
                }
            }
            let mut exists = tx.prepare("SELECT 1 FROM chunk_clusters WHERE cluster_id=?1 LIMIT 1")?;
            let mut ins = tx.prepare(
                "INSERT INTO cluster_entrances (cluster_id, x, y, plane, neighbor_dir) VALUES (?1,?2,?3,?4,?5)"
            )?;
            for (cid, x, y, plane, dir) in entrances.iter() {
                println!("Cluster Entrance");

                println!("{} {} {} {} {}", cid, x, y, plane, dir);
                let ok: Option<i64> = exists.query_row(params![cid], |r| r.get(0)).optional()?;
                if ok.is_some() {
                    ins.execute(params![cid, x, y, plane, &dir.to_string()])?;
                }
            }
            Ok(())
        })?;
    }

    stats.entrances_created = entrances.len();
    Ok(stats)
}

fn ensure_chunk_labels<'a>(
    tiles_db: &Connection,
    cache: &'a mut BTreeMap<i32, HashMap<(i32,i32), HashMap<(i32,i32), i64>>>,
    plane: i32,
    cx: i32,
    cz: i32,
    policy: &MovementPolicy,
) -> Result<Option<&'a HashMap<(i32,i32), i64>>> {
    if cx < i32::MIN/2 || cz < i32::MIN/2 { return Ok(None); }
    let v = cache.entry(plane).or_default();
    if !v.contains_key(&(cx,cz)) {
        let m = compute_labels_for_chunk(tiles_db, plane, cx, cz, policy)?;
        v.insert((cx,cz), m);
    }
    Ok(v.get(&(cx,cz)))
}

fn compute_chunk_labels(
    tiles_db: &Connection,
    cache: &mut BTreeMap<i32, HashMap<(i32,i32), HashMap<(i32,i32), i64>>>,
    plane: i32,
    cx: i32,
    cz: i32,
    policy: &MovementPolicy,
) -> Result<HashMap<(i32,i32), i64>> {
    // compute and store for this chunk
    let m = compute_labels_for_chunk(tiles_db, plane, cx, cz, policy)?;
    cache.entry(plane).or_default().insert((cx,cz), m.clone());
    Ok(m)
}

fn compute_labels_for_chunk(
    tiles_db: &Connection,
    plane: i32,
    cx: i32,
    cz: i32,
    policy: &MovementPolicy,
) -> Result<HashMap<(i32,i32), i64>> {
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
    for &start in walkable.iter() {
        if visited.contains(&start) { continue; }
        let mut comp: Vec<(i32,i32)> = Vec::new();
        let mut q: VecDeque<(i32,i32)> = VecDeque::new();
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

fn dir_from(dx: i32, dy: i32) -> char {
    match (dx,dy) {
        (1,0) => 'E',
        (-1,0) => 'W',
        (0,1) => 'N',
        (0,-1) => 'S',
        _ => '?',
    }
}
fn opp_dir(d: char) -> char {
    match d {
        'E' => 'W',
        'W' => 'E',
        'N' => 'S',
        'S' => 'N',
        _ => '?',
    }
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
    use crate::commands::cluster::cluster_builder;
    use tempfile::NamedTempFile;

    #[test]
    fn empty_db_produces_no_entrances() -> Result<()> {
        let mut tiles_dbf = NamedTempFile::new().unwrap();
        let mut out_dbf = NamedTempFile::new().unwrap();
        let mut tiles = Connection::open(tiles_dbf.path())?;
        let mut out = Connection::open(out_dbf.path())?;
        crate::db::create_tables(&mut tiles)?;
        crate::db::create_tables(&mut out)?;
        let cfg = Config::default();
        // Build clusters first to populate chunk_clusters referenced by entrances
        cluster_builder::build_clusters(&tiles, &mut out, &cfg)?;
        let stats = discover_entrances(&tiles, &mut out, &cfg)?;
        assert_eq!(stats.entrances_created, 0);
        Ok(())
    }

    #[test]
    fn adjacent_across_chunk_boundary_creates_pair_of_entrances() -> Result<()> {
        let tiles_dbf = NamedTempFile::new().unwrap();
        let out_dbf = NamedTempFile::new().unwrap();
        let mut tiles = Connection::open(tiles_dbf.path())?;
        let mut out = Connection::open(out_dbf.path())?;
        crate::db::create_tables(&mut tiles)?;
        crate::db::create_tables(&mut out)?;

        // Insert chunks for FK integrity (both tiles and out DB)
        tiles.execute("INSERT INTO chunks(chunk_x, chunk_z, chunk_size, tile_count) VALUES (0,0,64,0)", [])?;
        tiles.execute("INSERT INTO chunks(chunk_x, chunk_z, chunk_size, tile_count) VALUES (1,0,64,0)", [])?;
        out.execute("INSERT INTO chunks(chunk_x, chunk_z, chunk_size, tile_count) VALUES (0,0,64,0)", [])?;
        out.execute("INSERT INTO chunks(chunk_x, chunk_z, chunk_size, tile_count) VALUES (1,0,64,0)", [])?;

        // Two adjacent walkable tiles across boundary: (63,0) in chunk (0,0) and (64,0) in chunk (1,0)
        tiles.execute(
            "INSERT INTO tiles(x,y,plane,chunk_x,chunk_z,flag,blocked,walk_mask,blocked_mask,walk_data) VALUES (63,0,0,0,0,0,0,0,0,'')",
            [],
        )?;
        tiles.execute(
            "INSERT INTO tiles(x,y,plane,chunk_x,chunk_z,flag,blocked,walk_mask,blocked_mask,walk_data) VALUES (64,0,0,1,0,0,0,0,0,'')",
            [],
        )?;

        let cfg = Config::default();
        let stats = discover_entrances(&tiles, &mut out, &cfg)?;
        assert_eq!(stats.chunks_processed, 1 /* only (0,0,0) will iterate map; neighbor chunk is lazy-fetched */ + 0);

        // Expect two entrance rows (each side of the boundary)
        let count: i64 = out.query_row("SELECT COUNT(*) FROM cluster_entrances", [], |r| r.get(0))?;
        assert_eq!(count, 2);
        Ok(())
    }
}
