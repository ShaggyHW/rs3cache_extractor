use anyhow::Result;
use rusqlite::{params, Connection, OptionalExtension};
use std::collections::{BTreeSet, HashSet, VecDeque};

use super::config::Config;
use super::db::{ensure_schema, with_tx};
use super::neighbor_policy::{MovementPolicy, Offset};

#[derive(Clone, Debug, Default)]
pub struct BuildStats {
    pub chunks_processed: usize,
    pub clusters_created: usize,
}

pub fn build_clusters(
    tiles_db: &Connection,
    out_db: &mut Connection,
    cfg: &Config,
) -> Result<BuildStats> {
    // ensure_schema(out_db)?;

    let mut stats = BuildStats::default();
    println!("Starting cluster build");

    // Determine planes to process
    let planes: Vec<i32> = if let Some(p) = &cfg.planes {
        p.clone()
    } else {
        let mut st = tiles_db.prepare("SELECT DISTINCT plane FROM tiles WHERE blocked=0")?;
        let rows = st.query_map([], |r| r.get::<_, i32>(0))?;
        let mut v: Vec<i32> = Vec::new();
        for r in rows { v.push(r?); }
        v.sort_unstable();
        v
    };

    // Movement policy
    let policy = MovementPolicy::default();
    let offsets = policy.neighbor_offsets();

    // Prepare statements reused across planes
    let mut is_walk_stmt = tiles_db.prepare(
        "SELECT blocked, walk_mask FROM tiles WHERE x=?1 AND y=?2 AND plane=?3",
    )?;
    let mut walk_data_stmt = tiles_db.prepare(
        "SELECT walk_data FROM tiles WHERE x=?1 AND y=?2 AND plane=?3",
    )?;

    for plane in planes.into_iter() {
        // Load all walkable tiles for this plane
        let mut tiles_stmt = tiles_db.prepare(
            "SELECT x, y FROM tiles WHERE blocked=0 AND plane=?1",
        )?;
        let rows = tiles_stmt.query_map(params![plane], |row| {
            let x: i32 = row.get(0)?;
            let y: i32 = row.get(1)?;
            Ok((x, y))
        })?;
        let mut walkable: HashSet<(i32, i32)> = HashSet::new();
        for r in rows { walkable.insert(r?); }
        if walkable.is_empty() {
            println!("Plane {plane} has no walkable tiles, skipping");
            continue;
        }

        println!("Processing plane {plane} with {} walkable tiles", walkable.len());

        // Seeds: lexicographically sorted to prefer (0,0) if present
        let mut seeds: Vec<(i32,i32)> = walkable.iter().cloned().collect();
        seeds.sort_unstable();

        let mut components: Vec<Vec<(i32,i32)>> = Vec::new();
        let mut visited: HashSet<(i32,i32)> = HashSet::new();

        for start in seeds.into_iter() {
            if visited.contains(&start) { continue; }
            println!("Processing seed {start:?}");

            // BFS with 64x64 bounding box limit
            let mut comp: Vec<(i32,i32)> = Vec::new();
            let mut q: VecDeque<(i32,i32)> = VecDeque::new();
            let (mut min_x, mut max_x) = (start.0, start.0);
            let (mut min_y, mut max_y) = (start.1, start.1);
            visited.insert(start);
            q.push_back(start);

            while let Some((sx, sy)) = q.pop_front() {
                comp.push((sx, sy));
                for &Offset(dx, dy) in offsets.iter() {
                    let nx = sx + dx;
                    let ny = sy + dy;
                    let n = (nx, ny);
                    if visited.contains(&n) { continue; }
                    if !walkable.contains(&n) { continue; }
                    if !can_step(&mut is_walk_stmt, &mut walk_data_stmt, &policy, sx, sy, nx, ny, plane)? { continue; }

                    // Check bounding box constraint (max size 64x64)
                    let new_min_x = min_x.min(nx);
                    let new_max_x = max_x.max(nx);
                    let new_min_y = min_y.min(ny);
                    let new_max_y = max_y.max(ny);
                    let width = (new_max_x - new_min_x + 1).abs() as i32;
                    let height = (new_max_y - new_min_y + 1).abs() as i32;
                    if width > 64 || height > 64 { continue; }

                    // Accept
                    min_x = new_min_x; max_x = new_max_x;
                    min_y = new_min_y; max_y = new_max_y;
                    visited.insert(n);
                    q.push_back(n);
                }
            }

            comp.sort_unstable();
            println!("Component {comp:?}");
            components.push(comp);
        }

        println!("Plane {plane} has {} components", components.len());

        // Deterministic ordering: by first tile then length
        components.sort_by(|a,b| a.first().cmp(&b.first()).then(a.len().cmp(&b.len())));

        
        let comps_len = components.len();
        if !cfg.dry_run {
            println!("Inserting {} clusters for plane {}", comps_len, plane);
            with_tx(out_db, |tx| {
                // Clear existing clusters on this plane (idempotent rebuild per plane)
                let mut sel_ids = tx.prepare(
                    "SELECT cluster_id FROM clusters WHERE plane=?1",
                )?;
                let rows = sel_ids.query_map(params![plane], |r| r.get::<_, i64>(0))?;
                let mut to_del: BTreeSet<i64> = BTreeSet::new();
                for r in rows { to_del.insert(r?); }
                drop(sel_ids);

                if !to_del.is_empty() {
                    let mut del_tiles = tx.prepare("DELETE FROM cluster_tiles WHERE cluster_id=?1")?;
                    let mut del_clusters = tx.prepare("DELETE FROM clusters WHERE cluster_id=?1")?;
                    for cid in to_del.iter() {
                        del_tiles.execute(params![cid])?;
                        del_clusters.execute(params![cid])?;
                    }
                }

                let mut insert_cluster = tx.prepare(
                    "INSERT INTO clusters (cluster_id, plane, label, tile_count)
                     VALUES (?1,?2,?3,?4)
                     ON CONFLICT(cluster_id) DO UPDATE SET plane=excluded.plane, label=excluded.label, tile_count=excluded.tile_count",
                )?;
                let mut insert_tile = tx.prepare(
                    "INSERT INTO cluster_tiles (cluster_id, x, y, plane) VALUES (?1,?2,?3,?4)",
                )?;

                for (idx, comp) in components.iter().enumerate() {
                    println!("Processing component {comp:?}");
                    let local_index = idx as i64;
                    let cluster_id = deterministic_cluster_id_plane(plane as i64, local_index);
                    let tile_count = comp.len() as i64;

                    insert_cluster.execute(params![cluster_id, plane, local_index, tile_count])?;
                    for &(tx_x, tx_y) in comp.iter() {
                        insert_tile.execute(params![cluster_id, tx_x, tx_y, plane])?;
                    }
                }
                Ok(())
            })?;
        }

        stats.chunks_processed += 1; // interpret as planes_processed
        stats.clusters_created += comps_len;
        println!("Plane {plane} complete: {comps_len} clusters");
    }

    println!(
        "Cluster build complete: {} planes processed, {} clusters created",
        stats.chunks_processed,
        stats.clusters_created
    );

    Ok(stats)
}

fn deterministic_cluster_id_plane(plane: i64, local_index: i64) -> i64 {
    // Layout: [plane:8][local_index:56] — deterministic per plane
    let p = (plane & 0xFF) << 56;
    let li = local_index & 0x00FF_FFFF_FFFF_FFFF;
    p | li
}

fn can_step(
    is_walk_stmt: &mut rusqlite::Statement<'_>,
    walk_data_stmt: &mut rusqlite::Statement<'_>,
    policy: &MovementPolicy,
    sx: i32,
    sy: i32,
    nx: i32,
    ny: i32,
    plane: i32,
) -> Result<bool> {
    let dx = nx - sx;
    let dy = ny - sy;
    if dx == 0 && dy == 0 { return Ok(false); }

    // Both endpoints must be walkable
    if !is_walkable(is_walk_stmt, sx, sy, plane)? { return Ok(false); }
    if !is_walkable(is_walk_stmt, nx, ny, plane)? { return Ok(false); }

    // Cardinal move
    if dx == 0 || dy == 0 {
        let dir = if dx == 1 { 'E' } else if dx == -1 { 'W' } else if dy == 1 { 'N' } else { 'S' };
        return can_cross_cardinal(walk_data_stmt, sx, sy, plane, dir);
    }

    // Diagonal move
    let h_dir = if dx == 1 { 'E' } else { 'W' };
    let v_dir = if dy == 1 { 'N' } else { 'S' };
    let mx1 = sx + dx; // horizontal neighbor x
    let my1 = sy;      // horizontal neighbor y
    let mx2 = sx;      // vertical neighbor x
    let my2 = sy + dy; // vertical neighbor y

    // Both intermediate tiles must be within walkable set if corner cutting is disabled.
    if !policy.allow_corner_cut {
        if !is_walkable(is_walk_stmt, mx1, my1, plane)? { return Ok(false); }
        if !is_walkable(is_walk_stmt, mx2, my2, plane)? { return Ok(false); }
        // Require four cardinal edge passes around the corner to avoid squeezing through walls
        if !can_cross_cardinal(walk_data_stmt, sx, sy, plane, h_dir)? { return Ok(false); }
        if !can_cross_cardinal(walk_data_stmt, sx, sy, plane, v_dir)? { return Ok(false); }
        // From horizontal neighbor into dest (vertical)
        if !can_cross_cardinal(walk_data_stmt, mx1, my1, plane, v_dir)? { return Ok(false); }
        // From vertical neighbor into dest (horizontal)
        if !can_cross_cardinal(walk_data_stmt, mx2, my2, plane, h_dir)? { return Ok(false); }
        return Ok(true);
    }

    // Corner cut allowed: permit either L-shaped path around the corner
    let path_h_then_v =
        is_walkable(is_walk_stmt, mx1, my1, plane)? &&
        can_cross_cardinal(walk_data_stmt, sx, sy, plane, h_dir)? &&
        can_cross_cardinal(walk_data_stmt, mx1, my1, plane, v_dir)?;

    let path_v_then_h =
        is_walkable(is_walk_stmt, mx2, my2, plane)? &&
        can_cross_cardinal(walk_data_stmt, sx, sy, plane, v_dir)? &&
        can_cross_cardinal(walk_data_stmt, mx2, my2, plane, h_dir)?;

    Ok(path_h_then_v || path_v_then_h)
}

fn is_walkable(stmt: &mut rusqlite::Statement<'_>, x: i32, y: i32, plane: i32) -> Result<bool> {
    let row: Option<(Option<i64>, Option<i64>)> = stmt
        .query_row(params![x, y, plane], |r| Ok((r.get(0)?, r.get(1)?)))
        .optional()?;
    if let Some((blocked, walk_mask)) = row {
        let b = blocked.unwrap_or(1);
        let w = walk_mask.unwrap_or(0);
        Ok(b == 0 && w != 0)
    } else {
        Ok(false)
    }
}

fn can_cross_cardinal(stmt: &mut rusqlite::Statement<'_>, x: i32, y: i32, plane: i32, d: char) -> Result<bool> {
    let (dx, dy) = match d { 'N' => (0,1), 'S' => (0,-1), 'E' => (1,0), 'W' => (-1,0), _ => (0,0) };
    if dx == 0 && dy == 0 { return Ok(false); }
    let nx = x + dx;
    let ny = y + dy;
    let a = walk_flags(stmt, x, y, plane)?;
    let b = walk_flags(stmt, nx, ny, plane)?;
    let allow = |m: Option<bool>| m.unwrap_or(true);
    Ok(match d {
        'N' => allow(a.get("top").copied()) && allow(b.get("bottom").copied()),
        'S' => allow(a.get("bottom").copied()) && allow(b.get("top").copied()),
        'E' => allow(a.get("right").copied()) && allow(b.get("left").copied()),
        'W' => allow(a.get("left").copied()) && allow(b.get("right").copied()),
        _ => true,
    })
}

fn walk_flags(stmt: &mut rusqlite::Statement<'_>, x: i32, y: i32, plane: i32) -> Result<std::collections::HashMap<String, bool>> {
    use serde_json::Value;
    let row: Option<Option<String>> = stmt.query_row(params![x, y, plane], |r| r.get(0)).optional()?;
    let mut out = std::collections::HashMap::new();
    if let Some(Some(s)) = row {
        if let Ok(Value::Object(map)) = serde_json::from_str::<Value>(&s) {
            for (k, v) in map.into_iter() {
                out.insert(k, v.as_bool().unwrap_or(false));
            }
        }
    }
    Ok(out)
}
