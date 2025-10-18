use anyhow::Result;
use rusqlite::{params, Connection, OptionalExtension};
use std::collections::{HashSet, VecDeque};

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
    ensure_schema(out_db)?;

    let mut stats = BuildStats::default();

    // Collect distinct (chunk_x, chunk_z, plane) candidates from tiles
    let mut stmt =
        tiles_db.prepare("SELECT DISTINCT chunk_x, chunk_z, plane FROM tiles WHERE blocked = 0")?;
    let rows = stmt.query_map([], |row| {
        let cx: i32 = row.get(0)?;
        let cz: i32 = row.get(1)?;
        let plane: i32 = row.get(2)?;
        Ok((cx, cz, plane))
    })?;

    let mut candidates: Vec<(i32, i32, i32)> = Vec::new();
    for r in rows {
        let (cx, cz, pl) = r?;
        candidates.push((cx, cz, pl));
    }

    // Apply filters from cfg
    candidates.retain(|(cx, cz, plane)| {
        if let Some(planes) = &cfg.planes {
            if !planes.contains(&plane) {
                return false;
            }
        }
        if let Some((xmin, xmax, zmin, zmax)) = cfg.chunk_range {
            if *cx < xmin || *cx > xmax || *cz < zmin || *cz > zmax {
                return false;
            }
        }
        true
    });

    // Sort deterministically
    candidates.sort_unstable();

    // Movement policy (defaults; tune later to match Python exactly)
    let policy = MovementPolicy::default();
    let offsets = policy.neighbor_offsets();

    for (chunk_x, chunk_z, plane) in candidates.into_iter() {
        // Load walkable tile coordinates for this chunk+plane
        let mut tiles_stmt = tiles_db.prepare(
            "SELECT x, y FROM tiles WHERE blocked=0 AND plane=?1 AND chunk_x=?2 AND chunk_z=?3",
        )?;
        let tile_rows = tiles_stmt.query_map(params![plane, chunk_x, chunk_z], |row| {
            let x: i32 = row.get(0)?;
            let y: i32 = row.get(1)?;
            Ok((x, y))
        })?;
        let mut walkable: HashSet<(i32, i32)> = HashSet::new();
        for r in tile_rows {
            walkable.insert(r?);
        }
        if walkable.is_empty() {
            continue;
        }

        // Prepare statements for walkability and edge checks in this scope
        let mut is_walk_stmt = tiles_db.prepare(
            "SELECT blocked, walk_mask FROM tiles WHERE x=?1 AND y=?2 AND plane=?3",
        )?;
        let mut walk_data_stmt = tiles_db.prepare(
            "SELECT walk_data FROM tiles WHERE x=?1 AND y=?2 AND plane=?3",
        )?;

        // Connected components using BFS
        let mut components: Vec<Vec<(i32, i32)>> = Vec::new();
        let mut visited: HashSet<(i32, i32)> = HashSet::new();
        for &start in walkable.iter() {
            if visited.contains(&start) {
                continue;
            }
            let mut comp: Vec<(i32, i32)> = Vec::new();
            let mut q: VecDeque<(i32, i32)> = VecDeque::new();
            visited.insert(start);
            q.push_back(start);
            while let Some((sx, sy)) = q.pop_front() {
                comp.push((sx, sy));
                for &Offset(dx, dy) in offsets.iter() {
                    let nx = sx + dx;
                    let ny = sy + dy;
                    let n = (nx, ny);
                    if !visited.contains(&n) && walkable.contains(&n) {
                        if !can_step(&mut is_walk_stmt, &mut walk_data_stmt, &policy, sx, sy, nx, ny, plane)? {
                            continue;
                        }
                        visited.insert(n);
                        q.push_back(n);
                    }
                }
                
            }
            // Stable order within component
            comp.sort_unstable();
            components.push(comp);
        }

        // Deterministic ordering of components: by first tile, then length
        components.sort_by(|a, b| a.first().cmp(&b.first()).then(a.len().cmp(&b.len())));

        // Write to output DB in a transaction
        let comps_len = components.len();
        if !cfg.dry_run {
            with_tx(out_db, |tx| {
                // Idempotence: clear prior rows for this scope
                tx.execute(
                    "DELETE FROM cluster_tiles WHERE cluster_id IN (
                        SELECT cluster_id FROM chunk_clusters WHERE chunk_x=?1 AND chunk_z=?2 AND plane=?3
                    )",
                    params![chunk_x, chunk_z, plane],
                )?;
                tx.execute(
                    "DELETE FROM chunk_clusters WHERE chunk_x=?1 AND chunk_z=?2 AND plane=?3",
                    params![chunk_x, chunk_z, plane],
                )?;

                let mut insert_cluster = tx.prepare(
                    "INSERT INTO chunk_clusters (cluster_id, chunk_x, chunk_z, plane, label, tile_count) VALUES (?1,?2,?3,?4,?5,?6)",
                )?;
                let mut insert_tile = tx.prepare(
                    "INSERT INTO cluster_tiles (cluster_id, x, y, plane) VALUES (?1,?2,?3,?4)",
                )?;
                for (idx, comp) in components.iter().enumerate() {
                    let local_index = idx as i64;
                    println!(
                        "Plane: {}, Chunk X: {}, Chunk Z: {}, Local Index: {}",
                        plane, chunk_x, chunk_z, local_index
                    );
                    let cluster_id = deterministic_cluster_id(
                        plane as i64,
                        chunk_x as i64,
                        chunk_z as i64,
                        local_index,
                    );
                    println!("Cluster ID: {}", cluster_id);
                    let tile_count = comp.len() as i64;
                    println!("Tile Count: {}", tile_count);

                    insert_cluster.execute(params![
                        cluster_id,
                        chunk_x,
                        chunk_z,
                        plane,
                        local_index,
                        tile_count
                    ])?;
                    // Persist tiles for this cluster
                    for &(tx_x, tx_y) in comp.iter() {
                        insert_tile.execute(params![cluster_id, tx_x, tx_y, plane])?;
                    }
                }
                Ok(())
            })?;
        }

        stats.chunks_processed += 1;
        stats.clusters_created += comps_len;
    }

    Ok(stats)
}

fn deterministic_cluster_id(plane: i64, chunk_x: i64, chunk_z: i64, local_index: i64) -> i64 {
    // Construct a stable 64-bit identifier from inputs; simple mix to avoid collisions.
    // Layout: [plane:4][chunk_x:24][chunk_z:24][local_index:12]
    let p = (plane & 0xF) << 60;
    let cx = (chunk_x & 0xFFFFFF) << 36;
    let cz = (chunk_z & 0xFFFFFF) << 12;
    let li = (local_index & 0xFFF);
    p | cx | cz | li
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
        'N' => allow(a.get("bottom").copied()) && allow(b.get("top").copied()),
        'S' => allow(a.get("top").copied()) && allow(b.get("bottom").copied()),
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
