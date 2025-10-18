use anyhow::{anyhow, Result};
use rusqlite::{params, Connection, OptionalExtension};

use super::config::Config;
use super::db::{ensure_schema, with_tx};
use super::neighbor_policy::MovementPolicy;

#[derive(Clone, Debug, Default)]
pub struct JpsStats {
    pub planes_processed: usize,
    pub tiles_examined: usize,
    pub spans_written: usize,
    pub jumps_written: usize,
}

pub fn build_jps(tiles_db: &Connection, out_db: &mut Connection, cfg: &Config) -> Result<JpsStats> {
    ensure_schema(out_db)?;

    // Load movement policy from out_db (single row policy_id=1)
    let policy = load_policy(out_db)?;

    // Determine scope
    let planes = load_planes(tiles_db, cfg)?;

    let mut stats = JpsStats::default();

    if cfg.dry_run {
        // Estimate only by counting tiles in scope
        for plane in planes.iter().copied() {
            let (xmin, xmax, ymin, ymax) = tile_bounds_for_scope(tiles_db, cfg, plane)?;
            let cnt: i64 = tiles_db.query_row(
                "SELECT COUNT(*) FROM tiles WHERE plane=?1 AND blocked=0 AND x BETWEEN ?2 AND ?3 AND y BETWEEN ?4 AND ?5",
                params![plane, xmin, xmax, ymin, ymax],
                |r| r.get(0),
            )?;
            stats.planes_processed += 1;
            stats.tiles_examined += cnt as usize;
        }
        return Ok(stats);
    }

    with_tx(out_db, |tx| {
        // Prepared statements against tiles_db
        let mut walk_stmt = tiles_db.prepare("SELECT blocked, walk_mask FROM tiles WHERE x=?1 AND y=?2 AND plane=?3")?;

        for plane in planes.iter().copied() {
            let (xmin, xmax, ymin, ymax) = tile_bounds_for_scope(tiles_db, cfg, plane)?;

            // Idempotence: clear existing JPS data for this scoped region
            tx.execute(
                "DELETE FROM jps_jump WHERE plane=?1 AND x BETWEEN ?2 AND ?3 AND y BETWEEN ?4 AND ?5",
                params![plane, xmin, xmax, ymin, ymax],
            )?;
            tx.execute(
                "DELETE FROM jps_spans WHERE plane=?1 AND x BETWEEN ?2 AND ?3 AND y BETWEEN ?4 AND ?5",
                params![plane, xmin, xmax, ymin, ymax],
            )?;

            // Iterate walkable tiles in scope deterministically by (y,x)
            let mut q = tiles_db.prepare(
                "SELECT x, y FROM tiles WHERE plane=?1 AND blocked=0 AND x BETWEEN ?2 AND ?3 AND y BETWEEN ?4 AND ?5 ORDER BY y, x",
            )?;
            let rows = q.query_map(params![plane, xmin, xmax, ymin, ymax], |row| {
                let x: i32 = row.get(0)?; let y: i32 = row.get(1)?; Ok((x,y))
            })?;

            // Prepare inserts
            let mut ins_span = tx.prepare(
                "INSERT INTO jps_spans (x, y, plane, left_block_at, right_block_at, up_block_at, down_block_at)
                 VALUES (?1,?2,?3,?4,?5,?6,?7)"
            )?;
            let mut ins_jump = tx.prepare(
                "INSERT INTO jps_jump (x, y, plane, dir, next_x, next_y, forced_mask)
                 VALUES (?1,?2,?3,?4,?5,?6,?7)"
            )?;

            for r in rows {
                let (x,y) = r?;
                stats.tiles_examined += 1;

                // Spans
                let left_block_at = scan_until_block(&mut walk_stmt, x, y, plane, -1, 0)?;
                let right_block_at = scan_until_block(&mut walk_stmt, x, y, plane, 1, 0)?;
                let up_block_at = scan_until_block(&mut walk_stmt, x, y, plane, 0, 1)?;
                let down_block_at = scan_until_block(&mut walk_stmt, x, y, plane, 0, -1)?;
                ins_span.execute(params![x, y, plane, left_block_at, right_block_at, up_block_at, down_block_at])?;
                stats.spans_written += 1;

                // Jumps for 4 cardinals (0..3) and optionally 4 diagonals (4..7) based on policy
                // Direction encoding: 0:N,1:E,2:S,3:W, 4:NE,5:SE,6:SW,7:NW
                for dir in 0..8i32 {
                    if dir >= 4 && !policy.allow_diagonals { continue; }
                    let (dx,dy) = dir_to_delta(dir);
                    if let Some((nx,ny,forced_mask)) = next_jump(&mut walk_stmt, x, y, plane, dx, dy, &policy)? {
                        ins_jump.execute(params![x, y, plane, dir, nx, ny, forced_mask])?;
                        stats.jumps_written += 1;
                    } else {
                        // Still insert a row with NULL to mark processed? Prefer sparse: skip when no jump
                    }
                }
            }

            stats.planes_processed += 1;
        }
        Ok(())
    })?;

    Ok(stats)
}

fn load_policy(db: &Connection) -> Result<MovementPolicy> {
    let row: Option<(i64,i64,i64)> = db
        .query_row(
            "SELECT allow_diagonals, allow_corner_cut, unit_radius_tiles FROM movement_policy WHERE policy_id=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?;
    let (ad, cc, r) = row.ok_or_else(|| anyhow!("movement_policy missing (policy_id=1)"))?;
    Ok(MovementPolicy { allow_diagonals: ad != 0, allow_corner_cut: cc != 0, unit_radius_tiles: r as i32 })
}

fn load_planes(tiles_db: &Connection, cfg: &Config) -> Result<Vec<i32>> {
    if let Some(p) = &cfg.planes { return Ok(p.clone()); }
    // Default: all planes present in tiles
    let mut v = Vec::new();
    let mut q = tiles_db.prepare("SELECT DISTINCT plane FROM tiles ORDER BY plane")?;
    let rows = q.query_map([], |r| r.get::<_, i32>(0))?;
    for r in rows { v.push(r?); }
    Ok(v)
}

fn tile_bounds_for_scope(tiles_db: &Connection, cfg: &Config, plane: i32) -> Result<(i32,i32,i32,i32)> {
    if let Some((xmin,xmax,zmin,zmax)) = cfg.chunk_range {
        let x0 = xmin * 64; let x1 = xmax * 64 + 63; let y0 = zmin * 64; let y1 = zmax * 64 + 63;
        return Ok((x0, x1, y0, y1));
    }
    // Fallback to plane min/max
    let row: (i32,i32,i32,i32) = tiles_db.query_row(
        "SELECT MIN(x), MAX(x), MIN(y), MAX(y) FROM tiles WHERE plane=?1",
        params![plane],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
    )?;
    Ok(row)
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

fn scan_until_block(stmt: &mut rusqlite::Statement<'_>, x: i32, y: i32, plane: i32, dx: i32, dy: i32) -> Result<Option<i32>> {
    let mut cx = x; let mut cy = y;
    loop {
        let nx = cx + dx; let ny = cy + dy;
        if !is_walkable(stmt, nx, ny, plane)? {
            // Return coordinate along axis where a block is encountered
            return Ok(if dx != 0 { Some(nx) } else { Some(ny) });
        }
        cx = nx; cy = ny;
        // Safety break: extremely long runs will still finish when hitting world bounds
        if (cx - x).abs() > 10000 || (cy - y).abs() > 10000 { return Ok(None); }
    }
}

fn dir_to_delta(dir: i32) -> (i32,i32) {
    match dir {
        0 => (0, 1),   // N (in DB, y+)
        1 => (1, 0),   // E
        2 => (0, -1),  // S
        3 => (-1, 0),  // W
        4 => (1, 1),   // NE
        5 => (1, -1),  // SE
        6 => (-1, -1), // SW
        7 => (-1, 1),  // NW
        _ => (0, 0),
    }
}

fn next_jump(
    stmt: &mut rusqlite::Statement<'_>,
    x: i32,
    y: i32,
    plane: i32,
    dx: i32,
    dy: i32,
    policy: &MovementPolicy,
) -> Result<Option<(i32,i32,i32)>> {
    // step until we hit obstacle or forced neighbor per JPS rules
    let mut cx = x; let mut cy = y;
    let diagonal = dx != 0 && dy != 0;

    loop {
        let nx = cx + dx; let ny = cy + dy;
        // Corner cutting rule for diagonal
        if diagonal && !policy.allow_corner_cut {
            // Require both adjacent cardinals
            if !is_walkable(stmt, cx + dx, cy, plane)? || !is_walkable(stmt, cx, cy + dy, plane)? {
                // cannot proceed diagonally from current cell
                // if we have advanced at least once, current cell is a jump target
                if cx != x || cy != y { return Ok(Some((cx, cy, 0))); }
                else { return Ok(None); }
            }
        }
        if !is_walkable(stmt, nx, ny, plane)? {
            // blocked ahead: last walkable is a jump point (if we moved at least once)
            if cx != x || cy != y { return Ok(Some((cx, cy, 0))); }
            else { return Ok(None); }
        }

        // Check for forced neighbors at (nx,ny)
        if has_forced_neighbor(stmt, nx, ny, plane, dx, dy)? {
            return Ok(Some((nx, ny, 0)));
        }

        // For diagonal, also stop if either cardinal direction from (nx,ny) would encounter a forced neighbor soon
        if diagonal {
            if has_forced_neighbor(stmt, nx, ny, plane, dx, 0)? || has_forced_neighbor(stmt, nx, ny, plane, 0, dy)? {
                return Ok(Some((nx, ny, 0)));
            }
        }

        cx = nx; cy = ny;
        // Safety: avoid infinite loop in degenerate data
        if (cx - x).abs() > 10000 || (cy - y).abs() > 10000 { return Ok(None); }
    }
}

fn has_forced_neighbor(
    stmt: &mut rusqlite::Statement<'_>,
    x: i32,
    y: i32,
    plane: i32,
    dx: i32,
    dy: i32,
) -> Result<bool> {
    // Straight directions
    if dx != 0 && dy == 0 {
        // E or W
        let sx = if dx > 0 { 1 } else { -1 };
        // up side
        let blocked_up = !is_walkable(stmt, x - sx, y + 1, plane)?;      // tile adjacent above the path
        let pass_up = is_walkable(stmt, x, y + 1, plane)?;                // next to path after step
        // down side
        let blocked_dn = !is_walkable(stmt, x - sx, y - 1, plane)?;
        let pass_dn = is_walkable(stmt, x, y - 1, plane)?;
        return Ok((blocked_up && pass_up) || (blocked_dn && pass_dn));
    }
    if dy != 0 && dx == 0 {
        let sy = if dy > 0 { 1 } else { -1 };
        let blocked_l = !is_walkable(stmt, x - 1, y - sy, plane)?;
        let pass_l = is_walkable(stmt, x - 1, y, plane)?;
        let blocked_r = !is_walkable(stmt, x + 1, y - sy, plane)?;
        let pass_r = is_walkable(stmt, x + 1, y, plane)?;
        return Ok((blocked_l && pass_l) || (blocked_r && pass_r));
    }
    // Diagonal directions: forced neighbor if a block exists next to path causing a need to turn
    if dx != 0 && dy != 0 {
        // If either of the adjacent cells orthogonal to motion is blocked while the cell beyond is open
        let fx1 = x - dx; let fy1 = y; // cell adjacent horizontally behind move
        let fx2 = x; let fy2 = y - dy; // cell adjacent vertically behind move
        let cond1 = !is_walkable(stmt, fx1, fy1, plane)? && is_walkable(stmt, x - dx, y + dy, plane)?;
        let cond2 = !is_walkable(stmt, fx2, fy2, plane)? && is_walkable(stmt, x + dx, y - dy, plane)?;
        return Ok(cond1 || cond2);
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn builds_basic_jps_for_simple_corridor() -> Result<()> {
        let tiles_dbf = NamedTempFile::new().unwrap();
        let out_dbf = NamedTempFile::new().unwrap();
        let mut tiles = Connection::open(tiles_dbf.path())?;
        let mut out = Connection::open(out_dbf.path())?;
        crate::db::create_tables(&mut tiles)?;
        crate::db::create_tables(&mut out)?;

        // Seed chunk and a 1x3 corridor at y=0: (-1,0),(0,0),(1,0)
        tiles.execute("INSERT INTO chunks(chunk_x, chunk_z, chunk_size, tile_count) VALUES (0,0,64,0)", [])?;
        for x in -1..=1 {
            tiles.execute(
                "INSERT INTO tiles(x,y,plane,chunk_x,chunk_z,flag,blocked,walk_mask,blocked_mask,walk_data) VALUES (?1,0,0,0,0,0,0,1,0,'{}')",
                params![x],
            )?;
        }

        let cfg = Config::default();
        let stats = build_jps(&tiles, &mut out, &cfg)?;
        assert!(stats.tiles_examined >= 3);

        // Expect spans and some jumps present for the center tile (0,0)
        let cnt_spans: i64 = out.query_row(
            "SELECT COUNT(*) FROM jps_spans",
            [],
            |r| r.get(0),
        )?;
        assert!(cnt_spans >= 3);

        // Build jumps specifically for E and W from (0,0)
        let exist_e: Option<(i32,i32)> = out
            .query_row(
                "SELECT next_x, next_y FROM jps_jump WHERE x=0 AND y=0 AND plane=0 AND dir=1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        assert!(exist_e.is_some());

        Ok(())
    }
}
