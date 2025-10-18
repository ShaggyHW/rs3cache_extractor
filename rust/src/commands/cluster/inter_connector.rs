use anyhow::Result;
use rusqlite::{params, Connection, OptionalExtension};
use std::collections::BTreeSet;

use super::config::Config;
use super::db::{ensure_schema, with_tx};

#[derive(Clone, Debug, Default)]
pub struct InterStats {
    pub entrances_examined: usize,
    pub inter_edges_created: usize,
}

pub fn build_inter_edges(tiles_db: &Connection, out_db: &mut Connection, cfg: &Config) -> Result<InterStats> {
    ensure_schema(out_db)?;

    let mut stats = InterStats::default();

    // Load cost for a cardinal step from meta
    let (cost_card, _cost_diag) = load_movement_costs(out_db)?;

    // Fetch entrances from out_db
    let mut entrances: Vec<(i64, i32, i32, i32, char)> = Vec::new();
    {
        let mut q = out_db.prepare(
            "SELECT entrance_id, x, y, plane, neighbor_dir FROM cluster_entrances ORDER BY plane, x, y, entrance_id",
        )?;
        let rows = q.query_map([], |row| {
            let id: i64 = row.get(0)?;
            let x: i32 = row.get(1)?;
            let y: i32 = row.get(2)?;
            let plane: i32 = row.get(3)?;
            let d: String = row.get(4)?;
            Ok((id, x, y, plane, d.chars().next().unwrap_or('?')))
        })?;
        for r in rows { entrances.push(r?); }
    }

    // Filter by cfg (planes, chunk-range) using derived chunk indices from x,y
    entrances.retain(|&(_id, x, y, plane, _)| {
        if let Some(planes) = &cfg.planes { if !planes.contains(&plane) { return false; } }
        if let Some((xmin,xmax,zmin,zmax)) = cfg.chunk_range {
            let cx = x >> 6;
            let cz = y >> 6;
            if cx < xmin || cx > xmax || cz < zmin || cz > zmax { return false; }
        }
        true
    });

    // Deterministic order already ensured

    if !cfg.dry_run {
        with_tx(out_db, |tx| {
            // Idempotence: clear prior interconnections where entrance_from belongs to scope
            if !entrances.is_empty() {
                let mut del = tx.prepare("DELETE FROM cluster_interconnections WHERE entrance_from=?1")?;
                let mut seen: BTreeSet<i64> = BTreeSet::new();
                for (eid, _x, _y, _pl, _d) in entrances.iter() {
                    if seen.insert(*eid) {
                        del.execute(params![eid])?;
                    }
                }
            }

            // Prepared statements
            let mut is_walk_stmt = tiles_db.prepare(
                "SELECT blocked, walk_mask FROM tiles WHERE x=?1 AND y=?2 AND plane=?3",
            )?;
            let mut walk_data_stmt = tiles_db.prepare(
                "SELECT walk_data FROM tiles WHERE x=?1 AND y=?2 AND plane=?3",
            )?;

            let mut opp_q = tx.prepare(
                "SELECT entrance_id FROM cluster_entrances WHERE plane=?1 AND x=?2 AND y=?3 AND neighbor_dir=?4 LIMIT 1",
            )?;

            let mut ins = tx.prepare(
                "INSERT INTO cluster_interconnections (entrance_from, entrance_to, cost)
                 VALUES (?1,?2,?3)
                 ON CONFLICT(entrance_from, entrance_to)
                 DO UPDATE SET cost = MIN(cluster_interconnections.cost, excluded.cost)",
            )?;

            for (eid, x, y, plane, d) in entrances.iter().copied() {
                stats.entrances_examined += 1;
                let (dx, dy) = dir_delta(d);
                if dx == 0 && dy == 0 { continue; }

                // Both tiles walkable and pass-through allowed across boundary
                if !is_walkable(&mut is_walk_stmt, x, y, plane)? { continue; }
                if !is_walkable(&mut is_walk_stmt, x + dx, y + dy, plane)? { continue; }
                if !can_cross(&mut walk_data_stmt, x, y, plane, d)? { continue; }

                let opp_dir = opposite(d);
                let opp_id: Option<i64> = opp_q
                    .query_row(params![plane, x + dx, y + dy, opp_dir_as_str(opp_dir)], |r| r.get(0))
                    .optional()?;
                let Some(opp_eid) = opp_id else { continue; };

                println!("Inter");
                println!("{} {} {} {} {} {}", eid, opp_eid, cost_card, opp_eid, eid, cost_card);
                // Insert both directions with cardinal cost
                ins.execute(params![eid, opp_eid, cost_card])?;
                ins.execute(params![opp_eid, eid, cost_card])?;
                stats.inter_edges_created += 2;
            }

            Ok(())
        })?;
    } else {
        // Dry-run: estimate creations based on checks
        let mut is_walk_stmt = tiles_db.prepare(
            "SELECT blocked, walk_mask FROM tiles WHERE x=?1 AND y=?2 AND plane=?3",
        )?;
        let mut walk_data_stmt = tiles_db.prepare(
            "SELECT walk_data FROM tiles WHERE x=?1 AND y=?2 AND plane=?3",
        )?;
        for (_eid, x, y, plane, d) in entrances.iter().copied() {
            stats.entrances_examined += 1;
            let (dx, dy) = dir_delta(d);
            if dx == 0 && dy == 0 { continue; }
            if !is_walkable(&mut is_walk_stmt, x, y, plane)? { continue; }
            if !is_walkable(&mut is_walk_stmt, x + dx, y + dy, plane)? { continue; }
            if !can_cross(&mut walk_data_stmt, x, y, plane, d)? { continue; }
            stats.inter_edges_created += 2;
        }
    }

    Ok(stats)
}

fn load_movement_costs(db: &Connection) -> Result<(i64,i64)> {
    let straight: String = db.query_row("SELECT value FROM meta WHERE key='movement_cost_straight'", [], |r| r.get(0))?;
    let diagonal: String = db.query_row("SELECT value FROM meta WHERE key='movement_cost_diagonal'", [], |r| r.get(0))?;
    let sc = straight.parse::<i64>().unwrap_or(1024);
    let dc = diagonal.parse::<i64>().unwrap_or(1448);
    Ok((sc, dc))
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

fn can_cross(stmt: &mut rusqlite::Statement<'_>, x: i32, y: i32, plane: i32, d: char) -> Result<bool> {
    let (dx, dy) = dir_delta(d);
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

fn dir_delta(d: char) -> (i32,i32) {
    match d {
        'N' => (0, 1),
        'S' => (0, -1),
        'E' => (1, 0),
        'W' => (-1, 0),
        _ => (0, 0),
    }
}

fn opposite(d: char) -> char {
    match d {
        'N' => 'S',
        'S' => 'N',
        'E' => 'W',
        'W' => 'E',
        _ => '?',
    }
}

fn opp_dir_as_str(d: char) -> &'static str { match d { 'N' => "N", 'S' => "S", 'E' => "E", 'W' => "W", _ => "?" } }

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn creates_bidirectional_edges_with_cardinal_cost() -> Result<()> {
        let tiles_dbf = NamedTempFile::new().unwrap();
        let out_dbf = NamedTempFile::new().unwrap();
        let mut tiles = Connection::open(tiles_dbf.path())?;
        let mut out = Connection::open(out_dbf.path())?;
        crate::db::create_tables(&mut tiles)?;
        crate::db::create_tables(&mut out)?;

        // Seed chunks
        tiles.execute("INSERT INTO chunks(chunk_x, chunk_z, chunk_size, tile_count) VALUES (0,0,64,0)", [])?;
        tiles.execute("INSERT INTO chunks(chunk_x, chunk_z, chunk_size, tile_count) VALUES (1,0,64,0)", [])?;
        out.execute("INSERT INTO chunks(chunk_x, chunk_z, chunk_size, tile_count) VALUES (0,0,64,0)", [])?;
        out.execute("INSERT INTO chunks(chunk_x, chunk_z, chunk_size, tile_count) VALUES (1,0,64,0)", [])?;

        // Two adjacent walkable tiles across boundary
        tiles.execute(
            r#"INSERT INTO tiles(x,y,plane,chunk_x,chunk_z,flag,blocked,walk_mask,blocked_mask,walk_data)
                VALUES (63,0,0,0,0,0,0,1,0,'{"top":true,"bottom":true,"left":true,"right":true}')"#,
            [],
        )?;
        tiles.execute(
            r#"INSERT INTO tiles(x,y,plane,chunk_x,chunk_z,flag,blocked,walk_mask,blocked_mask,walk_data)
                VALUES (64,0,0,1,0,0,0,1,0,'{"top":true,"bottom":true,"left":true,"right":true}')"#,
            [],
        )?;

        // Entrances on each side of border
        out.execute("INSERT INTO chunk_clusters(cluster_id, chunk_x, chunk_z, plane, label, tile_count) VALUES (1,0,0,0,0,1)", [])?;
        out.execute("INSERT INTO chunk_clusters(cluster_id, chunk_x, chunk_z, plane, label, tile_count) VALUES (2,1,0,0,0,1)", [])?;
        out.execute("INSERT INTO cluster_entrances(cluster_id, x, y, plane, neighbor_dir) VALUES (1,63,0,0,'E')", [])?;
        out.execute("INSERT INTO cluster_entrances(cluster_id, x, y, plane, neighbor_dir) VALUES (2,64,0,0,'W')", [])?;

        let cfg = Config::default();
        let stats = build_inter_edges(&tiles, &mut out, &cfg)?;
        assert_eq!(stats.entrances_examined, 2);
        let cnt: i64 = out.query_row("SELECT COUNT(*) FROM cluster_interconnections", [], |r| r.get(0))?;
        assert_eq!(cnt, 2);
        let cost_card: String = out.query_row("SELECT value FROM meta WHERE key='movement_cost_straight'", [], |r| r.get(0))?;
        let expected = cost_card.parse::<i64>().unwrap_or(1024);
        let max_cost: i64 = out.query_row("SELECT MAX(cost) FROM cluster_interconnections", [], |r| r.get(0))?;
        assert_eq!(max_cost, expected);
        Ok(())
    }
}
