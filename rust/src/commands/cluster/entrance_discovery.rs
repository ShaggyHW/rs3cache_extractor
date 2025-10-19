use anyhow::Result;
use rusqlite::{params, Connection};
use std::collections::{BTreeSet, HashMap};

use super::config::Config;
use super::db::with_tx;
use super::neighbor_policy::Offset;

#[derive(Clone, Debug, Default)]
pub struct EntrancesStats {
    pub chunks_processed: usize,
    pub entrances_created: usize,
}

pub fn discover_entrances(_tiles_db: &Connection, out_db: &mut Connection, cfg: &Config) -> Result<EntrancesStats> {
    // ensure_schema(out_db)?;
    // Determine planes from cluster_tiles or use cfg.planes
    let planes: Vec<i32> = if let Some(p) = &cfg.planes {
        p.clone()
    } else {
        let mut st = out_db.prepare("SELECT DISTINCT plane FROM cluster_tiles")?;
        let rows = st.query_map([], |r| r.get::<_, i32>(0))?;
        let mut v: Vec<i32> = Vec::new();
        for r in rows { v.push(r?); }
        v.sort_unstable();
        v
    };

    let card: [Offset; 4] = [Offset(1,0), Offset(-1,0), Offset(0,1), Offset(0,-1)];

    let mut stats = EntrancesStats::default();
    // (cluster_id, x, y, plane, neighbor_dir)
    let mut entrances: BTreeSet<(i64,i32,i32,i32,String)> = BTreeSet::new();
    let mut affected_clusters: BTreeSet<i64> = BTreeSet::new();
    // Teleport endpoints to process after boundary entrances
    // (cluster_id, x, y, plane, edge_id, is_src)
    let mut tele_eps: Vec<(i64,i32,i32,i32,i64,bool)> = Vec::new();

    for plane in planes.into_iter() {
        // Build tile->cluster map for this plane from cluster_tiles
        let mut stmt = out_db.prepare(
            "SELECT x, y, cluster_id FROM cluster_tiles WHERE plane=?1",
        )?;
        let rows = stmt.query_map(params![plane], |row| {
            let x: i32 = row.get(0)?;
            let y: i32 = row.get(1)?;
            let cid: i64 = row.get(2)?;
            Ok((x,y,cid))
        })?;
        let mut map: HashMap<(i32,i32), i64> = HashMap::new();
        for r in rows { let (x,y,cid) = r?; map.insert((x,y), cid); }
        if map.is_empty() { continue; }

        for (&(x,y), &cid) in map.iter() {
            for &Offset(dx,dy) in &card {
                let nx = x + dx;
                let ny = y + dy;
                if let Some(&ncid) = map.get(&(nx,ny)) {
                    if ncid != cid {
                        let dir = dir_from(dx,dy).to_string();
                        let opp = opp_dir(&dir).to_string();
                        entrances.insert((cid, x, y, plane, dir));
                        entrances.insert((ncid, nx, ny, plane, opp));
                        affected_clusters.insert(cid);
                        affected_clusters.insert(ncid);
                    }
                }
            }
        }

        // Add teleport endpoints as entrances (neighbor_dir = 'TP') on this plane
        let mut estmt = out_db.prepare(
            "SELECT edge_id, src_x, src_y, src_plane, dst_x, dst_y, dst_plane FROM abstract_teleport_edges \
             WHERE (src_plane = ?1 OR dst_plane = ?1)"
        )?;
        let edge_rows = estmt.query_map(params![plane], |row| {
            let edge_id: i64 = row.get(0)?;
            let src_x: Option<i32> = row.get(1)?;
            let src_y: Option<i32> = row.get(2)?;
            let src_plane: Option<i32> = row.get(3)?;
            let dst_x: i32 = row.get(4)?;
            let dst_y: i32 = row.get(5)?;
            let dst_plane: i32 = row.get(6)?;
            Ok((edge_id, src_x, src_y, src_plane, dst_x, dst_y, dst_plane))
        })?;
        for er in edge_rows {
            let (edge_id, src_x, src_y, src_plane, dst_x, dst_y, dst_plane) = er?;
            if Some(plane) == src_plane {
                if let (Some(sx), Some(sy)) = (src_x, src_y) {
                    if let Some(&cid) = map.get(&(sx, sy)) {
                        tele_eps.push((cid, sx, sy, plane, edge_id, true));
                        affected_clusters.insert(cid);
                    }
                }
            }
            if plane == dst_plane {
                if let Some(&cid) = map.get(&(dst_x, dst_y)) {
                    tele_eps.push((cid, dst_x, dst_y, plane, edge_id, false));
                    affected_clusters.insert(cid);
                }
            }
        }

        stats.chunks_processed += 1; // interpret as planes processed
    }

    if !cfg.dry_run {
        with_tx(out_db, |tx| {
            if !affected_clusters.is_empty() {
                let mut del = tx.prepare("DELETE FROM cluster_entrances WHERE cluster_id = ?1")?;
                for cid in affected_clusters.iter() { del.execute(params![cid])?; }
            }
            // Insert boundary and placeholder teleport entrances (dedup by UNIQUE)
            let mut ins = tx.prepare(
                "INSERT OR IGNORE INTO cluster_entrances (cluster_id, x, y, plane, neighbor_dir) VALUES (?1,?2,?3,?4,?5)"
            )?;
            for (cid, x, y, plane, dir) in entrances.iter() {
                ins.execute(params![cid, x, y, plane, dir])?;
            }

            // Insert teleports with edge_id, then fetch entrance_id and update edge linkage
            let mut ins_tp = tx.prepare(
                "INSERT OR IGNORE INTO cluster_entrances (cluster_id, x, y, plane, neighbor_dir, teleport_edge_id) \
                 VALUES (?1,?2,?3,?4,'TP',?5)"
            )?;
            let mut sel_eid = tx.prepare(
                "SELECT entrance_id FROM cluster_entrances WHERE cluster_id=?1 AND x=?2 AND y=?3 AND plane=?4 AND neighbor_dir='TP'"
            )?;
            let mut upd_src = tx.prepare("UPDATE abstract_teleport_edges SET src_entrance=?1 WHERE edge_id=?2")?;
            let mut upd_dst = tx.prepare("UPDATE abstract_teleport_edges SET dst_entrance=?1 WHERE edge_id=?2")?;
            for (cid, x, y, plane, edge_id, is_src) in tele_eps.iter() {
                ins_tp.execute(params![cid, x, y, plane, edge_id])?;
                let entrance_id: i64 = sel_eid.query_row(params![cid, x, y, plane], |r| r.get(0))?;
                if *is_src {
                    upd_src.execute(params![entrance_id, edge_id])?;
                } else {
                    upd_dst.execute(params![entrance_id, edge_id])?;
                }
            }
            Ok(())
        })?;
    }

    // Count unique teleport entrances (dedup by cluster+xy+plane)
    let mut tp_unique: BTreeSet<(i64,i32,i32,i32)> = BTreeSet::new();
    for (cid, x, y, plane, _edge_id, _is_src) in tele_eps.iter() {
        tp_unique.insert((*cid, *x, *y, *plane));
    }
    stats.entrances_created = entrances.len() + tp_unique.len();
    Ok(stats)
}

// chunk-based helpers removed — entrances now derive from persisted cluster_tiles

fn dir_from(dx: i32, dy: i32) -> &'static str {
    match (dx,dy) {
        (1,0) => "E",
        (-1,0) => "W",
        (0,1) => "N",
        (0,-1) => "S",
        _ => "?",
    }
}
fn opp_dir(d: &str) -> &'static str {
    match d {
        "E" => "W",
        "W" => "E",
        "N" => "S",
        "S" => "N",
        _ => "?",
    }
}
// deterministic_cluster_id no longer needed here

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
