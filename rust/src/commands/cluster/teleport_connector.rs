use anyhow::Result;
use rusqlite::{params, Connection, OptionalExtension};
use std::collections::{BTreeMap, HashMap, HashSet};

use super::config::Config;
use super::db::{with_tx};
use super::neighbor_policy::{MovementPolicy, Offset};

#[derive(Clone, Debug, Default)]
pub struct TeleportStats {
    pub entrances_created: usize,
    pub edges_created: usize,
}

// Phase A: ensure teleport entrances exist (before Intra)
pub fn ensure_teleport_entrances(tiles_db: &Connection, out_db: &mut Connection, cfg: &Config) -> Result<TeleportStats> {
    //ensure_schema(out_db)?;

    // Cache label maps per (plane,cx,cz)
    let mut label_cache: BTreeMap<(i32,i32,i32), HashMap<(i32,i32), i64>> = BTreeMap::new();
    let policy = MovementPolicy::default();

    // Load abstract teleport edges
    let mut edges: Vec<(i64, Option<i32>, Option<i32>, Option<i32>, Option<i32>, Option<i32>, Option<i32>, i64)> = Vec::new();
    {
        let mut q = out_db.prepare(
            "SELECT edge_id, src_x, src_y, src_plane, dst_x, dst_y, dst_plane, cost FROM abstract_teleport_edges ORDER BY edge_id"
        )?;
        let rows = q.query_map([], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, Option<i32>>(1)?, r.get::<_, Option<i32>>(2)?, r.get::<_, Option<i32>>(3)?,
                r.get::<_, Option<i32>>(4)?, r.get::<_, Option<i32>>(5)?, r.get::<_, Option<i32>>(6)?,
                r.get::<_, i64>(7)?,
            ))
        })?;
        for r in rows { edges.push(r?); }
    }

    // Pre-delete existing teleport entrances in scope for idempotence
    if !cfg.dry_run {
        with_tx(out_db, |tx| {
            // Build list of entrance_ids to delete: those with non-null teleport_edge_id and in scope
            let mut scoped_ids: Vec<i64> = Vec::new();
            let mut q = tx.prepare(
                "SELECT entrance_id, x, y, plane FROM cluster_entrances WHERE teleport_edge_id IS NOT NULL"
            )?;
            let rows = q.query_map([], |r| Ok((
                r.get::<_, i64>(0)?, r.get::<_, i32>(1)?, r.get::<_, i32>(2)?, r.get::<_, i32>(3)?
            )))?;
            for r in rows {
                let (eid, x, y, plane) = r?;
                if let Some(planes) = &cfg.planes { if !planes.contains(&plane) { continue; } }
                if let Some((xmin,xmax,zmin,zmax)) = cfg.chunk_range {
                    let cx = x >> 6; let cz = y >> 6; if cx < xmin || cx > xmax || cz < zmin || cz > zmax { continue; }
                }
                scoped_ids.push(eid);
            }
            drop(q);
            let mut del = tx.prepare("DELETE FROM cluster_entrances WHERE entrance_id=?1")?;
            for eid in scoped_ids { del.execute(params![eid])?; }
            Ok(())
        })?;
    }

    let mut created: usize = 0;

    // Helper: insert entrance for endpoint (x,y,plane) with teleport_edge_id; choose free neighbor_dir
    let mut ins = out_db.prepare(
        "INSERT INTO cluster_entrances (cluster_id, x, y, plane, neighbor_dir, teleport_edge_id) VALUES (?1,?2,?3,?4,?5,?6)"
    )?;
    let mut existing_dirs_stmt = out_db.prepare(
        "SELECT neighbor_dir FROM cluster_entrances WHERE cluster_id=?1 AND x=?2 AND y=?3 AND plane=?4"
    )?;

    for (edge_id, sx, sy, spl, dx, dy, dpl, _cost) in edges.iter().copied() {
        // process both endpoints independently; src may be NULL (e.g., lodestone) -> only create dst entrance
        for (x_opt, y_opt, p_opt) in [(sx, sy, spl), (dx, dy, dpl)].into_iter() {
            let (x, y, plane) = match (x_opt, y_opt, p_opt) { (Some(x), Some(y), Some(p)) => (x, y, p), _ => continue };
            if let Some(planes) = &cfg.planes { if !planes.contains(&plane) { continue; } }
            if let Some((xmin,xmax,zmin,zmax)) = cfg.chunk_range {
                let cx = x >> 6; let cz = y >> 6; if cx < xmin || cx > xmax || cz < zmin || cz > zmax { continue; }
            }
            // Compute cluster_id via label map
            let lbl = ensure_labels(tiles_db, &mut label_cache, plane, x >> 6, y >> 6, &policy)?;
            let Some(&cluster_id) = lbl.get(&(x,y)) else { continue; };
            // Choose an available neighbor_dir among N,E,S,W avoiding conflicts
            let mut used: HashSet<char> = HashSet::new();
            {
                let rows = existing_dirs_stmt.query_map(params![cluster_id, x, y, plane], |r| {
                    let s: String = r.get(0)?; Ok(s.chars().next().unwrap_or('?'))
                })?;
                for r in rows { used.insert(r?); }
            }
            let dirs = ['N','E','S','W'];
            let dir = dirs.into_iter().find(|d| !used.contains(d));
            let Some(dir) = dir else {
                // No free direction slot; skip to preserve unique constraint
                continue;
            };
            if !cfg.dry_run {
                ins.execute(params![cluster_id, x, y, plane, &dir.to_string(), edge_id])?;
            }
            created += 1;
        }
    }

    Ok(TeleportStats { entrances_created: created, edges_created: 0 })
}

// Phase C: create teleport interconnections (after Inter)
pub fn create_teleport_edges(out_db: &mut Connection, cfg: &Config) -> Result<TeleportStats> {
    // ensure_schema(out_db)?;

    // Collect teleport entrances in scope
    let mut teleport_entrances: Vec<(i64,i64,i32,i32,i32)> = Vec::new(); // (entrance_id, teleport_edge_id, x, y, plane)
    {
        let mut q = out_db.prepare(
            "SELECT entrance_id, teleport_edge_id, x, y, plane FROM cluster_entrances WHERE teleport_edge_id IS NOT NULL ORDER BY entrance_id"
        )?;
        let rows = q.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)))?;
        for r in rows { teleport_entrances.push(r?); }
    }
    // Filter by scope
    teleport_entrances.retain(|&(_eid, _teid, x, y, plane)| {
        if let Some(planes) = &cfg.planes { if !planes.contains(&plane) { return false; } }
        if let Some((xmin,xmax,zmin,zmax)) = cfg.chunk_range {
            let cx = x >> 6; let cz = y >> 6; if cx < xmin || cx > xmax || cz < zmin || cz > zmax { return false; }
        }
        true
    });

    // Build maps from (edge_id, (x,y,plane)) -> entrance_id for pairing
    let mut by_edge: BTreeMap<i64, Vec<(i64, i32, i32, i32)>> = BTreeMap::new();
    for (eid, teid, x, y, pl) in teleport_entrances.into_iter() {
        by_edge.entry(teid).or_default().push((eid, x, y, pl));
    }

    let mut created = 0usize;

    if !cfg.dry_run {
        with_tx(out_db, |tx| {
            // Pre-delete existing interconnections from teleport entrances in scope for idempotence
            let mut dels: Vec<i64> = Vec::new();
            let mut qd = tx.prepare(
                "SELECT entrance_id FROM cluster_entrances WHERE teleport_edge_id IS NOT NULL"
            )?;
            let rows = qd.query_map([], |r| Ok(r.get::<_, i64>(0)?))?;
            for r in rows { dels.push(r?); }
            drop(qd);
            let mut del = tx.prepare("DELETE FROM cluster_interconnections WHERE entrance_from=?1")?;
            for eid in dels { del.execute(params![eid])?; }

            // Prepared statements
            let mut q_edge = tx.prepare(
                "SELECT kind, src_x, src_y, src_plane, dst_x, dst_y, dst_plane, cost FROM abstract_teleport_edges WHERE edge_id=?1"
            )?;
            let mut ins = tx.prepare(
                "INSERT INTO cluster_interconnections (entrance_from, entrance_to, cost)
                 VALUES (?1,?2,?3)
                 ON CONFLICT(entrance_from, entrance_to)
                 DO UPDATE SET cost = MIN(cluster_interconnections.cost, excluded.cost)"
            )?;

            let mut all_entrances_scoped: Vec<(i64, i64, i32, i32, i32)> = Vec::new(); // (eid, cluster_id, x, y, plane)
            {
                let mut qa = tx.prepare(
                    "SELECT entrance_id, cluster_id, x, y, plane FROM cluster_entrances ORDER BY entrance_id"
                )?;
                let rows = qa.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)))?;
                for r in rows {
                    let (eid, cid, x, y, plane): (i64,i64,i32,i32,i32) = r?;
                    if let Some(planes) = &cfg.planes { if !planes.contains(&plane) { continue; } }
                    if let Some((xmin,xmax,zmin,zmax)) = cfg.chunk_range {
                        let cx = x >> 6; let cz = y >> 6; if cx < xmin || cx > xmax || cz < zmin || cz > zmax { continue; }
                    }
                    all_entrances_scoped.push((eid, cid, x, y, plane));
                }
            }
            {
                let mut q_dst_eids = tx.prepare(
                    "SELECT dst_entrance FROM abstract_teleport_edges WHERE src_entrance IS NULL AND dst_entrance IS NOT NULL"
                )?;
                let rows = q_dst_eids.query_map([], |r| r.get::<_, i64>(0))?;
                let mut del = tx.prepare("DELETE FROM cluster_interconnections WHERE entrance_from=?1 AND entrance_to=?2")?;
                for r in rows {
                    let dst_eid: i64 = r?;
                    for (from_eid, _cid, _x, _y, _pl) in all_entrances_scoped.iter().copied() {
                        del.execute(params![from_eid, dst_eid])?;
                    }
                }
            }
            let mut q_dst_cluster = tx.prepare("SELECT cluster_id FROM cluster_entrances WHERE entrance_id=?1")?;

            for (edge_id, entries) in by_edge.iter() {
                // Get abstract edge to know src/dst coords
                let row: Option<(String, Option<i32>,Option<i32>,Option<i32>,Option<i32>,Option<i32>,Option<i32>,i64)> = q_edge
                    .query_row(params![edge_id], |r| Ok((
                        r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?, r.get(6)?, r.get(7)?,
                    )))
                    .optional()?;
                let Some((kind, sx,sy,spl, dx,dy,dpl, cost)) = row else { continue }; // skip if missing
                // Find entrance ids matching src and dst
                let mut src_eid: Option<i64> = None;
                let mut dst_eid: Option<i64> = None;
                for (eid, x, y, pl) in entries.iter().copied() {
                    if let (Some(sx), Some(sy), Some(spl)) = (sx,sy,spl) {
                        if x == sx && y == sy && pl == spl { src_eid = Some(eid); }
                    }
                    if let (Some(dx), Some(dy), Some(dpl)) = (dx,dy,dpl) {
                        if x == dx && y == dy && pl == dpl { dst_eid = Some(eid); }
                    }
                }
                // Only create directed edge if both endpoints exist
                if let (Some(from), Some(to)) = (src_eid, dst_eid) {
                    ins.execute(params![from, to, cost])?;
                    created += 1;
                    // Doors are bidirectional; others remain one-way
                    if kind == "door" {
                        ins.execute(params![to, from, cost])?;
                        created += 1;
                    }
                } else if src_eid.is_none() {
                }
            }
            Ok(())
        })?;
    } else {
        // Dry-run: approximate number of edges that would be created (those with both endpoints present)
        let conn = out_db;
        let mut q_edge = conn.prepare(
            "SELECT kind, src_x, src_y, src_plane, dst_x, dst_y, dst_plane FROM abstract_teleport_edges WHERE edge_id=?1"
        )?;
        for (edge_id, entries) in by_edge.iter() {
            let row: Option<(String, Option<i32>,Option<i32>,Option<i32>,Option<i32>,Option<i32>,Option<i32>)> = q_edge
                .query_row(params![edge_id], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?)))
                .optional()?;
            if let Some((kind, sx,sy,spl, dx,dy,dpl)) = row {
                let mut has_src = false; let mut has_dst = false;
                for (_eid, x, y, pl) in entries.iter().copied() {
                    if let (Some(sx), Some(sy), Some(spl)) = (sx,sy,spl) { if x==sx && y==sy && pl==spl { has_src = true; } }
                    if let (Some(dx), Some(dy), Some(dpl)) = (dx,dy,dpl) { if x==dx && y==dy && pl==dpl { has_dst = true; } }
                }
                if has_src && has_dst {
                    created += if kind == "door" { 2 } else { 1 };
                }
            }
        }
    }

    Ok(TeleportStats { entrances_created: 0, edges_created: created })
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
    use std::collections::{HashSet, VecDeque};
    // Load walkable tiles in chunk
    let x0 = cx * 64; let x1 = x0 + 63;
    let y0 = cz * 64; let y1 = y0 + 63;
    let mut tiles_stmt = tiles_db.prepare(
        "SELECT x, y FROM tiles WHERE blocked=0 AND plane=?1 AND x BETWEEN ?2 AND ?3 AND y BETWEEN ?4 AND ?5",
    )?;
    let rows = tiles_stmt.query_map(params![plane, x0, x1, y0, y1], |row| {
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

fn deterministic_cluster_id(plane: i64, chunk_x: i64, chunk_z: i64, local_index: i64) -> i64 {
    let p  = (plane & 0xF) << 60;
    let cx = (chunk_x & 0xFFFFFF) << 36;
    let cz = (chunk_z & 0xFFFFFF) << 12;
    let li = (local_index & 0xFFF);
    p | cx | cz | li
}
