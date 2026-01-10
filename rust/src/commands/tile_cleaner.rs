use anyhow::{anyhow, Context, Result};
use rusqlite::{params, params_from_iter, types::Value, Connection, OptionalExtension, Row};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::path::Path;
use std::sync::mpsc;
use std::thread;

use rayon::prelude::*;

type Tile = (i32, i32, i32);

#[derive(Clone, Copy, Debug, Default)]
struct WalkMaskOverride {
    force_mask: Option<i64>,
    or_mask: i64,
}

fn dir_to_bit(dir: &str) -> Option<i64> {
    match dir {
        "left" => Some(1 << 0),
        "bottom" => Some(1 << 1),
        "right" => Some(1 << 2),
        "top" => Some(1 << 3),
        "topleft" => Some(1 << 4),
        "bottomleft" => Some(1 << 5),
        "bottomright" => Some(1 << 6),
        "topright" => Some(1 << 7),
        _ => None,
    }
}

fn diag_required_dirs(dir: &str) -> Option<[&'static str; 2]> {
    match dir {
        "topleft" => Some(["top", "left"]),
        "topright" => Some(["top", "right"]),
        "bottomleft" => Some(["bottom", "left"]),
        "bottomright" => Some(["bottom", "right"]),
        _ => None,
    }
}

fn build_fairy_ring_overrides(conn: &Connection) -> Result<std::sync::Arc<HashMap<Tile, WalkMaskOverride>>> {
    let mut out: HashMap<Tile, WalkMaskOverride> = HashMap::new();

    let mut stmt = match conn.prepare("SELECT x, y, plane FROM teleports_fairy_rings_nodes") {
        Ok(s) => s,
        Err(_) => return Ok(std::sync::Arc::new(out)),
    };

    let mut rows = stmt.query([])?;
    while let Some(r) = rows.next()? {
        let x: Option<i64> = r.get(0)?;
        let y: Option<i64> = r.get(1)?;
        let p: Option<i64> = r.get(2)?;
        let (Some(x), Some(y), Some(p)) = (x, y, p) else { continue; };
        let ring: Tile = (x as i32, y as i32, p as i32);

        out.entry(ring)
            .and_modify(|e| e.force_mask = Some(255))
            .or_insert(WalkMaskOverride { force_mask: Some(255), or_mask: 0 });

        let neighbors: [((i32, i32), &str); 8] = [
            ((-1, 0), "right"),
            ((1, 0), "left"),
            ((0, -1), "top"),
            ((0, 1), "bottom"),
            ((-1, -1), "topright"),
            ((1, -1), "topleft"),
            ((-1, 1), "bottomright"),
            ((1, 1), "bottomleft"),
        ];

        for &((dx, dy), dir) in &neighbors {
            let nt: Tile = (ring.0 + dx, ring.1 + dy, ring.2);
            let mut bits = dir_to_bit(dir).unwrap_or(0);
            if let Some(req) = diag_required_dirs(dir) {
                for d in req {
                    bits |= dir_to_bit(d).unwrap_or(0);
                }
            }
            out.entry(nt)
                .and_modify(|e| {
                    if e.force_mask.is_none() {
                        e.or_mask |= bits;
                    }
                })
                .or_insert(WalkMaskOverride { force_mask: None, or_mask: bits });
        }
    }

    Ok(std::sync::Arc::new(out))
}

fn center_tile(min_x: i32, max_x: i32, min_y: i32, max_y: i32, plane: i32) -> Tile {
    let (min_x, max_x) = if min_x <= max_x { (min_x, max_x) } else { (max_x, min_x) };
    let (min_y, max_y) = if min_y <= max_y { (min_y, max_y) } else { (max_y, min_y) };
    let cx = (min_x as i64 + max_x as i64) / 2;
    let cy = (min_y as i64 + max_y as i64) / 2;
    (cx as i32, cy as i32, plane)
}

const RECIP: &[(&str, &str)] = &[
    ("left", "right"),
    ("right", "left"),
    ("top", "bottom"),
    ("bottom", "top"),
    ("topleft", "bottomright"),
    ("topright", "bottomleft"),
    ("bottomleft", "topright"),
    ("bottomright", "topleft"),
];

fn key_delta(k: &str) -> Option<(i32, i32, i32)> {
    match k {
        "top" => Some((0, 1, 0)),
        "bottom" => Some((0, -1, 0)),
        "right" => Some((1, 0, 0)),
        "left" => Some((-1, 0, 0)),
        "topright" => Some((1, 1, 0)),
        "topleft" => Some((-1, 1, 0)),
        "bottomright" => Some((1, -1, 0)),
        "bottomleft" => Some((-1, -1, 0)),
        _ => None,
    }
}

fn diag_require(k: &str) -> Option<(&'static str, &'static str)> {
    match k {
        "topleft" => Some(("top", "left")),
        "topright" => Some(("top", "right")),
        "bottomleft" => Some(("bottom", "left")),
        "bottomright" => Some(("bottom", "right")),
        _ => None,
    }
}

struct WalkCache {
    raw: HashMap<Tile, HashMap<String, bool>>, 
    reconciled: HashMap<Tile, HashMap<String, bool>>, 
    overrides: std::sync::Arc<HashMap<Tile, WalkMaskOverride>>,
}

impl WalkCache {
    fn new() -> Self {
        Self {
            raw: HashMap::new(),
            reconciled: HashMap::new(),
            overrides: std::sync::Arc::new(HashMap::new()),
        }
    }

    fn new_with_overrides(overrides: std::sync::Arc<HashMap<Tile, WalkMaskOverride>>) -> Self {
        Self {
            raw: HashMap::new(),
            reconciled: HashMap::new(),
            overrides,
        }
    }

    // Mapping order for walk_mask bits: 0..7
    // [left, bottom, right, top, topleft, bottomleft, bottomright, topright]
    fn mask_dirs() -> [&'static str; 8] {
        [
            "left",
            "bottom",
            "right",
            "top",
            "topleft",
            "bottomleft",
            "bottomright",
            "topright",
        ]
    }

    fn decode_mask(mask: i64) -> HashMap<String, bool> {
        let mut out = HashMap::new();
        let dirs = Self::mask_dirs();
        for i in 0..8 {
            if (mask & (1 << i)) != 0 {
                out.insert(dirs[i].to_string(), true);
            }
        }
        out
    }

    fn encode_mask(map: &HashMap<String, bool>) -> i64 {
        let mut mask: i64 = 0;
        let dirs = Self::mask_dirs();
        for i in 0..8 {
            if map.get(dirs[i]).copied().unwrap_or(false) { mask |= 1 << i; }
        }
        mask
    }

    fn get_raw(&mut self, conn: &Connection, t: Tile) -> Result<HashMap<String, bool>> {
        if let Some(m) = self.raw.get(&t) { return Ok(m.clone()); }
        let (x, y, p) = t;
        let row: Option<Option<i64>> = conn
            .query_row(
                "SELECT walk_mask FROM tiles WHERE x=?1 AND y=?2 AND plane=?3",
                params![x, y, p],
                |row| Ok(row.get(0)?),
            )
            .optional()?;

        let Some(walk_mask) = row else {
            // Tile row does not exist; never allow overrides to "create" walkability for missing tiles.
            self.raw.insert(t, HashMap::new());
            return Ok(HashMap::new());
        };

        let mut w = walk_mask.unwrap_or(0);
        if let Some(ov) = self.overrides.get(&t) {
            if let Some(f) = ov.force_mask {
                w = f;
            } else {
                w |= ov.or_mask;
            }
        }

        let m = if w != 0 { Self::decode_mask(w) } else { HashMap::new() };
        self.raw.insert(t, m.clone());
        Ok(m)
    }

    fn get_reconciled(&mut self, conn: &Connection, t: Tile) -> Result<HashMap<String, bool>> {
        if let Some(m) = self.reconciled.get(&t) { return Ok(m.clone()); }
        let mut base = self.get_raw(conn, t)?;
        if base.is_empty() {
            self.reconciled.insert(t, HashMap::new());
            return Ok(HashMap::new());
        }
        let (tx, ty, tp) = t;
        
        for key in ["left", "right", "top", "bottom"] {
            if !base.get(key).copied().unwrap_or(false) { continue; }
            if let Some((dx, dy, dp)) = key_delta(key) {
                let n = (tx + dx, ty + dy, tp + dp);
                let nwalk = self.get_raw(conn, n)?;
                let recip = RECIP.iter().find(|(a, _)| *a == key).map(|(_, b)| *b).unwrap();
                let nrecip = nwalk.get(recip).copied().unwrap_or(false);
                if !nrecip { base.insert(key.to_string(), false); }
            }
        }
        for key in ["topleft", "topright", "bottomleft", "bottomright"] {
            if !base.get(key).copied().unwrap_or(false) { continue; }
            if let Some((r1, r2)) = diag_require(key) {
                if !(base.get(r1).copied().unwrap_or(false) && base.get(r2).copied().unwrap_or(false)) {
                    base.insert(key.to_string(), false);
                    continue;
                }
            }
            if let Some((dx, dy, dp)) = key_delta(key) {
                let n = (tx + dx, ty + dy, tp + dp);
                let nwalk = self.get_raw(conn, n)?;
                let recip = RECIP.iter().find(|(a, _)| *a == key).map(|(_, b)| *b).unwrap();
                let nrecip = nwalk.get(recip).copied().unwrap_or(false);
                if !nrecip { base.insert(key.to_string(), false); }
            }
        }
        self.reconciled.insert(t, base.clone());
        Ok(base)
    }
}

fn neighbors_from_reconciled(map: &HashMap<String, bool>, t: Tile) -> Vec<Tile> {
    let (x, y, p) = t;
    let mut out = Vec::new();
    for (k, allowed) in map.iter() {
        if !*allowed { continue; }
        if let Some((dx, dy, dp)) = key_delta(k) {
            out.push((x + dx, y + dy, p + dp));
        }
    }
    out
}

fn get_door_links(conn: &Connection) -> Result<HashMap<Tile, Vec<Tile>>> {
    let mut adj: HashMap<Tile, Vec<Tile>> = HashMap::new();
    let mut stmt = conn.prepare(
        "SELECT tile_inside_x, tile_inside_y, tile_inside_plane, tile_outside_x, tile_outside_y, tile_outside_plane FROM teleports_door_nodes",
    )?;
    let mut rows = stmt.query([])?;
    while let Some(r) = rows.next()? {
        let a: Tile = (r.get(0)?, r.get(1)?, r.get(2)?);
        let b: Tile = (r.get(3)?, r.get(4)?, r.get(5)?);
        adj.entry(a).or_default().push(b);
        adj.entry(b).or_default().push(a);
    }
    Ok(adj)
}

fn get_lodestones(conn: &Connection) -> Result<(HashSet<Tile>, Vec<Tile>)> {
    let mut set = HashSet::new();
    let mut list = Vec::new();
    let mut stmt = conn.prepare("SELECT dest_x, dest_y, dest_plane FROM teleports_lodestone_nodes")?;
    let mut rows = stmt.query([])?;
    while let Some(r) = rows.next()? {
        let t: Tile = (r.get(0)?, r.get(1)?, r.get(2)?);
        set.insert(t);
        list.push(t);
    }
    Ok((set, list))
}

fn get_object_transitions(conn: &Connection) -> Result<HashMap<Tile, Vec<Tile>>> {
    let mut adj: HashMap<Tile, Vec<Tile>> = HashMap::new();
    let mut stmt = conn.prepare(
        "SELECT orig_min_x, orig_max_x, orig_min_y, orig_max_y, orig_plane, dest_min_x, dest_max_x, dest_min_y, dest_max_y, dest_plane FROM teleports_object_nodes",
    )?;
    let mut rows = stmt.query([])?;
    while let Some(r) = rows.next()? {
        let o_min_x: Option<i64> = r.get(0)?;
        let o_max_x: Option<i64> = r.get(1)?;
        let o_min_y: Option<i64> = r.get(2)?;
        let o_max_y: Option<i64> = r.get(3)?;
        let o_plane: Option<i64> = r.get(4)?;
        let d_min_x: Option<i64> = r.get(5)?;
        let d_max_x: Option<i64> = r.get(6)?;
        let d_min_y: Option<i64> = r.get(7)?;
        let d_max_y: Option<i64> = r.get(8)?;
        let d_plane: Option<i64> = r.get(9)?;
        if [o_min_x, o_max_x, o_min_y, o_max_y, o_plane, d_min_x, d_max_x, d_min_y, d_max_y, d_plane]
            .iter()
            .any(|v| v.is_none())
        {
            continue;
        }
        let (o_min_x, o_max_x, o_min_y, o_max_y, o_plane, d_min_x, d_max_x, d_min_y, d_max_y, d_plane) = (
            o_min_x.unwrap() as i32,
            o_max_x.unwrap() as i32,
            o_min_y.unwrap() as i32,
            o_max_y.unwrap() as i32,
            o_plane.unwrap() as i32,
            d_min_x.unwrap() as i32,
            d_max_x.unwrap() as i32,
            d_min_y.unwrap() as i32,
            d_max_y.unwrap() as i32,
            d_plane.unwrap() as i32,
        );
        let origin = center_tile(o_min_x, o_max_x, o_min_y, o_max_y, o_plane);
        let dest = center_tile(d_min_x, d_max_x, d_min_y, d_max_y, d_plane);
        adj.entry(origin).or_default().push(dest);
        adj.entry(dest).or_default().push(origin);
    }
    Ok(adj)
}

fn get_npc_transitions(conn: &Connection) -> Result<HashMap<Tile, Vec<Tile>>> {
    let mut adj: HashMap<Tile, Vec<Tile>> = HashMap::new();
    let mut stmt = conn.prepare(
        "SELECT orig_min_x, orig_max_x, orig_min_y, orig_max_y, orig_plane, dest_min_x, dest_max_x, dest_min_y, dest_max_y, dest_plane FROM teleports_npc_nodes",
    )?;
    let mut rows = stmt.query([])?;
    while let Some(r) = rows.next()? {
        let o_min_x: Option<i64> = r.get(0)?;
        let o_max_x: Option<i64> = r.get(1)?;
        let o_min_y: Option<i64> = r.get(2)?;
        let o_max_y: Option<i64> = r.get(3)?;
        let o_plane: Option<i64> = r.get(4)?;
        let d_min_x: Option<i64> = r.get(5)?;
        let d_max_x: Option<i64> = r.get(6)?;
        let d_min_y: Option<i64> = r.get(7)?;
        let d_max_y: Option<i64> = r.get(8)?;
        let d_plane: Option<i64> = r.get(9)?;
        if [o_min_x, o_max_x, o_min_y, o_max_y, o_plane, d_min_x, d_max_x, d_min_y, d_max_y, d_plane]
            .iter()
            .any(|v| v.is_none())
        {
            continue;
        }
        let (o_min_x, o_max_x, o_min_y, o_max_y, o_plane, d_min_x, d_max_x, d_min_y, d_max_y, d_plane) = (
            o_min_x.unwrap() as i32,
            o_max_x.unwrap() as i32,
            o_min_y.unwrap() as i32,
            o_max_y.unwrap() as i32,
            o_plane.unwrap() as i32,
            d_min_x.unwrap() as i32,
            d_max_x.unwrap() as i32,
            d_min_y.unwrap() as i32,
            d_max_y.unwrap() as i32,
            d_plane.unwrap() as i32,
        );
        let origin = center_tile(o_min_x, o_max_x, o_min_y, o_max_y, o_plane);
        let dest = center_tile(d_min_x, d_max_x, d_min_y, d_max_y, d_plane);
        adj.entry(origin).or_default().push(dest);
        adj.entry(dest).or_default().push(origin);
    }
    Ok(adj)
}

fn get_item_dest_tiles(conn: &Connection) -> Result<Vec<Tile>> {
    let mut out: Vec<Tile> = Vec::new();
    let mut stmt = conn.prepare(
        "SELECT dest_min_x, dest_max_x, dest_min_y, dest_max_y, dest_plane FROM teleports_item_nodes",
    )?;
    let mut rows = stmt.query([])?;
    while let Some(r) = rows.next()? {
        let d_min_x: Option<i64> = r.get(0)?;
        let d_max_x: Option<i64> = r.get(1)?;
        let d_min_y: Option<i64> = r.get(2)?;
        let d_max_y: Option<i64> = r.get(3)?;
        let d_plane: Option<i64> = r.get(4)?;
        if [d_min_x, d_max_x, d_min_y, d_max_y, d_plane].iter().any(|v| v.is_none()) { continue; }
        let (d_min_x, d_max_x, d_min_y, d_max_y, d_plane) = (
            d_min_x.unwrap() as i32,
            d_max_x.unwrap() as i32,
            d_min_y.unwrap() as i32,
            d_max_y.unwrap() as i32,
            d_plane.unwrap() as i32,
        );
        out.push(center_tile(d_min_x, d_max_x, d_min_y, d_max_y, d_plane));
    }
    Ok(out)
}

fn get_ifslot_dest_tiles(conn: &Connection) -> Result<Vec<Tile>> {
    let mut out: Vec<Tile> = Vec::new();
    let mut stmt = conn.prepare(
        "SELECT dest_min_x, dest_max_x, dest_min_y, dest_max_y, dest_plane FROM teleports_ifslot_nodes",
    )?;
    let mut rows = stmt.query([])?;
    while let Some(r) = rows.next()? {
        let d_min_x: Option<i64> = r.get(0)?;
        let d_max_x: Option<i64> = r.get(1)?;
        let d_min_y: Option<i64> = r.get(2)?;
        let d_max_y: Option<i64> = r.get(3)?;
        let d_plane: Option<i64> = r.get(4)?;
        if [d_min_x, d_max_x, d_min_y, d_max_y, d_plane].iter().any(|v| v.is_none()) { continue; }
        let (d_min_x, d_max_x, d_min_y, d_max_y, d_plane) = (
            d_min_x.unwrap() as i32,
            d_max_x.unwrap() as i32,
            d_min_y.unwrap() as i32,
            d_max_y.unwrap() as i32,
            d_plane.unwrap() as i32,
        );
        out.push(center_tile(d_min_x, d_max_x, d_min_y, d_max_y, d_plane));
    }
    Ok(out)
}

fn reachable_tiles(
    conn: &Connection,
    start: Tile,
    overrides: std::sync::Arc<HashMap<Tile, WalkMaskOverride>>,
) -> Result<HashSet<Tile>> {
    println!("Loading door links...");
    let door = get_door_links(conn)?;
    println!("Loaded {} door link origins with {} total destinations", door.len(), door.values().map(|v| v.len()).sum::<usize>());
    println!("Loading lodestones...");
    let (lode_set, lodestones) = get_lodestones(conn)?;
    println!("Loaded {} lodestone destinations", lodestones.len());
    println!("Loading object transitions...");
    let obj = get_object_transitions(conn)?;
    println!("Loaded {} object transition origins with {} total destinations", obj.len(), obj.values().map(|v| v.len()).sum::<usize>());
    println!("Loading NPC transitions...");
    let npc = get_npc_transitions(conn)?;
    println!("Loaded {} NPC transition origins with {} total destinations", npc.len(), npc.values().map(|v| v.len()).sum::<usize>());
    println!("Loading item teleport destinations...");
    let item_dests = get_item_dest_tiles(conn)?;
    println!("Loaded {} item teleport destinations", item_dests.len());
    println!("Loading interface slot destinations...");
    let ifslot = get_ifslot_dest_tiles(conn)?;
    println!("Loaded {} interface slot destinations", ifslot.len());

    let mut cache = WalkCache::new_with_overrides(overrides);
    let mut q: VecDeque<Tile> = VecDeque::new();
    let mut vis: HashSet<Tile> = HashSet::new();

    q.push_back(start);
    vis.insert(start);

    // Important: seed BFS with teleport endpoints so destination tiles are retained in the cleaned DB
    // even if their origin tiles are not walk-reachable (e.g., one-way walk masks or data issues).
    println!("Seeding BFS with teleport endpoints (door/lodestone/object/npc)...");
    for &n in door.values().flatten() {
        if vis.insert(n) { q.push_back(n); }
    }
    for &n in &lodestones {
        if vis.insert(n) { q.push_back(n); }
    }
    for &n in obj.values().flatten() {
        if vis.insert(n) { q.push_back(n); }
    }
    for &n in npc.values().flatten() {
        if vis.insert(n) { q.push_back(n); }
    }

    for &n in &item_dests {
        if vis.insert(n) { q.push_back(n); }
    }

    let mut ifslot_enqueued = false;

    println!("Starting BFS from tile {:?}", start);
    let mut processed = 0usize;

    while let Some(t) = q.pop_front() {
        processed += 1;
        if processed % 10000 == 0 {
            println!("Processed {} tiles so far; queue length {}", processed, q.len());
        }
        let rec = cache.get_reconciled(conn, t)?;
        for n in neighbors_from_reconciled(&rec, t) {
            if vis.insert(n) { q.push_back(n); }
        }
        if let Some(v) = door.get(&t) {
            for &n in v {
                if vis.insert(n) { q.push_back(n); }
            }
        }
        if lode_set.contains(&t) {
            for &n in &lodestones {
                if vis.insert(n) { q.push_back(n); }
            }
        }
        if let Some(v) = obj.get(&t) {
            for &n in v {
                if vis.insert(n) { q.push_back(n); }
            }
        }
        if let Some(v) = npc.get(&t) {
            for &n in v {
                if vis.insert(n) { q.push_back(n); }
            }
        }
        if !ifslot_enqueued && !ifslot.is_empty() {
            for &n in &ifslot {
                if vis.insert(n) { q.push_back(n); }
            }
            ifslot_enqueued = true;
        }
    }

    println!("Finished BFS; processed {} tiles with {} reachable tiles discovered", processed, vis.len());

    Ok(vis)
}

fn get_create_table_sql(conn: &Connection, table: &str) -> Result<String> {
    let sql: Option<String> = conn
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?1",
            [table],
            |row| row.get(0),
        )
        .optional()?;
    sql.ok_or_else(|| anyhow!("Missing CREATE TABLE for {}", table))
}

fn get_table_columns(conn: &Connection, table: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({})", table))?;
    let mut rows = stmt.query([])?;
    let mut cols = Vec::new();
    while let Some(r) = rows.next()? {
        let name: String = r.get(1)?;
        cols.push(name);
    }
    if cols.is_empty() { Err(anyhow!("No columns for table {}", table)) } else { Ok(cols) }
}

fn read_row_values(row: &Row, ncols: usize) -> Result<Vec<Value>> {
    let mut out: Vec<Value> = Vec::with_capacity(ncols);
    for i in 0..ncols {
        let v = row.get::<usize, Value>(i)?;
        out.push(v);
    }
    Ok(out)
}

fn get_tiles_row(conn: &Connection, cols: &[String], t: Tile) -> Result<Option<Vec<Value>>> {
    let select = format!("SELECT {} FROM tiles WHERE x=?1 AND y=?2 AND plane=?3", cols.join(", "));
    let mut stmt = conn.prepare(&select)?;
    let mut rows = stmt.query(params![t.0, t.1, t.2])?;
    if let Some(r) = rows.next()? {
        Ok(Some(read_row_values(r, cols.len())?))
    } else {
        Ok(None)
    }
}

fn sanitize_walk_mask_for_reachable(base: &HashMap<String, bool>, tile: Tile, reachable: &HashSet<Tile>) -> i64 {
    let (x, y, p) = tile;
    let mut m = base.clone();
    for (k, v) in base.iter() {
        if !*v { continue; }
        if let Some((dx, dy, dp)) = key_delta(k) {
            let n = (x + dx, y + dy, p + dp);
            if !reachable.contains(&n) {
                m.insert(k.clone(), false);
            }
        }
    }
    WalkCache::encode_mask(&m)
}

fn create_tiles_and_insert(
    // Use this opened connection to read schema and index definitions
    src_meta: &Connection,
    // Use file path so workers can open their own read connections
    src_db_path: &Path,
    dst: &mut Connection,
    reachable: &HashSet<Tile>,
    overrides: std::sync::Arc<HashMap<Tile, WalkMaskOverride>>,
) -> Result<()> {
    println!("Creating destination tiles table and inserting reachable tiles...");
    let create_sql = get_create_table_sql(src_meta, "tiles")?;
    let cols = get_table_columns(src_meta, "tiles")?;
    let placeholders = (0..cols.len()).map(|_| "?").collect::<Vec<_>>().join(", ");
    let insert_sql = format!("INSERT INTO tiles ({}) VALUES ({})", cols.join(", "), placeholders);

    // Prepare destination: create table and start single writer transaction
    let tx = dst.transaction()?;
    tx.execute(&create_sql, [])?;

    // Channel between producers and single DB writer (this thread)
    let (tx_rows, rx_rows) = mpsc::channel::<Vec<Vec<Value>>>();

    // Columns/indices used by workers
    let walk_idx = cols.iter().position(|c| c == "walk_mask");
    let select_sql = format!("SELECT {} FROM tiles WHERE x=?1 AND y=?2 AND plane=?3", cols.join(", "));

    // Share reachable as read-only among workers
    let reachable_arc = std::sync::Arc::new(reachable.clone());

    // Spawn producers in a separate thread so this thread can consume and write
    let producer = {
        let tx_rows = tx_rows.clone();
        let select_sql = select_sql.clone();
        let cols_len = cols.len();
        let src_path = src_db_path.to_path_buf();
        let overrides_arc = overrides.clone();
        thread::spawn(move || {
            // Process tiles in parallel and stream to writer in batches
            let mut tiles: Vec<Tile> = reachable_arc.iter().copied().collect();
            tiles.shrink_to_fit();
            const BATCH: usize = 10_000;

            tiles
                .par_chunks(50_000)
                .for_each_with(tx_rows.clone(), |sender, chunk| {
                    // Each worker opens its own read-only connection and prepares statements
                    let conn = match Connection::open(&src_path) {
                        Ok(c) => c,
                        Err(e) => { eprintln!("worker open src db error: {}", e); return; }
                    };
                    let mut sel = match conn.prepare(&select_sql) {
                        Ok(s) => s,
                        Err(e) => { eprintln!("worker prepare select error: {}", e); return; }
                    };

                    let mut cache = WalkCache::new_with_overrides(overrides_arc.clone());
                    let mut out: Vec<Vec<Value>> = Vec::with_capacity(BATCH);
                    for &t in chunk.iter() {
                        match sel.query(params![t.0, t.1, t.2]) {
                            Ok(mut rows) => match rows.next() {
                                Ok(Some(r)) => {
                                    let mut row = match read_row_values(r, cols_len) {
                                        Ok(v) => v,
                                        Err(e) => { eprintln!("read row error for {:?}: {}", t, e); continue; }
                                    };
                                    if let Some(idx) = walk_idx {
                                        match cache.get_reconciled(&conn, t) {
                                            Ok(rec) => {
                                                let mut nm = sanitize_walk_mask_for_reachable(&rec, t, &reachable_arc);
                                                if let Some(ov) = overrides_arc.get(&t) {
                                                    if let Some(f) = ov.force_mask {
                                                        nm = f;
                                                    } else {
                                                        nm |= ov.or_mask;
                                                    }
                                                }
                                                row[idx] = Value::Integer(nm);
                                            }
                                            Err(e) => { eprintln!("reconcile error for {:?}: {}", t, e); }
                                        }
                                    }
                                    out.push(row);
                                    if out.len() >= BATCH {
                                        if let Err(e) = sender.send(std::mem::take(&mut out)) { eprintln!("send batch error: {}", e); break; }
                                    }
                                }
                                Ok(None) => { /* no row, skip */ }
                                Err(e) => { eprintln!("rows.next() error for {:?}: {}", t, e); }
                            },
                            Err(e) => { eprintln!("select prepare/exec error for {:?}: {}", t, e); }
                        }
                    }
                    if !out.is_empty() { let _ = sender.send(out); }
                });
            // Dropping tx_rows closes channel
        })
    };

    drop(tx_rows);

    // Consume and insert on this thread (owning the transaction/connection)
    let mut insert_stmt = tx.prepare(&insert_sql)?;
    let mut inserted = 0usize;
    for batch in rx_rows {
        for row in batch.into_iter() {
            insert_stmt.execute(params_from_iter(row.into_iter()))?;
            inserted += 1;
            if inserted % 5000 == 0 { println!("Inserted {} tiles so far...", inserted); }
        }
    }
    drop(insert_stmt);
    tx.commit()?;
    println!("Committed tiles insertion transaction");
    println!("Finished inserting {} tiles", inserted);

    // Ensure producers are done
    let _ = producer.join();

    // Recreate tile indexes on destination from source metadata
    let mut idx_stmt = src_meta.prepare(
        "SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name='tiles' AND sql IS NOT NULL",
    )?;
    let mut rows = idx_stmt.query([])?;
    let mut index_count = 0usize;
    while let Some(r) = rows.next()? {
        let sql: String = r.get(0)?;
        let _ = dst.execute(&sql, []);
        index_count += 1;
    }
    println!("Recreated {} tile indexes", index_count);

    Ok(())
}

fn copy_tables(src: &Connection, dst: &mut Connection, skip: &HashSet<String>) -> Result<()> {
    let mut stmt = src.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")?;
    let table_names: Vec<String> = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<std::result::Result<_, _>>()?;

    let tx = dst.transaction()?;

    for t in table_names {
        if skip.contains(&t) { continue; }
        println!("Copying table `{}`", t);
        let create_sql = match src.query_row(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?1",
            [&t],
            |row| row.get::<_, Option<String>>(0),
        ) {
            Ok(Some(s)) => s,
            _ => continue,
        };
        tx.execute(&create_sql, [])?;
        let cols = get_table_columns(src, &t)?;
        if cols.is_empty() { continue; }
        let select_sql = format!("SELECT {} FROM {}", cols.join(", "), t);
        let placeholders = (0..cols.len()).map(|_| "?").collect::<Vec<_>>().join(", ");
        let insert_sql = format!("INSERT INTO {} ({}) VALUES ({})", t, cols.join(", "), placeholders);
        let mut ins = tx.prepare(&insert_sql)?;
        let mut sel = src.prepare(&select_sql)?;
        let mut rows = sel.query([])?;
        let mut copied = 0usize;
        while let Some(r) = rows.next()? {
            let vals = read_row_values(r, cols.len())?;
            ins.execute(params_from_iter(vals.into_iter()))?;
            copied += 1;
            if copied % 5000 == 0 {
                println!("  Copied {} rows into `{}`", copied, t);
            }
        }
        println!("  Finished copying {} rows into `{}`", copied, t);
        let mut idx_stmt = src.prepare(
            "SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name=?1 AND sql IS NOT NULL",
        )?;
        let mut idx_rows = idx_stmt.query([&t])?;
        let mut index_count = 0usize;
        while let Some(ir) = idx_rows.next()? {
            let sql: Option<String> = ir.get(0)?;
            if let Some(sql) = sql {
                let _ = tx.execute(&sql, []);
                index_count += 1;
            }
        }
        println!("  Recreated {} indexes for `{}`", index_count, t);
    }

    tx.commit()?;
    println!("Finished copying auxiliary tables");
    Ok(())
}

fn copy_views(src: &Connection, dst: &mut Connection) -> Result<()> {
    let mut stmt = src.prepare("SELECT name, sql FROM sqlite_master WHERE type='view' AND sql IS NOT NULL")?;
    let mut rows = stmt.query([])?;
    let mut tx = dst.transaction()?;
    while let Some(r) = rows.next()? {
        let name: String = r.get(0)?;
        let sql: String = r.get(1)?;
        let _ = tx.execute(&sql, []);
        println!("Copied view `{}`", name);
    }
    tx.commit()?;
    println!("Finished copying views");
    Ok(())
}

pub fn cmd_tile_cleaner(src_db: &Path, out_db: &Path, start_x: i32, start_y: i32, start_plane: i32) -> Result<()> {
    println!("Starting tile cleaner from start tile ({}, {}, {})", start_x, start_y, start_plane);
    let src = Connection::open(src_db).with_context(|| format!("Open DB {}", src_db.display()))?;
    println!("Opened source database {}", src_db.display());
    src.execute_batch("PRAGMA foreign_keys=ON;")?;
    let start: Tile = (start_x, start_y, start_plane);
    println!("Computing reachable tiles...");
    let overrides = build_fairy_ring_overrides(&src)?;
    let reachable = reachable_tiles(&src, start, overrides.clone())?;
    println!("Identified {} reachable tiles", reachable.len());

    if out_db.exists() {
        println!("Removing existing output database {}", out_db.display());
        let _ = fs::remove_file(out_db);
    }
    let mut dst = Connection::open(out_db).with_context(|| format!("Create DB {}", out_db.display()))?;
    println!("Opened destination database {}", out_db.display());
    // Match Python behavior: avoid FK errors while creating/inserting tiles before copying 'chunks'
    dst.execute_batch("PRAGMA foreign_keys=OFF;")?;
    println!("Disabled foreign key checks on destination");

    create_tiles_and_insert(&src, src_db, &mut dst, &reachable, overrides)?;

    let mut skip = HashSet::new();
    skip.insert("tiles".to_string());
    copy_tables(&src, &mut dst, &skip)?;
    copy_views(&src, &mut dst)?;

    println!("Tile cleaning complete; output written to {}", out_db.display());
    Ok(())
}
