use anyhow::{anyhow, Context, Result};
use rusqlite::{params, params_from_iter, types::Value, Connection, OptionalExtension, Row};
use serde_json::Value as JsonValue;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::path::Path;

type Tile = (i32, i32, i32);

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
}

impl WalkCache {
    fn new() -> Self { Self { raw: HashMap::new(), reconciled: HashMap::new() } }

    fn parse_json_map(s: &str) -> HashMap<String, bool> {
        let mut out = HashMap::new();
        if let Ok(v) = serde_json::from_str::<JsonValue>(s) {
            if let Some(obj) = v.as_object() {
                for (k, vv) in obj.iter() {
                    let b = match vv {
                        JsonValue::Bool(b) => *b,
                        JsonValue::Number(n) => n.as_i64().unwrap_or(0) != 0,
                        JsonValue::String(t) => {
                            let t = t.trim().to_ascii_lowercase();
                            matches!(t.as_str(), "true" | "yes" | "y" | "on" | "1")
                        }
                        _ => false,
                    };
                    out.insert(k.to_ascii_lowercase(), b);
                }
            }
        }
        out
    }

    fn get_raw(&mut self, conn: &Connection, t: Tile) -> Result<HashMap<String, bool>> {
        if let Some(m) = self.raw.get(&t) { return Ok(m.clone()); }
        let (x, y, p) = t;
        let s: Option<String> = conn
            .query_row(
                "SELECT walk_data FROM tiles WHERE x=?1 AND y=?2 AND plane=?3",
                params![x, y, p],
                |row| row.get(0),
            )
            .optional()?;
        let m = s.map(|s| Self::parse_json_map(&s)).unwrap_or_default();
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
        let mut dests: Vec<Tile> = Vec::new();
        for dx in d_min_x..=d_max_x {
            for dy in d_min_y..=d_max_y {
                dests.push((dx, dy, d_plane));
            }
        }
        for ox in o_min_x..=o_max_x {
            for oy in o_min_y..=o_max_y {
                let origin = (ox, oy, o_plane);
                let e = adj.entry(origin).or_default();
                e.extend(dests.iter().copied());
            }
        }
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
        let mut dests: Vec<Tile> = Vec::new();
        for dx in d_min_x..=d_max_x {
            for dy in d_min_y..=d_max_y {
                dests.push((dx, dy, d_plane));
            }
        }
        for ox in o_min_x..=o_max_x {
            for oy in o_min_y..=o_max_y {
                let origin = (ox, oy, o_plane);
                let e = adj.entry(origin).or_default();
                e.extend(dests.iter().copied());
            }
        }
    }
    Ok(adj)
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
        for dx in d_min_x..=d_max_x {
            for dy in d_min_y..=d_max_y {
                out.push((dx, dy, d_plane));
            }
        }
    }
    Ok(out)
}

fn reachable_tiles(conn: &Connection, start: Tile) -> Result<HashSet<Tile>> {
    let door = get_door_links(conn)?;
    let (lode_set, lodestones) = get_lodestones(conn)?;
    let obj = get_object_transitions(conn)?;
    let npc = get_npc_transitions(conn)?;
    let ifslot = get_ifslot_dest_tiles(conn)?;

    let mut cache = WalkCache::new();
    let mut q: VecDeque<Tile> = VecDeque::new();
    let mut vis: HashSet<Tile> = HashSet::new();

    q.push_back(start);
    vis.insert(start);

    let mut ifslot_enqueued = false;

    while let Some(t) = q.pop_front() {
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

fn json_stringify_min(obj: &HashMap<String, bool>) -> String {
    let mut map: serde_json::Map<String, JsonValue> = serde_json::Map::new();
    for (k, v) in obj.iter() {
        map.insert(k.clone(), JsonValue::Bool(*v));
    }
    JsonValue::Object(map).to_string()
}

fn sanitize_walk_data_for_reachable(base: &HashMap<String, bool>, tile: Tile, reachable: &HashSet<Tile>) -> String {
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
    json_stringify_min(&m)
}

fn create_tiles_and_insert(
    src: &Connection,
    dst: &mut Connection,
    reachable: &HashSet<Tile>,
    cache: &mut WalkCache,
) -> Result<()> {
    let create_sql = get_create_table_sql(src, "tiles")?;
    let cols = get_table_columns(src, "tiles")?;
    let placeholders = (0..cols.len()).map(|_| "?").collect::<Vec<_>>().join(", ");
    let insert_sql = format!("INSERT INTO tiles ({}) VALUES ({})", cols.join(", "), placeholders);

    let tx = dst.transaction()?;
    tx.execute(&create_sql, [])?;

    let walk_idx = cols.iter().position(|c| c == "walk_data");

    {
        let mut insert_stmt = tx.prepare(&insert_sql)?;
        for &t in reachable.iter() {
            if let Some(mut row) = get_tiles_row(src, &cols, t)? {
                if let Some(idx) = walk_idx {
                    let rec = cache.get_reconciled(src, t)?;
                    let s = sanitize_walk_data_for_reachable(&rec, t, reachable);
                    row[idx] = Value::Text(s);
                }
                insert_stmt.execute(params_from_iter(row.into_iter()))?;
            }
        }
    }

    tx.commit()?;

    let mut idx_stmt = src.prepare(
        "SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name='tiles' AND sql IS NOT NULL",
    )?;
    let mut rows = idx_stmt.query([])?;
    while let Some(r) = rows.next()? {
        let sql: String = r.get(0)?;
        let _ = dst.execute(&sql, []);
    }

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
        while let Some(r) = rows.next()? {
            let vals = read_row_values(r, cols.len())?;
            ins.execute(params_from_iter(vals.into_iter()))?;
        }
        let mut idx_stmt = src.prepare(
            "SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name=?1 AND sql IS NOT NULL",
        )?;
        let mut idx_rows = idx_stmt.query([&t])?;
        while let Some(ir) = idx_rows.next()? {
            let sql: Option<String> = ir.get(0)?;
            if let Some(sql) = sql {
                let _ = tx.execute(&sql, []);
            }
        }
    }

    tx.commit()?;
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
        let _ = name;
    }
    tx.commit()?;
    Ok(())
}

pub fn cmd_tile_cleaner(src_db: &Path, out_db: &Path, start_x: i32, start_y: i32, start_plane: i32) -> Result<()> {
    let src = Connection::open(src_db).with_context(|| format!("Open DB {}", src_db.display()))?;
    src.execute_batch("PRAGMA foreign_keys=ON;")?;
    let start: Tile = (start_x, start_y, start_plane);
    let reachable = reachable_tiles(&src, start)?;

    if out_db.exists() { let _ = fs::remove_file(out_db); }
    let mut dst = Connection::open(out_db).with_context(|| format!("Create DB {}", out_db.display()))?;
    // Match Python behavior: avoid FK errors while creating/inserting tiles before copying 'chunks'
    dst.execute_batch("PRAGMA foreign_keys=OFF;")?;

    let mut cache = WalkCache::new();
    create_tiles_and_insert(&src, &mut dst, &reachable, &mut cache)?;

    let mut skip = HashSet::new();
    skip.insert("tiles".to_string());
    copy_tables(&src, &mut dst, &skip)?;
    copy_views(&src, &mut dst)?;

    Ok(())
}
