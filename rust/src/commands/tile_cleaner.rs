use anyhow::{anyhow, Context, Result};
use rusqlite::{params, params_from_iter, types::Value, Connection, OptionalExtension, Row};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, SyncSender};
use std::thread;

type Tile = (i32, i32, i32);

/// The tile maps take several million lookups, where SipHash's quality buys nothing
/// over three coordinates. This is the usual multiply-rotate mix instead.
#[derive(Default, Clone, Copy)]
struct TileHasher(u64);

impl std::hash::Hasher for TileHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
    #[inline]
    fn write_i32(&mut self, i: i32) {
        self.0 = (self.0.rotate_left(5) ^ (i as u32 as u64)).wrapping_mul(0x517c_c1b7_2722_0a95);
    }
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.write_i32(b as i32);
        }
    }
}

type TileBuildHasher = std::hash::BuildHasherDefault<TileHasher>;
type TileMap<V> = HashMap<Tile, V, TileBuildHasher>;
type TileSet = HashSet<Tile, TileBuildHasher>;

/// Tiles are streamed from the BFS to the writer thread in batches of this many,
/// with a bounded channel so a slow writer applies backpressure instead of
/// letting the queue of pending rows grow without limit.
const EMIT_BATCH: usize = 4096;
const CHANNEL_DEPTH: usize = 64;

/// The BFS is a few million random point lookups into a multi-GB table, so the
/// default 2 MB page cache turns almost every one into a `pread`. Mapping the file
/// and giving SQLite a real cache serves them from memory instead.
fn tune_read_conn(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "PRAGMA mmap_size=8589934592;
         PRAGMA cache_size=-1048576;
         PRAGMA temp_store=MEMORY;",
    )?;
    Ok(())
}

/// The destination is rebuilt from scratch on every run, so there is nothing to
/// protect against a crash mid-write.
fn tune_write_conn(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "PRAGMA journal_mode=OFF;
         PRAGMA synchronous=OFF;
         PRAGMA cache_size=-262144;
         PRAGMA temp_store=MEMORY;",
    )?;
    Ok(())
}

#[derive(Clone, Copy, Debug, Default)]
struct WalkMaskOverride {
    force_mask: Option<u8>,
    or_mask: u8,
}

fn dir_to_bit(dir: &str) -> Option<u8> {
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

fn build_fairy_ring_overrides(conn: &Connection) -> Result<std::sync::Arc<TileMap<WalkMaskOverride>>> {
    let mut out: TileMap<WalkMaskOverride> = TileMap::default();

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

// Walk masks are handled as raw bits throughout. Bit order, matching the values
// produced by `dir_to_bit` and stored in `tiles.walk_mask`:
//   0 left, 1 bottom, 2 right, 3 top, 4 topleft, 5 bottomleft, 6 bottomright, 7 topright
// Bits 0..4 are the cardinals, 4..8 the diagonals.
const CARDINALS: std::ops::Range<u8> = 0..4;
const DIAGONALS: std::ops::Range<u8> = 4..8;

/// (dx, dy) per direction bit. Planes are never crossed by a walk mask.
const DIR_DELTA: [(i32, i32); 8] = [
    (-1, 0),  // left
    (0, -1),  // bottom
    (1, 0),   // right
    (0, 1),   // top
    (-1, 1),  // topleft
    (-1, -1), // bottomleft
    (1, -1),  // bottomright
    (1, 1),   // topright
];

/// The bit a neighbour must have set for a step in this direction to be mutual.
const RECIP_BIT: [u8; 8] = [2, 3, 0, 1, 6, 7, 4, 5];

/// The cardinal bits a diagonal step also requires (0 for the cardinals themselves).
const DIAG_REQ: [u8; 8] = [
    0,
    0,
    0,
    0,
    (1 << 3) | (1 << 0), // topleft: top + left
    (1 << 1) | (1 << 0), // bottomleft: bottom + left
    (1 << 1) | (1 << 2), // bottomright: bottom + right
    (1 << 3) | (1 << 2), // topright: top + right
];

#[inline]
fn step(t: Tile, bit: u8) -> Tile {
    let (dx, dy) = DIR_DELTA[bit as usize];
    (t.0 + dx, t.1 + dy, t.2)
}

struct WalkCache {
    // Raw masks are shared between a tile and each of its neighbours, so caching pays.
    // Reconciled masks are not cached: the BFS pops every tile exactly once.
    raw: TileMap<u8>,
    overrides: std::sync::Arc<TileMap<WalkMaskOverride>>,
}

impl WalkCache {
    fn new_with_overrides(overrides: std::sync::Arc<TileMap<WalkMaskOverride>>) -> Self {
        Self {
            raw: TileMap::default(),
            overrides,
        }
    }

    fn get_raw(&mut self, conn: &Connection, t: Tile) -> Result<u8> {
        if let Some(m) = self.raw.get(&t) { return Ok(*m); }
        let (x, y, p) = t;
        // prepare_cached: this runs millions of times, so the SQL is parsed once.
        let row: Option<Option<i64>> = conn
            .prepare_cached("SELECT walk_mask FROM tiles WHERE x=?1 AND y=?2 AND plane=?3")?
            .query_row(params![x, y, p], |row| Ok(row.get(0)?))
            .optional()?;

        let Some(walk_mask) = row else {
            // Tile row does not exist; never allow overrides to "create" walkability for missing tiles.
            self.raw.insert(t, 0);
            return Ok(0);
        };

        // Only the low 8 bits are direction flags.
        let mut w = walk_mask.unwrap_or(0) as u8;
        if let Some(ov) = self.overrides.get(&t) {
            if let Some(f) = ov.force_mask {
                w = f;
            } else {
                w |= ov.or_mask;
            }
        }

        self.raw.insert(t, w);
        Ok(w)
    }

    /// Drops any direction whose neighbour does not permit the reverse step, and any
    /// diagonal whose two component cardinals are not both open.
    fn get_reconciled(&mut self, conn: &Connection, t: Tile) -> Result<u8> {
        let mut base = self.get_raw(conn, t)?;
        if base == 0 {
            return Ok(0);
        }

        for bit in CARDINALS {
            if base & (1 << bit) == 0 { continue; }
            let nwalk = self.get_raw(conn, step(t, bit))?;
            if nwalk & (1 << RECIP_BIT[bit as usize]) == 0 {
                base &= !(1 << bit);
            }
        }
        // Runs after the cardinal pass so it sees the cleared cardinals, as before.
        for bit in DIAGONALS {
            if base & (1 << bit) == 0 { continue; }
            let req = DIAG_REQ[bit as usize];
            if base & req != req {
                base &= !(1 << bit);
                continue;
            }
            let nwalk = self.get_raw(conn, step(t, bit))?;
            if nwalk & (1 << RECIP_BIT[bit as usize]) == 0 {
                base &= !(1 << bit);
            }
        }

        Ok(base)
    }
}

/// Appends the tiles reachable in one step from `t` to `out`, which is reused
/// across BFS iterations to avoid an allocation per tile.
fn neighbors_from_reconciled(mask: u8, t: Tile, out: &mut Vec<Tile>) {
    out.clear();
    for bit in 0..8u8 {
        if mask & (1 << bit) != 0 {
            out.push(step(t, bit));
        }
    }
}

fn get_door_links(conn: &Connection) -> Result<TileMap<Vec<Tile>>> {
    let mut adj: TileMap<Vec<Tile>> = TileMap::default();
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

fn get_lodestones(conn: &Connection) -> Result<(TileSet, Vec<Tile>)> {
    let mut set = TileSet::default();
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

/// Links the centre of each node's origin range to the centre of its destination
/// range, in both directions. Shared by every node table that carries a full
/// `orig_*` / `dest_*` pair.
fn get_range_transitions(conn: &Connection, table: &str) -> Result<TileMap<Vec<Tile>>> {
    let mut adj: TileMap<Vec<Tile>> = TileMap::default();
    let mut stmt = conn.prepare(&format!(
        "SELECT orig_min_x, orig_max_x, orig_min_y, orig_max_y, orig_plane, dest_min_x, dest_max_x, dest_min_y, dest_max_y, dest_plane FROM {}",
        table
    ))?;
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

fn table_exists(conn: &Connection, table: &str) -> Result<bool> {
    let found: Option<String> = conn
        .query_row(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?1 COLLATE NOCASE",
            [table],
            |row| row.get(0),
        )
        .optional()?;
    Ok(found.is_some())
}

fn get_object_transitions(conn: &Connection) -> Result<TileMap<Vec<Tile>>> {
    get_range_transitions(conn, "teleports_object_nodes")
}

fn get_npc_transitions(conn: &Connection) -> Result<TileMap<Vec<Tile>>> {
    get_range_transitions(conn, "teleports_npc_nodes")
}

/// Use-on nodes were added after some databases were built, so a missing table
/// means "no such transitions" rather than a corrupt DB.
fn get_useon_transitions(conn: &Connection) -> Result<TileMap<Vec<Tile>>> {
    if !table_exists(conn, "teleports_useOn_nodes")? {
        return Ok(TileMap::default());
    }
    get_range_transitions(conn, "teleports_useOn_nodes")
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

fn get_poa_dest_tiles(conn: &Connection) -> Result<Vec<Tile>> {
    let mut out: Vec<Tile> = Vec::new();
    let mut stmt = conn.prepare(
        "SELECT dest_min_x, dest_max_x, dest_min_y, dest_max_y, dest_plane FROM teleports_POA_nodes",
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

/// BFS over the walkable graph, emitting each tile to `sink` as soon as it is
/// expanded. A tile's final walk mask only depends on its own reconciled mask and
/// the neighbours that mask allows, and those neighbours are inserted into `vis`
/// during the same iteration that pops the tile -- so the mask written here is
/// identical to one computed against the completed reachable set.
fn reachable_tiles(
    conn: &Connection,
    start: Tile,
    overrides: std::sync::Arc<TileMap<WalkMaskOverride>>,
    sink: &SyncSender<Vec<(Tile, i64)>>,
) -> Result<usize> {
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
    println!("Loading use-on transitions...");
    let useon = get_useon_transitions(conn)?;
    println!("Loaded {} use-on transition origins with {} total destinations", useon.len(), useon.values().map(|v| v.len()).sum::<usize>());
    println!("Loading item teleport destinations...");
    let item_dests = get_item_dest_tiles(conn)?;
    println!("Loaded {} item teleport destinations", item_dests.len());
    println!("Loading interface slot destinations...");
    let ifslot = get_ifslot_dest_tiles(conn)?;
    println!("Loaded {} interface slot destinations", ifslot.len());
    println!("Loading POA teleport destinations...");
    let poa_dests = get_poa_dest_tiles(conn)?;
    println!("Loaded {} POA teleport destinations", poa_dests.len());

    let mut cache = WalkCache::new_with_overrides(overrides.clone());
    let mut q: VecDeque<Tile> = VecDeque::new();
    let mut vis: TileSet = TileSet::default();

    q.push_back(start);
    vis.insert(start);

    // Important: seed BFS with teleport endpoints so destination tiles are retained in the cleaned DB
    // even if their origin tiles are not walk-reachable (e.g., one-way walk masks or data issues).
    println!("Seeding BFS with teleport endpoints (door/lodestone/object/npc/use-on)...");
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
    for &n in useon.values().flatten() {
        if vis.insert(n) { q.push_back(n); }
    }

    println!("Seeding BFS with fairy ring tiles...");
    for (&t, ov) in overrides.iter() {
        if ov.force_mask.is_some() {
            if vis.insert(t) { q.push_back(t); }
        }
    }

    for &n in &item_dests {
        if vis.insert(n) { q.push_back(n); }
    }

    for &n in &poa_dests {
        if vis.insert(n) { q.push_back(n); }
    }

    let mut ifslot_enqueued = false;

    println!("Starting BFS from tile {:?}", start);
    let mut processed = 0usize;
    let mut batch: Vec<(Tile, i64)> = Vec::with_capacity(EMIT_BATCH);
    let mut nbuf: Vec<Tile> = Vec::with_capacity(8);

    while let Some(t) = q.pop_front() {
        processed += 1;
        if processed % 10000 == 0 {
            println!("Processed {} tiles so far; queue length {}", processed, q.len());
        }
        let rec = cache.get_reconciled(conn, t)?;
        neighbors_from_reconciled(rec, t, &mut nbuf);
        for &n in nbuf.iter() {
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
        if let Some(v) = useon.get(&t) {
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

        // Every neighbour this tile's mask allows is now in `vis`, so the sanitized
        // mask is final and the tile can go straight to the writer.
        let mut mask = sanitize_walk_mask_for_reachable(rec, t, &vis);
        if let Some(ov) = overrides.get(&t) {
            if let Some(f) = ov.force_mask {
                mask = f;
            } else {
                mask |= ov.or_mask;
            }
        }
        batch.push((t, mask as i64));
        if batch.len() >= EMIT_BATCH {
            sink.send(std::mem::take(&mut batch))
                .map_err(|_| anyhow!("tile writer stopped accepting rows"))?;
            batch = Vec::with_capacity(EMIT_BATCH);
        }
    }

    if !batch.is_empty() {
        sink.send(batch)
            .map_err(|_| anyhow!("tile writer stopped accepting rows"))?;
    }

    println!("Finished BFS; processed {} tiles with {} reachable tiles discovered", processed, vis.len());

    Ok(vis.len())
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

fn sanitize_walk_mask_for_reachable(base: u8, tile: Tile, reachable: &TileSet) -> u8 {
    let mut m = base;
    for bit in 0..8u8 {
        if base & (1 << bit) == 0 { continue; }
        if !reachable.contains(&step(tile, bit)) {
            m &= !(1 << bit);
        }
    }
    m
}

fn get_index_sql(conn: &Connection, table: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name=?1 AND sql IS NOT NULL",
    )?;
    let sqls = stmt
        .query_map([table], |row| row.get::<_, String>(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(sqls)
}

/// Consumes tiles as the BFS discovers them, fetching each row from the source and
/// inserting it into the destination under a single transaction. Hands the
/// destination connection back on join so the caller can copy the remaining tables.
fn spawn_tile_writer(
    src_db_path: &Path,
    mut dst: Connection,
    create_sql: String,
    cols: Vec<String>,
    index_sqls: Vec<String>,
    rx_rows: mpsc::Receiver<Vec<(Tile, i64)>>,
) -> thread::JoinHandle<Result<(Connection, usize)>> {
    let src_path: PathBuf = src_db_path.to_path_buf();
    thread::spawn(move || -> Result<(Connection, usize)> {
        let src = Connection::open(&src_path)
            .with_context(|| format!("Open DB {}", src_path.display()))?;
        tune_read_conn(&src)?;

        let placeholders = (0..cols.len()).map(|_| "?").collect::<Vec<_>>().join(", ");
        let insert_sql = format!("INSERT INTO tiles ({}) VALUES ({})", cols.join(", "), placeholders);
        let select_sql = format!("SELECT {} FROM tiles WHERE x=?1 AND y=?2 AND plane=?3", cols.join(", "));
        let walk_idx = cols.iter().position(|c| c == "walk_mask");
        let ncols = cols.len();

        let tx = dst.transaction()?;
        tx.execute(&create_sql, [])?;

        let mut inserted = 0usize;
        {
            let mut sel = src.prepare(&select_sql)?;
            let mut ins = tx.prepare(&insert_sql)?;
            for batch in rx_rows {
                for (t, mask) in batch {
                    let mut rows = sel.query(params![t.0, t.1, t.2])?;
                    let Some(r) = rows.next()? else { continue };
                    let mut row = read_row_values(r, ncols)?;
                    if let Some(idx) = walk_idx {
                        row[idx] = Value::Integer(mask);
                    }
                    ins.execute(params_from_iter(row.into_iter()))?;
                    inserted += 1;
                    if inserted % 100_000 == 0 {
                        println!("Inserted {} tiles so far...", inserted);
                    }
                }
            }
        }
        tx.commit()?;
        println!("Committed tiles insertion transaction");
        println!("Finished inserting {} tiles", inserted);

        // Indexes are created after the bulk insert so they are built in one pass.
        let mut index_count = 0usize;
        for sql in &index_sqls {
            let _ = dst.execute(sql, []);
            index_count += 1;
        }
        println!("Recreated {} tile indexes", index_count);

        Ok((dst, inserted))
    })
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
    tune_read_conn(&src)?;
    let start: Tile = (start_x, start_y, start_plane);
    let overrides = build_fairy_ring_overrides(&src)?;

    // Destination is prepared up front so the writer can run alongside the BFS.
    if out_db.exists() {
        println!("Removing existing output database {}", out_db.display());
        let _ = fs::remove_file(out_db);
    }
    let dst = Connection::open(out_db).with_context(|| format!("Create DB {}", out_db.display()))?;
    println!("Opened destination database {}", out_db.display());
    // Match Python behavior: avoid FK errors while creating/inserting tiles before copying 'chunks'
    dst.execute_batch("PRAGMA foreign_keys=OFF;")?;
    tune_write_conn(&dst)?;
    println!("Disabled foreign key checks on destination");

    let create_sql = get_create_table_sql(&src, "tiles")?;
    let cols = get_table_columns(&src, "tiles")?;
    let index_sqls = get_index_sql(&src, "tiles")?;

    let (tx_rows, rx_rows) = mpsc::sync_channel::<Vec<(Tile, i64)>>(CHANNEL_DEPTH);
    println!("Starting tile writer; reachable tiles are inserted as they are discovered");
    let writer = spawn_tile_writer(src_db, dst, create_sql, cols, index_sqls, rx_rows);

    println!("Computing reachable tiles...");
    let bfs = reachable_tiles(&src, start, overrides, &tx_rows);
    // Closing the channel lets the writer commit once the BFS is done.
    drop(tx_rows);

    // Prefer the writer's error: a failed BFS send is usually a symptom of it.
    let (mut dst, _inserted) = match writer.join() {
        Ok(res) => res?,
        Err(_) => return Err(anyhow!("tile writer thread panicked")),
    };
    let reachable_count = bfs?;
    println!("Identified {} reachable tiles", reachable_count);

    let mut skip = HashSet::new();
    skip.insert("tiles".to_string());
    copy_tables(&src, &mut dst, &skip)?;
    copy_views(&src, &mut dst)?;

    println!("Tile cleaning complete; output written to {}", out_db.display());
    Ok(())
}
