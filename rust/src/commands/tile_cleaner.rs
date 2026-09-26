//! `tile-cleaner`: keeps the tiles reachable from a start tile (by walking or by
//! teleport links) and writes them, together with copies of every other table,
//! index and view of the source, into a fresh database.
//!
//! Walk masks are held in memory in a [`MaskStore`] -- loaded from the source db
//! with parallel range scans, or fed directly by the extractor -- so the BFS runs
//! without any SQL. The source connection is only used for the teleport tables
//! and for copying the schema and the non-tile tables.

use anyhow::{anyhow, bail, Context, Result};
use rayon::slice::ParallelSliceMut;
use rusqlite::types::{Value, ValueRef};
use rusqlite::{params, params_from_iter, Connection, OpenFlags, OptionalExtension, Row};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Instant;

use crate::sqlite_btree::{serialize_db, table_root_page, FileSink, TreeWriter};

pub type Tile = (i32, i32, i32);

/// The tile maps take millions of lookups, where SipHash's quality buys nothing
/// over three coordinates. This is the usual multiply-rotate mix instead.
#[derive(Default, Clone, Copy)]
pub struct TileHasher(u64);

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

pub type TileBuildHasher = std::hash::BuildHasherDefault<TileHasher>;
type TileMap<V> = HashMap<Tile, V, TileBuildHasher>;
type TileSet = HashSet<Tile, TileBuildHasher>;

/// RegionID values of `tiles` rows that do not follow [`region_id_for`], keyed by tile.
pub type RegionExceptions = HashMap<Tile, Value, TileBuildHasher>;

/// The columns of the `tiles` table every db this program makes has, in order.
/// Only this layout takes the fast output path.
const TILE_COLUMNS: [&str; 5] = ["x", "y", "plane", "walk_mask", "RegionID"];

/// `RegionID` of a tile as every writer of `tiles` computes it:
/// `(regionX << 8) + regionY` with `regionX = x >> 6`, `regionY = y >> 6`.
#[inline]
pub fn region_id_for(x: i32, y: i32) -> i64 {
    (((x as i64) >> 6) << 8) + ((y as i64) >> 6)
}

// ---------------------------------------------------------------------------
// Mask store
// ---------------------------------------------------------------------------

/// The dense part of [`MaskStore`] (and of the BFS's visited set) covers mapsquares
/// sx in 0..128, sz in 0..256, i.e. tiles x in 0..8192, y in 0..16384, planes 0..4.
const SQUARES_X: usize = 128;
const SQUARES_Z: usize = 256;
const SQUARE_COUNT: usize = SQUARES_X * SQUARES_Z;
const DENSE_X: u32 = (SQUARES_X * 64) as u32;
const DENSE_Y: u32 = (SQUARES_Z * 64) as u32;
const PLANES: u32 = 4;
/// Tiles per mapsquare over all planes, indexed `plane*4096 + lz*64 + lx`.
pub const SQUARE_TILES: usize = 4 * 64 * 64;
const SQUARE_WORDS: usize = SQUARE_TILES / 64;

/// (square index, index within the square) of a tile in the dense range.
/// The `as u32` casts fold the `>= 0` checks into the upper-bound ones.
#[inline(always)]
fn dense_slot(t: Tile) -> Option<(usize, usize)> {
    let (x, y, p) = t;
    if (x as u32) < DENSE_X && (y as u32) < DENSE_Y && (p as u32) < PLANES {
        let sq = (x >> 6) as usize * SQUARES_Z + (y >> 6) as usize;
        let idx = ((p as usize) << 12) | (((y & 63) as usize) << 6) | (x & 63) as usize;
        Some((sq, idx))
    } else {
        None
    }
}

#[inline(always)]
fn bit_at(words: &[u64; SQUARE_WORDS], i: usize) -> bool {
    (words[i >> 6] >> (i & 63)) & 1 != 0
}

/// One mapsquare: the mask of every tile plus which tiles exist.
/// Invariant: `masks[i] == 0` wherever the presence bit is clear.
struct Square {
    masks: [u8; SQUARE_TILES],
    present: [u64; SQUARE_WORDS],
}

impl Square {
    fn empty() -> Box<Square> {
        Box::new(Square { masks: [0; SQUARE_TILES], present: [0; SQUARE_WORDS] })
    }

    #[allow(dead_code)] // only reached through insert_square, which the binary does not call yet
    fn count(&self) -> usize {
        self.present.iter().map(|w| w.count_ones() as usize).sum()
    }
}

/// Existence + walk mask (low 8 bits of `walk_mask`, NULL -> 0) of every row of a
/// `tiles` table.
pub struct MaskStore {
    /// Indexed `sx * SQUARES_Z + sz`; `None` until a tile of that square is inserted.
    squares: Box<[Option<Box<Square>>]>,
    /// Tiles outside the dense range (negative or large coordinates, planes outside 0..4).
    fallback: TileMap<u8>,
    len: usize,
    /// Only ever filled by [`MaskStore::load_from_db`]; tiles inserted by the
    /// extractor always follow [`region_id_for`].
    region_exceptions: RegionExceptions,
}

impl Default for MaskStore {
    fn default() -> Self {
        Self::new()
    }
}

/// How `load_from_db` splits the `tiles` table between its threads.
#[derive(Clone, Copy, Debug)]
enum ScanRange {
    /// Whole table; used when `x` does not lead the primary key, so x ranges
    /// could not be served by an index.
    All,
    IsNull,
    /// x < v (numeric values only).
    Below(i64),
    /// v0 <= x < v1.
    Between(i64, i64),
    /// x >= v, which also takes every TEXT and BLOB value since those sort
    /// after all numbers.
    AtLeast(i64),
}

/// Width in x of one `load_from_db` task. 64 keeps every mapsquare column inside a
/// single task, so no square is split between threads; narrower or wider tasks
/// measured no faster.
const LOAD_X_STEP: i64 = 64;
/// The scan is CPU bound in SQLite (~125 ns per row per thread); on the 32-thread
/// test machine 32 threads beat 16 (0.65 s vs 0.8 s for 84.75M rows) and 48 was slower.
const LOAD_MAX_THREADS: usize = 32;

impl MaskStore {
    pub fn new() -> Self {
        let mut squares = Vec::with_capacity(SQUARE_COUNT);
        squares.resize_with(SQUARE_COUNT, || None);
        MaskStore {
            squares: squares.into_boxed_slice(),
            fallback: TileMap::default(),
            len: 0,
            region_exceptions: RegionExceptions::default(),
        }
    }

    /// Marks row `t` as existing with this mask. Overwrites.
    pub fn insert(&mut self, t: Tile, mask: u8) {
        if !self.region_exceptions.is_empty() {
            self.region_exceptions.remove(&t);
        }
        self.put(t, mask);
    }

    /// `insert` without touching the RegionID exceptions.
    #[inline]
    fn put(&mut self, t: Tile, mask: u8) {
        match dense_slot(t) {
            Some((sq, i)) => {
                let s = self.squares[sq].get_or_insert_with(Square::empty);
                let (w, b) = (i >> 6, 1u64 << (i & 63));
                if s.present[w] & b == 0 {
                    s.present[w] |= b;
                    self.len += 1;
                }
                s.masks[i] = mask;
            }
            None => {
                if self.fallback.insert(t, mask).is_none() {
                    self.len += 1;
                }
            }
        }
    }

    /// Bulk insert of one fully present 64x64 mapsquare: `masks[plane*4096 + lz*64 + lx]`
    /// is the mask of tile (sx*64+lx, sz*64+lz, plane), all 16384 tiles exist.
    #[allow(dead_code)] // the extractor-fed path; the standalone command loads from the db
    pub fn insert_square(&mut self, sx: i32, sz: i32, masks: &[u8; SQUARE_TILES]) {
        if !self.region_exceptions.is_empty() {
            self.region_exceptions.retain(|&(x, y, _), _| (x >> 6) != sx || (y >> 6) != sz);
        }
        if (sx as u32) < SQUARES_X as u32 && (sz as u32) < SQUARES_Z as u32 {
            let s = self.squares[sx as usize * SQUARES_Z + sz as usize].get_or_insert_with(Square::empty);
            let before = s.count();
            s.masks.copy_from_slice(masks);
            s.present = [u64::MAX; SQUARE_WORDS];
            self.len += SQUARE_TILES - before;
        } else {
            for (i, &m) in masks.iter().enumerate() {
                let (plane, lz, lx) = ((i >> 12) as i32, ((i >> 6) & 63) as i32, (i & 63) as i32);
                self.put((sx * 64 + lx, sz * 64 + lz, plane), m);
            }
        }
    }

    #[inline]
    pub fn get(&self, t: Tile) -> Option<u8> {
        match dense_slot(t) {
            Some((sq, i)) => {
                let s = self.squares[sq].as_deref()?;
                if bit_at(&s.present, i) {
                    Some(s.masks[i])
                } else {
                    None
                }
            }
            None => {
                if self.fallback.is_empty() {
                    None
                } else {
                    self.fallback.get(&t).copied()
                }
            }
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Rows whose RegionID is not [`region_id_for`] their x/y, with the stored value.
    pub fn region_exceptions(&self) -> &RegionExceptions {
        &self.region_exceptions
    }

    /// Moves `other` into `self`. Only valid when the two hold disjoint tiles, which
    /// is what lets squares present in both be merged with a plain OR.
    fn absorb_disjoint(&mut self, other: MaskStore) {
        for (dst, src) in self.squares.iter_mut().zip(other.squares.into_vec()) {
            let Some(src) = src else { continue };
            match dst {
                None => *dst = Some(src),
                Some(d) => {
                    for (a, b) in d.present.iter_mut().zip(src.present.iter()) {
                        *a |= *b;
                    }
                    for (a, b) in d.masks.iter_mut().zip(src.masks.iter()) {
                        *a |= *b;
                    }
                }
            }
        }
        self.len += other.len;
        self.fallback.extend(other.fallback);
        self.region_exceptions.extend(other.region_exceptions);
    }

    /// Loads every row of `tiles` from `db_path`, scanning disjoint x ranges on
    /// several threads, each with its own read-only connection. Never writes to the
    /// source db.
    ///
    /// Errors if x/y/plane are not integers fitting i32 or walk_mask is neither an
    /// integer nor NULL. RegionID values that differ from [`region_id_for`] are kept
    /// in [`MaskStore::region_exceptions`].
    pub fn load_from_db(db_path: &Path) -> Result<Self> {
        let threads = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .min(LOAD_MAX_THREADS);
        Self::load_from_db_with(db_path, threads, LOAD_X_STEP)
    }

    /// `load_from_db` with an explicit thread count and task width in x.
    fn load_from_db_with(db_path: &Path, threads: usize, x_step: i64) -> Result<Self> {
        let probe = open_read_only(db_path)?;
        let info = table_info(&probe, "tiles")
            .with_context(|| format!("{}: reading the `tiles` table", db_path.display()))?;
        drop(probe);
        let has = |name: &str| info.iter().any(|(c, _)| c.eq_ignore_ascii_case(name));
        for required in ["x", "y", "plane", "walk_mask"] {
            if !has(required) {
                bail!("{}: table `tiles` has no `{}` column", db_path.display(), required);
            }
        }
        let with_region = has("RegionID");
        let x_leads_key = info.iter().any(|(c, pk)| *pk == 1 && c.eq_ignore_ascii_case("x"));

        let mut tasks = Vec::new();
        if x_leads_key {
            tasks.push(ScanRange::IsNull);
            tasks.push(ScanRange::Below(0));
            let mut lo = 0i64;
            while lo < DENSE_X as i64 {
                tasks.push(ScanRange::Between(lo, lo + x_step));
                lo += x_step;
            }
            tasks.push(ScanRange::AtLeast(lo));
        } else {
            tasks.push(ScanRange::All);
        }

        let threads = threads.clamp(1, tasks.len());
        let next = AtomicUsize::new(0);
        let failed = AtomicBool::new(false);
        let parts: Vec<Result<MaskStore>> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..threads)
                .map(|_| {
                    scope.spawn(|| -> Result<MaskStore> {
                        let run = || -> Result<MaskStore> {
                            let conn = open_read_only(db_path)?;
                            let mut store = MaskStore::new();
                            while !failed.load(Ordering::Relaxed) {
                                let Some(&task) = tasks.get(next.fetch_add(1, Ordering::Relaxed)) else {
                                    break;
                                };
                                scan_tiles(&conn, task, with_region, &mut store)
                                    .with_context(|| format!("{}: loading tiles ({:?})", db_path.display(), task))?;
                            }
                            Ok(store)
                        };
                        let res = run();
                        if res.is_err() {
                            failed.store(true, Ordering::Relaxed);
                        }
                        res
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().unwrap_or_else(|_| Err(anyhow!("tile loader thread panicked"))))
                .collect()
        });

        let mut store = MaskStore::new();
        for part in parts {
            store.absorb_disjoint(part?);
        }
        Ok(store)
    }
}

#[inline]
fn tile_coord(v: ValueRef<'_>, name: &str) -> Result<i32> {
    match v {
        ValueRef::Integer(i) => {
            i32::try_from(i).map_err(|_| anyhow!("tiles.{} value {} does not fit in i32", name, i))
        }
        other => Err(anyhow!("tiles.{} holds a non-integer value ({:?})", name, Value::from(other))),
    }
}

/// Inserts every row of one [`ScanRange`] into `store`.
fn scan_tiles(conn: &Connection, range: ScanRange, with_region: bool, store: &mut MaskStore) -> Result<()> {
    let cols = if with_region { "x, y, plane, walk_mask, RegionID" } else { "x, y, plane, walk_mask" };
    let (cond, args): (&str, Vec<i64>) = match range {
        ScanRange::All => ("", vec![]),
        ScanRange::IsNull => (" WHERE x IS NULL", vec![]),
        ScanRange::Below(v) => (" WHERE x < ?1", vec![v]),
        ScanRange::Between(a, b) => (" WHERE x >= ?1 AND x < ?2", vec![a, b]),
        ScanRange::AtLeast(v) => (" WHERE x >= ?1", vec![v]),
    };
    let mut stmt = conn.prepare_cached(&format!("SELECT {} FROM tiles{}", cols, cond))?;
    let mut rows = stmt.query(params_from_iter(args))?;
    while let Some(r) = rows.next()? {
        let x = tile_coord(r.get_ref(0)?, "x")?;
        let y = tile_coord(r.get_ref(1)?, "y")?;
        let p = tile_coord(r.get_ref(2)?, "plane")?;
        // Only the low 8 bits are direction flags, as in the BFS's `get_raw`.
        let mask = match r.get_ref(3)? {
            ValueRef::Integer(v) => v as u8,
            ValueRef::Null => 0,
            other => bail!("tiles.walk_mask of ({}, {}, {}) is not an integer ({:?})", x, y, p, Value::from(other)),
        };
        let t = (x, y, p);
        if with_region {
            let region = r.get_ref(4)?;
            if !matches!(region, ValueRef::Integer(v) if v == region_id_for(x, y)) {
                store.region_exceptions.insert(t, Value::from(region));
            }
        }
        store.put(t, mask);
    }
    Ok(())
}

/// The source is only ever read. Mapping it lets the loader threads share the
/// OS page cache instead of each copying pages into its own SQLite cache.
fn open_read_only(path: &Path) -> Result<Connection> {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("Open DB {} read-only", path.display()))?;
    conn.execute_batch(
        "PRAGMA mmap_size=8589934592;
         PRAGMA temp_store=MEMORY;",
    )?;
    Ok(conn)
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

/// Opens the output db. Foreign keys are off (the bundled SQLite defaults them on)
/// so tables can be filled in any order.
fn open_output(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path).with_context(|| format!("Open DB {}", path.display()))?;
    conn.execute_batch("PRAGMA foreign_keys=OFF;")?;
    tune_write_conn(&conn)?;
    Ok(conn)
}

// ---------------------------------------------------------------------------
// Fairy rings and walk masks
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default)]
struct WalkMaskOverride {
    force_mask: Option<u8>,
    or_mask: u8,
}

impl WalkMaskOverride {
    #[inline]
    fn apply(&self, mask: u8) -> u8 {
        match self.force_mask {
            Some(f) => f,
            None => mask | self.or_mask,
        }
    }
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

/// Fairy ring tiles are forced fully open and each of their eight neighbours gets
/// the direction(s) pointing back at the ring. A missing table means no rings.
fn build_fairy_ring_overrides(conn: &Connection) -> Result<TileMap<WalkMaskOverride>> {
    let mut out: TileMap<WalkMaskOverride> = TileMap::default();

    let mut stmt = match conn.prepare("SELECT x, y, plane FROM teleports_fairy_rings_nodes") {
        Ok(s) => s,
        Err(_) => return Ok(out),
    };

    let mut rows = stmt.query([])?;
    while let Some(r) = rows.next()? {
        let x: Option<i64> = r.get(0)?;
        let y: Option<i64> = r.get(1)?;
        let p: Option<i64> = r.get(2)?;
        let (Some(x), Some(y), Some(p)) = (x, y, p) else { continue };
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

    Ok(out)
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

/// A set of tiles over the same dense grid as [`MaskStore`], one bit per tile,
/// allocated a square at a time.
struct TileBits {
    squares: Box<[Option<Box<[u64; SQUARE_WORDS]>>]>,
    fallback: TileSet,
}

impl TileBits {
    fn new() -> Self {
        let mut squares = Vec::with_capacity(SQUARE_COUNT);
        squares.resize_with(SQUARE_COUNT, || None);
        TileBits { squares: squares.into_boxed_slice(), fallback: TileSet::default() }
    }

    /// Returns true if `t` was not in the set yet.
    #[inline]
    fn insert(&mut self, t: Tile) -> bool {
        match dense_slot(t) {
            Some((sq, i)) => {
                let words = self.squares[sq].get_or_insert_with(|| Box::new([0; SQUARE_WORDS]));
                let (w, b) = (i >> 6, 1u64 << (i & 63));
                let fresh = words[w] & b == 0;
                words[w] |= b;
                fresh
            }
            None => self.fallback.insert(t),
        }
    }

    #[inline]
    fn contains(&self, t: Tile) -> bool {
        match dense_slot(t) {
            Some((sq, i)) => self.squares[sq].as_deref().is_some_and(|words| bit_at(words, i)),
            None => self.fallback.contains(&t),
        }
    }
}

/// Drops any direction of `base` whose neighbour does not permit the reverse step
/// (`raw_of(bit)` is the raw mask of the neighbour in direction `bit`), and any
/// diagonal whose two component cardinals are not both open.
#[inline(always)]
fn reconcile(mut base: u8, raw_of: impl Fn(u8) -> u8) -> u8 {
    if base == 0 {
        return 0;
    }
    for bit in CARDINALS {
        if base & (1 << bit) == 0 {
            continue;
        }
        if raw_of(bit) & (1 << RECIP_BIT[bit as usize]) == 0 {
            base &= !(1 << bit);
        }
    }
    // Runs after the cardinal pass so it sees the cleared cardinals.
    for bit in DIAGONALS {
        if base & (1 << bit) == 0 {
            continue;
        }
        let req = DIAG_REQ[bit as usize];
        if base & req != req {
            base &= !(1 << bit);
            continue;
        }
        if raw_of(bit) & (1 << RECIP_BIT[bit as usize]) == 0 {
            base &= !(1 << bit);
        }
    }
    base
}

/// Offset of each direction's neighbour within a square's `plane*4096 + lz*64 + lx`
/// indexing, valid when the tile is not on the square's border.
const DIR_OFFSET: [isize; 8] = {
    let mut out = [0isize; 8];
    let mut b = 0;
    while b < 8 {
        out[b] = DIR_DELTA[b].0 as isize + 64 * DIR_DELTA[b].1 as isize;
        b += 1;
    }
    out
};

/// Raw and reconciled walk masks, read from a [`MaskStore`] with the fairy ring
/// overrides applied.
///
/// A tile's raw mask is its stored mask with its override applied, or 0 if it has
/// no row: overrides never "create" walkability for missing tiles.
struct Walker<'a> {
    masks: &'a MaskStore,
    overrides: &'a TileMap<WalkMaskOverride>,
    /// Tiles with an entry in `overrides`, so the common case skips the hash lookup.
    overridden: TileBits,
}

impl<'a> Walker<'a> {
    fn new(masks: &'a MaskStore, overrides: &'a TileMap<WalkMaskOverride>) -> Self {
        let mut overridden = TileBits::new();
        for &t in overrides.keys() {
            overridden.insert(t);
        }
        Walker { masks, overrides, overridden }
    }

    /// Raw mask of any tile.
    #[inline]
    fn raw(&self, t: Tile) -> u8 {
        match self.masks.get(t) {
            Some(w) if self.overridden.contains(t) => self.overrides.get(&t).map_or(w, |ov| ov.apply(w)),
            Some(w) => w,
            None => 0,
        }
    }

    /// Raw mask of tile `t`, which is entry `i` of square `s` whose override bits are `ov`.
    #[inline(always)]
    fn raw_in(&self, s: &Square, ov: Option<&[u64; SQUARE_WORDS]>, t: Tile, i: usize) -> u8 {
        // A missing tile's mask is 0 (Square invariant), so only overrides need the presence bit.
        let m = s.masks[i];
        match ov {
            Some(bits) if bit_at(bits, i) && bit_at(&s.present, i) => {
                self.overrides.get(&t).map_or(m, |o| o.apply(m))
            }
            _ => m,
        }
    }

    /// For a popped tile: `None` if it has no `tiles` row (then its reconciled mask is
    /// 0), else (reconciled mask, mask to write = reconciled mask with the override
    /// re-applied).
    #[inline]
    fn visit(&self, t: Tile) -> Option<(u8, u8)> {
        let (ov_t, rec) = match dense_slot(t) {
            Some((sq, i)) => {
                let s = self.masks.squares[sq].as_deref()?;
                if !bit_at(&s.present, i) {
                    return None;
                }
                let ov = self.overridden.squares[sq].as_deref();
                let ov_t = match ov {
                    Some(bits) if bit_at(bits, i) => self.overrides.get(&t),
                    _ => None,
                };
                let raw = ov_t.map_or(s.masks[i], |o| o.apply(s.masks[i]));
                let (lx, lz) = (i & 63, (i >> 6) & 63);
                let rec = if (1..63).contains(&lx) && (1..63).contains(&lz) {
                    // Every neighbour is in the same square.
                    reconcile(raw, |bit| {
                        let j = (i as isize + DIR_OFFSET[bit as usize]) as usize;
                        self.raw_in(s, ov, step(t, bit), j)
                    })
                } else {
                    reconcile(raw, |bit| self.raw(step(t, bit)))
                };
                (ov_t, rec)
            }
            None => {
                let &w = self.masks.fallback.get(&t)?;
                let ov_t = self.overrides.get(&t);
                let raw = ov_t.map_or(w, |o| o.apply(w));
                (ov_t, reconcile(raw, |bit| self.raw(step(t, bit))))
            }
        };
        Some((rec, ov_t.map_or(rec, |o| o.apply(rec))))
    }
}

// ---------------------------------------------------------------------------
// Teleports
// ---------------------------------------------------------------------------

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

/// Reads `N` nullable integer columns; `None` if any of them is NULL.
fn get_opt_ints<const N: usize>(r: &Row) -> Result<Option<[i32; N]>> {
    let mut out = [0i32; N];
    for (i, slot) in out.iter_mut().enumerate() {
        let v: Option<i64> = r.get(i)?;
        let Some(v) = v else { return Ok(None) };
        *slot = v as i32;
    }
    Ok(Some(out))
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
        let Some([o_min_x, o_max_x, o_min_y, o_max_y, o_plane, d_min_x, d_max_x, d_min_y, d_max_y, d_plane]) =
            get_opt_ints::<10>(r)?
        else {
            continue;
        };
        let origin = center_tile(o_min_x, o_max_x, o_min_y, o_max_y, o_plane);
        let dest = center_tile(d_min_x, d_max_x, d_min_y, d_max_y, d_plane);
        adj.entry(origin).or_default().push(dest);
        adj.entry(dest).or_default().push(origin);
    }
    Ok(adj)
}

/// The centre of each node's destination range, for node tables that only carry
/// `dest_*` (item, interface slot and POA teleports).
fn get_dest_tiles(conn: &Connection, table: &str) -> Result<Vec<Tile>> {
    let mut out: Vec<Tile> = Vec::new();
    let mut stmt = conn.prepare(&format!(
        "SELECT dest_min_x, dest_max_x, dest_min_y, dest_max_y, dest_plane FROM {}",
        table
    ))?;
    let mut rows = stmt.query([])?;
    while let Some(r) = rows.next()? {
        let Some([d_min_x, d_max_x, d_min_y, d_max_y, d_plane]) = get_opt_ints::<5>(r)? else { continue };
        out.push(center_tile(d_min_x, d_max_x, d_min_y, d_max_y, d_plane));
    }
    Ok(out)
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

struct Teleports {
    door: TileMap<Vec<Tile>>,
    lode_set: TileSet,
    lodestones: Vec<Tile>,
    obj: TileMap<Vec<Tile>>,
    npc: TileMap<Vec<Tile>>,
    useon: TileMap<Vec<Tile>>,
    item_dests: Vec<Tile>,
    ifslot: Vec<Tile>,
    poa_dests: Vec<Tile>,
}

fn load_teleports(conn: &Connection) -> Result<Teleports> {
    let door = get_door_links(conn)?;
    let (lode_set, lodestones) = get_lodestones(conn)?;
    let obj = get_range_transitions(conn, "teleports_object_nodes")?;
    let npc = get_range_transitions(conn, "teleports_npc_nodes")?;
    // Use-on nodes were added after some databases were built, so a missing table
    // means "no such transitions" rather than a corrupt DB.
    let useon = if table_exists(conn, "teleports_useOn_nodes")? {
        get_range_transitions(conn, "teleports_useOn_nodes")?
    } else {
        TileMap::default()
    };
    let item_dests = get_dest_tiles(conn, "teleports_item_nodes")?;
    let ifslot = get_dest_tiles(conn, "teleports_ifslot_nodes")?;
    let poa_dests = get_dest_tiles(conn, "teleports_POA_nodes")?;
    Ok(Teleports { door, lode_set, lodestones, obj, npc, useon, item_dests, ifslot, poa_dests })
}

// ---------------------------------------------------------------------------
// BFS
// ---------------------------------------------------------------------------

struct Reachable {
    /// Tiles visited, including ones that have no `tiles` row.
    visited: usize,
    /// Visited tiles that have a `tiles` row, with the walk mask to write, in BFS order.
    kept: Vec<(Tile, u8)>,
}

/// BFS over the walkable graph plus teleport links, seeded with the start tile,
/// every teleport endpoint and every fairy ring.
///
/// A tile's written mask is its reconciled mask with its override re-applied.
/// Directions leading outside the reachable set would have to be dropped too, but
/// there are none: every neighbour the reconciled mask allows is inserted into `vis`
/// in the same iteration that pops the tile.
fn reachable_tiles(
    masks: &MaskStore,
    overrides: &TileMap<WalkMaskOverride>,
    tp: &Teleports,
    start: Tile,
) -> Reachable {
    let walker = Walker::new(masks, overrides);

    // Every per-origin link in one map, so a popped tile costs one lookup instead of five.
    let mut jumps: TileMap<Vec<Tile>> = TileMap::default();
    for (&o, v) in tp.door.iter().chain(&tp.obj).chain(&tp.npc).chain(&tp.useon) {
        jumps.entry(o).or_default().extend_from_slice(v);
    }
    for &t in &tp.lode_set {
        jumps.entry(t).or_default().extend_from_slice(&tp.lodestones);
    }

    let mut vis = TileBits::new();
    let mut visited = 0usize;
    let mut q: VecDeque<Tile> = VecDeque::with_capacity(1 << 16);
    let mut kept: Vec<(Tile, u8)> = Vec::with_capacity(1 << 20);

    let mut enqueue = |t: Tile, vis: &mut TileBits, q: &mut VecDeque<Tile>| {
        if vis.insert(t) {
            visited += 1;
            q.push_back(t);
        }
    };

    enqueue(start, &mut vis, &mut q);
    // Teleport endpoints are seeded so destination tiles are retained even if their
    // origin tiles are not walk-reachable (one-way walk masks or data issues).
    let seeds = tp
        .door
        .values()
        .flatten()
        .chain(&tp.lodestones)
        .chain(tp.obj.values().flatten())
        .chain(tp.npc.values().flatten())
        .chain(tp.useon.values().flatten())
        .copied()
        .chain(overrides.iter().filter(|(_, ov)| ov.force_mask.is_some()).map(|(&t, _)| t))
        .chain(tp.item_dests.iter().copied())
        .chain(tp.poa_dests.iter().copied())
        // Interface slot teleports work from anywhere, so they are linked from the
        // first expanded tile; the start tile is always queued, so that is the same
        // as seeding them.
        .chain(tp.ifslot.iter().copied());
    for t in seeds {
        enqueue(t, &mut vis, &mut q);
    }

    while let Some(t) = q.pop_front() {
        let (rec, keep) = match walker.visit(t) {
            Some((rec, keep)) => (rec, Some(keep)),
            None => (0, None),
        };
        let mut bits = rec;
        while bits != 0 {
            let bit = bits.trailing_zeros() as u8;
            bits &= bits - 1;
            enqueue(step(t, bit), &mut vis, &mut q);
        }
        if let Some(v) = jumps.get(&t) {
            for &n in v {
                enqueue(n, &mut vis, &mut q);
            }
        }
        if let Some(mask) = keep {
            debug_assert!((0..8u8).all(|b| rec & (1 << b) == 0 || vis.contains(step(t, b))));
            kept.push((t, mask));
        }
    }

    Reachable { visited, kept }
}

// ---------------------------------------------------------------------------
// Output
// ---------------------------------------------------------------------------

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

/// (name, 1-based position in the primary key or 0) of every column.
fn table_info(conn: &Connection, table: &str) -> Result<Vec<(String, i64)>> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({})", table))?;
    let mut rows = stmt.query([])?;
    let mut cols = Vec::new();
    while let Some(r) = rows.next()? {
        cols.push((r.get::<_, String>(1)?, r.get::<_, i64>(5)?));
    }
    if cols.is_empty() {
        Err(anyhow!("No columns for table {}", table))
    } else {
        Ok(cols)
    }
}

fn get_table_columns(conn: &Connection, table: &str) -> Result<Vec<String>> {
    Ok(table_info(conn, table)?.into_iter().map(|(name, _)| name).collect())
}

fn read_row_values(row: &Row, ncols: usize) -> Result<Vec<Value>> {
    let mut out: Vec<Value> = Vec::with_capacity(ncols);
    for i in 0..ncols {
        out.push(row.get::<usize, Value>(i)?);
    }
    Ok(out)
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

#[derive(Default)]
struct CopyStats {
    tables: usize,
    rows: usize,
    indexes: usize,
    views: usize,
}

/// Copies every table except `skip` (schema, rows, then its indexes right after it)
/// in sqlite_master order.
fn copy_tables(src: &Connection, dst: &Connection, skip: &str, stats: &mut CopyStats) -> Result<()> {
    let mut stmt = src.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")?;
    let table_names: Vec<String> = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<std::result::Result<_, _>>()?;

    for t in table_names {
        if t == skip {
            continue;
        }
        let create_sql = match src.query_row(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?1",
            [&t],
            |row| row.get::<_, Option<String>>(0),
        ) {
            Ok(Some(s)) => s,
            _ => continue,
        };
        dst.execute(&create_sql, [])?;
        stats.tables += 1;
        let cols = get_table_columns(src, &t)?;
        let select_sql = format!("SELECT {} FROM {}", cols.join(", "), t);
        let placeholders = vec!["?"; cols.len()].join(", ");
        let insert_sql = format!("INSERT INTO {} ({}) VALUES ({})", t, cols.join(", "), placeholders);
        let mut ins = dst.prepare(&insert_sql)?;
        let mut sel = src.prepare(&select_sql)?;
        let mut rows = sel.query([])?;
        while let Some(r) = rows.next()? {
            let vals = read_row_values(r, cols.len())?;
            ins.execute(params_from_iter(vals))?;
            stats.rows += 1;
        }
        let mut idx_stmt = src.prepare(
            "SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name=?1 AND sql IS NOT NULL",
        )?;
        let mut idx_rows = idx_stmt.query([&t])?;
        while let Some(ir) = idx_rows.next()? {
            let sql: Option<String> = ir.get(0)?;
            if let Some(sql) = sql {
                let _ = dst.execute(&sql, []);
                stats.indexes += 1;
            }
        }
    }
    Ok(())
}

fn copy_views(src: &Connection, dst: &Connection, stats: &mut CopyStats) -> Result<()> {
    let mut stmt = src.prepare("SELECT name, sql FROM sqlite_master WHERE type='view' AND sql IS NOT NULL")?;
    let mut rows = stmt.query([])?;
    while let Some(r) = rows.next()? {
        let sql: String = r.get(1)?;
        let _ = dst.execute(&sql, []);
        stats.views += 1;
    }
    Ok(())
}

/// Every non-tile table (with its indexes) and every view.
fn copy_rest(src: &Connection, dst: &Connection, stats: &mut CopyStats) -> Result<()> {
    copy_tables(src, dst, "tiles", stats)?;
    copy_views(src, dst, stats)
}

/// Rows per multi-row INSERT of kept tiles. One statement per row costs a VDBE
/// program invocation each; batching amortises it. 200 * 5 parameters stays far
/// below SQLITE_MAX_VARIABLE_NUMBER.
const TILE_INSERT_BATCH: usize = 200;

fn insert_tiles_sql(cols: &[String], rows: usize) -> String {
    let one = format!("({})", vec!["?"; cols.len()].join(","));
    format!("INSERT INTO tiles ({}) VALUES {}", cols.join(", "), vec![one; rows].join(","))
}

/// Inserts the kept tiles into the empty `tiles` table of the already created
/// `out_db`, in one transaction.
///
/// `cols` must be [`TILE_COLUMNS`] and `rows` sorted by (x, y, plane), the primary
/// key order. Each row is written as x, y, plane and walk_mask = the given mask
/// (all integers) and RegionID = `region_exceptions[tile]` if present, else
/// [`region_id_for`]`(x, y)`.
///
/// Self-contained on purpose: this is the piece a direct b-tree page writer replaces.
fn write_tiles_rows(
    out_db: &Path,
    cols: &[String],
    rows: &[(Tile, u8)],
    region_exceptions: &RegionExceptions,
) -> Result<()> {
    if !cols.iter().map(String::as_str).eq(TILE_COLUMNS) {
        bail!("write_tiles_rows needs the columns {:?}, got {:?}", TILE_COLUMNS, cols);
    }
    debug_assert!(rows.windows(2).all(|w| w[0].0 < w[1].0));

    let mut conn = open_output(out_db)?;
    let tx = conn.transaction()?;
    {
        let has_exceptions = !region_exceptions.is_empty();
        let full_sql = insert_tiles_sql(cols, TILE_INSERT_BATCH);
        let mut full = tx.prepare_cached(&full_sql)?;
        for chunk in rows.chunks(TILE_INSERT_BATCH) {
            let mut tail;
            let stmt = if chunk.len() == TILE_INSERT_BATCH {
                &mut full
            } else {
                tail = tx.prepare_cached(&insert_tiles_sql(cols, chunk.len()))?;
                &mut tail
            };
            for (i, &(t, mask)) in chunk.iter().enumerate() {
                let (x, y, p) = t;
                let b = i * 5;
                stmt.raw_bind_parameter(b + 1, x)?;
                stmt.raw_bind_parameter(b + 2, y)?;
                stmt.raw_bind_parameter(b + 3, p)?;
                stmt.raw_bind_parameter(b + 4, mask as i64)?;
                match if has_exceptions { region_exceptions.get(&t) } else { None } {
                    Some(v) => stmt.raw_bind_parameter(b + 5, v)?,
                    None => stmt.raw_bind_parameter(b + 5, region_id_for(x, y))?,
                }
            }
            stmt.raw_execute()?;
        }
    }
    tx.commit()?;
    Ok(())
}

/// Fallback for a `tiles` table whose columns are not [`TILE_COLUMNS`]: fetches each
/// kept tile's full row from the source and replaces only `walk_mask`.
fn write_tiles_rows_generic(src: &Connection, out_db: &Path, cols: &[String], rows: &[(Tile, u8)]) -> Result<()> {
    let select_sql = format!("SELECT {} FROM tiles WHERE x=?1 AND y=?2 AND plane=?3", cols.join(", "));
    let insert_sql = format!(
        "INSERT INTO tiles ({}) VALUES ({})",
        cols.join(", "),
        vec!["?"; cols.len()].join(", ")
    );
    let walk_idx = cols.iter().position(|c| c == "walk_mask");

    let mut conn = open_output(out_db)?;
    let tx = conn.transaction()?;
    {
        let mut sel = src.prepare(&select_sql)?;
        let mut ins = tx.prepare(&insert_sql)?;
        for &(t, mask) in rows {
            let mut found = sel.query(params![t.0, t.1, t.2])?;
            let Some(r) = found.next()? else { continue };
            let mut row = read_row_values(r, cols.len())?;
            if let Some(idx) = walk_idx {
                row[idx] = Value::Integer(mask as i64);
            }
            ins.execute(params_from_iter(row))?;
        }
    }
    tx.commit()?;
    Ok(())
}

/// Removes a previous output and any journal left next to it, which SQLite would
/// otherwise try to apply to the new file.
fn remove_old_output(out_db: &Path) -> Result<()> {
    let mut paths = vec![out_db.to_path_buf()];
    for suffix in ["-journal", "-wal", "-shm"] {
        let mut p = out_db.as_os_str().to_owned();
        p.push(suffix);
        paths.push(p.into());
    }
    for p in paths {
        if p.exists() {
            fs::remove_file(&p).with_context(|| format!("Remove {}", p.display()))?;
        }
    }
    Ok(())
}

/// Writes the output db: `tiles` first, then the other tables (each followed by its
/// indexes) and the views, all as SQL copied from the source; then the kept tile
/// rows and the tiles indexes.
///
/// sqlite_master has to list, in order: tiles, tiles indexes, other tables and their
/// indexes, views. Without tiles indexes the rest can be copied before the rows, as
/// usual; with them, the rows and indexes go in first so the order is kept.
fn write_output(src: &Connection, masks: &MaskStore, out_db: &Path, rows: &[(Tile, u8)]) -> Result<()> {
    let create_sql = get_create_table_sql(src, "tiles")?;
    let cols = get_table_columns(src, "tiles")?;
    let tile_index_sqls = get_index_sql(src, "tiles")?;
    let canonical = cols.iter().map(String::as_str).eq(TILE_COLUMNS);

    // The usual case goes through the page writer; anything unusual (other
    // columns, secondary indexes on tiles, non-integer RegionIDs) through sqlite.
    let integer_regions = masks.region_exceptions().values().all(|v| matches!(v, Value::Integer(_)));
    if canonical && tile_index_sqls.is_empty() && integer_regions {
        return write_output_image(src, masks, out_db, &create_sql, rows);
    }

    remove_old_output(out_db)?;

    let t_schema = Instant::now();
    let mut stats = CopyStats::default();
    {
        let mut dst = open_output(out_db)?;
        let tx = dst.transaction()?;
        tx.execute(&create_sql, [])?;
        if tile_index_sqls.is_empty() {
            copy_rest(src, &tx, &mut stats)?;
        }
        tx.commit()?;
    }
    let mut d_schema = t_schema.elapsed();

    let t_rows = Instant::now();
    if canonical {
        write_tiles_rows(out_db, &cols, rows, masks.region_exceptions())?;
    } else {
        println!("tiles columns are {:?}, not {:?}; copying full rows from the source", cols, TILE_COLUMNS);
        write_tiles_rows_generic(src, out_db, &cols, rows)?;
    }
    let d_rows = t_rows.elapsed();

    if !tile_index_sqls.is_empty() {
        let t_rest = Instant::now();
        let mut dst = open_output(out_db)?;
        let tx = dst.transaction()?;
        // Indexes are created after the bulk insert so they are built in one pass.
        for sql in &tile_index_sqls {
            let _ = tx.execute(sql, []);
        }
        copy_rest(src, &tx, &mut stats)?;
        tx.commit()?;
        d_schema += t_rest.elapsed();
    }

    println!(
        "Wrote {} tiles rows in {:.0} ms; schema, {} tile indexes, {} other tables ({} rows, {} indexes) and {} views in {:.0} ms",
        rows.len(),
        d_rows.as_secs_f64() * 1e3,
        tile_index_sqls.len(),
        stats.tables,
        stats.rows,
        stats.indexes,
        stats.views,
        d_schema.as_secs_f64() * 1e3,
    );
    Ok(())
}

/// Builds the output database in memory (the `tiles` table first, then the
/// other tables with their indexes and the views, exactly as [`write_output`]
/// does on disk), then streams the tile rows into it as b-tree pages with
/// [`TreeWriter`], rewriting only the pages of an existing output that changed.
fn write_output_image(
    src: &Connection,
    masks: &MaskStore,
    out_db: &Path,
    create_sql: &str,
    rows: &[(Tile, u8)],
) -> Result<()> {
    let t_schema = Instant::now();
    let mut stats = CopyStats::default();
    let (image, root) = {
        let mut mem = Connection::open_in_memory()?;
        let tx = mem.transaction()?;
        tx.execute(create_sql, [])?;
        copy_rest(src, &tx, &mut stats)?;
        tx.commit()?;
        (serialize_db(&mem)?, table_root_page(&mem, "tiles")?)
    };
    let page_size = match u16::from_be_bytes([image[16], image[17]]) {
        1 => 65536,
        n => n as usize,
    };
    let d_schema = t_schema.elapsed();

    let t_rows = Instant::now();
    let sink = FileSink::create_or_replace(out_db, page_size)?;
    let mut tree = TreeWriter::new(&image, root, 3, sink)?;
    let exceptions = masks.region_exceptions();
    let mut column = None;
    for &(t, mask) in rows {
        let (x, y, p) = t;
        // A new leaf per mapsquare column, as in tiles.db, so a rebuild after
        // a local change rewrites only that column's pages.
        if column != Some(x >> 6) {
            column = Some(x >> 6);
            tree.break_leaf()?;
        }
        let region = match exceptions.get(&t) {
            Some(Value::Integer(v)) => *v,
            _ => region_id_for(x, y),
        };
        tree.push_ints(&[x as i64, y as i64, p as i64, mask as i64, region])?;
    }
    let (tree_stats, sink) = tree.finish()?;
    let sink = sink.stats();
    let d_rows = t_rows.elapsed();

    println!(
        "Wrote {} tiles rows in {:.0} ms ({} leaf pages; {} of {} pages changed on disk); \
         schema, {} other tables ({} rows, {} indexes) and {} views in {:.0} ms",
        rows.len(),
        d_rows.as_secs_f64() * 1e3,
        tree_stats.leaf_pages,
        sink.pages_written,
        sink.pages_written + sink.pages_unchanged,
        stats.tables,
        stats.rows,
        stats.indexes,
        stats.views,
        d_schema.as_secs_f64() * 1e3,
    );
    Ok(())
}

/// BFS + output. `src` is an open connection to the source db, used ONLY for the
/// teleport tables and for copying schema/tables/indexes/views; tile walk masks come
/// from `masks`.
pub fn run_cleaner(src: &Connection, masks: &MaskStore, out_db: &Path, start: Tile) -> Result<()> {
    if let Some(src_path) = src.path().filter(|p| !p.is_empty()) {
        if let (Ok(a), Ok(b)) = (fs::canonicalize(src_path), fs::canonicalize(out_db)) {
            if a == b {
                bail!("Output {} is the source database", out_db.display());
            }
        }
    }

    let t = Instant::now();
    let overrides = build_fairy_ring_overrides(src)?;
    let tp = load_teleports(src)?;
    let links = |m: &TileMap<Vec<Tile>>| (m.len(), m.values().map(Vec::len).sum::<usize>());
    let (door_o, door_d) = links(&tp.door);
    let (obj_o, obj_d) = links(&tp.obj);
    let (npc_o, npc_d) = links(&tp.npc);
    let (useon_o, useon_d) = links(&tp.useon);
    println!(
        "Loaded teleports in {:.0} ms: door {}/{}, object {}/{}, npc {}/{}, use-on {}/{} (origins/destinations); \
         {} lodestones, {} item, {} interface slot, {} POA destinations; {} fairy ring tiles",
        t.elapsed().as_secs_f64() * 1e3,
        door_o,
        door_d,
        obj_o,
        obj_d,
        npc_o,
        npc_d,
        useon_o,
        useon_d,
        tp.lodestones.len(),
        tp.item_dests.len(),
        tp.ifslot.len(),
        tp.poa_dests.len(),
        overrides.values().filter(|o| o.force_mask.is_some()).count(),
    );

    let t = Instant::now();
    let Reachable { visited, mut kept } = reachable_tiles(masks, &overrides, &tp, start);
    kept.par_sort_unstable_by_key(|&(t, _)| t);
    println!(
        "BFS from {:?} in {:.0} ms: {} tiles reached, {} of them have a tiles row",
        start,
        t.elapsed().as_secs_f64() * 1e3,
        visited,
        kept.len()
    );

    write_output(src, masks, out_db, &kept)
}

/// Standalone command: [`MaskStore::load_from_db`] then [`run_cleaner`].
pub fn cmd_tile_cleaner(src_db: &Path, out_db: &Path, start_x: i32, start_y: i32, start_plane: i32) -> Result<()> {
    let started = Instant::now();
    let start: Tile = (start_x, start_y, start_plane);
    println!("Starting tile cleaner from start tile {:?}", start);
    let src = open_read_only(src_db)?;

    let t = Instant::now();
    let masks = MaskStore::load_from_db(src_db)?;
    println!(
        "Loaded {} tiles from {} in {:.0} ms ({} RegionID exceptions)",
        masks.len(),
        src_db.display(),
        t.elapsed().as_secs_f64() * 1e3,
        masks.region_exceptions().len()
    );

    run_cleaner(&src, &masks, out_db, start)?;
    println!(
        "Tile cleaning complete in {:.2} s; output written to {}",
        started.elapsed().as_secs_f64(),
        out_db.display()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tempfile::TempDir;

    fn square_of(f: impl Fn(usize) -> u8) -> Box<[u8; SQUARE_TILES]> {
        let mut out = Box::new([0u8; SQUARE_TILES]);
        for (i, m) in out.iter_mut().enumerate() {
            *m = f(i);
        }
        out
    }

    #[test]
    fn mask_store_dense_and_fallback() {
        let mut m = MaskStore::new();
        assert!(m.is_empty());
        assert_eq!(m.get((5, 6, 0)), None);

        let tiles: [(Tile, u8); 9] = [
            ((5, 6, 0), 7),
            ((0, 0, 0), 0),            // present with mask 0
            ((8191, 16383, 3), 1),     // last dense tile
            ((-1, 5, 0), 2),           // negative x
            ((8192, 5, 0), 3),         // x past the dense range
            ((5, 16384, 0), 4),        // y past the dense range
            ((5, -64, 0), 11),         // negative y
            ((5, 6, 4), 5),            // plane 4
            ((5, 6, -1), 6),           // negative plane
        ];
        for &(t, mask) in &tiles {
            m.insert(t, mask);
        }
        assert_eq!(m.len(), tiles.len());
        for &(t, mask) in &tiles {
            assert_eq!(m.get(t), Some(mask), "{:?}", t);
        }

        // Missing tiles: same square, other plane, neighbours, fallback misses.
        for t in [(1, 0, 0), (6, 6, 0), (5, 6, 1), (5, 7, 0), (8191, 16383, 2), (-2, 5, 0), (8193, 5, 0), (5, 6, 5)] {
            assert_eq!(m.get(t), None, "{:?}", t);
        }

        // Overwrites keep the count.
        m.insert((5, 6, 0), 9);
        m.insert((-1, 5, 0), 10);
        assert_eq!(m.len(), tiles.len());
        assert_eq!(m.get((5, 6, 0)), Some(9));
        assert_eq!(m.get((-1, 5, 0)), Some(10));
        assert!(m.region_exceptions().is_empty());
    }

    #[test]
    fn mask_store_insert_square_indexing() {
        let masks = square_of(|i| (i * 7 % 251) as u8);
        let mut m = MaskStore::new();
        m.insert((3 * 64 + 5, 7 * 64 + 9, 2), 1); // already present: not counted twice
        m.insert_square(3, 7, &masks);
        assert_eq!(m.len(), SQUARE_TILES);
        for plane in 0..4 {
            for lz in 0..64 {
                for lx in 0..64 {
                    let i = (plane * 4096 + lz * 64 + lx) as usize;
                    assert_eq!(m.get((3 * 64 + lx, 7 * 64 + lz, plane)), Some(masks[i]));
                }
            }
        }
        for t in [(3 * 64 - 1, 7 * 64, 0), (4 * 64, 7 * 64, 0), (3 * 64, 7 * 64 - 1, 0), (3 * 64, 8 * 64, 0), (3 * 64, 7 * 64, 4)] {
            assert_eq!(m.get(t), None, "{:?}", t);
        }

        // Re-inserting a square replaces it.
        let other = square_of(|i| (i % 3) as u8);
        m.insert_square(3, 7, &other);
        assert_eq!(m.len(), SQUARE_TILES);
        assert_eq!(m.get((3 * 64 + 1, 7 * 64, 0)), Some(1));

        // Squares outside the dense grid go through the fallback map.
        m.insert_square(-1, 300, &masks);
        assert_eq!(m.len(), 2 * SQUARE_TILES);
        assert_eq!(m.get((-64 + 5, 300 * 64 + 9, 2)), Some(masks[2 * 4096 + 9 * 64 + 5]));
        assert_eq!(m.get((-64, 300 * 64, 0)), Some(masks[0]));
        assert_eq!(m.get((0, 300 * 64, 0)), None);
        assert_eq!(m.get((-65, 300 * 64, 0)), None);
    }

    fn formula(x: i32, y: i32) -> Value {
        Value::Integer(region_id_for(x, y))
    }

    /// (x, y, plane, walk_mask, last column) of the scenario's `tiles` rows.
    fn scenario_tiles() -> Vec<(i32, i32, i32, Value, Value)> {
        let i = Value::Integer;
        vec![
            (10, 10, 0, i(260), formula(10, 10)), // low 8 bits: right
            (11, 10, 0, i(5), formula(11, 10)),   // left + right
            (12, 10, 0, i(0), formula(12, 10)),   // blocks the step from (11, 10)
            (10, 11, 0, i(2), formula(10, 11)),   // one-way: (10, 10) has no top
            (20, 20, 0, Value::Null, formula(20, 20)),
            (100, 70, 0, i(0), i(999)),           // fairy ring
            (99, 70, 0, i(0), Value::Null),       // west of the ring: gets "right"
            (101, 71, 0, i(0), formula(101, 71)), // north-east of the ring, not reached
            (201, 300, 0, i(140), formula(201, 300)), // top + right + topright
            (201, 301, 0, i(2), formula(201, 301)),
            (202, 300, 0, i(1), formula(202, 300)),
            (202, 301, 0, i(32), formula(202, 301)), // bottomleft without its cardinals
            (-5, 3, 0, i(4), formula(-5, 3)),
            (-4, 3, 0, i(1), formula(-4, 3)),
            (400, 400, 0, i(0), Value::Text("r400".into())),
            (9000, 5, 1, i(0), formula(9000, 5)),
            (50, 50, 5, i(0), formula(50, 50)),
            (1000, 1000, 0, i(255), formula(1000, 1000)), // isolated
        ]
    }

    /// Rows (x, y, plane, walk_mask, last column) the cleaner must write for the
    /// scenario started at (10, 10, 0).
    fn scenario_expected(last: impl Fn(i32, i32) -> Value) -> Vec<Vec<Value>> {
        let rows: [(i32, i32, i32, i64); 14] = [
            (-5, 3, 0, 4),
            (-4, 3, 0, 1),
            (10, 10, 0, 4),
            (11, 10, 0, 1),
            (20, 20, 0, 0),
            (50, 50, 5, 0),
            (99, 70, 0, 4),
            (100, 70, 0, 255),
            (201, 300, 0, 140),
            (201, 301, 0, 2),
            (202, 300, 0, 1),
            (202, 301, 0, 0),
            (400, 400, 0, 0),
            (9000, 5, 1, 0),
        ];
        rows.iter()
            .map(|&(x, y, p, m)| {
                vec![Value::Integer(x as i64), Value::Integer(y as i64), Value::Integer(p as i64), Value::Integer(m), last(x, y)]
            })
            .collect()
    }

    fn scenario_region(x: i32, y: i32) -> Value {
        scenario_tiles().into_iter().find(|r| r.0 == x && r.1 == y).unwrap().4
    }

    /// A source db with the real schema, the scenario tiles and a few teleports.
    /// `tiles_sql` replaces the `tiles` table (the last column is then filled with
    /// the scenario's last-column values as given); `extra_sql` runs at the end.
    fn build_source(dir: &TempDir, name: &str, tiles_sql: Option<&str>, extra_sql: &str) -> PathBuf {
        let path = dir.path().join(name);
        let mut conn = Connection::open(&path).unwrap();
        crate::db::create_tables(&mut conn).unwrap();
        if let Some(sql) = tiles_sql {
            conn.execute_batch(&format!("DROP TABLE tiles; {}", sql)).unwrap();
        }
        let cols = get_table_columns(&conn, "tiles").unwrap();
        let tx = conn.transaction().unwrap();
        {
            let mut ins = tx
                .prepare(&format!("INSERT INTO tiles ({}) VALUES (?1, ?2, ?3, ?4, ?5)", cols.join(", ")))
                .unwrap();
            for (x, y, p, mask, last) in scenario_tiles() {
                ins.execute(params![x, y, p, mask, last]).unwrap();
            }
        }
        tx.execute_batch(
            "INSERT INTO teleports_door_nodes (id, tile_inside_x, tile_inside_y, tile_inside_plane,
                 tile_outside_x, tile_outside_y, tile_outside_plane, requirements)
                 VALUES (1, 11, 10, 0, 20, 20, 0, 'r1');
             INSERT INTO teleports_lodestone_nodes (id, lodestone, dest_x, dest_y, dest_plane)
                 VALUES (1, 'nowhere', 300, 300, 0);
             INSERT INTO teleports_object_nodes (id, orig_min_x, orig_max_x, orig_min_y, orig_max_y, orig_plane,
                 dest_min_x, dest_max_x, dest_min_y, dest_max_y, dest_plane)
                 VALUES (1, 10, 12, 10, 10, 0, -6, -4, 3, 3, 0);
             INSERT INTO teleports_npc_nodes (id, orig_min_x, orig_max_x, orig_min_y, orig_max_y, orig_plane,
                 dest_min_x, dest_max_x, dest_min_y, dest_max_y, dest_plane)
                 VALUES (1, 10, 10, 10, 10, 0, 400, 400, 400, 400, 0);
             INSERT INTO teleports_item_nodes (id, dest_min_x, dest_max_x, dest_min_y, dest_max_y, dest_plane)
                 VALUES (1, 200, 202, 300, 300, 0);
             INSERT INTO teleports_POA_nodes (id, dest_min_x, dest_max_x, dest_min_y, dest_max_y, dest_plane)
                 VALUES (1, 9000, 9000, 5, 5, 1);
             INSERT INTO teleports_ifslot_nodes (id, dest_min_x, dest_max_x, dest_min_y, dest_max_y, dest_plane)
                 VALUES (1, 50, 50, 50, 50, 5);
             INSERT INTO teleports_fairy_rings_nodes (id, x, y, plane, code) VALUES (1, 100, 70, 0, 'AAA');
             -- no origin: skipped, so (1000, 1000, 0) stays unreachable
             INSERT INTO teleports_useOn_nodes (id, dest_min_x, dest_max_x, dest_min_y, dest_max_y, dest_plane)
                 VALUES (1, 1000, 1000, 1000, 1000, 0);
             INSERT INTO teleports_requirements (id, metaInfo, key, value, comparison)
                 VALUES (1, 'm', 'k', 2.5, '>=');",
        )
        .unwrap();
        tx.execute_batch(extra_sql).unwrap();
        tx.commit().unwrap();
        path
    }

    fn query_rows(conn: &Connection, sql: &str) -> Vec<Vec<Value>> {
        let mut stmt = conn.prepare(sql).unwrap();
        let n = stmt.column_count();
        let mut rows = stmt.query([]).unwrap();
        let mut out = Vec::new();
        while let Some(r) = rows.next().unwrap() {
            out.push(read_row_values(r, n).unwrap());
        }
        out
    }

    fn master(conn: &Connection) -> Vec<(String, String)> {
        query_rows(conn, "SELECT type, name FROM sqlite_master ORDER BY rowid")
            .into_iter()
            .map(|r| match (&r[0], &r[1]) {
                (Value::Text(t), Value::Text(n)) => (t.clone(), n.clone()),
                other => panic!("{:?}", other),
            })
            .collect()
    }

    /// sqlite_master of the output for the `create_tables` schema, after `tiles` (and
    /// its indexes): each table followed by its index, then the view.
    fn expected_master_rest() -> Vec<(String, String)> {
        [
            ("table", "teleports_door_nodes"),
            ("index", "idx_tdoor_req"),
            ("table", "teleports_ifslot_nodes"),
            ("index", "idx_tif_req"),
            ("table", "teleports_item_nodes"),
            ("index", "idx_titem_req"),
            ("table", "teleports_lodestone_nodes"),
            ("index", "idx_tlode_req"),
            ("table", "teleports_npc_nodes"),
            ("index", "idx_tnpc_req"),
            ("table", "teleports_object_nodes"),
            ("index", "idx_tobj_req"),
            ("table", "teleports_fairy_rings_nodes"),
            ("index", "idx_tfairy_req"),
            ("table", "teleports_POA_nodes"),
            ("index", "idx_tpoa_req"),
            ("table", "teleports_useOn_nodes"),
            ("index", "idx_tuseon_req"),
            ("table", "teleports_requirements"),
            ("index", "idx_teleport_req_all"),
            ("view", "teleports_all"),
        ]
        .iter()
        .map(|&(t, n)| (t.to_string(), n.to_string()))
        .collect()
    }

    /// Runs the cleaner on `src_path` from (10, 10, 0) and returns the output connection.
    fn clean(src_path: &Path, out_path: &Path) -> Connection {
        let before = fs::read(src_path).unwrap();
        let masks = MaskStore::load_from_db(src_path).unwrap();
        let src = open_read_only(src_path).unwrap();
        run_cleaner(&src, &masks, out_path, (10, 10, 0)).unwrap();
        drop(src);
        assert_eq!(fs::read(src_path).unwrap(), before, "source db was modified");
        let out = Connection::open(out_path).unwrap();
        let check: String = out.query_row("PRAGMA integrity_check", [], |r| r.get(0)).unwrap();
        assert_eq!(check, "ok");
        let page_size: i64 = out.query_row("PRAGMA page_size", [], |r| r.get(0)).unwrap();
        assert_eq!(page_size, 4096);
        out
    }

    /// Every non-tile table of the output has the source's rows, storage classes included.
    fn assert_rest_copied(src_path: &Path, out: &Connection) {
        let src = open_read_only(src_path).unwrap();
        for (typ, name) in expected_master_rest() {
            let sql = format!("SELECT * FROM {} ORDER BY 1, 2", name);
            if typ != "index" {
                assert_eq!(query_rows(out, &sql), query_rows(&src, &sql), "{}", name);
            }
        }
        let sql_of = |conn: &Connection| {
            query_rows(conn, "SELECT type, name, tbl_name, sql FROM sqlite_master WHERE name != 'tiles' ORDER BY name")
        };
        let src_rest: Vec<_> = sql_of(&src).into_iter().filter(|r| r[2] != Value::Text("tiles".into())).collect();
        let out_rest: Vec<_> = sql_of(out).into_iter().filter(|r| r[2] != Value::Text("tiles".into())).collect();
        assert_eq!(out_rest, src_rest);
    }

    #[test]
    fn load_from_db_reads_every_row() {
        let dir = TempDir::new().unwrap();
        let path = build_source(&dir, "src.db", None, "");
        // Several threads with narrow tasks, so squares are split between threads and merged.
        for store in [MaskStore::load_from_db(&path).unwrap(), MaskStore::load_from_db_with(&path, 3, 16).unwrap()] {
            let tiles = scenario_tiles();
            assert_eq!(store.len(), tiles.len());
            for (x, y, p, mask, region) in tiles {
                let expected = match mask {
                    Value::Integer(v) => v as u8,
                    _ => 0,
                };
                assert_eq!(store.get((x, y, p)), Some(expected), "({}, {}, {})", x, y, p);
                assert_eq!(store.region_exceptions().get(&(x, y, p)).is_some(), region != formula(x, y));
                if let Some(v) = store.region_exceptions().get(&(x, y, p)) {
                    assert_eq!(*v, region);
                }
            }
            assert_eq!(store.region_exceptions().len(), 3);
            assert_eq!(store.get((300, 300, 0)), None);
            assert_eq!(store.get((12, 11, 0)), None);
        }
    }

    #[test]
    fn load_from_db_rejects_bad_rows() {
        let dir = TempDir::new().unwrap();
        let cases = [
            ("INSERT INTO tiles VALUES ('abc', 1, 0, 0, 0)", "tiles.x"),
            ("INSERT INTO tiles VALUES (70.5, 1, 0, 0, 0)", "tiles.x"),
            ("INSERT INTO tiles VALUES (-3.5, 1, 0, 0, 0)", "tiles.x"),
            ("INSERT INTO tiles VALUES (1099511627776, 1, 0, 0, 0)", "does not fit"),
            ("INSERT INTO tiles VALUES (7, 'y', 0, 0, 0)", "tiles.y"),
            ("INSERT INTO tiles VALUES (7, 1, 1.5, 0, 0)", "tiles.plane"),
            ("INSERT INTO tiles VALUES (7, 1, 0, 'open', 0)", "walk_mask"),
            ("INSERT INTO tiles VALUES (7, 1, 0, 2.5, 0)", "walk_mask"),
        ];
        for (i, (sql, needle)) in cases.iter().enumerate() {
            let path = build_source(&dir, &format!("bad{}.db", i), None, sql);
            let err = match MaskStore::load_from_db(&path) {
                Ok(_) => panic!("{} loaded without error", sql),
                Err(e) => format!("{:#}", e),
            };
            assert!(err.contains(needle), "{}: {}", sql, err);
        }
    }

    #[test]
    fn run_cleaner_end_to_end() {
        let dir = TempDir::new().unwrap();
        let src_path = build_source(&dir, "src.db", None, "");
        let out_path = dir.path().join("out.db");
        fs::write(&out_path, b"stale output").unwrap();
        let out = clean(&src_path, &out_path);

        let rows = query_rows(&out, "SELECT x, y, plane, walk_mask, RegionID FROM tiles ORDER BY x, y, plane");
        assert_eq!(rows, scenario_expected(scenario_region));

        let mut expected_master = vec![("table".to_string(), "tiles".to_string())];
        expected_master.extend(expected_master_rest());
        assert_eq!(master(&out), expected_master);
        let tiles_sql: String = out
            .query_row("SELECT sql FROM sqlite_master WHERE name = 'tiles'", [], |r| r.get(0))
            .unwrap();
        assert_eq!(tiles_sql, get_create_table_sql(&open_read_only(&src_path).unwrap(), "tiles").unwrap());
        assert_rest_copied(&src_path, &out);
    }

    #[test]
    fn run_cleaner_keeps_tiles_indexes_in_order() {
        let dir = TempDir::new().unwrap();
        let src_path = build_source(
            &dir,
            "src.db",
            None,
            "CREATE INDEX idx_tiles_region ON tiles(RegionID); CREATE INDEX idx_tiles_mask ON tiles(walk_mask, plane);",
        );
        let out_path = dir.path().join("out.db");
        let out = clean(&src_path, &out_path);

        let rows = query_rows(&out, "SELECT x, y, plane, walk_mask, RegionID FROM tiles ORDER BY x, y, plane");
        assert_eq!(rows, scenario_expected(scenario_region));
        let mut expected_master = vec![
            ("table".to_string(), "tiles".to_string()),
            ("index".to_string(), "idx_tiles_region".to_string()),
            ("index".to_string(), "idx_tiles_mask".to_string()),
        ];
        expected_master.extend(expected_master_rest());
        assert_eq!(master(&out), expected_master);
        let by_index: i64 = out
            .query_row("SELECT count(*) FROM tiles INDEXED BY idx_tiles_mask WHERE walk_mask = 0", [], |r| r.get(0))
            .unwrap();
        assert_eq!(by_index, 5);
        assert_rest_copied(&src_path, &out);
    }

    #[test]
    fn run_cleaner_generic_columns() {
        let dir = TempDir::new().unwrap();
        let src_path = build_source(
            &dir,
            "src.db",
            Some(
                "CREATE TABLE tiles (x INTEGER, y INTEGER, plane INTEGER, walk_mask INTEGER, note,
                 PRIMARY KEY (x, y, plane)) WITHOUT ROWID;",
            ),
            "",
        );
        let out_path = dir.path().join("out.db");
        let out = clean(&src_path, &out_path);
        let rows = query_rows(&out, "SELECT x, y, plane, walk_mask, note FROM tiles ORDER BY x, y, plane");
        assert_eq!(rows, scenario_expected(scenario_region));
        let cols = get_table_columns(&out, "tiles").unwrap();
        assert_eq!(cols, ["x", "y", "plane", "walk_mask", "note"]);
        assert_rest_copied(&src_path, &out);
    }
}
