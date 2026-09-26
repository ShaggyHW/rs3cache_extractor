//! `walkflags` — the rust replacement for
//! `node dist/cli walkflags -o cache:<dir> -s <out> --startx .. --startz ..`
//!
//! Can write the json files the node script produced, insert straight into
//! `tiles.db`, or both. Writing to the db skips ~17GB of json that `load-tiles`
//! would only read back to keep one byte per tile.

use anyhow::{bail, Context, Result};
use rayon::prelude::*;
use rusqlite::Connection;
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::collections::VecDeque;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Instant;

use crate::cache::source::{CacheSource, MAJOR_MAPSQUARES, MAJOR_OBJECTS, OBJECTS_PER_ARCHIVE};
use crate::commands::load_tiles::{read_overrides_file, region_id};
use crate::sqlite_btree::{
    encode_int_cell, ensure_replaceable, serialize_db, table_root_page, FileSink, PackedSegment,
    SinkStats, TreeStats, TreeWriter,
};
use crate::walk::diagnostics::{
    Diagnostics, KIND_CACHE, KIND_LOC_DROPPED, KIND_MAPSQUARE, KIND_OBJECT, KIND_OPCODE,
    KIND_TRUNCATED, KIND_WRITE,
};
use crate::walk::export::{collect_masks, mask_index, render_chunk, SQUARE_TILES};
use crate::walk::grid::{build_grid, Grid};
use crate::walk::mapsquare::{load_square, SquareData, CHUNK_SIZE, SQUARE_LEVELS};
use crate::walk::objdef::{
    parse_object, resolve_defs, DefStatus, ObjectDef, OpcodeIssue, RawObject,
};

type SquareCache = HashMap<(i32, i32), Option<Arc<SquareData>>>;

/// The walk masks of one mapsquare, indexed by [`mask_index`].
pub struct ChunkMasks {
    pub cz: i32,
    pub masks: Box<[u8; SQUARE_TILES]>,
    /// Indices of tiles that do not exist, ascending. A mapsquare contributes
    /// all of its tiles or none, so this is empty in practice.
    pub missing: Vec<u16>,
}

impl ChunkMasks {
    #[inline]
    pub fn exists(&self, idx: usize) -> bool {
        self.missing.is_empty() || self.missing.binary_search(&(idx as u16)).is_err()
    }
}

/// Every existing mapsquare of one column, in ascending `cz`.
pub struct ColumnMasks {
    pub cx: i32,
    pub chunks: Vec<ChunkMasks>,
}

#[derive(Default)]
struct ChunkOutput {
    masks: Option<ChunkMasks>,
    /// Loc ids skipped because no usable definition could be decoded.
    dropped_locs: Vec<u32>,
}

pub struct WalkflagsOpts<'a> {
    pub cache_dir: &'a Path,
    /// Write `<save>/walk/<x>-<z>.json`, as the node script did.
    pub save_dir: Option<&'a Path>,
    /// Write the tiles straight into this sqlite db.
    pub db: Option<&'a Path>,
    /// Overrides merged into the tiles; db output only.
    pub overrides: Option<&'a Path>,
    /// Where the full diagnostics log goes; `None` means console only.
    pub log: Option<&'a Path>,
    /// Wait until the db is on disk before returning.
    pub fsync: bool,
    pub area: Area,
}

/// Which mapsquares to extract.
#[derive(Clone, Copy, Debug)]
pub struct Area {
    pub startx: i32,
    pub startz: i32,
    pub sizex: i32,
    pub sizez: i32,
}

pub fn cmd_walkflags(opts: &WalkflagsOpts) -> Result<()> {
    if opts.save_dir.is_none() && opts.db.is_none() {
        bail!("nothing to do: pass --save for json output, --db for sqlite output, or both");
    }
    if opts.overrides.is_some() && opts.db.is_none() {
        bail!("--overrides only applies to --db output");
    }

    let started = Instant::now();
    let diag = Diagnostics::new(opts.log)?;
    let source = CacheSource::new(opts.cache_dir)?;

    let walk_dir = match opts.save_dir {
        Some(save) => {
            let dir = save.join("walk");
            fs::create_dir_all(&dir)
                .with_context(|| format!("creating output directory {}", dir.display()))?;
            println!("json output: {}", dir.display());
            Some(dir)
        }
        None => None,
    };

    let writer = match opts.db {
        Some(db) => {
            println!("db output  : {}", db.display());
            let overrides = match opts.overrides {
                Some(path) => read_overrides_file(path)?,
                None => Vec::new(),
            };
            Some(TilesWriter::create(db, &overrides)?)
        }
        None => None,
    };

    println!("cache      : {}", opts.cache_dir.display());
    if let Some(path) = diag.log_path() {
        println!("log        : {}", path);
    }

    let report = extract(
        &source,
        &diag,
        opts.area,
        walk_dir.as_deref(),
        writer.is_some(),
        |column| match &writer {
            Some(writer) => writer.send(column),
            None => Ok(()),
        },
    )?;

    let mut t_write = report.writer_wait;
    if let Some(writer) = writer {
        let t = Instant::now();
        let (stats, sink) = writer.finish()?;
        t_write += t.elapsed().as_secs_f64();
        report_tree(opts.db.unwrap(), &stats, &sink);
        if opts.fsync {
            t_write += fsync_path(opts.db.unwrap())?;
        }
    }

    println!(
        "Export complete: {} squares processed, {} tiles, {} errors, {:.1}s",
        report.squares,
        report.tiles,
        report.errors,
        started.elapsed().as_secs_f64()
    );
    // "db write" is wall time spent blocked on the writer thread plus its final
    // drain; the writing itself overlaps the decoding.
    println!(
        "phases: square load {:.1}s, chunk work {:.1}s, db write {:.1}s",
        report.t_load, report.t_work, t_write
    );
    diag.finish()?;
    Ok(())
}

/// Totals and timings of one [`extract`] run.
pub struct ExtractReport {
    pub squares: usize,
    pub tiles: usize,
    pub errors: usize,
    pub t_load: f64,
    pub t_work: f64,
    /// Time `on_column` spent blocked, i.e. writer backpressure.
    pub writer_wait: f64,
}

/// Decodes every mapsquare of `area`, one column at a time in ascending x.
///
/// With `want_masks`, each finished column's walk masks go to `on_column`, in
/// order. With `walk_dir`, every mapsquare is also written out as json.
pub fn extract(
    source: &CacheSource,
    diag: &Diagnostics,
    area: Area,
    walk_dir: Option<&Path>,
    want_masks: bool,
    mut on_column: impl FnMut(ColumnMasks) -> Result<()>,
) -> Result<ExtractReport> {
    let started = Instant::now();

    // The mapsquares are needed in random order below. Pulling the whole table
    // into memory first, alongside decoding the location definitions, turns
    // that into one sequential read that the output writes cannot slow down.
    let (defs, def_statuses) = std::thread::scope(|scope| {
        let warm = scope.spawn(|| source.open_table(MAJOR_MAPSQUARES)?.preload());
        let defs = load_object_defs(source, diag);
        warm.join().expect("cache prewarm thread panicked")?;
        defs
    })?;
    println!(
        "loaded {} location definitions in {:.1}s",
        defs.iter().filter(|q| q.is_some()).count(),
        started.elapsed().as_secs_f64()
    );

    let processed = AtomicUsize::new(0);
    let errors = AtomicUsize::new(0);
    let tiles_total = AtomicUsize::new(0);

    // A chunk needs its own mapsquare plus the eight neighbours, so squares are
    // kept for a sliding window of three columns instead of the whole world.
    let mut cache: SquareCache = HashMap::new();
    let mut dropped_loc_ids: HashMap<u32, usize> = HashMap::new();
    let (mut t_load, mut t_work, mut t_wait) = (0f64, 0f64, 0f64);
    let mut last_report = Instant::now();

    for cx in area.startx..area.startx + area.sizex {
        let t0 = Instant::now();
        // Chunks need the columns on either side. Loading a column at a time
        // is too little work to spread over many cores, so the columns ahead
        // are loaded in batches.
        let last = area.startx + area.sizex; // the padding column after the area
        let ahead = (cx + 1 + LOAD_AHEAD).min(last + 1).max(cx + 2);
        load_columns(
            source,
            &mut cache,
            cx - 1..ahead,
            area.startz - 1,
            area.startz + area.sizez + 1,
            &errors,
            diag,
        );
        cache.retain(|(kx, _), _| *kx >= cx - 1);
        t_load += t0.elapsed().as_secs_f64();
        let t1 = Instant::now();

        let column_out: Vec<ChunkOutput> = (area.startz..area.startz + area.sizez)
            .into_par_iter()
            .map_init(Grid::new, |grid, cz| {
                let square = match cache.get(&(cx, cz)).and_then(|q| q.as_ref()) {
                    Some(square) => square.clone(),
                    None => return ChunkOutput::default(),
                };
                let mut neighbours = Vec::with_capacity(9);
                for nz in cz - 1..=cz + 1 {
                    for nx in cx - 1..=cx + 1 {
                        if let Some(Some(neighbour)) = cache.get(&(nx, nz)) {
                            neighbours.push((nx, nz, neighbour.clone()));
                        }
                    }
                }
                build_grid(grid, cx, cz, &neighbours);
                let mut dropped_locs = Vec::new();
                grid.apply_locs(
                    &square.locs,
                    cx * CHUNK_SIZE,
                    cz * CHUNK_SIZE,
                    &defs,
                    &mut dropped_locs,
                );

                let mut ok = true;
                if let Some(walk_dir) = walk_dir {
                    let (json, tiles) = render_chunk(grid, cx, cz);
                    let path = walk_dir.join(format!("{}-{}.json", cx, cz));
                    match write_atomic(&path, json.as_bytes()) {
                        Ok(()) => {
                            // When masks are collected too, they carry the count.
                            if !want_masks {
                                tiles_total.fetch_add(tiles, Ordering::Relaxed);
                            }
                        }
                        Err(e) => {
                            diag.warn(
                                KIND_WRITE,
                                format!("mapsquare {},{} -> {}: {:#}", cx, cz, path.display(), e),
                            );
                            errors.fetch_add(1, Ordering::Relaxed);
                            ok = false;
                        }
                    }
                }

                if ok {
                    processed.fetch_add(1, Ordering::Relaxed);
                }

                let masks = if want_masks {
                    let mut masks = Box::new([0u8; SQUARE_TILES]);
                    let mut missing = Vec::new();
                    collect_masks(grid, cx, cz, &mut masks, &mut missing);
                    tiles_total.fetch_add(SQUARE_TILES - missing.len(), Ordering::Relaxed);
                    Some(ChunkMasks { cz, masks, missing })
                } else {
                    None
                };
                ChunkOutput { masks, dropped_locs }
            })
            .collect();

        t_work += t1.elapsed().as_secs_f64();

        let mut dropped_this_column = 0usize;
        let mut chunks = Vec::new();
        for out in column_out {
            for id in &out.dropped_locs {
                *dropped_loc_ids.entry(*id).or_insert(0usize) += 1;
                dropped_this_column += 1;
            }
            chunks.extend(out.masks);
        }
        diag.count(KIND_LOC_DROPPED, dropped_this_column);

        if want_masks {
            // Blocks only once the writer is several columns behind.
            let t2 = Instant::now();
            on_column(ColumnMasks { cx, chunks })
                .with_context(|| format!("handing over tiles of column {}", cx))?;
            t_wait += t2.elapsed().as_secs_f64();
        }

        // One progress line per second is plenty; a line per column scrolls the
        // diagnostics away.
        if last_report.elapsed().as_secs_f64() >= 1.0 || cx + 1 == area.startx + area.sizex {
            last_report = Instant::now();
            println!(
                "column {} done, {} squares, {:.1}s elapsed",
                cx,
                processed.load(Ordering::Relaxed),
                started.elapsed().as_secs_f64()
            );
        }
    }

    if !dropped_loc_ids.is_empty() {
        let mut worst: Vec<(u32, usize)> =
            dropped_loc_ids.iter().map(|(id, n)| (*id, *n)).collect();
        worst.sort_unstable_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        let total: usize = worst.iter().map(|q| q.1).sum();
        println!(
            "{} loc placements across {} distinct ids were skipped for lack of a \
             usable definition; worst offenders:",
            total,
            worst.len()
        );
        for (id, n) in worst.iter().take(10) {
            let reason = def_statuses
                .get(*id as usize)
                .copied()
                .unwrap_or(DefStatus::Absent)
                .describe();
            println!("  loc {:<8} {:>6} placements  ({})", id, n, reason);
        }
        // The full list, with reasons, always goes to the log.
        for (id, n) in &worst {
            let reason = def_statuses
                .get(*id as usize)
                .copied()
                .unwrap_or(DefStatus::Absent)
                .describe();
            diag.note(format!("loc {} skipped {} times: {}", id, n, reason));
        }
    }

    Ok(ExtractReport {
        squares: processed.load(Ordering::Relaxed),
        tiles: tiles_total.load(Ordering::Relaxed),
        errors: errors.load(Ordering::Relaxed),
        t_load,
        t_work,
        writer_wait: t_wait,
    })
}

/// Threads encoding, packing and comparing `tiles.db` pages, next to the
/// extraction's own.
const WRITER_THREADS: usize = 12;

/// Mapsquare columns decoded ahead of the one being processed.
const LOAD_AHEAD: i32 = 8;

/// Columns being encoded and cut into leaves at once, ahead of the one being
/// written out.
const PACK_AHEAD: usize = 6;

/// Page size of `tiles.db`. Larger pages mean fewer interior pages and less
/// per-page overhead for the 84.75M one-byte rows.
pub const TILES_PAGE_SIZE: usize = 16384;

/// Writes `tiles.db` on its own thread while the extraction runs.
///
/// The rows never go through sqlite's insert path: columns arrive in ascending
/// x, and walking each column x-major over its mapsquares' mask arrays yields
/// the rows in exact `(x, y, plane)` primary key order, which [`TreeWriter`]
/// packs straight into b-tree pages. Overrides are merged into that same
/// ordered stream, so the table comes out exactly as the old
/// insert-then-upsert sequence left it.
///
/// An existing `tiles.db` is updated in place and only pages that changed are
/// written. Every column starts a new leaf, so a changed mapsquare or override
/// only rewrites its own column's pages.
pub struct TilesWriter {
    tx: Option<SyncSender<ColumnMasks>>,
    handle: Option<JoinHandle<Result<(TreeStats, SinkStats)>>>,
}

impl TilesWriter {
    /// `db` must not exist, be empty, or be a `tiles.db` with the current
    /// schema (which is then rebuilt in place). `overrides` are
    /// `[x, y, plane, walk_mask]` in file order; a later line for the same tile
    /// wins, as with the sequential upserts they replace.
    pub fn create(db: &Path, overrides: &[[i64; 4]]) -> Result<Self> {
        let schema = tiles_schema()?;
        ensure_replaceable(db, &schema)?;
        let image = serialize_db(&schema)?;
        let root = table_root_page(&schema, "tiles")?;
        drop(schema);
        let sink = FileSink::create_or_replace(db, TILES_PAGE_SIZE)?;
        let mut tree = TreeWriter::new(&image, root, 3, sink)?;
        let mut overrides = Overrides::new(overrides);
        // Columns are ~40 mapsquares of 16 KB each, so a deep queue is cheap and
        // keeps the decoder from stalling on a momentarily slow write.
        let (tx, rx) = sync_channel::<ColumnMasks>(8);
        // The writer's tasks run on their own threads. In the shared pool its
        // long column tasks queue up in front of the extraction's short
        // parallel loops and stall them.
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(WRITER_THREADS)
            .thread_name(|i| format!("tiles-pack-{}", i))
            .build()
            .context("starting the tiles writer's thread pool")?;
        let handle = std::thread::Builder::new()
            .name("tiles-writer".into())
            .spawn(move || pool.install(|| -> Result<(TreeStats, SinkStats)> {
                // Columns are encoded and cut into leaves as independent
                // tasks, several at once; this thread only appends the
                // finished pages, in column order.
                let packer = tree.packer();
                type InFlight = VecDeque<Receiver<Result<PackedSegment>>>;
                let mut in_flight = InFlight::new();
                let drain = |tree: &mut TreeWriter<FileSink>, in_flight: &mut InFlight, keep: usize| {
                    while in_flight.len() > keep {
                        let segment = in_flight
                            .pop_front()
                            .unwrap()
                            .recv()
                            .map_err(|_| anyhow::anyhow!("a column packing task died"))??;
                        tree.push_segment(segment)?;
                    }
                    Ok::<(), anyhow::Error>(())
                };
                for column in rx {
                    let cx = column.cx as i64;
                    if overrides.has_columns_before(cx) {
                        drain(&mut tree, &mut in_flight, 0)?;
                        overrides.write_columns_before(&mut tree, Some(cx))?;
                    }
                    let column_overrides = overrides.take_column(cx).to_vec();
                    let (done, result) = sync_channel(1);
                    rayon::spawn(move || {
                        let parts: Vec<(Vec<u8>, Vec<u32>)> = (0..CHUNK_SIZE as usize)
                            .into_par_iter()
                            .map(|lx| encode_x(&column, lx, &column_overrides, packer.format4()))
                            .collect();
                        let _ = done.send(packer.pack(parts));
                    });
                    in_flight.push_back(result);
                    drain(&mut tree, &mut in_flight, PACK_AHEAD)?;
                }
                drain(&mut tree, &mut in_flight, 0)?;
                overrides.write_columns_before(&mut tree, None)?;
                let (stats, sink) = tree.finish()?;
                Ok((stats, sink.stats()))
            }))
            .context("spawning the tiles writer thread")?;
        Ok(TilesWriter { tx: Some(tx), handle: Some(handle) })
    }

    /// Blocks while the writer is behind. A send error means the writer died;
    /// the real cause surfaces from `finish`.
    pub fn send(&self, column: ColumnMasks) -> Result<()> {
        match self.tx.as_ref().unwrap().send(column) {
            Ok(()) => Ok(()),
            Err(_) => bail!("the tiles writer stopped accepting rows"),
        }
    }

    /// Writes the remaining rows and the interior pages. The data is handed to
    /// the kernel but not fsynced.
    pub fn finish(mut self) -> Result<(TreeStats, SinkStats)> {
        drop(self.tx.take());
        match self.handle.take().unwrap().join() {
            Ok(result) => result,
            Err(_) => bail!("the tiles writer thread panicked"),
        }
    }
}

impl Drop for TilesWriter {
    /// A writer dropped on an error path still drains and stops its thread,
    /// so the process never exits while it is halfway through a page.
    fn drop(&mut self) {
        drop(self.tx.take());
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

/// The full `tiles.db` schema, built in memory with the bulk-build page size.
pub fn tiles_schema() -> Result<Connection> {
    let mut conn = Connection::open_in_memory()?;
    // page_size has to be set before the first table exists.
    conn.execute_batch(&format!("PRAGMA page_size={};", TILES_PAGE_SIZE))?;
    crate::db::create_tables(&mut conn)?;
    Ok(conn)
}

/// Encodes the rows of one x of a column, in primary key order, with that
/// x's overrides merged in: every mapsquare of the column top to bottom, each
/// tile's planes in order. Returns the cells back to back and where each ends.
fn encode_x(
    column: &ColumnMasks,
    lx: usize,
    overrides: &[([i64; 3], i64)],
    format4: bool,
) -> (Vec<u8>, Vec<u32>) {
    let x = column.cx as i64 * CHUNK_SIZE as i64 + lx as i64;
    let tiles = column.chunks.len() * CHUNK_SIZE as usize * SQUARE_LEVELS;
    let mut cells = Vec::with_capacity(tiles * 16);
    let mut ends = Vec::with_capacity(tiles);
    // This x's overrides, still in key order.
    let first = overrides.partition_point(|(k, _)| k[0] < x);
    let last = overrides.partition_point(|(k, _)| k[0] <= x);
    let mut pending = overrides[first..last].iter().peekable();
    let mut emit = |cells: &mut Vec<u8>, values: &[i64; 5]| {
        encode_int_cell(values, format4, cells);
        ends.push(cells.len() as u32);
    };

    for chunk in &column.chunks {
        let y0 = chunk.cz as i64 * CHUNK_SIZE as i64;
        let region = region_id(x, y0);
        for lz in 0..CHUNK_SIZE as usize {
            let y = y0 + lz as i64;
            for plane in 0..SQUARE_LEVELS {
                let idx = mask_index(plane, lx, lz);
                if !chunk.exists(idx) {
                    continue;
                }
                let key = [x, y, plane as i64];
                let mut replaced = false;
                while let Some(&&(okey, mask)) = pending.peek() {
                    if okey > key {
                        break;
                    }
                    emit(&mut cells, &[okey[0], okey[1], okey[2], mask, region_id(okey[0], okey[1])]);
                    pending.next();
                    replaced |= okey == key;
                }
                if !replaced {
                    emit(&mut cells, &[x, y, plane as i64, chunk.masks[idx] as i64, region]);
                }
            }
        }
    }
    for &(okey, mask) in pending {
        emit(&mut cells, &[okey[0], okey[1], okey[2], mask, region_id(okey[0], okey[1])]);
    }
    (cells, ends)
}

/// The overrides, deduplicated (last line wins) and sorted by key, handed out
/// one mapsquare column (`x >> 6`) at a time.
struct Overrides {
    rows: Vec<([i64; 3], i64)>,
    next: usize,
}

impl Overrides {
    fn new(lines: &[[i64; 4]]) -> Self {
        let mut map = std::collections::BTreeMap::new();
        for &[x, y, plane, mask] in lines {
            map.insert([x, y, plane], mask);
        }
        Overrides { rows: map.into_iter().collect(), next: 0 }
    }

    /// Whether overrides of a column before `cx` are still waiting.
    fn has_columns_before(&self, cx: i64) -> bool {
        self.rows.get(self.next).is_some_and(|(key, _)| key[0] >> 6 < cx)
    }

    /// The overrides of column `cx`, which must not be behind the cursor.
    fn take_column(&mut self, cx: i64) -> &[([i64; 3], i64)] {
        let start = self.next;
        while self.next < self.rows.len() && self.rows[self.next].0[0] >> 6 == cx {
            self.next += 1;
        }
        &self.rows[start..self.next]
    }

    /// Writes the overrides of every column before `cx` (all of them for
    /// `None`) that has no mapsquares of its own, each column as its own
    /// segment.
    fn write_columns_before(&mut self, tree: &mut TreeWriter<FileSink>, cx: Option<i64>) -> Result<()> {
        while let Some(&(key, _)) = self.rows.get(self.next) {
            let column = key[0] >> 6;
            if cx.is_some_and(|cx| column >= cx) {
                break;
            }
            tree.break_leaf()?;
            for &(key, mask) in self.take_column(column) {
                tree.push_ints(&[key[0], key[1], key[2], mask, region_id(key[0], key[1])])?;
            }
        }
        Ok(())
    }
}

pub fn report_tree(db: &Path, stats: &TreeStats, sink: &SinkStats) {
    let total = sink.pages_written + sink.pages_unchanged;
    println!(
        "{}: {} tiles, {} leaf + {} interior pages (depth {}), {:.2} GB; {} of {} pages \
         changed, {:.1} MB written (compare {:.1}s, write {:.1}s)",
        db.display(),
        stats.rows,
        stats.leaf_pages,
        stats.interior_pages,
        stats.depth,
        stats.db_bytes as f64 / 1e9,
        sink.pages_written,
        total,
        sink.bytes_written as f64 / 1e6,
        sink.secs_compare,
        sink.secs_write
    );
    if sink.bytes_written > 0 {
        println!(
            "  the written pages are in the page cache; the kernel writes them back in the \
             background (--fsync waits for that)"
        );
    }
}

/// fsyncs a file and returns how long it took. The databases are written
/// without waiting for the disk; this is the explicit wait.
pub fn fsync_path(path: &Path) -> Result<f64> {
    let bytes = fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    let t = Instant::now();
    fs::File::open(path)
        .and_then(|f| f.sync_all())
        .with_context(|| format!("flushing {} to disk", path.display()))?;
    let secs = t.elapsed().as_secs_f64();
    println!(
        "fsync {}: {:.1}s ({:.0} MB/s over {:.2} GB)",
        path.display(),
        secs,
        if secs > 0.0 { bytes as f64 / 1e6 / secs } else { 0.0 },
        bytes as f64 / 1e9
    );
    Ok(secs)
}

/// Loads every mapsquare of `columns` not already in `cache`, all at once.
fn load_columns(
    source: &CacheSource,
    cache: &mut SquareCache,
    columns: std::ops::Range<i32>,
    zstart: i32,
    zend: i32,
    errors: &AtomicUsize,
    diag: &Diagnostics,
) {
    let missing: Vec<(i32, i32)> = columns
        .flat_map(|cx| (zstart..zend).map(move |cz| (cx, cz)))
        .filter(|key| !cache.contains_key(key))
        .collect();
    if missing.is_empty() {
        return;
    }
    let loaded: Vec<((i32, i32), Option<Arc<SquareData>>)> = missing
        .into_par_iter()
        .map(|(cx, cz)| match load_square(source, cx, cz) {
            Ok(square) => ((cx, cz), square),
            Err(e) => {
                diag.warn(
                    KIND_MAPSQUARE,
                    format!("mapsquare {},{} could not be decoded: {:#}", cx, cz, e),
                );
                errors.fetch_add(1, Ordering::Relaxed);
                ((cx, cz), None)
            }
        })
        .collect();
    cache.extend(loaded);
}

/// Decodes every location definition up front. The whole `js5-16` index is a
/// couple of megabytes, and having it resolved removes all locking from the
/// per-chunk work.
pub(crate) fn load_object_defs(
    source: &CacheSource,
    diag: &Diagnostics,
) -> Result<(Vec<Option<ObjectDef>>, Vec<DefStatus>)> {
    let table = source.open_table(MAJOR_OBJECTS)?;
    let minors: Vec<u32> = table
        .index
        .iter()
        .filter_map(|q| q.as_ref().map(|q| q.minor))
        .collect();

    let raw: std::sync::Mutex<Vec<Option<RawObject>>> = std::sync::Mutex::new(Vec::new());
    let statuses: std::sync::Mutex<Vec<DefStatus>> = std::sync::Mutex::new(Vec::new());
    minors.par_iter().try_for_each(|&minor| -> Result<()> {
        let (archive, ranges) = match table.archive(minor)? {
            Some(v) => v,
            None => {
                diag.warn(
                    KIND_CACHE,
                    format!("object archive {} is listed in the index but has no data", minor),
                );
                return Ok(());
            }
        };
        let mut parsed = Vec::with_capacity(ranges.len());
        for range in &ranges {
            let id = minor * OBJECTS_PER_ARCHIVE + range.fileid;
            let result = parse_object(&archive[range.start..range.end]);
            // Report what went wrong on the way in before the failure itself:
            // an unknown opcode desyncs the stream and is usually the cause.
            for issue in &result.issues {
                match issue {
                    OpcodeIssue::Unknown { opcode, position } => diag.warn(
                        KIND_OPCODE,
                        format!(
                            "object {}: unknown chunk 0x{:02X} at position {}, \
                             skipping one byte (later fields may be misread)",
                            id, opcode, position
                        ),
                    ),
                    OpcodeIssue::MissingTerminator => diag.warn(
                        KIND_TRUNCATED,
                        format!("object {}: reached end of file without a 0x00 opcode", id),
                    ),
                }
            }
            if let Some(e) = &result.error {
                diag.warn(
                    KIND_OBJECT,
                    format!(
                        "object {} failed to decode: {:#}; every placement of it \
                         will be skipped",
                        id, e
                    ),
                );
            }
            let status = if result.obj.is_some() {
                DefStatus::Ok
            } else {
                DefStatus::DecodeFailed
            };
            parsed.push((id as usize, result.obj, status));
        }
        {
            let mut guard = statuses.lock().unwrap();
            for (id, _, status) in &parsed {
                if guard.len() <= *id {
                    guard.resize(id + 1, DefStatus::Absent);
                }
                guard[*id] = *status;
            }
        }
        let mut guard = raw.lock().unwrap();
        for (id, obj, _) in parsed {
            if guard.len() <= id {
                guard.resize(id + 1, None);
            }
            guard[id] = obj;
        }
        Ok(())
    })?;

    let raw = raw.into_inner().unwrap();
    // Ids the archives never covered stay Absent; the rest were set above.
    let mut statuses = statuses.into_inner().unwrap();
    statuses.resize(raw.len(), DefStatus::Absent);
    let defs = resolve_defs(&raw, &mut statuses, diag);
    Ok((defs, statuses))
}

/// Dumps one location definition's raw bytes plus what the decoder made of
/// them. Exists to work out operand widths for opcodes the table is missing.
pub fn cmd_dump_object(cache_dir: &Path, id: u32) -> Result<()> {
    let source = CacheSource::new(cache_dir)?;
    let table = source.open_table(MAJOR_OBJECTS)?;
    let minor = id / OBJECTS_PER_ARCHIVE;
    let subid = id % OBJECTS_PER_ARCHIVE;
    let (archive, ranges) = table
        .archive(minor)?
        .ok_or_else(|| anyhow::anyhow!("object archive {} not found", minor))?;
    let range = ranges
        .iter()
        .find(|q| q.fileid == subid)
        .ok_or_else(|| anyhow::anyhow!("object {} not found in archive {}", id, minor))?;
    let buf = &archive[range.start..range.end];

    println!("object {} (archive {}, file {}), {} bytes", id, minor, subid, buf.len());
    for (i, chunk) in buf.chunks(16).enumerate() {
        let hex: Vec<String> = chunk.iter().map(|b| format!("{:02X}", b)).collect();
        println!("{:04X}  {}", i * 16, hex.join(" "));
    }

    let result = parse_object(buf);
    for issue in &result.issues {
        match issue {
            OpcodeIssue::Unknown { opcode, position } => {
                println!("issue: unknown chunk 0x{:02X} at position {}", opcode, position)
            }
            OpcodeIssue::MissingTerminator => println!("issue: no 0x00 terminator"),
        }
    }
    match (&result.obj, &result.error) {
        (Some(obj), _) => println!("decoded: {:?}", obj),
        (None, Some(e)) => println!("failed: {:#}", e),
        (None, None) => println!("failed with no error recorded"),
    }
    Ok(())
}

/// Writes via a temp file so an interrupted run cannot leave a half written
/// json behind for the loader to choke on.
fn write_atomic(path: &Path, data: &[u8]) -> Result<()> {
    let tmp = path.with_extension("json.tmp");
    {
        let file = fs::File::create(&tmp)?;
        let mut writer = std::io::BufWriter::with_capacity(1 << 20, file);
        writer.write_all(data)?;
        writer.flush()?;
    }
    fs::rename(&tmp, path)?;
    Ok(())
}
