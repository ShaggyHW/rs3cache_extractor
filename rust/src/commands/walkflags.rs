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
use std::sync::Arc;
use std::time::Instant;

use crate::cache::source::{CacheSource, MAJOR_OBJECTS, OBJECTS_PER_ARCHIVE};
use crate::walk::diagnostics::{
    Diagnostics, KIND_CACHE, KIND_LOC_DROPPED, KIND_MAPSQUARE, KIND_OBJECT, KIND_OPCODE,
    KIND_TRUNCATED, KIND_WRITE,
};
use crate::walk::export::{collect_rows, render_chunk, TileRow};
use crate::walk::grid::{build_grid, Grid};
use crate::walk::mapsquare::{load_square, SquareData, CHUNK_SIZE};
use crate::walk::objdef::{
    parse_object, resolve_defs, DefStatus, ObjectDef, OpcodeIssue, RawObject,
};

type SquareCache = HashMap<(i32, i32), Option<Arc<SquareData>>>;

#[derive(Default)]
struct ChunkOutput {
    rows: Vec<TileRow>,
    /// Loc ids skipped because no usable definition could be decoded.
    dropped_locs: Vec<u32>,
}

pub struct WalkflagsOpts<'a> {
    pub cache_dir: &'a Path,
    /// Write `<save>/walk/<x>-<z>.json`, as the node script did.
    pub save_dir: Option<&'a Path>,
    /// Insert rows straight into this sqlite db.
    pub db: Option<&'a Path>,
    /// Overrides applied after the tiles are in; db output only.
    pub overrides: Option<&'a Path>,
    /// Where the full diagnostics log goes; `None` means console only.
    pub log: Option<&'a Path>,
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

    let mut conn = match opts.db {
        Some(db) => {
            println!("db output  : {}", db.display());
            let mut conn = Connection::open(db)
                .with_context(|| format!("opening db at {}", db.display()))?;
            conn.execute_batch(
                "PRAGMA journal_mode=WAL;\nPRAGMA synchronous=NORMAL;\nPRAGMA temp_store=MEMORY;\nPRAGMA cache_size=-262144;",
            )?;
            crate::db::create_tables(&mut conn)?;
            conn.execute_batch("PRAGMA foreign_keys=OFF;")?;
            Some(conn)
        }
        None => None,
    };

    println!("cache      : {}", opts.cache_dir.display());
    if let Some(path) = diag.log_path() {
        println!("log        : {}", path);
    }

    let (defs, def_statuses) = load_object_defs(&source, &diag)?;
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

    for cx in opts.startx..opts.startx + opts.sizex {
        for column in [cx - 1, cx, cx + 1] {
            load_column(
                &source,
                &mut cache,
                column,
                opts.startz - 1,
                opts.startz + opts.sizez + 1,
                &errors,
                &diag,
            );
        }
        cache.retain(|(kx, _), _| *kx >= cx - 1);

        let want_rows = conn.is_some();
        let column_out: Vec<ChunkOutput> = (opts.startz..opts.startz + opts.sizez)
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
                if let Some(walk_dir) = walk_dir.as_deref() {
                    let (json, tiles) = render_chunk(grid, cx, cz);
                    let path = walk_dir.join(format!("{}-{}.json", cx, cz));
                    match write_atomic(&path, json.as_bytes()) {
                        Ok(()) => {
                            // When rows are collected too, they carry the count.
                            if !want_rows {
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

                let rows = if want_rows {
                    let mut rows = Vec::with_capacity(16384);
                    collect_rows(grid, cx, cz, &mut rows);
                    rows
                } else {
                    Vec::new()
                };
                ChunkOutput { rows, dropped_locs }
            })
            .collect();

        let mut dropped_this_column = 0usize;
        for out in &column_out {
            for id in &out.dropped_locs {
                *dropped_loc_ids.entry(*id).or_insert(0usize) += 1;
                dropped_this_column += 1;
            }
        }
        diag.count(KIND_LOC_DROPPED, dropped_this_column);

        if let Some(conn) = conn.as_mut() {
            let mut rows: Vec<TileRow> = column_out.into_iter().flat_map(|q| q.rows).collect();
            // Every column covers a disjoint, ascending x range, so sorting
            // within a column feeds sqlite globally ascending primary keys.
            rows.par_sort_unstable_by_key(|row| row.key);
            tiles_total.fetch_add(rows.len(), Ordering::Relaxed);
            insert_rows(conn, &rows)
                .with_context(|| format!("inserting tiles of column {}", cx))?;
        }

        let done = processed.load(Ordering::Relaxed);
        if done > 0 {
            println!(
                "column {} done, {} squares, {:.1}s elapsed",
                cx,
                done,
                started.elapsed().as_secs_f64()
            );
        }
    }

    if let Some(conn) = conn.as_mut() {
        if let Some(overrides) = opts.overrides {
            crate::commands::load_tiles::apply_overrides_file(overrides, conn)?;
        }
        conn.execute_batch("PRAGMA foreign_keys=ON;")?;
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

    println!(
        "Export complete: {} squares processed, {} tiles, {} errors, {:.1}s",
        processed.load(Ordering::Relaxed),
        tiles_total.load(Ordering::Relaxed),
        errors.load(Ordering::Relaxed),
        started.elapsed().as_secs_f64()
    );
    diag.finish()?;
    Ok(())
}

fn insert_rows(conn: &mut Connection, rows: &[TileRow]) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let tx = conn.transaction()?;
    {
        let mut stmt = tx.prepare_cached(
            "INSERT OR REPLACE INTO tiles (x, y, plane, walk_mask, RegionID) VALUES (?1, ?2, ?3, ?4, ?5)",
        )?;
        for row in rows {
            stmt.execute(rusqlite::params![
                row.x(),
                row.y(),
                row.plane(),
                row.walk_mask as i64,
                row.region_id()
            ])?;
        }
    }
    tx.commit()?;
    Ok(())
}

fn load_column(
    source: &CacheSource,
    cache: &mut SquareCache,
    cx: i32,
    zstart: i32,
    zend: i32,
    errors: &AtomicUsize,
    diag: &Diagnostics,
) {
    let missing: Vec<i32> = (zstart..zend).filter(|cz| !cache.contains_key(&(cx, *cz))).collect();
    if missing.is_empty() {
        return;
    }
    let loaded: Vec<(i32, Option<Arc<SquareData>>)> = missing
        .into_par_iter()
        .map(|cz| match load_square(source, cx, cz) {
            Ok(square) => (cz, square),
            Err(e) => {
                diag.warn(
                    KIND_MAPSQUARE,
                    format!("mapsquare {},{} could not be decoded: {:#}", cx, cz, e),
                );
                errors.fetch_add(1, Ordering::Relaxed);
                (cz, None)
            }
        })
        .collect();
    for (cz, square) in loaded {
        cache.insert((cx, cz), square);
    }
}

/// Decodes every location definition up front. The whole `js5-16` index is a
/// couple of megabytes, and having it resolved removes all locking from the
/// per-chunk work.
fn load_object_defs(
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
