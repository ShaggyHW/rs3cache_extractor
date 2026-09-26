//! `build` — the whole pipeline in one process: `walkflags --db` +
//! `import-xlsx` + `tile-cleaner`, producing the same `tiles.db` and
//! `worldReachableTiles.db` as running the three commands in sequence.
//!
//! Running them together removes most of the work between the steps and
//! overlaps the rest:
//!
//! * the spreadsheet downloads while the cache is being decoded;
//! * the walk masks the extractor produces are kept in a [`MaskStore`], so the
//!   BFS never reads the 84.75M tiles back out of `tiles.db`;
//! * the BFS runs against an in-memory copy of the schema with the imported
//!   teleports, so `worldReachableTiles.db` is built while `tiles.db` is still
//!   being written;
//! * both databases are rewritten in place, and only pages whose bytes changed
//!   are written, so a rebuild after editing the overrides or the spreadsheet
//!   writes megabytes instead of gigabytes.

use anyhow::{anyhow, Context, Result};
use rusqlite::Connection;
use std::path::Path;
use std::time::Instant;

use crate::cache::source::CacheSource;
use crate::commands::import_xlsx::{apply_plan, fetch_workbook, plan_import, ImportPlan};
use crate::commands::load_tiles::read_overrides_file;
use crate::commands::tile_cleaner::{run_cleaner, MaskStore, Tile};
use crate::commands::walkflags::{
    extract, fsync_path, report_tree, tiles_schema, Area, ColumnMasks, TilesWriter,
};
use crate::walk::diagnostics::Diagnostics;
use crate::walk::export::SQUARE_TILES;
use crate::walk::mapsquare::CHUNK_SIZE;

pub struct BuildOpts<'a> {
    pub cache_dir: &'a Path,
    pub db: &'a Path,
    pub out: &'a Path,
    pub overrides: Option<&'a Path>,
    /// Spreadsheet to import: a local .xlsx or a Google Sheets URL.
    pub xlsx: Option<&'a str>,
    pub start: Tile,
    pub log: Option<&'a Path>,
    pub fsync: bool,
    pub area: Area,
}

pub fn cmd_build(opts: &BuildOpts) -> Result<()> {
    let started = Instant::now();
    let diag = Diagnostics::new(opts.log)?;
    let source = CacheSource::new(opts.cache_dir)?;
    let overrides = match opts.overrides {
        Some(path) => read_overrides_file(path)?,
        None => Vec::new(),
    };
    println!("cache      : {}", opts.cache_dir.display());
    println!("tiles db   : {}", opts.db.display());
    println!("reachable  : {}", opts.out.display());
    if let Some(path) = diag.log_path() {
        println!("log        : {}", path);
    }

    // The download is pure network wait, so it goes first and runs alongside
    // everything up to the import.
    let workbook = opts.xlsx.map(|xlsx| {
        let xlsx = xlsx.to_string();
        std::thread::spawn(move || -> Result<(ImportPlan, f64)> {
            let t = Instant::now();
            let workbook = fetch_workbook(&xlsx)?;
            let plan = plan_import(&tiles_schema()?, workbook.path(), &[], &[])?;
            Ok((plan, t.elapsed().as_secs_f64()))
        })
    });

    let writer = TilesWriter::create(opts.db, &overrides)?;
    let mut masks = MaskStore::new();
    let report = extract(&source, &diag, opts.area, None, true, |column| {
        add_column(&mut masks, &column);
        writer.send(column)
    })?;
    // Overrides replace or add tiles exactly as they do in tiles.db.
    for &[x, y, plane, mask] in &overrides {
        if let (Ok(x), Ok(y), Ok(plane)) = (i32::try_from(x), i32::try_from(y), i32::try_from(plane)) {
            masks.insert((x, y, plane), mask as u8);
        }
    }
    println!(
        "extracted {} tiles from {} squares in {:.1}s (square load {:.1}s, chunk work {:.1}s, \
         waiting on the tiles.db writer {:.1}s)",
        report.tiles,
        report.squares,
        started.elapsed().as_secs_f64(),
        report.t_load,
        report.t_work,
        report.writer_wait
    );

    let plan = match workbook {
        Some(handle) => {
            let (plan, secs) = handle
                .join()
                .map_err(|_| anyhow!("spreadsheet thread panicked"))??;
            println!(
                "spreadsheet: {} rows parsed, ready after {:.1}s",
                plan.rows(),
                secs
            );
            Some(plan)
        }
        None => None,
    };
    // A spreadsheet that writes to `tiles` itself would make the masks stale;
    // that case waits for tiles.db and reloads them from it.
    let tiles_imported = plan.as_ref().is_some_and(|p| p.tables_touched.contains("tiles"));

    std::thread::scope(|scope| -> Result<()> {
        // worldReachableTiles.db, from memory, while tiles.db is still being
        // written.
        let cleaner = (!tiles_imported).then(|| {
            scope.spawn(|| -> Result<f64> {
                let t = Instant::now();
                let mut teleports = tiles_schema()?;
                if let Some(plan) = &plan {
                    apply_plan(&mut teleports, plan)?;
                }
                run_cleaner(&teleports, &masks, opts.out, opts.start)?;
                Ok(t.elapsed().as_secs_f64())
            })
        });

        let t = Instant::now();
        let (tree, sink) = writer.finish()?;
        let tiles_secs = t.elapsed().as_secs_f64();
        report_tree(opts.db, &tree, &sink);
        if let Some(plan) = &plan {
            let mut conn =
                Connection::open(opts.db).with_context(|| format!("opening {}", opts.db.display()))?;
            // No fsync at commit: see `--fsync`.
            conn.execute_batch("PRAGMA foreign_keys=ON;\nPRAGMA synchronous=OFF;")?;
            let rows = apply_plan(&mut conn, plan)?;
            println!("imported {} spreadsheet rows into {}", rows, opts.db.display());
        }

        match cleaner {
            Some(handle) => {
                let secs = handle
                    .join()
                    .map_err(|_| anyhow!("tile cleaner thread panicked"))??;
                println!("{} written in {:.1}s", opts.out.display(), secs);
            }
            None => {
                println!(
                    "the spreadsheet imports into `tiles`; reloading the masks from {}",
                    opts.db.display()
                );
                let masks = MaskStore::load_from_db(opts.db)?;
                let conn = Connection::open(opts.db)?;
                run_cleaner(&conn, &masks, opts.out, opts.start)?;
            }
        }
        println!(
            "waited {:.1}s for the tiles.db writer after the extraction",
            tiles_secs
        );
        Ok(())
    })?;

    if opts.fsync {
        fsync_path(opts.db)?;
        fsync_path(opts.out)?;
    }
    println!("build complete in {:.1}s", started.elapsed().as_secs_f64());
    diag.finish()?;
    Ok(())
}

/// Feeds one extracted column into the mask store.
fn add_column(masks: &mut MaskStore, column: &ColumnMasks) {
    for chunk in &column.chunks {
        if chunk.missing.is_empty() {
            masks.insert_square(column.cx, chunk.cz, &chunk.masks);
            continue;
        }
        let (x0, z0) = (column.cx * CHUNK_SIZE, chunk.cz * CHUNK_SIZE);
        for idx in 0..SQUARE_TILES {
            if chunk.exists(idx) {
                let plane = (idx / 4096) as i32;
                let lz = ((idx / 64) % 64) as i32;
                let lx = (idx % 64) as i32;
                masks.insert((x0 + lx, z0 + lz, plane), chunk.masks[idx]);
            }
        }
    }
}
