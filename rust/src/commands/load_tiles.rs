use anyhow::{Context, Result};
use rusqlite::Connection;
use serde::Deserialize;
use std::fs::{self, File};
use std::io::BufReader;
use std::path::Path;
use std::sync::mpsc;
use std::thread;
use rayon::prelude::*;

#[derive(Deserialize, Debug)]
struct FileRoot {
    #[serde(default)]
    chunk: Option<Chunk>,
    #[serde(default)]
    tiles: Vec<Tile>,
}

#[derive(Deserialize, Debug, Default)]
struct Chunk {
    #[serde(default)]
    x: Option<i64>,
    #[serde(default)]
    z: Option<i64>,
    #[serde(rename = "chunkSize", default)]
    chunk_size: Option<i64>,
}

#[derive(Deserialize, Debug)]
struct Tile {
    x: i64,
    y: i64,
    plane: i64,
    #[serde(default)]
    flag: Option<i64>,
    #[serde(default)]
    blocked: Option<bool>,
    #[serde(rename = "walkMask", default)]
    walk_mask: Option<i64>,
}

pub fn cmd_load_tiles(json_folder: &Path, db_path: &Path) -> Result<()> {
    println!("Using JSON folder: {}", json_folder.display());
    println!("Using DB file    : {}", db_path.display());

    let mut conn = Connection::open(db_path)
        .with_context(|| format!("Failed to open DB at {}", db_path.display()))?;

    conn.execute_batch(
        "PRAGMA journal_mode=WAL;\nPRAGMA synchronous=NORMAL;\nPRAGMA temp_store=MEMORY;",
    )?;

    crate::db::create_tables(&mut conn)?;
    load_json_files(json_folder, &mut conn)?;

    println!("Tiles successfully loaded into {}", db_path.display());
    Ok(())
}

fn load_json_files(folder: &Path, conn: &mut Connection) -> Result<()> {
    if !folder.exists() {
        anyhow::bail!("JSON folder not found: {}", folder.display());
    }

    // Gather JSON files
    let mut file_entries: Vec<_> = fs::read_dir(folder)?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().map(|e| e == "json").unwrap_or(false))
        .collect();
    file_entries.sort();

    // Channel for streaming parsed batches to a single DB writer
    let (tx_msg, rx_msg) = mpsc::channel::<FileBatch>();

    // Spawn parallel producers to read/parse JSON files and send batches
    let producer = {
        let tx_msg = tx_msg.clone();
        thread::spawn(move || {
            file_entries
                .into_par_iter()
                .for_each_with(tx_msg, |s, path| {
                    if let Err(e) = parse_file_and_stream(&path, s) {
                        eprintln!("Error processing {}: {}", path.display(), e);
                    }
                });
            // Dropping sender closes the channel
        })
    };

    drop(tx_msg);

    // Optimize SQLite for bulk load and avoid maintaining indexes during insert
    conn.execute_batch(
        "PRAGMA foreign_keys=OFF;\nDROP INDEX IF EXISTS idx_tiles_walkable;",
    )?;

    // Single transaction and prepared statement reused for entire stream
    let txw = conn.transaction()?;
    let mut tiles_stmt = txw.prepare(
        "INSERT OR REPLACE INTO tiles (x, y, plane, flag, blocked, walk_mask, RegionID) VALUES (?, ?, ?, ?, ?, ?, ?)",
    )?;

    // Drain messages as they arrive and insert rows
    for batch in rx_msg {
        if batch.tile_rows.is_empty() { continue; }
        for row in batch.tile_rows {
            let (x, y, plane, flag, blocked, walk_mask, region_id) = row;
            tiles_stmt.execute(rusqlite::params![x, y, plane, flag, blocked, walk_mask, region_id])?;
        }
    }

    drop(tiles_stmt);
    txw.commit()?;

    // Recreate index and restore FK checks after load
    conn.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_tiles_walkable ON tiles(x, y, plane) WHERE blocked = 0;\nPRAGMA foreign_keys=ON;",
    )?;

    // Ensure producers are finished
    let _ = producer.join();
    Ok(())
}

type TileRow = (
    i64,         // x
    i64,         // y
    i64,         // plane
    Option<i64>, // flag
    i64,         // blocked
    Option<i64>, // walk_mask
    i64,         // RegionID
);

struct FileBatch {
    tile_rows: Vec<TileRow>,
}

fn parse_file_and_stream(path: &Path, sender: &mpsc::Sender<FileBatch>) -> Result<()> {
    println!("Loading {}...", path.display());
    let file = File::open(&path).with_context(|| format!("open {}", path.display()))?;
    let reader = BufReader::new(file);
    let data: FileRoot = serde_json::from_reader(reader)
        .with_context(|| format!("parse JSON {}", path.display()))?;

    if data.tiles.is_empty() {
        return Ok(());
    }


    // Send chunk meta first so writer can insert chunk row
    sender.send(FileBatch {
        tile_rows: Vec::new(),
    }).map_err(|e| anyhow::anyhow!(e))?;

    // Stream tile rows in small chunks to bound memory
    const SUB_BATCH: usize = 1_000_000;
    let mut rows: Vec<TileRow> = Vec::with_capacity(SUB_BATCH);
    for t in data.tiles.into_iter() {
        let blocked_int = if t.blocked.unwrap_or(false) { 1i64 } else { 0i64 };
        if t.blocked == Some(true) {
            continue;
        }
        // Compute RegionID from x,y: regionId = (regionX << 8) + regionY,
        // where regionX = x >> 6 and regionY = y >> 6
        let region_x = t.x >> 6;
        let region_y = t.y >> 6;
        let region_id = (region_x << 8) + region_y;
        rows.push((
            t.x,
            t.y,
            t.plane,
            t.flag,
            blocked_int,
            t.walk_mask,
            region_id,
        ));
        if rows.len() >= SUB_BATCH {
            sender.send(FileBatch {
                tile_rows: std::mem::take(&mut rows),
            }).map_err(|e| anyhow::anyhow!(e))?;
        }
    }

    if !rows.is_empty() {
        sender.send(FileBatch {
            tile_rows: rows,
        }).map_err(|e| anyhow::anyhow!(e))?;
    }

    Ok(())
}
