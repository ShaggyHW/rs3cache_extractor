use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
mod util;
mod db;
mod commands;

#[derive(Parser, Debug)]
#[command(name = "rs3cache_extractor", version, about = "Tools for RS3 cache extraction")] 
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Load tiles JSON into tiles.db
    LoadTiles {
        /// Path to JSON folder (defaults to repo_root/out/walk)
        #[arg(long)]
        json_dir: Option<PathBuf>,
        /// Path to SQLite DB (defaults to repo_root/tiles.db)
        #[arg(long)]
        db: Option<PathBuf>,
        #[arg(long, help = "Path to overrides file with lines: x,y,z,walk_mask (comma-separated)")]
        overrides: Option<PathBuf>,
    },

    /// Import XLSX or Google Sheet into worldReachableTiles.db using the native Rust importer
    ImportXlsx {
        /// Path to .xlsx file or Google Sheets URL
        #[arg(long)]
        xlsx: String,
        /// Path to SQLite DB (default: worldReachableTiles.db)
        #[arg(long, default_value = "tiles.db")]
        db: PathBuf,
        /// Parse and validate only; do not modify the DB
        #[arg(long)]
        dry_run: bool,
        /// Tables to DELETE FROM before inserting
        #[arg(long, num_args = 1..)]
        truncate: Vec<String>,
        /// Only import the specified sheets/tables
        #[arg(long, num_args = 1..)]
        sheets: Vec<String>,
    },

    /// Build worldReachableTiles.db from tiles.db by BFS + teleports
    TileCleaner {
        /// Source SQLite DB (default: repo_root/tiles.db)
        #[arg(long)]
        src: Option<PathBuf>,
        /// Output SQLite DB (default: repo_root/worldReachableTiles.db)
        #[arg(long)]
        out: Option<PathBuf>,
        /// Start tile X (default: 3200)
        #[arg(long, default_value_t = 3200)]
        start_x: i32,
        /// Start tile Y (default: 3200)
        #[arg(long, default_value_t = 3200)]
        start_y: i32,
        /// Start plane (default: 0)
        #[arg(long, default_value_t = 0)]
        start_plane: i32,
    }


   
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::LoadTiles { json_dir, db, overrides } => {
            let (def_json, def_db) = util::default_paths();
            let json_folder = json_dir.unwrap_or(def_json);
            let db_path = db.unwrap_or(def_db);
            commands::load_tiles::cmd_load_tiles(&json_folder, &db_path, overrides.as_deref())
        }
        Commands::ImportXlsx { xlsx, db, dry_run, truncate, sheets } => {
            commands::import_xlsx::cmd_import_xlsx(&xlsx, &db, dry_run, &truncate, &sheets)
        }
        Commands::TileCleaner { src, out, start_x, start_y, start_plane } => {
            let root = util::repo_root();
            let src_path = src.unwrap_or(root.join("tiles.db"));
            let out_path = out.unwrap_or(root.join("worldReachableTiles.db"));
            commands::tile_cleaner::cmd_tile_cleaner(&src_path, &out_path, start_x, start_y, start_plane)
        }
        }
}
// (All DB schema and loading logic is now in `db` and `commands` modules.)