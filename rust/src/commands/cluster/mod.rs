use anyhow::Result;
use clap::{Args, Subcommand};
use std::path::PathBuf;

pub mod config;
pub mod logging;
pub mod db;
pub mod models;
pub mod cluster_builder;
pub mod neighbor_policy;
pub mod entrance_discovery;
pub mod intra_connector;
pub mod inter_connector;
pub mod jps_accelerator;
pub mod executor;
pub mod teleport_connector;

#[derive(Args, Debug, Clone)]
pub struct CommonOpts {
    /// Source SQLite DB containing tiles (default: repo_root/tiles.db or CLUSTER_TILES_DB)
    #[arg(long = "tiles-db")]
    pub tiles_db: Option<PathBuf>,
    /// Output SQLite DB for cluster artifacts (default: repo_root/worldReachableTiles.db or CLUSTER_OUT_DB)
    #[arg(long = "out-db")]
    pub out_db: Option<PathBuf>,
    /// Planes to include (comma-separated or repeated)
    #[arg(long = "planes", value_delimiter = ',')]
    pub planes: Option<Vec<i32>>,
    /// Chunk range filter: x_min:x_max,z_min:z_max
    #[arg(long = "chunk-range")]
    pub chunk_range: Option<String>,
    /// Number of worker threads (rayon)
    #[arg(long = "threads")]
    pub threads: Option<usize>,
    /// Dry run: compute only, no writes
    #[arg(long = "dry-run")]
    pub dry_run: bool,
    /// Store path blobs in intra stage
    #[arg(long = "store-paths")]
    pub store_paths: bool,
    /// Log level (trace|debug|info|warn|error)
    #[arg(long = "log-level")]
    pub log_level: Option<String>,
}

#[derive(Subcommand, Debug, Clone)]
pub enum ClusterCommand {
    /// Partition tiles into clusters per chunk
    #[command(name = "build-clusters")]
    BuildClusters,
    /// Discover entrances between adjacent clusters
    #[command(name = "entrance-discovery")]
    EntranceDiscovery,
    /// Build intra-cluster connections (optional path storage)
    #[command(name = "intra-connector")]
    IntraConnector,
    /// Build inter-cluster connections across entrances
    #[command(name = "inter-connector")]
    InterConnector,
    /// Precompute JPS acceleration structures
    #[command(name = "jps-accelerator")]
    JpsAccelerator,
    /// Inspect neighbor policy (stub)
    #[command(name = "neighbor-policy")]
    NeighborPolicy,
    /// Run full executor pipeline with resume/force controls
    #[command(name = "exec")]
    Exec {
        /// Resume from last incomplete stage
        #[arg(long)]
        resume: bool,
        /// Force re-run of all stages --
        #[arg(long)]
        force: bool,
    },
}

pub fn cmd_cluster(common: CommonOpts, sub: ClusterCommand) -> Result<()> {
    // Merge CLI options with environment defaults (env overrides CLI when set)
    let mut cfg = config::Config::default();
    // Start with CLI
    cfg.tiles_db = common.tiles_db.clone();
    cfg.out_db = common.out_db.clone();
    cfg.planes = common.planes.clone();
    cfg.chunk_range = common
        .chunk_range
        .as_deref()
        .and_then(parse_chunk_range_cli);
    cfg.threads = common.threads;
    cfg.dry_run = common.dry_run;
    cfg.store_paths = common.store_paths;
    cfg.log_level = common.log_level.clone();
    // Overlay env (env > CLI precedence as designed)
    let env_cfg = config::Config::from_env_defaults();
    if env_cfg.tiles_db.is_some() { cfg.tiles_db = env_cfg.tiles_db; }
    if env_cfg.out_db.is_some() { cfg.out_db = env_cfg.out_db; }
    if env_cfg.planes.is_some() { cfg.planes = env_cfg.planes; }
    if env_cfg.chunk_range.is_some() { cfg.chunk_range = env_cfg.chunk_range; }
    if env_cfg.threads.is_some() { cfg.threads = env_cfg.threads; }
    if env_cfg.dry_run { cfg.dry_run = true; }
    if env_cfg.store_paths { cfg.store_paths = true; }
    if env_cfg.log_level.is_some() { cfg.log_level = env_cfg.log_level; }

    // Init logging and thread pool
    logging::init(cfg.log_level.as_deref());
    if let Some(n) = cfg.threads {
        let _ = rayon::ThreadPoolBuilder::new().num_threads(n).build_global();
    }

    // Resolve DB paths
    let root = crate::util::repo_root();
    let db_path = root.join("worldReachableTiles.db");
    let tiles_path = db_path.clone();
    let out_path = db_path.clone();

    // Open connections as needed per subcommand
    match sub {
        ClusterCommand::NeighborPolicy => {
            // Stub: just touch MovementPolicy to validate linkage
            let _p = neighbor_policy::MovementPolicy::default();
            Ok(())
        }
        ClusterCommand::BuildClusters => {
            let mut out = db::open_rw(&out_path)?;
            let tiles = db::open_ro(&tiles_path)?;
            let _ = cluster_builder::build_clusters(&tiles, &mut out, &cfg)?;
            Ok(())
        }
        ClusterCommand::EntranceDiscovery => {
            let mut out = db::open_rw(&out_path)?;
            let tiles = db::open_ro(&tiles_path)?;
            let _ = entrance_discovery::discover_entrances(&tiles, &mut out, &cfg)?;
            Ok(())
        }
        ClusterCommand::IntraConnector => {
            let mut out = db::open_rw(&out_path)?;
            let tiles = db::open_ro(&tiles_path)?;
            let _ = intra_connector::build_intra_edges(&tiles, &mut out, &cfg)?;
            Ok(())
        }
        ClusterCommand::InterConnector => {
            let mut out = db::open_rw(&out_path)?;
            let tiles = db::open_ro(&tiles_path)?;
            let _ = inter_connector::build_inter_edges(&tiles, &mut out, &cfg)?;
            Ok(())
        }
        ClusterCommand::JpsAccelerator => {
            let mut out = db::open_rw(&out_path)?;
            let tiles = db::open_ro(&tiles_path)?;
            let _ = jps_accelerator::build_jps(&tiles, &mut out, &cfg)?;
            Ok(())
        }
        ClusterCommand::Exec { resume, force } => {
            let mut out = db::open_rw(&out_path)?;
            let tiles = db::open_ro(&tiles_path)?;
            let _ = executor::run_pipeline(&tiles, &mut out, &cfg, executor::ExecOptions { resume, force })?;
            Ok(())
        }
    }
}

fn parse_chunk_range_cli(s: &str) -> Option<(i32,i32,i32,i32)> {
    let parts: Vec<&str> = s.split(',').collect();
    if parts.len() != 2 { return None; }
    let x = parts[0].split(':').collect::<Vec<_>>();
    let z = parts[1].split(':').collect::<Vec<_>>();
    if x.len() != 2 || z.len() != 2 { return None; }
    let x_min = x[0].trim().parse::<i32>().ok()?;
    let x_max = x[1].trim().parse::<i32>().ok()?;
    let z_min = z[0].trim().parse::<i32>().ok()?;
    let z_max = z[1].trim().parse::<i32>().ok()?;
    Some((x_min, x_max, z_min, z_max))
}
