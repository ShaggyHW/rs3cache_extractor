use std::{env, path::PathBuf};

#[derive(Clone, Debug, Default)]
pub struct Config {
    pub tiles_db: Option<PathBuf>,
    pub out_db: Option<PathBuf>,
    pub planes: Option<Vec<i32>>,
    pub chunk_range: Option<(i32, i32, i32, i32)>,
    pub threads: Option<usize>,
    pub dry_run: bool,
    pub store_paths: bool,
    pub log_level: Option<String>,
}

impl Config {
    pub fn from_env_defaults() -> Self {
        let tiles_db = env::var("CLUSTER_TILES_DB").ok().map(PathBuf::from);
        let out_db = env::var("CLUSTER_OUT_DB").ok().map(PathBuf::from);
        let planes = env::var("CLUSTER_PLANES").ok().and_then(|s| parse_planes(&s));
        let chunk_range = env::var("CLUSTER_CHUNK_RANGE").ok().and_then(|s| parse_chunk_range(&s));
        let threads = env::var("CLUSTER_THREADS").ok().and_then(|s| s.parse::<usize>().ok());
        let dry_run = env::var("CLUSTER_DRY_RUN").ok().map(|v| v == "1" || v.eq_ignore_ascii_case("true")).unwrap_or(false);
        let store_paths = env::var("CLUSTER_STORE_PATHS").ok().map(|v| v == "1" || v.eq_ignore_ascii_case("true")).unwrap_or(false);
        let log_level = env::var("CLUSTER_LOG_LEVEL").ok();
        Self { tiles_db, out_db, planes, chunk_range, threads, dry_run, store_paths, log_level }
    }
}

fn parse_planes(input: &str) -> Option<Vec<i32>> {
    let v = input
        .split(',')
        .filter_map(|p| {
            let t = p.trim();
            if t.is_empty() { None } else { t.parse::<i32>().ok() }
        })
        .collect::<Vec<_>>();
    if v.is_empty() { None } else { Some(v) }
}

fn parse_chunk_range(input: &str) -> Option<(i32, i32, i32, i32)> {
    // format: x_min:x_max,z_min:z_max
    let parts: Vec<&str> = input.split(',').collect();
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_planes_and_chunk_range() {
        assert_eq!(super::parse_planes(""), None);
        assert_eq!(super::parse_planes("0,1, 2"), Some(vec![0,1,2]));
        assert_eq!(super::parse_chunk_range("0:10,5:15"), Some((0,10,5,15)));
        assert_eq!(super::parse_chunk_range("bad"), None);
    }

    #[test]
    fn test_from_env_defaults_reads_values() {
        std::env::set_var("CLUSTER_TILES_DB", "/tmp/tiles.db");
        std::env::set_var("CLUSTER_OUT_DB", "/tmp/world.db");
        std::env::set_var("CLUSTER_PLANES", "0,1");
        std::env::set_var("CLUSTER_CHUNK_RANGE", "1:2,3:4");
        std::env::set_var("CLUSTER_THREADS", "8");
        std::env::set_var("CLUSTER_DRY_RUN", "true");
        std::env::set_var("CLUSTER_STORE_PATHS", "1");
        std::env::set_var("CLUSTER_LOG_LEVEL", "debug");

        let cfg = Config::from_env_defaults();
        assert_eq!(cfg.tiles_db.as_ref().unwrap().to_string_lossy(), "/tmp/tiles.db");
        assert_eq!(cfg.out_db.as_ref().unwrap().to_string_lossy(), "/tmp/world.db");
        assert_eq!(cfg.planes, Some(vec![0,1]));
        assert_eq!(cfg.chunk_range, Some((1,2,3,4)));
        assert_eq!(cfg.threads, Some(8));
        assert!(cfg.dry_run);
        assert!(cfg.store_paths);
        assert_eq!(cfg.log_level.as_deref(), Some("debug"));

        // cleanup
        std::env::remove_var("CLUSTER_TILES_DB");
        std::env::remove_var("CLUSTER_OUT_DB");
        std::env::remove_var("CLUSTER_PLANES");
        std::env::remove_var("CLUSTER_CHUNK_RANGE");
        std::env::remove_var("CLUSTER_THREADS");
        std::env::remove_var("CLUSTER_DRY_RUN");
        std::env::remove_var("CLUSTER_STORE_PATHS");
        std::env::remove_var("CLUSTER_LOG_LEVEL");
    }
}
