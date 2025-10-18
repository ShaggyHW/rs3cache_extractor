use std::path::{PathBuf};

pub const DB_FILE: &str = "tiles.db";
pub const JSON_REL_PATH: &str = "out/walk";

pub fn repo_root() -> PathBuf {
    // CARGO_MANIFEST_DIR points at rust/; parent is the repo root.
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).parent().unwrap().to_path_buf()
}

pub fn default_paths() -> (PathBuf, PathBuf) {
    let root = repo_root();
    (root.join(JSON_REL_PATH), root.join(DB_FILE))
}
