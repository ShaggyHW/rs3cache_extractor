use anyhow::Result;
use rusqlite::Connection;
use tempfile::NamedTempFile;

use rs3cache_extractor::commands::cluster::config::Config;
use rs3cache_extractor::commands::cluster::executor::{self, ExecOptions};

fn seed_minimal_world(tiles: &mut Connection, out: &mut Connection) -> Result<()> {
    rs3cache_extractor::db::create_tables(tiles)?;
    rs3cache_extractor::db::create_tables(out)?;

    // Seed chunks (not strictly required thanks to ensure_out_chunks, but keeps symmetry)
    tiles.execute("INSERT INTO chunks(chunk_x, chunk_z, chunk_size, tile_count) VALUES (0,0,64,0)", [])?;
    out.execute("INSERT INTO chunks(chunk_x, chunk_z, chunk_size, tile_count) VALUES (0,0,64,0)", [])?;

    // Two adjacent walkable tiles to allow clusters and a potential entrance
    tiles.execute(
        "INSERT INTO tiles(x,y,plane,chunk_x,chunk_z,flag,blocked,walk_mask,blocked_mask,walk_data) VALUES (0,0,0,0,0,0,0,1,0,'{}')",
        [],
    )?;
    tiles.execute(
        "INSERT INTO tiles(x,y,plane,chunk_x,chunk_z,flag,blocked,walk_mask,blocked_mask,walk_data) VALUES (1,0,0,0,0,0,0,1,0,'{}')",
        [],
    )?;

    Ok(())
}

#[test]
fn pipeline_runs_all_stages_force() -> Result<()> {
    let tiles_dbf = NamedTempFile::new().unwrap();
    let out_dbf = NamedTempFile::new().unwrap();
    let mut tiles = Connection::open(tiles_dbf.path())?;
    let mut out = Connection::open(out_dbf.path())?;
    seed_minimal_world(&mut tiles, &mut out)?;

    let cfg = Config::default();
    let stats = executor::run_pipeline(&tiles, &mut out, &cfg, ExecOptions { resume: false, force: true })?;
    assert!(stats.ran_build && stats.ran_entrances && stats.ran_intra && stats.ran_inter && stats.ran_jps);

    // Meta flags set
    for key in [
        "cluster_stage_build",
        "cluster_stage_entrances",
        "cluster_stage_intra",
        "cluster_stage_inter",
        "cluster_stage_jps",
    ] {
        let v: String = out.query_row("SELECT value FROM meta WHERE key=?1", [key], |r| r.get(0))?;
        assert_eq!(v, "done");
    }
    Ok(())
}

#[test]
fn pipeline_resume_skips_completed() -> Result<()> {
    let tiles_dbf = NamedTempFile::new().unwrap();
    let out_dbf = NamedTempFile::new().unwrap();
    let mut tiles = Connection::open(tiles_dbf.path())?;
    let mut out = Connection::open(out_dbf.path())?;
    seed_minimal_world(&mut tiles, &mut out)?;

    let cfg = Config::default();
    let _ = executor::run_pipeline(&tiles, &mut out, &cfg, ExecOptions { resume: false, force: false })?;

    // Second run with resume should skip everything (all meta keys already done)
    let stats2 = executor::run_pipeline(&tiles, &mut out, &cfg, ExecOptions { resume: true, force: false })?;
    assert_eq!(stats2.ran_build, false);
    assert_eq!(stats2.ran_entrances, false);
    assert_eq!(stats2.ran_intra, false);
    assert_eq!(stats2.ran_inter, false);
    assert_eq!(stats2.ran_jps, false);

    // Force run should re-run all
    let stats3 = executor::run_pipeline(&tiles, &mut out, &cfg, ExecOptions { resume: false, force: true })?;
    assert!(stats3.ran_build && stats3.ran_entrances && stats3.ran_intra && stats3.ran_inter && stats3.ran_jps);
    Ok(())
}
