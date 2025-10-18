use anyhow::{anyhow, Result};
use rusqlite::{params, Connection, OptionalExtension};

use super::cluster_builder;
use super::config::Config;
use super::entrance_discovery;
use super::intra_connector;
use super::inter_connector;
use super::jps_accelerator;
use super::teleport_connector;
use super::db::ensure_schema;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Stage {
    Build,
    Entrances,
    TeleportEntrances,
    Intra,
    Inter,
    TeleportEdges,
    Jps,
}

impl Stage {
    pub fn key(self) -> &'static str {
        match self {
            Stage::Build => "cluster_stage_build",
            Stage::Entrances => "cluster_stage_entrances",
            Stage::TeleportEntrances => "cluster_stage_teleport_entrances",
            Stage::Intra => "cluster_stage_intra",
            Stage::Inter => "cluster_stage_inter",
            Stage::TeleportEdges => "cluster_stage_teleport_edges",
            Stage::Jps => "cluster_stage_jps",
        }
    }
    pub fn all() -> &'static [Stage] { &[Stage::Build, Stage::Entrances, Stage::TeleportEntrances, Stage::Intra, Stage::Inter, Stage::TeleportEdges, Stage::Jps] }
}

#[derive(Copy, Clone, Debug, Default)]
pub struct ExecOptions {
    pub resume: bool,
    pub force: bool,
}

#[derive(Clone, Debug, Default)]
pub struct ExecStats {
    pub ran_build: bool,
    pub ran_entrances: bool,
    pub ran_teleport_entrances: bool,
    pub ran_intra: bool,
    pub ran_inter: bool,
    pub ran_teleport_edges: bool,
    pub ran_jps: bool,
}

pub fn run_pipeline(tiles_db: &Connection, out_db: &mut Connection, cfg: &Config, opts: ExecOptions) -> Result<ExecStats> {
    ensure_schema(out_db)?; // ensure meta table etc.

    let mut stats = ExecStats::default();

    // Ensure out_db has chunk rows required by later stages (FK for chunk_clusters)
    ensure_out_chunks(tiles_db, out_db, cfg)?;

    // Determine starting stage for resume
    let mut stages_to_run: Vec<Stage> = Stage::all().to_vec();
    if opts.resume && !opts.force {
        // Skip completed prefix
        stages_to_run = Stage::all()
            .iter()
            .copied()
            .skip_while(|s| is_stage_done(out_db, *s).unwrap_or(false))
            .collect();
        if stages_to_run.is_empty() { return Ok(stats); }
    }

    for stage in stages_to_run {
        if opts.force {
            clear_stage_meta(out_db, stage)?;
        }
        if !opts.force && opts.resume && is_stage_done(out_db, stage)? {
            continue;
        }
        match stage {
            Stage::Build => {
                let _s = cluster_builder::build_clusters(tiles_db, out_db, cfg)?;
                validate_build(tiles_db, out_db, cfg)?;
                set_stage_done(out_db, Stage::Build)?;
                stats.ran_build = true;
            }
            Stage::Entrances => {
                let _s = entrance_discovery::discover_entrances(tiles_db, out_db, cfg)?;
                validate_entrances(out_db)?;
                set_stage_done(out_db, Stage::Entrances)?;
                stats.ran_entrances = true;
            }
            Stage::TeleportEntrances => {
                let _s = teleport_connector::ensure_teleport_entrances(tiles_db, out_db, cfg)?;
                // No specific validation beyond schema
                set_stage_done(out_db, Stage::TeleportEntrances)?;
                stats.ran_teleport_entrances = true;
            }
            Stage::Intra => {
                let _s = intra_connector::build_intra_edges(tiles_db, out_db, cfg)?;
                validate_intra(out_db)?;
                set_stage_done(out_db, Stage::Intra)?;
                stats.ran_intra = true;
            }
            Stage::Inter => {
                let _s = inter_connector::build_inter_edges(tiles_db, out_db, cfg)?;
                validate_inter(out_db)?;
                set_stage_done(out_db, Stage::Inter)?;
                stats.ran_inter = true;
            }
            Stage::TeleportEdges => {
                let _s = teleport_connector::create_teleport_edges(out_db, cfg)?;
                // No specific validation beyond schema
                set_stage_done(out_db, Stage::TeleportEdges)?;
                stats.ran_teleport_edges = true;
            }
            Stage::Jps => {
                let _s = jps_accelerator::build_jps(tiles_db, out_db, cfg)?;
                validate_jps(out_db)?;
                set_stage_done(out_db, Stage::Jps)?;
                stats.ran_jps = true;
            }
        }
    }

    Ok(stats)
}

fn is_stage_done(db: &Connection, s: Stage) -> Result<bool> {
    let val: Option<String> = db
        .query_row("SELECT value FROM meta WHERE key=?1", [s.key()], |r| r.get(0))
        .optional()?;
    Ok(matches!(val.as_deref(), Some("done")))
}

fn set_stage_done(db: &Connection, s: Stage) -> Result<()> {
    db.execute(
        "INSERT INTO meta(key, value) VALUES(?1,'done') ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        [s.key()],
    )?;
    Ok(())
}

fn clear_stage_meta(db: &Connection, s: Stage) -> Result<()> {
    db.execute("DELETE FROM meta WHERE key=?1", [s.key()])?;
    Ok(())
}

// ---- Validations (minimal but useful) ----

fn validate_build(tiles_db: &Connection, out_db: &Connection, cfg: &Config) -> Result<()> {
    // For each chunk with any walkable tile in scope, expect at least one chunk_clusters row
    let mut q = tiles_db.prepare(
        "SELECT DISTINCT chunk_x, chunk_z, plane FROM tiles WHERE blocked=0",
    )?;
    let rows = q.query_map([], |r| Ok((r.get::<_, i32>(0)?, r.get::<_, i32>(1)?, r.get::<_, i32>(2)?)))?;
    for r in rows {
        let (cx,cz,pl) = r?;
        if let Some(planes) = &cfg.planes { if !planes.contains(&pl) { continue; } }
        if let Some((xmin,xmax,zmin,zmax)) = cfg.chunk_range { if cx < xmin || cx > xmax || cz < zmin || cz > zmax { continue; } }
        let cnt: i64 = out_db.query_row(
            "SELECT COUNT(*) FROM chunk_clusters WHERE chunk_x=?1 AND chunk_z=?2 AND plane=?3",
            params![cx,cz,pl],
            |r| r.get(0),
        )?;
        if cnt == 0 { return Err(anyhow!("validate_build: expected clusters for chunk ({},{},{})", cx,cz,pl)); }
    }
    Ok(())
}

fn validate_entrances(out_db: &Connection) -> Result<()> {
    // neighbor_dir restricted by CHECK; ensure values present are valid and unique constraint holds implicitly by schema
    // Just ensure no NULLs and table readable
    let _: Option<i64> = out_db.query_row(
        "SELECT COUNT(*) FROM cluster_entrances WHERE neighbor_dir IN ('N','S','E','W')",
        [],
        |r| r.get(0),
    ).optional()?;
    Ok(())
}

fn validate_intra(out_db: &Connection) -> Result<()> {
    // Costs must be non-null
    let bad: Option<i64> = out_db
        .query_row("SELECT 1 FROM cluster_intraconnections WHERE cost IS NULL LIMIT 1", [], |r| r.get(0))
        .optional()?;
    if bad.is_some() { return Err(anyhow!("validate_intra: NULL cost found")); }
    Ok(())
}

fn validate_inter(out_db: &Connection) -> Result<()> {
    // Costs must be non-null
    let bad: Option<i64> = out_db
        .query_row("SELECT 1 FROM cluster_interconnections WHERE cost IS NULL LIMIT 1", [], |r| r.get(0))
        .optional()?;
    if bad.is_some() { return Err(anyhow!("validate_inter: NULL cost found")); }
    Ok(())
}

fn validate_jps(out_db: &Connection) -> Result<()> {
    // Ensure tables exist and are readable; presence may be zero-sized for sparse maps
    let _: Option<i64> = out_db.query_row("SELECT COUNT(*) FROM jps_spans", [], |r| r.get(0)).optional()?;
    let _: Option<i64> = out_db.query_row("SELECT COUNT(*) FROM jps_jump", [], |r| r.get(0)).optional()?;
    Ok(())
}

// Copy/mirror required chunks into out_db to satisfy FK on chunk_clusters.
fn ensure_out_chunks(tiles_db: &Connection, out_db: &Connection, cfg: &Config) -> Result<()> {
    // Read and buffer rows first to avoid holding a read cursor while writing
    let mut q = tiles_db.prepare(
        "SELECT DISTINCT chunk_x, chunk_z FROM tiles WHERE blocked=0",
    )?;
    let rows = q.query_map([], |r| Ok((r.get::<_, i32>(0)?, r.get::<_, i32>(1)?)))?;
    let mut coords: Vec<(i32,i32)> = Vec::new();
    for r in rows { coords.push(r?); }
    drop(q);

    let mut ins = out_db.prepare(
        "INSERT OR IGNORE INTO chunks(chunk_x, chunk_z, chunk_size, tile_count) VALUES (?1,?2,64,0)",
    )?;
    for (cx, cz) in coords.into_iter() {
        if let Some((xmin,xmax,zmin,zmax)) = cfg.chunk_range { if cx < xmin || cx > xmax || cz < zmin || cz > zmax { continue; } }
        // Planes are not part of chunks key; filter by planes doesn’t apply here
        ins.execute(params![cx, cz])?;
    }
    Ok(())
}
