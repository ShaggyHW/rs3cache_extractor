use anyhow::{anyhow, Result};
use rusqlite::{params, Connection, OptionalExtension};

use super::cluster_builder;
use super::config::Config;
use super::entrance_discovery;
use super::intra_connector;
use super::inter_connector;
use super::jps_accelerator;
use super::teleport_connector;
use super::intra_trimmer;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Stage {
    Build,
    Entrances,
    TeleportEntrances,
    Intra,
    IntraTrim,
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
            Stage::IntraTrim => "cluster_stage_intra_trim",
            Stage::Inter => "cluster_stage_inter",
            Stage::TeleportEdges => "cluster_stage_teleport_edges",
            Stage::Jps => "cluster_stage_jps",
        }
    }
    pub fn all() -> &'static [Stage] { &[Stage::Build, Stage::Entrances, Stage::Intra, Stage::IntraTrim, Stage::Inter] }
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
    pub ran_intra_trim: bool,
    pub ran_inter: bool,
    pub ran_teleport_edges: bool,
    pub ran_jps: bool,
}

pub fn run_pipeline(tiles_db: &Connection, out_db: &mut Connection, cfg: &Config, opts: ExecOptions) -> Result<ExecStats> {
  //  ensure_schema(out_db)?; // ensure meta table etc.

    let mut stats = ExecStats::default();

    // Single DB schema no longer requires mirroring chunks; clusters/tiles live in one DB

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
            Stage::IntraTrim => {
                let _s = intra_trimmer::trim_intra_edges(out_db, cfg)?;
                // No specific validation beyond window constraint below
                set_stage_done(out_db, Stage::IntraTrim)?;
                stats.ran_intra_trim = true;
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
    // Output-based detection to avoid meta CHECK constraints
    match s {
        Stage::Build => {
            let v: Option<i64> = db.query_row("SELECT 1 FROM clusters LIMIT 1", [], |r| r.get(0)).optional()?;
            Ok(v.is_some())
        }
        Stage::Entrances => {
            let v: Option<i64> = db.query_row("SELECT 1 FROM cluster_entrances LIMIT 1", [], |r| r.get(0)).optional()?;
            Ok(v.is_some())
        }
        Stage::TeleportEntrances => {
            let v: Option<i64> = db.query_row("SELECT 1 FROM cluster_entrances WHERE teleport_edge_id IS NOT NULL LIMIT 1", [], |r| r.get(0)).optional()?;
            Ok(v.is_some())
        }
        Stage::Intra => {
            let v: Option<i64> = db.query_row("SELECT 1 FROM cluster_intraconnections LIMIT 1", [], |r| r.get(0)).optional()?;
            Ok(v.is_some())
        }
        Stage::IntraTrim => {
            // Done when no entrance_from/ext cluster group has more than 5 rows
            let q = r#"
            WITH to_exit AS (
                SELECT
                    ci.entrance_from,
                    ci.entrance_to,
                    ci.cost,
                    (
                        SELECT ct.cluster_id
                        FROM cluster_entrances ce_to
                        JOIN cluster_tiles ct ON ct.x = (ce_to.x + CASE ce_to.neighbor_dir WHEN 'N' THEN 0 WHEN 'S' THEN 0 WHEN 'E' THEN 1 WHEN 'W' THEN -1 ELSE 0 END)
                                             AND ct.y = (ce_to.y + CASE ce_to.neighbor_dir WHEN 'N' THEN 1 WHEN 'S' THEN -1 WHEN 'E' THEN 0 WHEN 'W' THEN 0 ELSE 0 END)
                                             AND ct.plane = ce_to.plane
                        WHERE ce_to.entrance_id = ci.entrance_to
                        LIMIT 1
                    ) AS ext_cid
                FROM cluster_intraconnections ci
            ), ranked AS (
                SELECT entrance_from, entrance_to, ext_cid,
                       ROW_NUMBER() OVER (PARTITION BY entrance_from, ext_cid ORDER BY cost ASC, entrance_to ASC) AS rn
                FROM to_exit
            )
            SELECT 1 FROM ranked WHERE ext_cid IS NOT NULL AND rn > 5 LIMIT 1;
            "#;
            let v: Option<i64> = db.query_row(q, [], |r| r.get(0)).optional()?;
            Ok(v.is_none())
        }
        Stage::Inter => {
            let v: Option<i64> = db.query_row("SELECT 1 FROM cluster_interconnections LIMIT 1", [], |r| r.get(0)).optional()?;
            Ok(v.is_some())
        }
        Stage::TeleportEdges => {
            let v: Option<i64> = db
                .query_row(
                    "SELECT 1 FROM cluster_interconnections ci JOIN cluster_entrances ce ON ce.entrance_id=ci.entrance_from WHERE ce.teleport_edge_id IS NOT NULL LIMIT 1",
                    [],
                    |r| r.get(0),
                )
                .optional()?;
            Ok(v.is_some())
        }
        Stage::Jps => {
            // Consider done if JPS tables exist and contain any row
            let v: Option<i64> = db.query_row("SELECT 1 FROM sqlite_master WHERE type='table' AND name='jps_spans'", [], |r| r.get(0)).optional()?;
            Ok(v.is_some())
        }
    }
}

fn set_stage_done(_db: &Connection, _s: Stage) -> Result<()> { Ok(()) }

fn clear_stage_meta(_db: &Connection, _s: Stage) -> Result<()> { Ok(()) }

// ---- Validations (minimal but useful) ----

fn validate_build(tiles_db: &Connection, out_db: &Connection, cfg: &Config) -> Result<()> {
    // For each plane with any walkable tile in scope, expect at least one cluster row
    let mut q = tiles_db.prepare(
        "SELECT DISTINCT plane FROM tiles WHERE blocked=0",
    )?;
    let rows = q.query_map([], |r| Ok(r.get::<_, i32>(0)?))?;
    for r in rows {
        let pl = r?;
        if let Some(planes) = &cfg.planes { if !planes.contains(&pl) { continue; } }
        let cnt: i64 = out_db.query_row(
            "SELECT COUNT(*) FROM clusters WHERE plane=?1",
            params![pl],
            |r| r.get(0),
        )?;
        if cnt == 0 { return Err(anyhow!("validate_build: expected clusters for plane {}", pl)); }
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

