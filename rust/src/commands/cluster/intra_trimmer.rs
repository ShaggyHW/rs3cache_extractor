use anyhow::Result;
use rusqlite::{Connection, OptionalExtension};

use super::config::Config;
use super::db::with_tx;

#[derive(Clone, Debug, Default)]
pub struct TrimStats {
    pub rows_before: i64,
    pub rows_to_delete: i64,
    pub rows_after: i64,
}

pub fn trim_intra_edges(out_db: &mut Connection, cfg: &Config) -> Result<TrimStats> {
    println!("[intra_trimmer] Starting trim_intra_edges; dry_run={}", cfg.dry_run);
    let rows_before: i64 = out_db.query_row(
        "SELECT COUNT(*) FROM cluster_intraconnections",
        [],
        |r| r.get(0),
    )?;
    println!("[intra_trimmer] rows_before={}", rows_before);

    // Count how many would be deleted
    let count_sql = r#"
    WITH to_exit AS (
        SELECT
            ci.rowid AS ci_rowid,
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
        SELECT ci_rowid, entrance_from, entrance_to, cost, ext_cid,
               ROW_NUMBER() OVER (PARTITION BY entrance_from, ext_cid ORDER BY cost ASC, entrance_to ASC) AS rn
        FROM to_exit
    )
    SELECT COUNT(*) FROM ranked WHERE ext_cid IS NOT NULL AND rn > 1;
    "#;

    let rows_to_delete: i64 = out_db.query_row(count_sql, [], |r| r.get(0))?;
    println!("[intra_trimmer] rows_to_delete={}", rows_to_delete);

    if cfg.dry_run || rows_to_delete == 0 {
        let rows_after = rows_before - rows_to_delete;
        if cfg.dry_run {
            println!(
                "[intra_trimmer] Dry run; skipping deletion. rows_after would be {}",
                rows_after
            );
        } else {
            println!(
                "[intra_trimmer] rows_to_delete is zero; no deletion needed. rows_after remains {}",
                rows_after
            );
        }
        return Ok(TrimStats { rows_before, rows_to_delete, rows_after });
    }

    println!("[intra_trimmer] Executing deletion transaction for {} rows", rows_to_delete);
    with_tx(out_db, |tx| {
        let del_sql = r#"
        WITH to_exit AS (
            SELECT
                ci.rowid AS ci_rowid,
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
            SELECT ci_rowid, entrance_from, entrance_to, cost, ext_cid,
                   ROW_NUMBER() OVER (PARTITION BY entrance_from, ext_cid ORDER BY cost ASC, entrance_to ASC) AS rn
            FROM to_exit
        )
        DELETE FROM cluster_intraconnections
        WHERE rowid IN (
            SELECT ci_rowid FROM ranked WHERE ext_cid IS NOT NULL AND rn > 1
        );
        "#;
        tx.execute_batch(del_sql)?;
        println!("[intra_trimmer] Deletion transaction executed");
        Ok(())
    })?;

    let rows_after: i64 = out_db.query_row(
        "SELECT COUNT(*) FROM cluster_intraconnections",
        [],
        |r| r.get(0),
    )?;
    println!(
        "[intra_trimmer] Finished trim_intra_edges; rows_before={}, rows_to_delete={}, rows_after={}",
        rows_before, rows_to_delete, rows_after
    );

    Ok(TrimStats { rows_before, rows_to_delete, rows_after })
}
