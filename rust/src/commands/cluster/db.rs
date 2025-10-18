use anyhow::Result;
use rusqlite::{Connection, OpenFlags, Transaction, TransactionBehavior};
use std::path::Path;
use std::time::Duration;

pub fn open_ro<P: AsRef<Path>>(path: P) -> Result<Connection> {
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    // Wait a bit for locks to clear when writer holds the DB
    conn.busy_timeout(Duration::from_millis(5000))?;
    Ok(conn)
}

pub fn open_rw<P: AsRef<Path>>(path: P) -> Result<Connection> {
    let mut conn = Connection::open(path)?;
    // Allow some wait time for concurrent readers/writers
    conn.busy_timeout(Duration::from_millis(5000))?;
    // Enable WAL for concurrent reads while writing; relax sync for speed, enforce FKs
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA foreign_keys=ON;")?;
    Ok(conn)
}

pub fn ensure_schema(conn: &mut Connection) -> Result<()> {
    crate::db::create_tables(conn)
}

pub fn with_tx<T, F: FnOnce(&Transaction) -> Result<T>>(conn: &mut Connection, f: F) -> Result<T> {
    // IMMEDIATE to acquire a reserved lock up-front, reducing mid-transaction lock errors
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let out = f(&tx)?;
    tx.commit()?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::OptionalExtension;
    use tempfile::NamedTempFile;

    #[test]
    fn ensure_schema_creates_required_tables() -> Result<()> {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path();
        let mut conn = open_rw(path)?;
        ensure_schema(&mut conn)?;

        // Verify a few key tables
        for t in [
            "chunks",
            "tiles",
            "chunk_clusters",
            "cluster_entrances",
            "cluster_interconnections",
            "cluster_intraconnections",
        ] {
            let exists: Option<i64> = conn
                .query_row(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1",
                    [t],
                    |row| row.get(0),
                )
                .optional()?;
            assert!(exists.is_some(), "expected table {} to exist", t);
        }

        Ok(())
    }
}
