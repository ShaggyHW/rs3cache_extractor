//! Port of `GameCacheLoader` from `src/cache/sqlite.ts`: reads NXT
//! `js5-<major>.jcache` sqlite files directly.

use anyhow::{bail, Context, Result};
use rusqlite::{Connection, OpenFlags};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use super::compression::decompress;
use super::index::{parse_index, unpack_sqlite_archive, CacheIndex, SubFileRange};

#[allow(dead_code)]
pub const MAJOR_CONFIG: u32 = 2;
pub const MAJOR_MAPSQUARES: u32 = 5;
pub const MAJOR_OBJECTS: u32 = 16;

/// Archive size used to map a flat file id onto (minor, subid) for major 16.
pub const OBJECTS_PER_ARCHIVE: u32 = 256;

struct Preloaded {
    /// Compressed blob and the `VERSION` column, indexed by key.
    blobs: Vec<Option<(Box<[u8]>, u32)>>,
    /// Keys the scan could not take (odd keys, NULL data); these still go
    /// through the per-key query so they fail exactly as before.
    unreadable: Vec<i64>,
}

pub struct CacheTable {
    conn: Mutex<Connection>,
    pub index: Vec<Option<CacheIndex>>,
    /// Every compressed blob, indexed by key, once [`CacheTable::preload`] ran.
    preloaded: OnceLock<Preloaded>,
}

impl CacheTable {
    /// Raw (still compressed) blob for a minor id, with its stored version.
    pub fn raw(&self, minor: u32) -> Result<Option<(Vec<u8>, u32)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached("SELECT DATA, VERSION FROM cache WHERE KEY=?1")?;
        let mut rows = stmt.query([minor])?;
        match rows.next()? {
            Some(row) => Ok(Some((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)? as u32))),
            None => Ok(None),
        }
    }

    /// The client updates `cache_index` first and fetches the archives it names
    /// later, lazily. Until then a row holds whatever it had before, often an
    /// empty placeholder, so any row whose version differs from the index is
    /// not the current file and must not be decoded as if it were.
    fn check_current(&self, minor: u32, stored: Option<u32>) -> Result<()> {
        let Some(entry) = self.index.get(minor as usize).and_then(|q| q.as_ref()) else {
            return Ok(());
        };
        match stored {
            Some(version) if version == entry.version => Ok(()),
            Some(version) => bail!(
                "archive {} is out of date in this cache (stored version {}, index wants {}); \
                 the game client has not downloaded it since the last update",
                minor,
                version,
                entry.version
            ),
            None => bail!(
                "archive {} is listed in the index but not downloaded to this cache yet",
                minor
            ),
        }
    }

    /// Reads the whole table into memory in one scan, so later lookups never
    /// touch the file. For the mapsquares that is ~55 MB compressed; holding it
    /// is what keeps the lookups fast while a large database is being written,
    /// because under memory pressure the kernel evicts the cache file's pages
    /// to make room for the output and every lookup then queues behind the
    /// writes.
    pub fn preload(&self) -> Result<()> {
        if self.preloaded.get().is_some() {
            return Ok(());
        }
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT KEY, DATA, VERSION FROM cache")?;
        let mut rows = stmt.query([])?;
        let mut blobs: Vec<Option<(Box<[u8]>, u32)>> = Vec::new();
        let mut unreadable = Vec::new();
        while let Some(row) = rows.next()? {
            let key: i64 = row.get(0)?;
            let (Ok(ukey), Ok(data), Ok(version)) =
                (usize::try_from(key), row.get::<_, Vec<u8>>(1), row.get::<_, i64>(2))
            else {
                unreadable.push(key);
                continue;
            };
            let key = ukey;
            if blobs.len() <= key {
                blobs.resize(key + 1, None);
            }
            blobs[key] = Some((data.into_boxed_slice(), version as u32));
        }
        let _ = self.preloaded.set(Preloaded { blobs, unreadable });
        Ok(())
    }

    pub fn file(&self, minor: u32) -> Result<Option<Vec<u8>>> {
        if let Some(pre) = self.preloaded.get() {
            match pre.blobs.get(minor as usize) {
                Some(Some((raw, version))) => {
                    self.check_current(minor, Some(*version))?;
                    return Ok(Some(decompress(raw)?));
                }
                _ if !pre.unreadable.contains(&(minor as i64)) => {
                    self.check_current(minor, None)?;
                    return Ok(None);
                }
                _ => {}
            }
        }
        match self.raw(minor)? {
            Some((raw, version)) => {
                self.check_current(minor, Some(version))?;
                Ok(Some(decompress(&raw)?))
            }
            None => {
                self.check_current(minor, None)?;
                Ok(None)
            }
        }
    }

    /// Decompressed archive plus the byte ranges of its subfiles.
    pub fn archive(&self, minor: u32) -> Result<Option<(Vec<u8>, Vec<SubFileRange>)>> {
        let entry = match self.index.get(minor as usize).and_then(|q| q.as_ref()) {
            Some(entry) => entry,
            None => return Ok(None),
        };
        let file = match self.file(minor)? {
            Some(file) => file,
            None => return Ok(None),
        };
        let ranges = unpack_sqlite_archive(&file, &entry.subindices)
            .with_context(|| format!("unpacking archive {}", minor))?;
        Ok(Some((file, ranges)))
    }
}

pub struct CacheSource {
    dir: PathBuf,
    tables: Mutex<HashMap<u32, Arc<CacheTable>>>,
}

impl CacheSource {
    pub fn new(dir: &Path) -> Result<Self> {
        if !dir.is_dir() {
            bail!("cache directory {} does not exist", dir.display());
        }
        Ok(CacheSource { dir: dir.to_path_buf(), tables: Mutex::new(HashMap::new()) })
    }

    pub fn open_table(&self, major: u32) -> Result<Arc<CacheTable>> {
        if let Some(table) = self.tables.lock().unwrap().get(&major) {
            return Ok(table.clone());
        }
        let table = Arc::new(self.load_table(major)?);
        self.tables.lock().unwrap().insert(major, table.clone());
        Ok(table)
    }

    fn load_table(&self, major: u32) -> Result<CacheTable> {
        let dbfile = self.dir.join(format!("js5-{}.jcache", major));
        if !dbfile.exists() {
            bail!("cache index {} does not exist at {}", major, dbfile.display());
        }
        prewarm(&dbfile);
        let conn = Connection::open_with_flags(
            &dbfile,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .with_context(|| format!("opening {}", dbfile.display()))?;

        let raw: Vec<u8> = conn
            .query_row("SELECT DATA FROM cache_index", [], |row| row.get(0))
            .with_context(|| format!("reading cache_index of {}", dbfile.display()))?;
        let indexfile = decompress(&raw)?;
        let index = parse_index(&indexfile)
            .with_context(|| format!("parsing cache_index of {}", dbfile.display()))?;
        warn_if_outdated(&conn, &index, &dbfile)?;

        Ok(CacheTable { conn: Mutex::new(conn), index, preloaded: OnceLock::new() })
    }
}

/// One up-front line when the client has only partly applied an update, so a
/// run against such a cache says why instead of just failing square by square.
fn warn_if_outdated(conn: &Connection, index: &[Option<CacheIndex>], dbfile: &Path) -> Result<()> {
    let mut stored: HashMap<u32, u32> = HashMap::new();
    let mut stmt = conn.prepare("SELECT KEY, VERSION FROM cache")?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        if let (Ok(key), Ok(version)) = (row.get::<_, i64>(0), row.get::<_, i64>(1)) {
            stored.insert(key as u32, version as u32);
        }
    }
    let (mut stale, mut missing) = (0, 0);
    for entry in index.iter().flatten() {
        match stored.get(&entry.minor) {
            Some(&version) if version == entry.version => {}
            Some(_) => stale += 1,
            None => missing += 1,
        }
    }
    if stale + missing > 0 {
        eprintln!(
            "warning: {} is only partly updated: {} archives out of date, {} not downloaded, \
             of {} in its index. Those will fail to load; let the game client finish \
             downloading, or point --cache at an up to date cache.",
            dbfile.display(),
            stale,
            missing,
            index.iter().flatten().count()
        );
    }
    Ok(())
}

/// Reads a cache file front to back once so the page cache holds it before the
/// random per-key lookups start. On a cold cache this is one sequential read
/// instead of thousands of scattered ones, which on a busy disk is the
/// difference between well under a second and most of a minute; on a warm
/// cache it costs a few milliseconds. Failures are ignored: sqlite reports any
/// real problem with the file itself.
fn prewarm(path: &Path) {
    use std::io::Read;
    if let Ok(mut file) = std::fs::File::open(path) {
        let mut buf = vec![0u8; 4 << 20];
        while matches!(file.read(&mut buf), Ok(n) if n > 0) {}
    }
}
