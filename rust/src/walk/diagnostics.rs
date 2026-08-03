//! Console + file diagnostics for the walkflags extraction.
//!
//! The js exporter warned on every malformed opcode, unresolvable loc and
//! failed mapsquare (`console.warn` in `opcode_reader.ts` / `mapsquare.ts`,
//! `output.log` in `exportwalk.ts`). This keeps the same signal, but since the
//! rust version decodes each object exactly once instead of once per
//! placement, repeats are counted rather than reprinted.

use anyhow::{Context, Result};
use std::collections::BTreeMap;
use std::fmt::Display;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Mutex;

/// Categories are printed in this order in the summary.
pub const KIND_CACHE: &str = "cache read";
pub const KIND_MAPSQUARE: &str = "mapsquare decode";
pub const KIND_OBJECT: &str = "object decode";
pub const KIND_OPCODE: &str = "object unknown opcode";
pub const KIND_TRUNCATED: &str = "object missing terminator";
pub const KIND_MORPH: &str = "loc morph unresolved";
pub const KIND_LOC_DROPPED: &str = "loc dropped (no definition)";
pub const KIND_WRITE: &str = "output write";

/// How many messages of one kind reach the console before it goes quiet. The
/// log file always gets every one.
const CONSOLE_LIMIT: usize = 25;

struct Inner {
    counts: BTreeMap<String, usize>,
    file: Option<BufWriter<File>>,
}

pub struct Diagnostics {
    inner: Mutex<Inner>,
    log_path: Option<String>,
}

impl Diagnostics {
    pub fn new(log_path: Option<&Path>) -> Result<Self> {
        let file = match log_path {
            Some(path) => Some(BufWriter::new(
                File::create(path)
                    .with_context(|| format!("creating log file {}", path.display()))?,
            )),
            None => None,
        };
        Ok(Diagnostics {
            inner: Mutex::new(Inner { counts: BTreeMap::new(), file }),
            log_path: log_path.map(|p| p.display().to_string()),
        })
    }

    pub fn log_path(&self) -> Option<&str> {
        self.log_path.as_deref()
    }

    /// Records one problem: always counted, always written to the log file,
    /// printed to stderr until this kind hits [`CONSOLE_LIMIT`].
    pub fn warn(&self, kind: &str, message: impl Display) {
        let mut inner = self.inner.lock().unwrap();
        let count = inner.counts.entry(kind.to_string()).or_insert(0);
        *count += 1;
        let seen = *count;

        let line = format!("[{}] {}", kind, message);
        if let Some(file) = inner.file.as_mut() {
            let _ = writeln!(file, "{}", line);
        }
        if seen <= CONSOLE_LIMIT {
            eprintln!("{}", line);
            if seen == CONSOLE_LIMIT {
                let tail = match self.log_path.as_deref() {
                    Some(path) => format!("; further ones go to {} only", path),
                    None => String::new(),
                };
                eprintln!(
                    "[{}] reached {} console messages, suppressing the rest{}",
                    kind, CONSOLE_LIMIT, tail
                );
            }
        }
    }

    /// Writes a line to the log file only, without counting it as a problem.
    pub fn note(&self, message: impl Display) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(file) = inner.file.as_mut() {
            let _ = writeln!(file, "[note] {}", message);
        }
    }

    /// Records a problem without any console output; for per-occurrence noise
    /// that is only meaningful in aggregate.
    pub fn count(&self, kind: &str, n: usize) {
        if n == 0 {
            return;
        }
        let mut inner = self.inner.lock().unwrap();
        *inner.counts.entry(kind.to_string()).or_insert(0) += n;
    }

    pub fn total(&self) -> usize {
        self.inner.lock().unwrap().counts.values().sum()
    }

    /// Prints the per-kind totals and flushes the log file.
    pub fn finish(&self) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let counts: Vec<(String, usize)> =
            inner.counts.iter().map(|(k, v)| (k.clone(), *v)).collect();

        if counts.is_empty() {
            println!("diagnostics: no problems reported");
        } else {
            println!("diagnostics:");
            for (kind, count) in &counts {
                let line = format!("  {:<32} {}", kind, count);
                println!("{}", line);
                if let Some(file) = inner.file.as_mut() {
                    let _ = writeln!(file, "[summary] {} = {}", kind, count);
                }
            }
            if let Some(path) = self.log_path.as_deref() {
                println!("  full detail written to {}", path);
            }
        }
        if let Some(file) = inner.file.as_mut() {
            file.flush()?;
        }
        Ok(())
    }
}
