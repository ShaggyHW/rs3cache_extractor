//! Port of the js5 index format (`src/opcodes/cacheindex.json`) and the sqlite
//! archive layout from `src/cache/index.ts`.

use anyhow::{bail, Result};

use super::reader::Reader;

#[derive(Debug, Clone)]
#[allow(dead_code)] // crc/version/name are part of the format, not yet needed here
pub struct CacheIndex {
    pub minor: u32,
    pub crc: u32,
    pub version: u32,
    pub name: Option<u32>,
    pub subindices: Vec<u32>,
}

/// Parses a decompressed `cache_index` blob into one entry per minor id.
///
/// The result is indexed by minor id, with gaps left as `None`.
pub fn parse_index(buf: &[u8]) -> Result<Vec<Option<CacheIndex>>> {
    let mut r = Reader::new(buf);
    let format = r.ubyte()?;
    if format >= 6 {
        r.uint()?; // timestamp
    }
    let flags = r.ubyte()?;
    let has_names = flags & 0x1 != 0;
    let has_hashes = flags & 0x2 != 0;
    let has_encryption = flags & 0x4 != 0;

    let count = if format >= 7 { r.varuint()? } else { r.ushort()? as u32 } as usize;

    // The index is a "chunked array": every field is stored as one column
    // spanning all entries, rather than row by row.
    let mut minors = Vec::with_capacity(count);
    let mut acc: i64 = 0;
    for _ in 0..count {
        acc += r.ushort()? as i64;
        minors.push(acc as u32);
    }

    let mut names = vec![None; count];
    if has_names {
        for name in names.iter_mut() {
            *name = Some(r.uint()?);
        }
    }

    let mut crcs = Vec::with_capacity(count);
    for _ in 0..count {
        crcs.push(r.uint()?);
    }

    if has_encryption {
        // uncompressed_crc, then size + uncompressed_size
        r.skip(count * 4)?;
        r.skip(count * 8)?;
    }
    if has_hashes {
        r.skip(count * 64)?;
    }

    let mut versions = Vec::with_capacity(count);
    for _ in 0..count {
        versions.push(r.uint()?);
    }

    let mut subcounts = Vec::with_capacity(count);
    for _ in 0..count {
        subcounts.push(r.varuint()? as usize);
    }

    let mut subindices = Vec::with_capacity(count);
    for &subcount in &subcounts {
        let mut acc: i64 = 0;
        let mut subs = Vec::with_capacity(subcount);
        for _ in 0..subcount {
            acc += r.ushort()? as i64;
            subs.push(acc as u32);
        }
        subindices.push(subs);
    }

    if has_names {
        for &subcount in &subcounts {
            r.skip(subcount * 4)?;
        }
    }

    let maxminor = minors.iter().copied().max().unwrap_or(0) as usize;
    let mut out: Vec<Option<CacheIndex>> = vec![None; maxminor + 1];
    for i in 0..count {
        out[minors[i] as usize] = Some(CacheIndex {
            minor: minors[i],
            crc: crcs[i],
            version: versions[i],
            name: names[i],
            subindices: std::mem::take(&mut subindices[i]),
        });
    }
    Ok(out)
}

/// Byte range of one subfile inside a decompressed archive.
#[derive(Debug, Clone, Copy)]
pub struct SubFileRange {
    pub fileid: u32,
    pub start: usize,
    pub end: usize,
}

/// Splits a decompressed sqlite archive into its subfiles.
///
/// Layout is one unknown byte, then `subids.len() + 1` big endian offsets: the
/// first is the start of file 0, each following one is the end of a file.
pub fn unpack_sqlite_archive(buf: &[u8], subids: &[u32]) -> Result<Vec<SubFileRange>> {
    if subids.len() == 1 {
        return Ok(vec![SubFileRange { fileid: subids[0], start: 0, end: buf.len() }]);
    }
    let needed = 1 + 4 + subids.len() * 4;
    if buf.len() < needed {
        bail!("truncated sqlite archive: {} bytes, need {}", buf.len(), needed);
    }
    let mut r = Reader::new(buf);
    r.ubyte()?; // unknown
    let mut fileoffset = r.uint()? as usize;
    let mut files = Vec::with_capacity(subids.len());
    for &fileid in subids {
        let endoffset = r.uint()? as usize;
        if fileoffset > endoffset || endoffset > buf.len() {
            bail!("sqlite archive subfile out of range: {}..{} of {}", fileoffset, endoffset, buf.len());
        }
        files.push(SubFileRange { fileid, start: fileoffset, end: endoffset });
        fileoffset = endoffset;
    }
    Ok(files)
}
