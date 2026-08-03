//! Port of `src/cache/compression.ts` (read path only).

use anyhow::{bail, Context, Result};
use flate2::read::{GzDecoder, ZlibDecoder};
use std::io::Read;

/// Decompresses a container as it is stored in the cache.
///
/// NXT sqlite caches only ever use the 0x5a variant, the others are here so a
/// downloaded/openrs2 container would not silently fall over.
pub fn decompress(input: &[u8]) -> Result<Vec<u8>> {
    if input.is_empty() {
        bail!("empty compressed buffer");
    }
    match input[0] {
        0 => uncompressed(input),
        1 => bail!("bzip2 compressed cache containers are not supported by the rust extractor"),
        2 => gzip(input),
        3 => bail!("lzma compressed cache containers are not supported by the rust extractor"),
        0x5a => zlib_sqlite(input),
        other => bail!("unknown compression type ({})", other),
    }
}

fn read_u32(buf: &[u8], at: usize) -> Result<u32> {
    if at + 4 > buf.len() {
        bail!("truncated container header");
    }
    Ok(u32::from_be_bytes([buf[at], buf[at + 1], buf[at + 2], buf[at + 3]]))
}

fn uncompressed(input: &[u8]) -> Result<Vec<u8>> {
    let size = read_u32(input, 1)? as usize;
    if 5 + size > input.len() {
        bail!("truncated uncompressed container");
    }
    Ok(input[5..5 + size].to_vec())
}

fn gzip(input: &[u8]) -> Result<Vec<u8>> {
    let compressed_size = read_u32(input, 1)? as usize;
    if 9 + compressed_size > input.len() {
        bail!("truncated gzip container");
    }
    let mut out = Vec::new();
    GzDecoder::new(&input[9..9 + compressed_size])
        .read_to_end(&mut out)
        .context("gzip decompress failed (possibly a missing or wrong xtea key)")?;
    Ok(out)
}

/// `5a4c4201` magic, big endian uncompressed size, then a raw zlib stream.
fn zlib_sqlite(input: &[u8]) -> Result<Vec<u8>> {
    if input.len() < 8 {
        bail!("truncated sqlite zlib container");
    }
    let uncompressed_size = read_u32(input, 4)? as usize;
    let mut out = Vec::with_capacity(uncompressed_size);
    ZlibDecoder::new(&input[8..])
        .read_to_end(&mut out)
        .context("sqlite zlib decompress failed")?;
    Ok(out)
}
