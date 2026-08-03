//! Port of the parts of `src/3d/mapsquare.ts` that feed collision:
//! `getMapsquareData`, `TileGrid.addMapsquare` and the loc list.

use anyhow::{bail, Context, Result};
use std::sync::Arc;

use crate::cache::index::SubFileRange;
use crate::cache::reader::Reader;
use crate::cache::source::{CacheSource, MAJOR_MAPSQUARES};

pub const CHUNK_SIZE: i32 = 64;
pub const SQUARE_LEVELS: usize = 4;
pub const WORLD_STRIDE: i32 = 128;

/// Subfile ids inside a mapsquare archive (`cacheMapFiles`).
const FILE_LOCATIONS: u32 = 0;
const FILE_SQUARES: u32 = 3;
const FILE_SQUARE_NXT: u32 = 5;

/// One placed location, as read from the `l` file.
#[derive(Debug, Clone, Copy)]
pub struct LocUse {
    pub id: u32,
    /// Tile position relative to the mapsquare origin.
    pub x: i32,
    pub y: i32,
    pub plane: usize,
    pub loctype: u8,
    pub rotation: u8,
}

/// Everything a mapsquare contributes to the collision grid.
///
/// Heights, overlays and underlays are all rendering-only, so they are decoded
/// far enough to keep the stream aligned and then dropped.
pub struct SquareData {
    /// `TileProps.settings` per tile, indexed `level * 4096 + z * 64 + x`.
    pub settings: Vec<u8>,
    pub locs: Vec<LocUse>,
}

#[inline]
pub fn settings_index(level: usize, x: usize, z: usize) -> usize {
    level * (CHUNK_SIZE as usize * CHUNK_SIZE as usize) + z * CHUNK_SIZE as usize + x
}

/// Reads and decodes one mapsquare. Returns `None` when the square does not
/// exist, matching `getMapsquareData` returning null.
pub fn load_square(source: &CacheSource, chunkx: i32, chunkz: i32) -> Result<Option<Arc<SquareData>>> {
    // getMapsquareData indexes with `chunkx + chunkz * worldStride` without
    // range checking chunkx, so a padding neighbour at chunkx 128 aliases onto
    // mapsquare (0, chunkz + 1). That is reproduced here on purpose: the js
    // exporter's output depends on it at the world edges.
    let squareindex = chunkx + chunkz * WORLD_STRIDE;
    if squareindex < 0 {
        return Ok(None);
    }
    let squareindex = squareindex as u32;
    let table = source.open_table(MAJOR_MAPSQUARES)?;
    let (archive, ranges) = match table.archive(squareindex)? {
        Some(v) => v,
        None => return Ok(None),
    };

    let find = |id: u32| -> Option<SubFileRange> { ranges.iter().copied().find(|q| q.fileid == id) };

    let tilerange = match find(FILE_SQUARES) {
        Some(r) => r,
        // `subindices.indexOf(cacheMapFiles.squares) == -1` -> null
        None => return Ok(None),
    };

    let mut settings = decode_tiles(&archive[tilerange.start..tilerange.end])
        .with_context(|| format!("decoding tiles of mapsquare {},{}", chunkx, chunkz))?;

    if let Some(nxtrange) = find(FILE_SQUARE_NXT) {
        apply_nxt_tiles(&archive[nxtrange.start..nxtrange.end], &mut settings)
            .with_context(|| format!("decoding nxt tiles of mapsquare {},{}", chunkx, chunkz))?;
    }

    let locs = match find(FILE_LOCATIONS) {
        Some(r) => decode_locations(&archive[r.start..r.end])
            .with_context(|| format!("decoding locations of mapsquare {},{}", chunkx, chunkz))?,
        None => Vec::new(),
    };

    Ok(Some(Arc::new(SquareData { settings, locs })))
}

/// `mapsquare_tiles` for buildnr >= 936.
///
/// Produces the `settings` byte per tile; the js code only uses `tile.settings`
/// from this file once nxt data is present, but the file still has to be walked
/// to know where each tile's fields end.
fn decode_tiles(buf: &[u8]) -> Result<Vec<u8>> {
    let mut r = Reader::new(buf);
    // magic: ["tuple","uint","ubyte"] for buildnr >= 936
    r.skip(5)?;

    let mut settings = vec![0u8; 16384];
    // The file is ordered level-major, then x, then z (see addMapsquare's
    // tileindex = z + x * zsize, incremented by xsize * zsize per level).
    for i in 0..16384usize {
        let flags = r.ubyte()?;
        if flags & 0x1 != 0 {
            r.ubyte()?; // shape
            r.varushort()?; // overlay
        }
        let value = if flags & 0x2 != 0 { r.ubyte()? } else { 0 };
        if flags & 0x4 != 0 {
            r.varushort()?; // underlay
        }
        if flags & 0x8 != 0 {
            r.skip(2)?; // height (ushort for buildnr >= 936)
        }
        let level = i / 4096;
        let rem = i % 4096;
        let x = rem / 64;
        let z = rem % 64;
        settings[settings_index(level, x, z)] = value;
    }
    Ok(settings)
}

/// `mapsquare_tiles_nxt`: an opcode map with one 66x66 array per level.
///
/// When present it overrides `settings` with the remapped nxt flags, exactly as
/// `addMapsquare` does.
fn apply_nxt_tiles(buf: &[u8], settings: &mut [u8]) -> Result<()> {
    let mut r = Reader::new(buf);
    loop {
        if r.eof() {
            break;
        }
        let op = r.ubyte()?;
        if op == 0x6a {
            r.uint()?; // magic
            continue;
        }
        if op > 0x03 {
            bail!("unexpected opcode 0x{:02x} in nxt tile file", op);
        }
        let level = op as usize;
        for i in 0..4356usize {
            let flags = r.ubyte()?;
            r.skip(2)?; // height (ushort for buildnr >= 936)
            if flags & 0x1 != 0 {
                if flags & 0x10 != 0 {
                    r.skip(2)?; // waterheight
                }
                let underlay = r.varushort()?;
                if underlay != 0 {
                    r.skip(2)?; // underlaycolor
                }
                let overlay = r.varushort()?;
                if flags & 0x10 != 0 {
                    r.varushort()?; // overlay_under
                }
                if overlay != 0 {
                    r.ubyte()?; // shape
                }
                if overlay != 0 && flags & 0x10 != 0 {
                    r.varushort()?; // underlay_under
                }
            }
            // The array is 66x66 with a one tile border; only the inner 64x64
            // maps onto real tiles (nxtfloor[(x + 1) * 66 + z + 1]).
            let x = i / 66;
            let z = i % 66;
            if x == 0 || x > 64 || z == 0 || z > 64 {
                continue;
            }
            // 1 visible, 2 blocking, 4 bridge/flag2, 8 roofed, 32 forcedraw, 64 roofoverhang
            let mut newsettings = 0u8;
            if flags & 2 != 0 {
                newsettings |= 1;
            }
            if flags & 4 != 0 {
                newsettings |= 2;
            }
            if flags & 8 != 0 {
                newsettings |= 4;
            }
            if flags & 32 != 0 {
                newsettings |= 8;
            }
            if flags & 64 != 0 {
                newsettings |= 16;
            }
            if flags & 16 != 0 {
                newsettings |= 128; // water, a flag that doesn't exist in java
            }
            settings[settings_index(level, x - 1, z - 1)] = newsettings;
        }
    }
    Ok(())
}

/// `mapsquare_locations`: delta encoded ids, each with a delta encoded list of
/// placements, both terminated by a zero header.
fn decode_locations(buf: &[u8]) -> Result<Vec<LocUse>> {
    let mut r = Reader::new(buf);
    let mut locs = Vec::new();
    let mut idcounter: i64 = -1;
    loop {
        let header = r.tailed_varushort()?;
        if header == 0 {
            break;
        }
        idcounter += header as i64;
        let id = idcounter;

        let mut location: i64 = 0;
        loop {
            let usheader = r.varushort()?;
            if usheader == 0 {
                break;
            }
            location += usheader as i64 - 1;
            let loc = location as u32;
            let y = (loc & 0x3f) as i32;
            let x = ((loc >> 6) & 0x3f) as i32;
            let plane = ((loc >> 12) & 0x3) as usize;
            let data = r.ubyte()?;
            let rotation = data & 0x3;
            let loctype = (data >> 2) & 0x1f;
            if data & 0x80 != 0 {
                skip_loc_extra(&mut r)?;
            }
            locs.push(LocUse { id: id as u32, x, y, plane, loctype, rotation });
        }
    }
    Ok(locs)
}

fn skip_loc_extra(r: &mut Reader) -> Result<()> {
    let flags = r.ubyte()?;
    if flags & 0x01 != 0 {
        r.skip(8)?; // rotation: 4 shorts
    }
    for bit in 1..8 {
        if flags & (1 << bit) != 0 {
            r.skip(2)?;
        }
    }
    Ok(())
}
