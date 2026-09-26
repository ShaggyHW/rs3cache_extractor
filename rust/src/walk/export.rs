//! Port of `exportWalkFlags` (`src/scripts/exportwalk.ts`).
//!
//! The derived per-tile flags feed two sinks: the json files the js version
//! wrote, and direct rows for `tiles.db`. Both go through [`tile_flags`] so the
//! two can never drift apart.

use super::grid::Grid;
use super::mapsquare::{CHUNK_SIZE, SQUARE_LEVELS};

/// Neighbour offsets in `dirMap` order, with the index of the opposite
/// direction in the neighbour's own walk mask.
const DELTAS: [(i32, i32, u32); 8] = [
    (-1, 0, 3),
    (0, -1, 4),
    (1, 0, 1),
    (0, 1, 2),
    (-1, 1, 7),
    (-1, -1, 8),
    (1, -1, 5),
    (1, 1, 6),
];

/// A diagonal is only walkable if both adjacent straight directions are.
const DIAGONAL_DEPS: [&[usize]; 8] = [&[], &[], &[], &[], &[0, 3], &[0, 1], &[2, 1], &[2, 3]];

const WALK_KEYS: [&str; 8] = [
    "left",
    "bottom",
    "right",
    "top",
    "topleft",
    "bottomleft",
    "bottomright",
    "topright",
];

pub struct TileFlags {
    pub settings: u8,
    pub center_blocked: bool,
    pub allowed_mask: u8,
    pub blocked_mask: u8,
    pub alloweds: [bool; 8],
}

/// Resolves one tile's walkability, or `None` when the tile does not exist.
#[inline]
fn tile_flags(grid: &Grid, gx: i32, gz: i32, plane: i32) -> Option<TileFlags> {
    let col = grid.walk_mask_at(gx, gz, plane)?;
    let settings = grid.settings_at(gx, gz, plane).unwrap_or(0);

    let mut allowed_mask: u8 = 0;
    let mut blocked_mask: u8 = 0;
    let mut alloweds = [false; 8];

    for i in 0..8usize {
        let bit = 1u8 << i;
        let mut blocked = col & (1 << (i + 1)) != 0;
        let (ddx, ddz, opp) = DELTAS[i];
        if let Some(ncol) = grid.walk_mask_at(gx + ddx, gz + ddz, plane) {
            if ncol & 1 != 0 || ncol & (1 << opp) != 0 {
                blocked = true;
            }
        }
        if !blocked {
            for &dep in DIAGONAL_DEPS[i] {
                if !alloweds[dep] {
                    blocked = true;
                    break;
                }
            }
        }
        if blocked {
            blocked_mask |= bit;
        } else {
            allowed_mask |= bit;
            alloweds[i] = true;
        }
    }

    Some(TileFlags {
        settings,
        center_blocked: col & 1 != 0,
        allowed_mask,
        blocked_mask,
        alloweds,
    })
}

/// Visits every tile of a mapsquare in the js iteration order: plane, then z,
/// then x.
#[inline]
fn for_each_tile(grid: &Grid, chunkx: i32, chunkz: i32, mut f: impl FnMut(i32, i32, i32, TileFlags)) {
    let rectx = chunkx * CHUNK_SIZE;
    let rectz = chunkz * CHUNK_SIZE;
    for plane in 0..SQUARE_LEVELS as i32 {
        for dz in 0..CHUNK_SIZE {
            for dx in 0..CHUNK_SIZE {
                let gx = rectx + dx;
                let gz = rectz + dz;
                if let Some(flags) = tile_flags(grid, gx, gz, plane) {
                    f(gx, gz, plane, flags);
                }
            }
        }
    }
}

/// Tiles per mapsquare: `SQUARE_LEVELS` levels of `CHUNK_SIZE` x `CHUNK_SIZE`.
pub const SQUARE_TILES: usize = SQUARE_LEVELS * (CHUNK_SIZE * CHUNK_SIZE) as usize;

/// Index of a tile in a [`collect_masks`] array.
#[inline]
pub fn mask_index(plane: usize, lx: usize, lz: usize) -> usize {
    plane * (CHUNK_SIZE * CHUNK_SIZE) as usize + lz * CHUNK_SIZE as usize + lx
}

/// The `walkMask` of every tile of one mapsquare, which is all `tiles.db`
/// keeps of it, indexed by [`mask_index`]. Tiles that do not exist are listed
/// in `missing` (ascending); a mapsquare normally contributes all of its tiles.
pub fn collect_masks(
    grid: &Grid,
    chunkx: i32,
    chunkz: i32,
    masks: &mut [u8; SQUARE_TILES],
    missing: &mut Vec<u16>,
) {
    let rectx = chunkx * CHUNK_SIZE;
    let rectz = chunkz * CHUNK_SIZE;
    for plane in 0..SQUARE_LEVELS {
        for dz in 0..CHUNK_SIZE {
            for dx in 0..CHUNK_SIZE {
                let idx = mask_index(plane, dx as usize, dz as usize);
                match tile_flags(grid, rectx + dx, rectz + dz, plane as i32) {
                    Some(flags) => masks[idx] = flags.allowed_mask,
                    None => {
                        masks[idx] = 0;
                        missing.push(idx as u16);
                    }
                }
            }
        }
    }
    // The loop visits indices in ascending order already.
    debug_assert!(missing.windows(2).all(|w| w[0] < w[1]));
}

#[inline]
fn push_int(out: &mut String, mut v: i64) {
    if v < 0 {
        out.push('-');
        v = -v;
    }
    let mut buf = [0u8; 20];
    let mut i = buf.len();
    loop {
        i -= 1;
        buf[i] = b'0' + (v % 10) as u8;
        v /= 10;
        if v == 0 {
            break;
        }
    }
    out.push_str(std::str::from_utf8(&buf[i..]).unwrap());
}

#[inline]
fn push_bool(out: &mut String, v: bool) {
    out.push_str(if v { "true" } else { "false" });
}

/// Serialises one mapsquare in exactly the shape `JSON.stringify` produces for
/// the js exporter, so both outputs can be diffed byte for byte.
pub fn render_chunk(grid: &Grid, chunkx: i32, chunkz: i32) -> (String, usize) {
    let mut out = String::with_capacity(4 << 20);
    out.push_str("{\"chunk\":{\"x\":");
    push_int(&mut out, chunkx as i64);
    out.push_str(",\"z\":");
    push_int(&mut out, chunkz as i64);
    out.push_str(",\"chunkSize\":");
    push_int(&mut out, CHUNK_SIZE as i64);
    out.push_str("},\"tiles\":[");

    let mut count = 0usize;
    for_each_tile(grid, chunkx, chunkz, |gx, gz, plane, flags| {
        if count > 0 {
            out.push(',');
        }
        count += 1;

        out.push_str("{\"x\":");
        push_int(&mut out, gx as i64);
        out.push_str(",\"y\":");
        push_int(&mut out, gz as i64);
        out.push_str(",\"plane\":");
        push_int(&mut out, plane as i64);
        out.push_str(",\"flag\":");
        push_int(&mut out, flags.settings as i64);
        out.push_str(",\"blocked\":");
        push_bool(&mut out, flags.center_blocked);
        out.push_str(",\"walkMask\":");
        push_int(&mut out, flags.allowed_mask as i64);
        out.push_str(",\"blockedMask\":");
        push_int(&mut out, flags.blocked_mask as i64);
        out.push_str(",\"walk\":{");
        for (i, key) in WALK_KEYS.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            out.push('"');
            out.push_str(key);
            out.push_str("\":");
            push_bool(&mut out, flags.alloweds[i]);
        }
        out.push_str("}}");
    });

    out.push_str("]}");
    (out, count)
}
