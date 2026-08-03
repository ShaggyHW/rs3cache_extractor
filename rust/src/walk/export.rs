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

/// One `tiles` row, with the primary key packed so a column's worth of rows can
/// be sorted into `(x, y, plane)` order cheaply.
#[derive(Clone, Copy)]
pub struct TileRow {
    pub key: u64,
    pub walk_mask: u8,
}

impl TileRow {
    #[inline]
    pub fn x(&self) -> i64 {
        (self.key >> 32) as i64
    }
    #[inline]
    pub fn y(&self) -> i64 {
        ((self.key >> 8) & 0xff_ffff) as i64
    }
    #[inline]
    pub fn plane(&self) -> i64 {
        (self.key & 0xff) as i64
    }
    /// `regionId = (regionX << 8) + regionY`, matching `load-tiles`.
    #[inline]
    pub fn region_id(&self) -> i64 {
        ((self.x() >> 6) << 8) + (self.y() >> 6)
    }
}

/// Collects the rows `load-tiles` would have derived from this mapsquare's json.
pub fn collect_rows(grid: &Grid, chunkx: i32, chunkz: i32, out: &mut Vec<TileRow>) {
    for_each_tile(grid, chunkx, chunkz, |gx, gz, plane, flags| {
        out.push(TileRow {
            key: ((gx as u64) << 32) | ((gz as u64) << 8) | plane as u64,
            walk_mask: flags.allowed_mask,
        });
    });
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
