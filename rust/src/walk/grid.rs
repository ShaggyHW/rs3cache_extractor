//! Collision-only port of `TileGrid` plus the collision half of
//! `blendUnderlays` and `mapsquareObjects`.

use std::sync::Arc;

use super::mapsquare::{settings_index, LocUse, SquareData, CHUNK_SIZE, SQUARE_LEVELS};
use super::objdef::ObjectDef;

/// Same padding `parseMapsquare` uses when `padfloor` is set.
pub const FLOOR_PADDING: i32 = 20;
pub const GRID_SIZE: i32 = CHUNK_SIZE + FLOOR_PADDING * 2;

const XSIZE: usize = GRID_SIZE as usize;
const ZSIZE: usize = GRID_SIZE as usize;
const LEVELSTEP: usize = XSIZE * ZSIZE;
const CELLS: usize = LEVELSTEP * SQUARE_LEVELS;

/// Walk/sight blocking is stored as a 9 bit mask instead of the js `boolean[9]`.
/// Index 0 is the tile centre, 1-8 are the directions used by the loc types.
pub struct Grid {
    xoffset: i32,
    zoffset: i32,
    present: Vec<bool>,
    settings: Vec<u8>,
    walk: Vec<u16>,
    sight: Vec<u16>,
    /// Index of the cell holding this cell's *effective* collision data.
    eff: Vec<u32>,
    effective_level: Vec<i8>,
}

impl Grid {
    pub fn new() -> Self {
        Grid {
            xoffset: 0,
            zoffset: 0,
            present: vec![false; CELLS],
            settings: vec![0; CELLS],
            walk: vec![0; CELLS],
            sight: vec![0; CELLS],
            eff: vec![0; CELLS],
            effective_level: vec![0; CELLS],
        }
    }

    pub fn reset(&mut self, chunkx: i32, chunkz: i32) {
        self.xoffset = chunkx * CHUNK_SIZE - FLOOR_PADDING;
        self.zoffset = chunkz * CHUNK_SIZE - FLOOR_PADDING;
        self.present.fill(false);
        self.settings.fill(0);
        self.walk.fill(0);
        self.sight.fill(0);
        for (i, e) in self.eff.iter_mut().enumerate() {
            *e = i as u32;
        }
        self.effective_level.fill(0);
    }

    /// Grid cell index for world tile coordinates, or `None` when outside the
    /// grid (the js `getTile` returns undefined there).
    #[inline]
    fn index(&self, x: i32, z: i32, level: i32) -> Option<usize> {
        if !(0..SQUARE_LEVELS as i32).contains(&level) {
            return None;
        }
        let lx = x - self.xoffset;
        let lz = z - self.zoffset;
        if lx < 0 || lz < 0 || lx >= XSIZE as i32 || lz >= ZSIZE as i32 {
            return None;
        }
        Some(level as usize * LEVELSTEP + lz as usize * XSIZE + lx as usize)
    }

    #[inline]
    fn tile(&self, x: i32, z: i32, level: i32) -> Option<usize> {
        let idx = self.index(x, z, level)?;
        if self.present[idx] {
            Some(idx)
        } else {
            None
        }
    }

    /// `TileGrid.addMapsquare`, restricted to what collision needs.
    pub fn add_mapsquare(&mut self, square: &SquareData, rectx: i32, rectz: i32) {
        for z in 0..CHUNK_SIZE {
            for x in 0..CHUNK_SIZE {
                let gx = rectx + x;
                let gz = rectz + z;
                for level in 0..SQUARE_LEVELS {
                    let idx = match self.index(gx, gz, level as i32) {
                        Some(idx) => idx,
                        None => continue,
                    };
                    let settings = square.settings[settings_index(level, x as usize, z as usize)];
                    self.present[idx] = true;
                    self.settings[idx] = settings;
                    // TileProps' constructor seeds walk[0] from the blocking flag.
                    self.walk[idx] = if settings & 1 != 0 { 1 } else { 0 };
                    self.sight[idx] = 0;
                }
            }
        }
    }

    /// The collision half of `blendUnderlays`: resolve effective levels and
    /// re-point the effective collision data of bridge tiles.
    pub fn resolve_effective_levels(&mut self) {
        for lz in 0..ZSIZE as i32 {
            for lx in 0..XSIZE as i32 {
                let x = self.xoffset + lx;
                let z = self.zoffset + lz;
                let flag2 = match self.tile(x, z, 1) {
                    Some(idx) => self.settings[idx] & 2 != 0,
                    None => false,
                };
                let leveloffset: i32 = if flag2 { -1 } else { 0 };

                for level in 0..SQUARE_LEVELS as i32 {
                    let current = match self.tile(x, z, level) {
                        Some(idx) => idx,
                        None => continue,
                    };
                    let effective_level = level + leveloffset;
                    if effective_level != level {
                        if let Some(target) = self.tile(x, z, effective_level) {
                            // effectiveTile.effectiveCollision = currenttile.rawCollision
                            self.eff[target] = current as u32;
                        }
                    }
                    self.effective_level[current] = effective_level as i8;
                }
            }
        }
    }

    /// The collision half of `mapsquareObjects`, for one mapsquare's locs.
    /// Returns the ids of placements that had to be dropped for lack of a
    /// usable definition (the js version warned once per occurrence).
    pub fn apply_locs(
        &mut self,
        locs: &[LocUse],
        originx: i32,
        originz: i32,
        defs: &[Option<ObjectDef>],
        dropped: &mut Vec<u32>,
    ) {
        // Type 9 is actually a diagonal wall; 12-21 are roof types.
        const FULL_COLLISION_TYPES: [u8; 13] =
            [9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21];
        const STRAIGHT_WALL_DIRS: [usize; 4] = [1, 4, 3, 2];

        for inst in locs {
            let def = match defs.get(inst.id as usize).copied().flatten() {
                Some(def) => def,
                // resolveMorphedObject threw; the js version drops the loc.
                None => {
                    dropped.push(inst.id);
                    continue;
                }
            };
            let x = inst.x + originx;
            let z = inst.y + originz;
            let calling = match self.tile(x, z, inst.plane as i32) {
                Some(idx) => idx,
                None => continue,
            };

            let (mut sizex, mut sizez) = (def.width as i32, def.length as i32);
            if inst.rotation % 2 == 1 {
                std::mem::swap(&mut sizex, &mut sizez);
            }

            if def.probably_nocollision {
                continue;
            }
            let level = self.effective_level[calling] as i32;
            let blocks_sight = !def.maybe_allows_lineofsight;

            for dz in 0..sizez {
                for dx in 0..sizex {
                    let idx = match self.tile(x + dx, z + dz, level) {
                        Some(idx) => idx,
                        None => continue,
                    };
                    let col = self.eff[idx] as usize;
                    match inst.loctype {
                        22 if def.maybe_blocks_movement => {
                            self.walk[col] |= 1;
                        }
                        0 => {
                            let dir = STRAIGHT_WALL_DIRS[(inst.rotation & 3) as usize];
                            self.walk[col] |= 1 << dir;
                            if blocks_sight {
                                self.sight[col] |= 1 << dir;
                            }
                        }
                        2 => {
                            let dir_a = STRAIGHT_WALL_DIRS[(inst.rotation & 3) as usize];
                            let dir_b = STRAIGHT_WALL_DIRS[((inst.rotation + 1) & 3) as usize];
                            self.walk[col] |= (1 << dir_a) | (1 << dir_b);
                            if blocks_sight {
                                self.sight[col] |= (1 << dir_a) | (1 << dir_b);
                            }
                        }
                        1 | 3 => {
                            let dir = 5 + inst.rotation as usize;
                            self.walk[col] |= 1 << dir;
                            if blocks_sight {
                                self.sight[col] |= 1 << dir;
                            }
                        }
                        t if FULL_COLLISION_TYPES.contains(&t) => {
                            self.walk[col] |= 1;
                            if blocks_sight {
                                self.sight[col] |= 1;
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    /// Effective walk mask of a tile, or `None` when the tile does not exist.
    #[inline]
    pub fn walk_mask_at(&self, x: i32, z: i32, level: i32) -> Option<u16> {
        let idx = self.tile(x, z, level)?;
        Some(self.walk[self.eff[idx] as usize])
    }

    #[inline]
    pub fn settings_at(&self, x: i32, z: i32, level: i32) -> Option<u8> {
        let idx = self.tile(x, z, level)?;
        Some(self.settings[idx])
    }
}

/// Convenience wrapper for building the grid of one output chunk.
pub fn build_grid(
    grid: &mut Grid,
    chunkx: i32,
    chunkz: i32,
    neighbours: &[(i32, i32, Arc<SquareData>)],
) {
    grid.reset(chunkx, chunkz);
    for (cx, cz, square) in neighbours {
        grid.add_mapsquare(square, cx * CHUNK_SIZE, cz * CHUNK_SIZE);
    }
    grid.resolve_effective_levels();
}
