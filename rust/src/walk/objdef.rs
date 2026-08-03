//! Port of the `objects` config decoder (`src/opcodes/objects.jsonc`).
//!
//! Only the fields that matter for collision are kept; every other opcode is
//! still decoded far enough to keep the stream aligned.

use anyhow::Result;

use super::diagnostics::{Diagnostics, KIND_MORPH};
use crate::cache::reader::Reader;

/// Build number this decoder targets. `CacheFileSource.getBuildNr()` returns
/// `latestBuildNumber` for NXT sqlite caches, so the js version resolves every
/// `["match","buildnr",...]` branch against this same number. Every branch in
/// this file is already resolved for it; the constant records which.
#[allow(dead_code)]
pub const BUILD_NR: u32 = 940;

#[derive(Debug, Clone, Copy, Default)]
pub struct RawObject {
    pub width: Option<u8>,
    pub length: Option<u8>,
    pub probably_nocollision: bool,
    pub maybe_allows_lineofsight: bool,
    pub maybe_blocks_movement: bool,
    /// Raw morph target from `morphs_1` / `morphs_2`, before sentinel handling.
    pub morphs_1: Option<u32>,
    pub morphs_2: Option<u32>,
}

impl RawObject {
    /// `defaultMorphId`: morphs_2 wins over morphs_1, and two sentinel values
    /// (one per cache era) mean "no morph".
    fn default_morph_id(&self) -> i32 {
        let mut newid: i64 = -1;
        if let Some(v) = self.morphs_1 {
            newid = v as i64;
        }
        if let Some(v) = self.morphs_2 {
            newid = v as i64;
        }
        if newid == (1 << 15) - 1 || newid == (1 << 16) - 1 {
            -1
        } else {
            newid as i32
        }
    }

    fn has_morphs(&self) -> bool {
        self.morphs_1.is_some() || self.morphs_2.is_some()
    }
}

/// A fully resolved location definition, as `resolveMorphedObject` produces it.
#[derive(Debug, Clone, Copy)]
pub struct ObjectDef {
    /// From the morphed def (`{...rawloc, ...morphtarget}`).
    pub width: u8,
    pub length: u8,
    /// Collision flags come from the *raw* def, matching `mapsquareObjects`.
    pub probably_nocollision: bool,
    pub maybe_allows_lineofsight: bool,
    pub maybe_blocks_movement: bool,
}

/// `item_modelid` resolves to varuint for buildnr >= 670.
#[inline]
fn item_modelid(r: &mut Reader) -> Result<u32> {
    r.varuint()
}

/// Something the decoder had to paper over. The js `opcodesParser` printed the
/// equivalent via `console.warn` / `console.log`.
#[derive(Debug, Clone)]
pub enum OpcodeIssue {
    /// Not in the opcode table; the js decoder skips one byte and carries on,
    /// which usually means everything after this point is misaligned.
    Unknown { opcode: u8, position: usize },
    /// Ran out of bytes before the 0x00 terminator.
    MissingTerminator,
}

/// Outcome of decoding one object definition.
///
/// Issues are reported even when the decode later dies, because the js version
/// prints its `unknown chunk` warnings as it goes and only then throws — and an
/// earlier unknown opcode is usually the *cause* of the overflow that follows.
pub struct ObjectParse {
    /// `None` when decoding failed outright.
    pub obj: Option<RawObject>,
    pub issues: Vec<OpcodeIssue>,
    pub error: Option<anyhow::Error>,
}

pub fn parse_object(buf: &[u8]) -> ObjectParse {
    let mut issues = Vec::new();
    match parse_object_inner(buf, &mut issues) {
        Ok(obj) => ObjectParse { obj: Some(obj), issues, error: None },
        Err(error) => ObjectParse { obj: None, issues, error: Some(error) },
    }
}

fn parse_object_inner(buf: &[u8], issues: &mut Vec<OpcodeIssue>) -> Result<RawObject> {
    let mut r = Reader::new(buf);
    let mut obj = RawObject::default();

    loop {
        if r.eof() {
            issues.push(OpcodeIssue::MissingTerminator);
            break;
        }
        let op = r.ubyte()?;
        if op == 0 {
            break;
        }
        match op {
            // models
            0x01 => {
                let count = r.ubyte()? as usize;
                for _ in 0..count {
                    r.ubyte()?; // type
                    let values = r.ubyte()? as usize;
                    for _ in 0..values {
                        item_modelid(&mut r)?;
                    }
                }
            }
            0x02 | 0x03 => r.skip_string()?, // name, examine
            // models_05
            0x05 => {
                let count = r.ubyte()? as usize;
                for _ in 0..count {
                    r.ubyte()?;
                    let values = r.ubyte()? as usize;
                    for _ in 0..values {
                        item_modelid(&mut r)?;
                    }
                }
                let tail = r.ubyte()? as usize;
                for _ in 0..tail {
                    item_modelid(&mut r)?;
                    item_modelid(&mut r)?;
                }
            }
            0x0e => obj.width = Some(r.ubyte()?),
            0x0f => obj.length = Some(r.ubyte()?),
            0x11 => obj.probably_nocollision = true,
            0x12 => obj.maybe_allows_lineofsight = true,
            0x13 => r.skip(1)?, // deletable (bool)
            0x15 | 0x16 | 0x17 => {} // morphFloor, unknown_16, occludes_1
            0x18 => {
                r.varuint()?;
            } // probably_animation
            0x1b => obj.maybe_blocks_movement = true,
            0x1c | 0x1d => r.skip(1)?, // wallkit_related_1C, ambient
            0x1e..=0x22 => r.skip_string()?, // actions_0..4
            0x27 => r.skip(1)?,        // contrast
            // color_replacements / material_replacements: varushort count, 2x ushort each
            0x28 | 0x29 => {
                let count = r.varushort()? as usize;
                r.skip(count * 4)?;
            }
            // recolourPalette: varushort count of byte
            0x2a => {
                let count = r.varushort()? as usize;
                r.skip(count)?;
            }
            0x2c | 0x2d => r.skip(2)?,
            0x36..=0x39 => {}
            0x3c => r.skip(2)?,
            0x3e => {}
            0x40 => {}
            0x41..=0x43 => r.skip(2)?, // scaleX/Y/Z
            0x44 => r.skip(2)?,        // mapscene_old
            0x45 => r.skip(1)?,        // dummy_45
            0x46..=0x48 => r.skip(2)?, // translateX/Y/Z
            0x49 | 0x4a => {}
            0x4b => r.skip(1)?,
            // morphs_1: uint, varushort-counted list of item_modelid, item_modelid
            0x4d => {
                r.skip(4)?;
                let count = r.varushort()? as usize;
                let mut first: Option<u32> = None;
                for i in 0..count {
                    let v = item_modelid(&mut r)?;
                    if i == 0 {
                        first = Some(v);
                    }
                }
                let unk3 = item_modelid(&mut r)?;
                obj.morphs_1 = Some(first.unwrap_or(unk3));
            }
            0x4e => r.skip(3)?, // light source related
            0x4f => {
                r.skip(2 + 2 + 1)?;
                let count = r.varushort()? as usize;
                r.skip(count * 2)?;
            }
            0x51 => r.skip(1)?,
            0x52 => {}
            0x58 => {}
            0x59 | 0x5a | 0x5b => {}
            // morphs_2: uint, item_modelid, varushort-counted list, item_modelid
            0x5c => {
                r.skip(4)?;
                let unk2 = item_modelid(&mut r)?;
                let count = r.varushort()? as usize;
                for _ in 0..count {
                    item_modelid(&mut r)?;
                }
                item_modelid(&mut r)?;
                obj.morphs_2 = Some(unk2);
            }
            0x5d => r.skip(2)?, // tilt_xz
            0x5e => {}
            0x5f => r.skip(2)?, // probably_morphCeilingOffset (short for buildnr >= 596)
            0x60 | 0x61 | 0x62 => {}
            0x63 | 0x64 => r.skip(3)?, // ubyte + ushort
            0x65 => r.skip(1)?,
            0x66 => r.skip(2)?, // mapscene
            0x67 => {}          // occludes_2
            0x68 => r.skip(1)?,
            0x69 => {}
            // headModels: varushort count of { varuint, ubyte }
            0x6a => {
                let count = r.varushort()? as usize;
                for _ in 0..count {
                    r.varuint()?;
                    r.skip(1)?;
                }
            }
            0x6b => r.skip(2)?, // mapFunction
            0x71 => r.skip(1)?,
            0x96..=0x9a => r.skip_string()?, // members actions
            0xa0 => {
                let count = r.varushort()? as usize;
                r.skip(count * 2)?;
            }
            0xa2 => r.skip(4)?,
            0xa3 => r.skip(4)?,
            0xa4 | 0xa5 | 0xa6 | 0xa7 => r.skip(2)?,
            0xa8 | 0xa9 => {}
            0xaa | 0xab => {
                r.varushort()?;
            }
            0xad => r.skip(4)?,
            0xb1 => {}
            0xb2 => r.skip(1)?,
            0xba => r.skip(1)?,
            0xbc | 0xbd => {}
            0xbe..=0xc3 => r.skip(2)?, // action_cursors_0..5
            0xc4 | 0xc5 => r.skip(1)?,
            0xc6 | 0xc7 | 0xc8 => {}
            // unknown_C9: 6x varshort, same byte layout as varushort
            0xc9 => {
                for _ in 0..6 {
                    r.varushort()?;
                }
            }
            0xca => r.skip(1)?,
            0xcb => {}
            0xcc => {
                let count = r.ubyte()? as usize;
                r.skip(count * 27)?;
            }
            // objects.jsonc declares 0xCD as a single byte, but it is really a
            // nested structure (Hoor2 gs_cache_defs.c op 205). Reading one byte
            // desynced the rest of every definition that uses it.
            0xcd => {
                r.skip(2 + 2 + 2)?; // leading, v1, v2
                let flags = r.ubyte()?;
                if flags & 0x1 != 0 {
                    let outer = r.ubyte()? as usize;
                    for _ in 0..outer {
                        r.skip(1)?;
                        let inner = r.ubyte()? as usize;
                        for _ in 0..inner {
                            r.skip(4)?;
                            r.varuint()?;
                            let t = r.ubyte()?;
                            r.skip(t.min(3) as usize)?;
                        }
                    }
                }
                if flags & 0x2 != 0 {
                    let outer = r.ubyte()? as usize;
                    for _ in 0..outer {
                        r.skip(1)?;
                        let inner = r.ubyte()? as usize;
                        for _ in 0..inner {
                            r.skip(4)?;
                            r.varuint()?;
                        }
                    }
                }
                for bit in [0x4u8, 0x8] {
                    if flags & bit != 0 {
                        let outer = r.ubyte()? as usize;
                        for _ in 0..outer {
                            r.skip(1)?;
                            let inner = r.ubyte()? as usize;
                            r.skip(inner * 8)?;
                        }
                    }
                }
                if flags & 0x10 != 0 {
                    let count = r.ubyte()? as usize;
                    r.skip(count * 8)?;
                }
                r.skip(2)?; // trailing
            }
            0xde => r.skip(1)?,
            // Opcodes below are absent from src/opcodes/objects.jsonc, which is
            // why the js decoder desynced on them. Widths taken from the NXT
            // decoder in Hoor2 (launcher/src/payload/gs_cache_defs.c,
            // gs_cache_object_def) where they are ops 108-110 and 206.
            0x6c | 0x6d | 0x6e => {}
            0xce => {
                r.skip(2)?;
                let count = r.ubyte()? as usize;
                for _ in 0..count {
                    // flags, xyz, byte, float, 24-bit, 2x ushort, 3x int
                    r.skip(1 + 12 + 1 + 4 + 3 + 4 + 12)?;
                }
            }
            // extra: extrasmap
            0xf9 => {
                let count = r.ubyte()? as usize;
                for _ in 0..count {
                    let ty = r.ubyte()?;
                    r.utribyte()?;
                    if ty == 0 {
                        r.skip(4)?;
                    }
                    if ty == 1 {
                        r.skip_string()?;
                    }
                }
            }
            // Unknown opcode: the js decoder warns and skips a single byte.
            _ => {
                issues.push(OpcodeIssue::Unknown { opcode: op, position: r.scan - 1 });
                if !r.eof() {
                    r.skip(1)?;
                }
            }
        }
    }
    Ok(obj)
}

/// Why a loc id has no usable definition, so a dropped placement can say what
/// actually went wrong instead of just disappearing.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DefStatus {
    Ok,
    /// No such file in the objects index.
    Absent,
    /// The file exists but the opcode stream could not be decoded.
    DecodeFailed,
    /// Decoded, but it morphs into a definition that could not be loaded.
    MorphUnresolved,
}

impl DefStatus {
    pub fn describe(self) -> &'static str {
        match self {
            DefStatus::Ok => "ok",
            DefStatus::Absent => "not present in the objects index",
            DefStatus::DecodeFailed => "definition failed to decode",
            DefStatus::MorphUnresolved => "morph target could not be loaded",
        }
    }
}

/// Resolves raw defs into morphed defs, mirroring `resolveMorphedObject`.
///
/// A loc whose morph target is missing throws in the js version and the caller
/// drops the location entirely, so those become `None` here too — but the id
/// and the reason are reported so the drop is visible.
pub fn resolve_defs(
    raw: &[Option<RawObject>],
    statuses: &mut [DefStatus],
    diag: &Diagnostics,
) -> Vec<Option<ObjectDef>> {
    raw.iter()
        .enumerate()
        .map(|(id, entry)| {
            // Ids with no entry at all are normal (sparse archives); only a
            // loc actually referencing one is worth reporting, which happens
            // at placement time.
            let rawloc = (*entry)?;
            let morph_id = rawloc.default_morph_id();
            let (width, length) = if rawloc.has_morphs() && morph_id != -1 {
                match raw.get(morph_id as usize).copied().flatten() {
                    Some(target) => {
                        (target.width.or(rawloc.width), target.length.or(rawloc.length))
                    }
                    None => {
                        diag.warn(
                            KIND_MORPH,
                            format!(
                                "object {} morphs to {}, which could not be loaded; \
                                 every placement of it will be skipped",
                                id, morph_id
                            ),
                        );
                        if let Some(status) = statuses.get_mut(id) {
                            *status = DefStatus::MorphUnresolved;
                        }
                        return None;
                    }
                }
            } else {
                (rawloc.width, rawloc.length)
            };
            Some(ObjectDef {
                width: width.unwrap_or(1),
                length: length.unwrap_or(1),
                probably_nocollision: rawloc.probably_nocollision,
                maybe_allows_lineofsight: rawloc.maybe_allows_lineofsight,
                maybe_blocks_movement: rawloc.maybe_blocks_movement,
            })
        })
        .collect()
}
