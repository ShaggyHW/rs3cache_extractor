//! Big-endian byte reader mirroring the primitives in `src/opcode_reader.ts`.

use anyhow::{bail, Result};

pub struct Reader<'a> {
    pub buf: &'a [u8],
    pub scan: usize,
}

// The full primitive set is kept even where the walkflags decoders only skip
// over a field, so further cache formats can be ported without re-deriving it.
#[allow(dead_code)]
impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Reader { buf, scan: 0 }
    }

    #[inline]
    pub fn eof(&self) -> bool {
        self.scan >= self.buf.len()
    }

    #[inline]
    pub fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.scan)
    }

    #[inline]
    pub fn skip(&mut self, n: usize) -> Result<()> {
        if self.remaining() < n {
            bail!("buffer overflow: skip {} at {}/{}", n, self.scan, self.buf.len());
        }
        self.scan += n;
        Ok(())
    }

    #[inline]
    pub fn ubyte(&mut self) -> Result<u8> {
        if self.scan >= self.buf.len() {
            bail!("buffer overflow: ubyte at {}/{}", self.scan, self.buf.len());
        }
        let v = self.buf[self.scan];
        self.scan += 1;
        Ok(v)
    }

    #[inline]
    pub fn byte(&mut self) -> Result<i8> {
        Ok(self.ubyte()? as i8)
    }

    #[inline]
    pub fn ushort(&mut self) -> Result<u16> {
        if self.scan + 2 > self.buf.len() {
            bail!("buffer overflow: ushort at {}/{}", self.scan, self.buf.len());
        }
        let v = u16::from_be_bytes([self.buf[self.scan], self.buf[self.scan + 1]]);
        self.scan += 2;
        Ok(v)
    }

    #[inline]
    pub fn short(&mut self) -> Result<i16> {
        Ok(self.ushort()? as i16)
    }

    #[inline]
    pub fn utribyte(&mut self) -> Result<u32> {
        if self.scan + 3 > self.buf.len() {
            bail!("buffer overflow: utribyte at {}/{}", self.scan, self.buf.len());
        }
        let b = &self.buf[self.scan..self.scan + 3];
        self.scan += 3;
        Ok(((b[0] as u32) << 16) | ((b[1] as u32) << 8) | b[2] as u32)
    }

    #[inline]
    pub fn uint(&mut self) -> Result<u32> {
        if self.scan + 4 > self.buf.len() {
            bail!("buffer overflow: uint at {}/{}", self.scan, self.buf.len());
        }
        let v = u32::from_be_bytes([
            self.buf[self.scan],
            self.buf[self.scan + 1],
            self.buf[self.scan + 2],
            self.buf[self.scan + 3],
        ]);
        self.scan += 4;
        Ok(v)
    }

    #[inline]
    pub fn int(&mut self) -> Result<i32> {
        Ok(self.uint()? as i32)
    }

    /// 1 or 2 byte unsigned short, high bit of the first byte marks the 2-byte form.
    #[inline]
    pub fn varushort(&mut self) -> Result<u32> {
        let first = self.ubyte()?;
        if first & 0x80 == 0 {
            return Ok(first as u32);
        }
        let second = self.ubyte()?;
        Ok((((first & 0x7f) as u32) << 8) | second as u32)
    }

    /// 2 or 4 byte unsigned int, high bit of the first word marks the 4-byte form.
    #[inline]
    pub fn varuint(&mut self) -> Result<u32> {
        let first = self.ushort()?;
        if first & 0x8000 == 0 {
            return Ok(first as u32);
        }
        let second = self.ushort()?;
        Ok((((first & 0x7fff) as u32) << 16) | second as u32)
    }

    /// Chained varushorts; a chunk of exactly 0x7fff means "add and keep reading".
    #[inline]
    pub fn tailed_varushort(&mut self) -> Result<u32> {
        const OVERFLOW_CHUNK: u32 = 0x7fff;
        let mut sum: u32 = 0;
        loop {
            let v = self.varushort()?;
            sum = sum.wrapping_add(v);
            if v != OVERFLOW_CHUNK {
                return Ok(sum);
            }
        }
    }

    /// Null terminated string.
    pub fn string(&mut self) -> Result<String> {
        let start = self.scan;
        while self.scan < self.buf.len() && self.buf[self.scan] != 0 {
            self.scan += 1;
        }
        if self.scan >= self.buf.len() {
            bail!("unterminated string at {}", start);
        }
        let s = String::from_utf8_lossy(&self.buf[start..self.scan]).into_owned();
        self.scan += 1;
        Ok(s)
    }

    /// Skips a null terminated string without allocating.
    pub fn skip_string(&mut self) -> Result<()> {
        while self.scan < self.buf.len() {
            let b = self.buf[self.scan];
            self.scan += 1;
            if b == 0 {
                return Ok(());
            }
        }
        bail!("unterminated string")
    }
}
