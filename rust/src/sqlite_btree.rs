//! Writes the b-tree of an empty `WITHOUT ROWID` table straight into a sqlite
//! database file, bypassing sqlite's insert path.
//!
//! Loading 84.75M tiles through `INSERT` costs sqlite a VDBE program and a
//! b-tree descent per row, and its page splits leave every leaf ~12% empty.
//! Rows that already arrive in primary key order can instead be packed into
//! full pages and streamed out in one sequential write. The result is an
//! ordinary database: sqlite reads, updates and integrity-checks it like any
//! other, only the pages are fuller.
//!
//! The schema is still created by sqlite, in memory: [`TreeWriter`] takes that
//! database's serialized image, in which the table's root page is empty, adds
//! the leaf and interior pages after its last page, rewrites the root page as
//! the top of the new tree and fixes up the header.
//!
//! Pages go to a [`PageSink`]. [`FileSink`] writes them over whatever the
//! target file already holds, comparing page by page and skipping the ones
//! that are already there byte for byte. The layout is a pure function of the
//! rows, so rebuilding a database from mostly unchanged input rewrites only
//! the pages that actually changed, instead of the whole file.
//!
//! A `WITHOUT ROWID` table is an *index* b-tree, which is a B-tree rather than a
//! B+tree: every row is stored exactly once, and the rows that separate two
//! children live in the parent page, not in the leaves. Rows are therefore
//! promoted out of the stream as dividers while the leaves are cut.
//!
//! Format reference: <https://www.sqlite.org/fileformat2.html>.

use anyhow::{bail, ensure, Context, Result};
use rusqlite::{Connection, DatabaseName};
use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

const PAGE_LEAF_INDEX: u8 = 0x0a;
const PAGE_INTERIOR_INDEX: u8 = 0x02;
const LEAF_HEADER: usize = 8;
const INTERIOR_HEADER: usize = 12;
/// sqlite locks the bytes at this file offset, so the page containing it (the
/// "lock-byte page") is never used for data. A b-tree that points at it reads
/// as corrupt.
const PENDING_BYTE: u64 = 0x4000_0000;

/// Header offsets (all big endian).
const HDR_PAGE_SIZE: usize = 16;
const HDR_WRITE_VERSION: usize = 18;
const HDR_RESERVED: usize = 20;
const HDR_CHANGE_COUNTER: usize = 24;
const HDR_PAGE_COUNT: usize = 28;
const HDR_SCHEMA_FORMAT: usize = 44;
const HDR_AUTOVACUUM_ROOT: usize = 52;
const HDR_VERSION_VALID_FOR: usize = 92;
const HDR_SQLITE_VERSION: usize = 96;

/// How often the two end-of-level repairs ran, so the tests can prove they
/// exercised them.
#[cfg(test)]
static EDGE_HITS: [std::sync::atomic::AtomicUsize; 2] = [
    std::sync::atomic::AtomicUsize::new(0),
    std::sync::atomic::AtomicUsize::new(0),
];

#[derive(Debug, Default, Clone, Copy)]
pub struct TreeStats {
    pub rows: u64,
    pub leaf_pages: u64,
    /// Interior pages below the root.
    pub interior_pages: u64,
    /// Levels including the leaves; 1 means the root is the only leaf.
    pub depth: u32,
    /// Size of the finished database.
    pub db_bytes: u64,
}

/// Serializes the main database of `conn`, typically an in-memory one holding
/// the schema, into the exact bytes sqlite would have written to a file.
pub fn serialize_db(conn: &Connection) -> Result<Vec<u8>> {
    Ok(conn.serialize(DatabaseName::Main)?.to_vec())
}

/// Root page number of a table.
pub fn table_root_page(conn: &Connection, table: &str) -> Result<u32> {
    conn.query_row(
        "SELECT rootpage FROM sqlite_master WHERE type='table' AND name=?1",
        [table],
        |r| r.get(0),
    )
    .with_context(|| format!("looking up the root page of table {}", table))
}

/// Destination of finished pages.
pub trait PageSink {
    /// Stores page `number` (1-based). Pages mostly arrive in ascending order.
    fn put(&mut self, number: u32, page: &[u8]) -> Result<()>;
    /// Stores consecutive pages starting at `first`.
    fn put_run(&mut self, first: u32, pages: &[u8], page_size: usize) -> Result<()> {
        for (i, page) in pages.chunks(page_size).enumerate() {
            self.put(first + i as u32, page)?;
        }
        Ok(())
    }
    /// The change counter of the database being replaced, 0 if none. The new
    /// header's counter is bumped past it so any open connection notices.
    fn previous_change_counter(&self) -> u32 {
        0
    }
    /// All pages are in; the database is `page_count` pages long.
    fn finish(&mut self, page_count: u32) -> Result<()>;
}

/// Cells of one page under construction, stored back to back.
struct PageCells {
    bytes: Vec<u8>,
    /// End offset of each cell within `bytes`.
    ends: Vec<u32>,
}

impl PageCells {
    fn with_capacity(page_size: usize) -> Self {
        PageCells {
            bytes: Vec::with_capacity(page_size),
            ends: Vec::with_capacity(page_size / 8),
        }
    }

    fn empty() -> Self {
        PageCells {
            bytes: Vec::new(),
            ends: Vec::new(),
        }
    }

    #[inline]
    fn len(&self) -> usize {
        self.ends.len()
    }

    #[inline]
    fn push(&mut self, cell: &[u8]) {
        self.bytes.extend_from_slice(cell);
        self.ends.push(self.bytes.len() as u32);
    }

    fn clear(&mut self) {
        self.bytes.clear();
        self.ends.clear();
    }

    /// Removes the last cell and returns its bytes.
    fn pop(&mut self) -> Vec<u8> {
        let end = self.ends.pop().expect("pop from an empty page") as usize;
        let start = self.ends.last().copied().unwrap_or(0) as usize;
        let cell = self.bytes[start..end].to_vec();
        self.bytes.truncate(start);
        cell
    }

    /// Lays the cells out as a b-tree page: header, cell pointer array, and the
    /// cell content packed against the end of the usable area.
    fn render(&self, out: &mut [u8], page_type: u8, right_child: Option<u32>, usable: usize) {
        let header = if right_child.is_some() {
            INTERIOR_HEADER
        } else {
            LEAF_HEADER
        };
        let content_start = usable - self.bytes.len();
        debug_assert!(header + 2 * self.ends.len() <= content_start);
        out.fill(0);
        out[0] = page_type;
        // [1..3] first freeblock: none
        out[3..5].copy_from_slice(&(self.ends.len() as u16).to_be_bytes());
        // A content area starting at 65536 is stored as 0.
        out[5..7].copy_from_slice(&(content_start as u32 as u16).to_be_bytes());
        // [7] fragmented free bytes: none
        if let Some(right) = right_child {
            out[8..12].copy_from_slice(&right.to_be_bytes());
        }
        let mut start = 0u32;
        for (i, &end) in self.ends.iter().enumerate() {
            let at = header + 2 * i;
            out[at..at + 2].copy_from_slice(&((content_start as u32 + start) as u16).to_be_bytes());
            start = end;
        }
        out[content_start..usable].copy_from_slice(&self.bytes);
    }
}

/// Cuts one segment's cells into leaves off the writer thread; see
/// [`TreeWriter::push_segment`]. Cheap to copy to worker threads.
#[derive(Clone, Copy, Debug)]
pub struct LeafPacker {
    page_size: usize,
    usable: usize,
    max_local: usize,
    format4: bool,
}

/// A segment's cells, as encoded, plus (usually) their leaves already cut and
/// rendered by [`LeafPacker::pack`].
pub struct PackedSegment {
    /// Cells back to back with their end offsets, in key order, in parts.
    parts: Vec<(Vec<u8>, Vec<u32>)>,
    packed: Option<Packed>,
}

struct Packed {
    rows: u64,
    /// The first cell, which becomes the divider in front of the segment.
    head: Vec<u8>,
    /// Finished leaves, rendered back to back, each followed by a divider.
    pages: Vec<u8>,
    dividers: Vec<Vec<u8>>,
    /// The last leaf, left open exactly as the sequential writer leaves it.
    open: PageCells,
}

impl LeafPacker {
    pub fn format4(&self) -> bool {
        self.format4
    }

    fn check(&self, cell: &[u8]) -> Result<()> {
        let (len, prefix) = get_varint(cell);
        ensure!(
            len as usize + prefix == cell.len() && len as usize <= self.max_local,
            "malformed cell or a record that would need an overflow page ({} bytes)",
            cell.len()
        );
        Ok(())
    }

    /// Cuts a segment exactly as [`TreeWriter::push_segment`] would row by
    /// row, assuming the tree already has a leaf in front of it (anything else
    /// is sorted out by the writer, which then falls back to the rows).
    pub fn pack(&self, parts: Vec<(Vec<u8>, Vec<u32>)>) -> Result<PackedSegment> {
        let total: usize = parts.iter().map(|(_, ends)| ends.len()).sum();
        if total < 2 {
            return Ok(PackedSegment { parts, packed: None });
        }
        let mut cells = parts.iter().flat_map(|(bytes, ends)| {
            let mut start = 0usize;
            ends.iter().map(move |&end| {
                let cell = &bytes[start..end as usize];
                start = end as usize;
                cell
            })
        });
        let head = cells.next().unwrap();
        self.check(head)?;

        let mut pages = Vec::with_capacity(total * 17 / 16 + self.page_size);
        let mut dividers = Vec::new();
        let mut leaf = PageCells::with_capacity(self.page_size);
        let mut used = LEAF_HEADER;
        // The same held/pending dance as the writer: a row that does not fit
        // is promoted, and the full leaf waits until something follows it.
        let mut held: Option<PageCells> = None;
        let mut pending: Option<&[u8]> = None;
        let render = |pages: &mut Vec<u8>, cells: &PageCells| {
            let at = pages.len();
            pages.resize(at + self.page_size, 0);
            cells.render(
                &mut pages[at..at + self.usable],
                PAGE_LEAF_INDEX,
                None,
                self.usable,
            );
        };
        for cell in cells {
            self.check(cell)?;
            if let Some(divider) = pending.take() {
                let full = held.take().unwrap();
                render(&mut pages, &full);
                dividers.push(divider.to_vec());
                leaf = full;
                leaf.clear();
                leaf.push(cell);
                used = LEAF_HEADER + 2 + cell.len();
                continue;
            }
            if used + 2 + cell.len() <= self.usable || leaf.len() == 0 {
                leaf.push(cell);
                used += 2 + cell.len();
                continue;
            }
            held = Some(std::mem::replace(&mut leaf, PageCells::empty()));
            pending = Some(cell);
            used = LEAF_HEADER;
        }
        // The segment ends here; settle as `settle_pending` does.
        if let Some(divider) = pending {
            let mut full = held.take().unwrap();
            let full_used = LEAF_HEADER + 2 * full.len() + full.bytes.len();
            if full_used + 2 + divider.len() <= self.usable {
                full.push(divider);
                leaf = full;
            } else {
                let last = full.pop();
                ensure!(full.len() > 0, "a full leaf with a single row");
                render(&mut pages, &full);
                dividers.push(last);
                leaf = full;
                leaf.clear();
                leaf.push(divider);
            }
        }
        let head = head.to_vec();
        Ok(PackedSegment {
            parts,
            packed: Some(Packed {
                rows: total as u64,
                head,
                pages,
                dividers,
                open: leaf,
            }),
        })
    }
}

/// Streams rows, in strictly ascending primary key order, into the empty b-tree
/// of one `WITHOUT ROWID` table of a database image.
pub struct TreeWriter<S: PageSink> {
    sink: S,
    page_size: usize,
    usable: usize,
    /// Largest record that still fits in a cell without spilling to an
    /// overflow page.
    max_local: usize,
    schema_format: u32,
    root_page: u32,
    /// Page 1 of the image; its header is finalised last.
    page1: Vec<u8>,
    next_page: u32,
    lock_page: u32,
    key_columns: usize,
    prev_key: Vec<i64>,
    have_prev: bool,

    leaf: PageCells,
    /// Bytes the current leaf would occupy: header + pointers + cells.
    leaf_used: usize,
    /// A full leaf that is not written yet, and the row promoted after it. The
    /// leaf is only written once another row arrives, because a stream that
    /// ends on a promoted row has to take one row back out of it.
    held: Option<PageCells>,
    pending: Option<Vec<u8>>,
    /// Set by [`TreeWriter::break_leaf`]: the next row ends the current leaf.
    force_break: bool,
    /// `(left child page, divider)` for the level above the leaves. Dividers
    /// are kept as leaf cells (length varint + record): an interior cell is the
    /// child pointer followed by exactly those bytes.
    dividers: Vec<(u32, Vec<u8>)>,

    page_buf: Vec<u8>,
    cell_buf: Vec<u8>,
    stats: TreeStats,
}

impl<S: PageSink> TreeWriter<S> {
    /// `image` is a complete database (see [`serialize_db`]) in which
    /// `root_page` is the empty root of the `WITHOUT ROWID` table to fill.
    /// `key_columns` is the number of leading record columns that form the
    /// primary key; it is only used to check that rows arrive in order.
    ///
    /// Every page of the image except page 1 and the root goes to `sink` right
    /// away; the tree follows after them.
    pub fn new(image: &[u8], root_page: u32, key_columns: usize, mut sink: S) -> Result<Self> {
        ensure!(
            image.len() >= 512 && &image[..16] == b"SQLite format 3\0",
            "not a sqlite database image"
        );
        let page_size = match be16(image, HDR_PAGE_SIZE) {
            1 => 65536,
            n => n as usize,
        };
        ensure!(image[HDR_WRITE_VERSION] != 2, "the image is in WAL mode");
        // Auto-vacuum databases track every page's parent in pointer-map pages,
        // which appended pages would have to be registered in.
        ensure!(
            be32(image, HDR_AUTOVACUUM_ROOT) == 0,
            "the image uses auto_vacuum"
        );
        ensure!(
            image.len() % page_size == 0,
            "the image is not a whole number of pages"
        );
        let image_pages = (image.len() / page_size) as u32;
        ensure!(
            be32(image, HDR_PAGE_COUNT) == image_pages
                && be32(image, HDR_CHANGE_COUNTER) == be32(image, HDR_VERSION_VALID_FOR),
            "the page count in the image header is stale"
        );
        let lock_page = lock_byte_page(page_size);
        ensure!(image_pages < lock_page, "the image is too large");
        ensure!(
            root_page >= 2 && root_page <= image_pages,
            "root page {} is out of range",
            root_page
        );
        let root = &image[(root_page as usize - 1) * page_size..root_page as usize * page_size];
        ensure!(
            root[0] == PAGE_LEAF_INDEX && be16(root, 3) == 0,
            "the table is not an empty WITHOUT ROWID table (root page type 0x{:02x}, {} cells)",
            root[0],
            be16(root, 3)
        );

        for number in 2..=image_pages {
            if number != root_page {
                let at = (number as usize - 1) * page_size;
                sink.put(number, &image[at..at + page_size])?;
            }
        }

        let usable = page_size - image[HDR_RESERVED] as usize;
        Ok(TreeWriter {
            sink,
            page_size,
            usable,
            // Index b-tree cells spill to overflow pages above this size.
            max_local: (usable - 12) * 64 / 255 - 23,
            schema_format: be32(image, HDR_SCHEMA_FORMAT),
            root_page,
            page1: image[..page_size].to_vec(),
            next_page: image_pages + 1,
            lock_page,
            key_columns,
            prev_key: vec![0; key_columns],
            have_prev: false,
            leaf: PageCells::with_capacity(page_size),
            leaf_used: LEAF_HEADER,
            held: None,
            pending: None,
            force_break: false,
            dividers: Vec::new(),
            page_buf: vec![0u8; page_size],
            cell_buf: Vec::with_capacity(128),
            stats: TreeStats::default(),
        })
    }

    /// Whether records use the zero-length serial types for 0 and 1 (schema
    /// format 4); pass to [`encode_int_cell`] when encoding cells elsewhere.
    pub fn format4(&self) -> bool {
        self.schema_format >= 4
    }

    /// Appends one row of integer columns, in table record order (for a
    /// `WITHOUT ROWID` table: the primary key columns first, then the rest).
    #[inline]
    pub fn push_ints(&mut self, values: &[i64]) -> Result<()> {
        ensure!(
            values.len() >= self.key_columns,
            "row has fewer columns than the key"
        );
        let key = &values[..self.key_columns];
        if self.have_prev {
            ensure!(
                key > &self.prev_key[..],
                "rows must arrive in strictly ascending key order: {:?} after {:?}",
                key,
                self.prev_key
            );
        }
        self.prev_key.copy_from_slice(key);
        self.have_prev = true;

        let mut cell = std::mem::take(&mut self.cell_buf);
        cell.clear();
        encode_int_cell(values, self.format4(), &mut cell);
        let result = self.push_cell(&cell);
        self.cell_buf = cell;
        result
    }

    /// A packer for cutting segments on other threads, see [`push_segment`].
    ///
    /// [`push_segment`]: TreeWriter::push_segment
    pub fn packer(&self) -> LeafPacker {
        LeafPacker {
            page_size: self.page_size,
            usable: self.usable,
            max_local: self.max_local,
            format4: self.format4(),
        }
    }

    /// Appends one whole segment: the same as [`break_leaf`], then its rows,
    /// then the segment's end. When the segment was cut by
    /// [`LeafPacker::pack`] (the usual case) this only writes out its ready
    /// pages; otherwise it goes row by row. Either way the pages are identical.
    ///
    /// [`break_leaf`]: TreeWriter::break_leaf
    pub fn push_segment(&mut self, segment: PackedSegment) -> Result<()> {
        self.settle_pending()?;
        match segment.packed {
            Some(packed) if self.leaf.len() > 0 => {
                let previous = std::mem::replace(&mut self.leaf, packed.open);
                let page = self.write_page(&previous, PAGE_LEAF_INDEX, None)?;
                self.stats.leaf_pages += 1;
                self.dividers.push((page, packed.head));
                let numbers = self.write_rendered_run(&packed.pages)?;
                self.stats.leaf_pages += numbers.len() as u64;
                self.dividers.extend(numbers.into_iter().zip(packed.dividers));
                self.leaf_used = LEAF_HEADER + 2 * self.leaf.len() + self.leaf.bytes.len();
                self.force_break = false;
                self.have_prev = false;
                self.stats.rows += packed.rows;
            }
            _ => {
                self.break_leaf()?;
                for (cells, ends) in &segment.parts {
                    self.push_cells(cells, ends)?;
                }
                self.settle_pending()?;
            }
        }
        Ok(())
    }

    /// Appends rows already encoded with [`encode_int_cell`], stored back to
    /// back in `cells` with each one ending at the matching entry of `ends`.
    /// This is how rows encoded on several threads go in; the caller is
    /// responsible for their order, which is not rechecked here.
    pub fn push_cells(&mut self, cells: &[u8], ends: &[u32]) -> Result<()> {
        let mut start = 0usize;
        for &end in ends {
            self.push_cell(&cells[start..end as usize])?;
            start = end as usize;
        }
        self.have_prev = false;
        Ok(())
    }

    /// One leaf cell: the record's length as a varint, then the record.
    #[inline]
    fn push_cell(&mut self, cell: &[u8]) -> Result<()> {
        let (len, prefix) = get_varint(cell);
        ensure!(
            len as usize + prefix == cell.len() && len as usize <= self.max_local,
            "malformed cell or a record that would need an overflow page ({} bytes)",
            cell.len()
        );
        self.stats.rows += 1;

        if let Some(divider) = self.pending.take() {
            // The previous leaf is final now that it has a successor.
            let held = self.held.take().expect("a pending divider without its leaf");
            let page = self.write_page(&held, PAGE_LEAF_INDEX, None)?;
            self.stats.leaf_pages += 1;
            self.dividers.push((page, divider));
            self.leaf = held;
            self.leaf.clear();
            self.leaf.push(cell);
            self.leaf_used = LEAF_HEADER + 2 + cell.len();
            return Ok(());
        }

        let needed = 2 + cell.len();
        let fits = self.leaf_used + needed <= self.usable;
        if (fits && !self.force_break) || self.leaf.len() == 0 {
            self.leaf.push(cell);
            self.leaf_used += needed;
            self.force_break = false;
            return Ok(());
        }

        // Leaf full, or a break was asked for: this row becomes the divider
        // between it and the next. The next row always takes over the held
        // leaf's buffers.
        self.force_break = false;
        self.held = Some(std::mem::replace(&mut self.leaf, PageCells::empty()));
        self.pending = Some(cell.to_vec());
        self.leaf_used = LEAF_HEADER;
        Ok(())
    }

    /// Makes the next row start a new segment: it becomes a divider and the
    /// rows after it go into a fresh leaf.
    ///
    /// Without breaks, a row that changes size shifts every leaf boundary after
    /// it, so every later page differs from the previous build. Breaking at
    /// fixed points in the key space (say, every mapsquare column) makes each
    /// segment's leaves depend only on that segment's rows, at the cost of one
    /// partly filled leaf per segment, and an in-place rebuild then rewrites
    /// only the segments that changed.
    pub fn break_leaf(&mut self) -> Result<()> {
        // A promoted row still waiting for a leaf to its right belongs to the
        // segment that is ending; settle it there first.
        self.settle_pending()?;
        self.force_break = self.leaf.len() > 0;
        Ok(())
    }

    /// Resolves a promoted row that no leaf followed. If it was promoted only
    /// because of a requested break and still fits, it goes back into its
    /// leaf. Otherwise the leaf is full: its last row becomes the divider and
    /// the promoted row a leaf on its own (a full leaf always keeps rows).
    fn settle_pending(&mut self) -> Result<()> {
        let Some(divider) = self.pending.take() else {
            return Ok(());
        };
        let mut held = self.held.take().expect("a pending divider without its leaf");
        let held_used = LEAF_HEADER + 2 * held.len() + held.bytes.len();
        if held_used + 2 + divider.len() <= self.usable {
            held.push(&divider);
            self.leaf = held;
            self.leaf_used = held_used + 2 + divider.len();
            return Ok(());
        }

        #[cfg(test)]
        EDGE_HITS[0].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let last = held.pop();
        ensure!(held.len() > 0, "a full leaf with a single row");
        let page = self.write_page(&held, PAGE_LEAF_INDEX, None)?;
        self.stats.leaf_pages += 1;
        self.dividers.push((page, last));
        self.leaf = held;
        self.leaf.clear();
        self.leaf.push(&divider);
        self.leaf_used = LEAF_HEADER + 2 + divider.len();
        Ok(())
    }

    /// Writes the remaining pages, the root and the header, and finishes the
    /// sink. Does not fsync.
    pub fn finish(mut self) -> Result<(TreeStats, S)> {
        self.settle_pending()?;

        let leaf = std::mem::replace(&mut self.leaf, PageCells::empty());
        self.stats.leaf_pages += 1;
        self.stats.depth = 1;
        if self.dividers.is_empty() {
            // Everything fits in one leaf, which is the root itself (empty if
            // there were no rows at all).
            self.write_root(&leaf, PAGE_LEAF_INDEX, None)?;
        } else {
            let last = self.write_page(&leaf, PAGE_LEAF_INDEX, None)?;
            let dividers = std::mem::take(&mut self.dividers);
            self.build_interior(dividers, last)?;
        }

        let page_count = self.next_page - 1;
        let counter = be32(&self.page1, HDR_CHANGE_COUNTER)
            .max(self.sink.previous_change_counter())
            .wrapping_add(1);
        self.page1[HDR_CHANGE_COUNTER..HDR_CHANGE_COUNTER + 4].copy_from_slice(&counter.to_be_bytes());
        self.page1[HDR_PAGE_COUNT..HDR_PAGE_COUNT + 4].copy_from_slice(&page_count.to_be_bytes());
        self.page1[HDR_VERSION_VALID_FOR..HDR_VERSION_VALID_FOR + 4].copy_from_slice(&counter.to_be_bytes());
        // sqlite stamps its version whenever it bumps the change counter; an
        // in-memory image never had it set.
        self.page1[HDR_SQLITE_VERSION..HDR_SQLITE_VERSION + 4]
            .copy_from_slice(&(rusqlite::version_number() as u32).to_be_bytes());
        let page1 = std::mem::take(&mut self.page1);
        self.sink.put(1, &page1)?;
        self.sink.finish(page_count)?;
        self.stats.db_bytes = page_count as u64 * self.page_size as u64;
        Ok((self.stats, self.sink))
    }

    /// Builds the interior levels bottom up until one page remains, which is
    /// written over the root.
    ///
    /// A level is `c0 k0 c1 k1 ... c(n-1) k(n-1) cn`: children with a divider
    /// between each pair. A page takes cells `(ci, ki)` while they fit; the
    /// next divider moves up a level and the child before it becomes the
    /// page's right-most pointer.
    fn build_interior(&mut self, mut level: Vec<(u32, Vec<u8>)>, mut last: u32) -> Result<()> {
        let mut page = PageCells::with_capacity(self.page_size);
        loop {
            self.stats.depth += 1;
            let n = level.len();
            let cell_size = |key: &[u8]| 4 + key.len();
            let total: usize = level.iter().map(|(_, k)| 2 + cell_size(k)).sum();
            if INTERIOR_HEADER + total <= self.usable {
                page.clear();
                for (child, key) in &level {
                    push_interior_cell(&mut page, &mut self.cell_buf, *child, key);
                }
                return self.write_root(&page, PAGE_INTERIOR_INDEX, Some(last));
            }

            let mut up: Vec<(u32, Vec<u8>)> = Vec::new();
            let mut i = 0usize;
            while i < n {
                // Greedy fill from cell i.
                let mut used = INTERIOR_HEADER;
                let mut j = i;
                while j < n && used + 2 + cell_size(&level[j].1) <= self.usable {
                    used += 2 + cell_size(&level[j].1);
                    j += 1;
                }
                if j == n {
                    // The rest fits: the final page of this level.
                    page.clear();
                    for (child, key) in &level[i..n] {
                        push_interior_cell(&mut page, &mut self.cell_buf, *child, key);
                    }
                    last = self.write_page(&page, PAGE_INTERIOR_INDEX, Some(last))?;
                    self.stats.interior_pages += 1;
                    break;
                }
                // Cells i..j fit and key j moves up. If that would leave no
                // cell at all for the final page, stop one cell earlier.
                if j + 1 == n {
                    #[cfg(test)]
                    EDGE_HITS[1].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    j -= 1;
                }
                ensure!(j > i, "interior page cannot hold a single cell");
                page.clear();
                for (child, key) in &level[i..j] {
                    push_interior_cell(&mut page, &mut self.cell_buf, *child, key);
                }
                let right = level[j].0;
                let number = self.write_page(&page, PAGE_INTERIOR_INDEX, Some(right))?;
                self.stats.interior_pages += 1;
                up.push((number, std::mem::take(&mut level[j].1)));
                i = j + 1;
            }
            level = up;
        }
    }

    fn write_page(&mut self, cells: &PageCells, page_type: u8, right: Option<u32>) -> Result<u32> {
        let mut buf = std::mem::take(&mut self.page_buf);
        cells.render(&mut buf[..self.usable], page_type, right, self.usable);
        let result = self.write_rendered(&buf);
        self.page_buf = buf;
        result
    }

    /// Appends finished pages and returns their numbers, stepping over the
    /// lock-byte page like [`write_rendered`](Self::write_rendered).
    fn write_rendered_run(&mut self, pages: &[u8]) -> Result<Vec<u32>> {
        let ps = self.page_size;
        let n = pages.len() / ps;
        let mut numbers = Vec::with_capacity(n);
        let mut i = 0;
        while i < n {
            if self.next_page == self.lock_page {
                self.sink.put(self.next_page, &vec![0u8; ps])?;
                self.next_page += 1;
            }
            let room = if self.next_page < self.lock_page {
                (self.lock_page - self.next_page) as usize
            } else {
                usize::MAX
            };
            let span = (n - i).min(room);
            ensure!(
                (self.next_page as u64 + span as u64) < u32::MAX as u64,
                "database would exceed the page number limit"
            );
            self.sink
                .put_run(self.next_page, &pages[i * ps..(i + span) * ps], ps)?;
            numbers.extend(self.next_page..self.next_page + span as u32);
            self.next_page += span as u32;
            i += span;
        }
        Ok(numbers)
    }

    /// Appends one finished page and returns its number.
    fn write_rendered(&mut self, page: &[u8]) -> Result<u32> {
        if self.next_page == self.lock_page {
            // Keep the file contiguous but leave the page empty, as sqlite does.
            self.sink.put(self.next_page, &vec![0u8; self.page_size])?;
            self.next_page += 1;
        }
        let number = self.next_page;
        ensure!(number < u32::MAX, "database would exceed the page number limit");
        self.next_page += 1;
        self.sink.put(number, page)?;
        Ok(number)
    }

    fn write_root(&mut self, cells: &PageCells, page_type: u8, right: Option<u32>) -> Result<()> {
        cells.render(&mut self.page_buf[..self.usable], page_type, right, self.usable);
        self.sink.put(self.root_page, &self.page_buf)
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SinkStats {
    pub pages_written: u64,
    /// Pages the file already held byte for byte.
    pub pages_unchanged: u64,
    pub bytes_written: u64,
    /// Time spent reading the old pages and comparing, and writing.
    pub secs_compare: f64,
    pub secs_write: f64,
}

/// Contiguous pages are compared and written in runs of this size.
const RUN_BYTES: usize = 8 << 20;

/// Writes a database image over the file at a path, creating it if needed.
///
/// Every page is compared with what the file already holds at that offset and
/// only differing pages are written, coalesced into runs; the file is
/// truncated to the new size at the end. The result is byte for byte the
/// image, however much of it was already there.
///
/// No other connection may use the database meanwhile: its pages change in
/// place, not atomically.
pub struct FileSink {
    path: PathBuf,
    file: File,
    page_size: usize,
    old_len: u64,
    old_change_counter: u32,
    run_first: u32,
    run: Vec<u8>,
    stats: SinkStats,
}

impl FileSink {
    /// Also deletes any `-journal`, `-wal` and `-shm` beside `path`: they
    /// belong to the content being replaced, and sqlite would otherwise replay
    /// a leftover journal over the new pages.
    pub fn create_or_replace(path: &Path, page_size: usize) -> Result<Self> {
        for suffix in ["-journal", "-wal", "-shm"] {
            let mut side = path.as_os_str().to_owned();
            side.push(suffix);
            match std::fs::remove_file(&side) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e).with_context(|| format!("removing {:?}", side)),
            }
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .with_context(|| format!("opening {} for writing", path.display()))?;
        let old_len = file.metadata()?.len();
        let mut old_change_counter = 0;
        if old_len >= 100 {
            let mut header = [0u8; 100];
            file.read_exact_at(&mut header, 0)?;
            if &header[..16] == b"SQLite format 3\0" {
                old_change_counter = be32(&header, HDR_CHANGE_COUNTER);
            }
        }
        Ok(FileSink {
            path: path.to_path_buf(),
            file,
            page_size,
            old_len,
            old_change_counter,
            run_first: 0,
            run: Vec::with_capacity(RUN_BYTES),
            stats: SinkStats::default(),
        })
    }

    pub fn stats(&self) -> SinkStats {
        self.stats
    }

    fn flush_run(&mut self) -> Result<()> {
        if self.run.is_empty() {
            return Ok(());
        }
        let run = std::mem::take(&mut self.run);
        let result = self.sync_range(self.run_first, &run);
        self.run = run;
        self.run.clear();
        result
    }

    /// Makes pages `first..` of the file equal `pages`: compares them with
    /// what the file holds, in parallel chunks, and writes only the runs that
    /// differ.
    fn sync_range(&mut self, first: u32, pages: &[u8]) -> Result<()> {
        use rayon::prelude::*;
        const CHUNK_PAGES: usize = 64;
        let ps = self.page_size;
        let t_compare = std::time::Instant::now();
        let offset = (first as u64 - 1) * ps as u64;
        let n = pages.len() / ps;
        // Only whole pages that the old file fully covers can be skipped.
        let old_pages = (self.old_len.saturating_sub(offset) / ps as u64).min(n as u64) as usize;
        let file = &self.file;
        let same: Vec<bool> = if old_pages == 0 {
            vec![false; n]
        } else {
            let chunks: Vec<Vec<bool>> = pages
                .par_chunks(CHUNK_PAGES * ps)
                .enumerate()
                .map(|(c, chunk)| -> std::io::Result<Vec<bool>> {
                    let base = c * CHUNK_PAGES;
                    let covered = old_pages.saturating_sub(base).min(chunk.len() / ps);
                    let mut old = vec![0u8; covered * ps];
                    file.read_exact_at(&mut old, offset + (base * ps) as u64)?;
                    Ok((0..chunk.len() / ps)
                        .map(|k| k < covered && chunk[k * ps..(k + 1) * ps] == old[k * ps..(k + 1) * ps])
                        .collect())
                })
                .collect::<std::io::Result<_>>()
                .with_context(|| format!("reading {}", self.path.display()))?;
            chunks.concat()
        };
        let secs_compare = t_compare.elapsed().as_secs_f64();

        let t_write = std::time::Instant::now();
        let mut i = 0;
        while i < n {
            if same[i] {
                self.stats.pages_unchanged += 1;
                i += 1;
                continue;
            }
            let start = i;
            while i < n && !same[i] {
                i += 1;
            }
            self.file
                .write_all_at(&pages[start * ps..i * ps], offset + (start * ps) as u64)
                .with_context(|| format!("writing {}", self.path.display()))?;
            self.stats.pages_written += (i - start) as u64;
            self.stats.bytes_written += ((i - start) * ps) as u64;
        }
        self.stats.secs_compare += secs_compare;
        self.stats.secs_write += t_write.elapsed().as_secs_f64();
        Ok(())
    }
}

impl PageSink for FileSink {
    fn put(&mut self, number: u32, page: &[u8]) -> Result<()> {
        debug_assert_eq!(page.len(), self.page_size);
        let contiguous = !self.run.is_empty()
            && number as u64 == self.run_first as u64 + (self.run.len() / self.page_size) as u64;
        if !contiguous {
            self.flush_run()?;
            self.run_first = number;
        }
        self.run.extend_from_slice(page);
        if self.run.len() >= RUN_BYTES {
            let next = self.run_first + (self.run.len() / self.page_size) as u32;
            self.flush_run()?;
            self.run_first = next;
        }
        Ok(())
    }

    fn put_run(&mut self, first: u32, pages: &[u8], page_size: usize) -> Result<()> {
        debug_assert_eq!(page_size, self.page_size);
        // Taken as is: no copy into the run buffer.
        self.flush_run()?;
        self.sync_range(first, pages)
    }

    fn previous_change_counter(&self) -> u32 {
        self.old_change_counter
    }

    fn finish(&mut self, page_count: u32) -> Result<()> {
        self.flush_run()?;
        let len = page_count as u64 * self.page_size as u64;
        if self.file.metadata()?.len() != len {
            self.file
                .set_len(len)
                .with_context(|| format!("resizing {}", self.path.display()))?;
        }
        Ok(())
    }
}

/// Fails unless the database at `path` does not exist, is empty, or has
/// exactly the schema of `schema` (same objects, same SQL, same order): a
/// guard against overwriting an unrelated database in place.
pub fn ensure_replaceable(path: &Path, schema: &Connection) -> Result<()> {
    match std::fs::metadata(path) {
        Ok(m) if m.len() > 0 => {}
        _ => return Ok(()),
    }
    let existing = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        .and_then(|c| master_entries(&c));
    match existing {
        Ok(entries) if entries == master_entries(schema)? => Ok(()),
        _ => bail!(
            "{} already exists and is not a database this command built; delete it before running",
            path.display()
        ),
    }
}

fn master_entries(conn: &Connection) -> rusqlite::Result<Vec<(String, String, String, Option<String>)>> {
    let mut stmt = conn.prepare("SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY rowid")?;
    let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))?;
    rows.collect()
}

/// Page number of the lock-byte page for a page size.
fn lock_byte_page(page_size: usize) -> u32 {
    (PENDING_BYTE / page_size as u64) as u32 + 1
}

/// An interior cell: the left child's page number, then the divider's leaf
/// cell bytes unchanged.
fn push_interior_cell(page: &mut PageCells, scratch: &mut Vec<u8>, child: u32, cell: &[u8]) {
    scratch.clear();
    scratch.extend_from_slice(&child.to_be_bytes());
    scratch.extend_from_slice(cell);
    page.push(scratch);
}

/// Appends one leaf cell holding a record of integers: the record's length
/// as a varint, then the record, with the serial types sqlite itself picks
/// (`sqlite3VdbeSerialType`): the smallest width that holds the value, and the
/// zero-length types 8/9 for 0 and 1 from schema format 4 on.
#[inline]
pub fn encode_int_cell(values: &[i64], format4: bool, out: &mut Vec<u8>) {
    const MAX_COLUMNS: usize = 16;
    assert!(values.len() <= MAX_COLUMNS, "at most {} columns", MAX_COLUMNS);
    let mut types = [0u8; MAX_COLUMNS];
    let mut widths = [0usize; MAX_COLUMNS];
    let mut body = 0;
    for (i, &v) in values.iter().enumerate() {
        let (t, w) = serial_type(v, format4);
        types[i] = t;
        widths[i] = w;
        body += w;
    }
    let header_len = 1 + values.len();
    put_varint(out, (header_len + body) as u64);
    out.push(header_len as u8);
    out.extend_from_slice(&types[..values.len()]);
    for (i, &v) in values.iter().enumerate() {
        out.extend_from_slice(&v.to_be_bytes()[8 - widths[i]..]);
    }
}

#[inline]
fn serial_type(v: i64, format4: bool) -> (u8, usize) {
    let u = if v < 0 { !v as u64 } else { v as u64 };
    if u <= 127 {
        if format4 && (v == 0 || v == 1) {
            (8 + v as u8, 0)
        } else {
            (1, 1)
        }
    } else if u <= 32767 {
        (2, 2)
    } else if u <= 8_388_607 {
        (3, 3)
    } else if u <= 2_147_483_647 {
        (4, 4)
    } else if u <= 0x7fff_ffff_ffff {
        (5, 6)
    } else {
        (6, 8)
    }
}

#[cfg(test)]
fn varint_len(v: u64) -> usize {
    if v > 0x00ff_ffff_ffff_ffff {
        return 9;
    }
    let bits = 64 - v.leading_zeros() as usize;
    bits.max(1).div_ceil(7)
}

/// sqlite's big-endian varint: 7 bits per byte with a continuation bit, except
/// that a 9th byte carries a full 8 bits.
fn put_varint(out: &mut Vec<u8>, v: u64) {
    if v <= 0x7f {
        out.push(v as u8);
        return;
    }
    if v > 0x00ff_ffff_ffff_ffff {
        let mut buf = [0u8; 9];
        buf[8] = v as u8;
        let mut rest = v >> 8;
        for i in (0..8).rev() {
            buf[i] = (rest as u8 & 0x7f) | 0x80;
            rest >>= 7;
        }
        out.extend_from_slice(&buf);
        return;
    }
    let mut buf = [0u8; 9];
    let mut n = 0;
    let mut rest = v;
    while rest > 0 {
        buf[n] = (rest as u8 & 0x7f) | 0x80;
        rest >>= 7;
        n += 1;
    }
    buf[0] &= 0x7f;
    buf[..n].reverse();
    out.extend_from_slice(&buf[..n]);
}

fn get_varint(buf: &[u8]) -> (u64, usize) {
    let mut v: u64 = 0;
    for i in 0..8 {
        v = (v << 7) | (buf[i] & 0x7f) as u64;
        if buf[i] & 0x80 == 0 {
            return (v, i + 1);
        }
    }
    ((v << 8) | buf[8] as u64, 9)
}

fn be16(buf: &[u8], at: usize) -> u16 {
    u16::from_be_bytes([buf[at], buf[at + 1]])
}

fn be32(buf: &[u8], at: usize) -> u32 {
    u32::from_be_bytes([buf[at], buf[at + 1], buf[at + 2], buf[at + 3]])
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::params;

    const TILES_SQL: &str = "CREATE TABLE tiles (x INTEGER, y INTEGER, plane INTEGER, walk_mask INTEGER, \
         RegionID INTEGER, PRIMARY KEY (x, y, plane)) WITHOUT ROWID;";

    /// The in-memory schema a test database is built from: the tiles table
    /// between two ordinary objects, one of them holding a row.
    fn schema(page_size: usize) -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(&format!(
            "PRAGMA page_size={};\nCREATE TABLE other (a INTEGER PRIMARY KEY, b TEXT);\n{}\n\
             CREATE INDEX other_b ON other(b);",
            page_size, TILES_SQL
        ))
        .unwrap();
        conn.execute("INSERT INTO other (a, b) VALUES (1, 'one')", [])
            .unwrap();
        conn
    }

    /// A deterministic spread of widths: 0/1 (zero-length), bytes, shorts,
    /// negatives and the occasional 6/8-byte value, so cell sizes vary.
    fn rows(n: usize) -> Vec<[i64; 5]> {
        let mut state: u64 = 0x2545_f491_4f6c_dd1d;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let mut out = Vec::with_capacity(n);
        let mut x = -3i64;
        let mut y = 0i64;
        for _ in 0..n {
            let r = next();
            if r % 5 == 0 {
                x += 1 + (r >> 8) as i64 % 3;
                y = (r >> 16) as i64 % 4;
            } else {
                y += 1 + (r >> 24) as i64 % 2;
            }
            let plane = (r >> 32) as i64 % 4;
            let mask = match r % 7 {
                0 => 0,
                1 => 1,
                2 => 255,
                3 => -5,
                4 => 1 << 40,
                5 => i64::MIN + (r >> 40) as i64,
                _ => (r >> 20) as i64 % 300,
            };
            out.push([x, y * 4 + plane, plane, mask, ((x >> 6) << 8) + (y >> 6)]);
        }
        out
    }

    fn check(path: &Path, expected: &[[i64; 5]]) {
        let conn = Connection::open(path).unwrap();
        let ic: String = conn
            .query_row("PRAGMA integrity_check", [], |r| r.get(0))
            .unwrap();
        assert_eq!(ic, "ok", "integrity_check for {} rows", expected.len());
        let mut stmt = conn
            .prepare("SELECT x, y, plane, walk_mask, RegionID FROM tiles ORDER BY x, y, plane")
            .unwrap();
        let got: Vec<[i64; 5]> = stmt
            .query_map([], |r| {
                Ok([r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?])
            })
            .unwrap()
            .collect::<std::result::Result<_, _>>()
            .unwrap();
        assert_eq!(got.len(), expected.len());
        assert!(got == expected, "content differs for {} rows", expected.len());
        // Point lookups exercise the b-tree search through the dividers.
        let mut lookup = conn
            .prepare("SELECT walk_mask FROM tiles WHERE x=?1 AND y=?2 AND plane=?3")
            .unwrap();
        for row in expected.iter().step_by(7) {
            let m: i64 = lookup
                .query_row(params![row[0], row[1], row[2]], |r| r.get(0))
                .unwrap();
            assert_eq!(m, row[3]);
        }
        let other: String = conn
            .query_row("SELECT b FROM other WHERE a=1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(other, "one");
    }

    /// Builds the database for `data` into `path`, over whatever is there.
    fn fill(path: &Path, page_size: usize, data: &[[i64; 5]]) -> (TreeStats, SinkStats) {
        let conn = schema(page_size);
        let image = serialize_db(&conn).unwrap();
        let root = table_root_page(&conn, "tiles").unwrap();
        let sink = FileSink::create_or_replace(path, page_size).unwrap();
        let mut tree = TreeWriter::new(&image, root, 3, sink).unwrap();
        for row in data {
            tree.push_ints(row).unwrap();
        }
        let (stats, sink) = tree.finish().unwrap();
        (stats, sink.stats())
    }

    #[test]
    fn records_match_sqlite() {
        // Every value's serial type must be exactly what sqlite would choose,
        // so the pages are indistinguishable from sqlite-written ones.
        let dir = tempfile::tempdir().unwrap();
        let conn = Connection::open(dir.path().join("r.db")).unwrap();
        conn.execute_batch(TILES_SQL).unwrap();
        let data = rows(3000);
        let mut ins = conn
            .prepare("INSERT INTO tiles VALUES (?1, ?2, ?3, ?4, ?5)")
            .unwrap();
        for row in &data {
            ins.execute(params![row[0], row[1], row[2], row[3], row[4]])
                .unwrap();
        }
        drop(ins);
        drop(conn);
        // Byte-for-byte comparison against the leaf cells sqlite wrote itself.
        let mut rec = Vec::new();
        let bytes = std::fs::read(dir.path().join("r.db")).unwrap();
        let ps = be16(&bytes, 16) as usize;
        let pages = bytes.len() / ps;
        let mut sorted = data.clone();
        sorted.sort();
        let mut matched = 0;
        for p in 1..pages {
            let page = &bytes[p * ps..(p + 1) * ps];
            if page[0] != PAGE_LEAF_INDEX {
                continue;
            }
            for i in 0..be16(page, 3) as usize {
                let off = be16(page, LEAF_HEADER + 2 * i) as usize;
                let (len, n) = get_varint(&page[off..]);
                let record = &page[off + n..off + n + len as usize];
                // Decode the key and find the row to re-encode it.
                let hdr = record[0] as usize;
                let mut body = hdr;
                let mut key = [0i64; 3];
                for (c, k) in key.iter_mut().enumerate() {
                    let t = record[1 + c];
                    let w = match t {
                        8 | 9 => 0,
                        1 => 1,
                        2 => 2,
                        3 => 3,
                        4 => 4,
                        5 => 6,
                        6 => 8,
                        _ => panic!(),
                    };
                    let mut v: i64 = if w > 0 && record[body] & 0x80 != 0 { -1 } else { 0 };
                    for b in &record[body..body + w] {
                        v = (v << 8) | *b as i64;
                    }
                    if t == 9 {
                        v = 1;
                    }
                    *k = v;
                    body += w;
                }
                let row = sorted.iter().find(|r| r[..3] == key).unwrap();
                rec.clear();
                encode_int_cell(row, true, &mut rec);
                assert_eq!(&rec[..], &page[off..off + n + len as usize], "cell for {:?}", row);
                matched += 1;
            }
        }
        assert!(matched > 0);
    }

    #[test]
    fn builds_valid_trees_of_every_shape() {
        // Small pages give deep trees from few rows; every count up to a few
        // hundred walks the end-of-stream cases across leaf boundaries. All
        // of them go into the same file, so each build also replaces a
        // database of a different size in place.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("shapes.db");
        let all = rows(6000);
        for (page_size, sweep) in [(512usize, 800usize), (1024, 300)] {
            for n in (0..sweep).chain([997, 1500, 2048, 4000, 6000, 3]) {
                let mut data = all[..n].to_vec();
                data.sort();
                data.dedup_by(|a, b| a[..3] == b[..3]);
                let (stats, _) = fill(&path, page_size, &data);
                assert_eq!(stats.rows as usize, data.len());
                assert_eq!(std::fs::metadata(&path).unwrap().len(), stats.db_bytes);
                check(&path, &data);
            }
        }
        for (i, hits) in EDGE_HITS.iter().enumerate() {
            assert!(
                hits.load(std::sync::atomic::Ordering::Relaxed) > 0,
                "edge case {} never ran",
                i
            );
        }
    }

    /// Like [`fill`], with a leaf break every `segment` rows, the way the
    /// tiles writer breaks at every mapsquare column.
    fn fill_segmented(path: &Path, data: &[[i64; 5]], segment: usize) -> SinkStats {
        let conn = schema(4096);
        let image = serialize_db(&conn).unwrap();
        let root = table_root_page(&conn, "tiles").unwrap();
        let sink = FileSink::create_or_replace(path, 4096).unwrap();
        let mut tree = TreeWriter::new(&image, root, 3, sink).unwrap();
        for (i, row) in data.iter().enumerate() {
            if i % segment == 0 {
                tree.break_leaf().unwrap();
            }
            tree.push_ints(row).unwrap();
        }
        tree.finish().unwrap().1.stats()
    }

    #[test]
    fn pre_encoded_cells_match_row_by_row() {
        // push_cells is the multi-threaded path; it must lay out exactly what
        // push_ints does, breaks included.
        let dir = tempfile::tempdir().unwrap();
        let mut data = rows(20_000);
        data.sort();
        data.dedup_by(|a, b| a[..3] == b[..3]);
        let build = |path: &Path, batched: bool| {
            let conn = schema(1024);
            let image = serialize_db(&conn).unwrap();
            let root = table_root_page(&conn, "tiles").unwrap();
            let sink = FileSink::create_or_replace(path, 1024).unwrap();
            let mut tree = TreeWriter::new(&image, root, 3, sink).unwrap();
            for segment in data.chunks(777) {
                tree.break_leaf().unwrap();
                if batched {
                    let (mut cells, mut ends) = (Vec::new(), Vec::new());
                    for row in segment {
                        encode_int_cell(row, tree.format4(), &mut cells);
                        ends.push(cells.len() as u32);
                    }
                    tree.push_cells(&cells, &ends).unwrap();
                } else {
                    for row in segment {
                        tree.push_ints(row).unwrap();
                    }
                }
            }
            tree.finish().unwrap();
            std::fs::read(path).unwrap()
        };
        let a = build(&dir.path().join("a.db"), false);
        let b = build(&dir.path().join("b.db"), true);
        assert!(a == b, "push_cells and push_ints disagree");
        check(&dir.path().join("b.db"), &data);
    }

    #[test]
    fn packed_segments_match_row_by_row() {
        // push_segment with segments cut on other threads must lay out exactly
        // what the row-by-row writer does, including segments of 0, 1 and 2
        // rows, a first segment, and segments ending on a promoted row.
        let dir = tempfile::tempdir().unwrap();
        let mut data = rows(30_000);
        data.sort();
        data.dedup_by(|a, b| a[..3] == b[..3]);
        let sizes = [
            0usize, 1, 2, 1, 1, 3, 0, 29, 30, 31, 57, 58, 59, 700, 1, 2, 2000, 0, 1,
        ];
        let build = |path: &Path, packed: bool, page_size: usize| {
            let conn = schema(page_size);
            let image = serialize_db(&conn).unwrap();
            let root = table_root_page(&conn, "tiles").unwrap();
            let sink = FileSink::create_or_replace(path, page_size).unwrap();
            let mut tree = TreeWriter::new(&image, root, 3, sink).unwrap();
            let packer = tree.packer();
            let (mut at, mut i) = (0usize, 0usize);
            while at < data.len() {
                let n = sizes[i % sizes.len()].min(data.len() - at);
                i += 1;
                let segment = &data[at..at + n];
                at += n;
                // Split each segment into a few parts, as the columns are.
                let mut parts = Vec::new();
                for piece in segment.chunks(13) {
                    let (mut cells, mut ends) = (Vec::new(), Vec::new());
                    for row in piece {
                        encode_int_cell(row, packer.format4(), &mut cells);
                        ends.push(cells.len() as u32);
                    }
                    parts.push((cells, ends));
                }
                if packed {
                    tree.push_segment(packer.pack(parts).unwrap()).unwrap();
                } else {
                    tree.break_leaf().unwrap();
                    for row in segment {
                        tree.push_ints(row).unwrap();
                    }
                }
            }
            tree.finish().unwrap();
            std::fs::read(path).unwrap()
        };
        for page_size in [512usize, 1024, 4096] {
            let a = build(&dir.path().join("a.db"), false, page_size);
            let b = build(&dir.path().join("b.db"), true, page_size);
            assert!(
                a == b,
                "push_segment and push_ints disagree at page size {}",
                page_size
            );
            check(&dir.path().join("b.db"), &data);
        }
    }

    #[test]
    fn rewrites_only_changed_pages() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("inplace.db");
        let mut data = rows(100_000);
        data.sort();
        data.dedup_by(|a, b| a[..3] == b[..3]);

        let (_, first) = fill(&path, 4096, &data);
        assert_eq!(first.pages_unchanged, 0);
        let fresh = std::fs::read(&path).unwrap();

        // Same input: only page 1 (the change counter) is rewritten.
        let (_, again) = fill(&path, 4096, &data);
        assert_eq!(again.pages_written, 1, "{:?}", again);
        check(&path, &data);

        // A same-width change in the middle touches its leaf (or the interior
        // page holding it) plus page 1.
        let mid = (data.len() / 2..)
            .find(|&i| (2..126).contains(&data[i][3]))
            .unwrap();
        data[mid][3] += 1;
        let (_, one) = fill(&path, 4096, &data);
        assert!(one.pages_written <= 3, "{:?}", one);
        check(&path, &data);

        // Growing and shrinking the file both land on exactly the fresh bytes.
        let mut more = data.clone();
        for k in 0..50_000i64 {
            more.push([10_000_000 + k, 0, 0, k, 0]);
        }
        fill(&path, 4096, &more);
        check(&path, &more);
        data[mid][3] -= 1;
        fill(&path, 4096, &data);
        let mut after = std::fs::read(&path).unwrap();
        // Only the header's change counters differ from the first build.
        for at in [HDR_CHANGE_COUNTER, HDR_VERSION_VALID_FOR] {
            after[at..at + 4].copy_from_slice(&fresh[at..at + 4]);
        }
        assert!(after == fresh, "rebuild over a larger file is not byte-identical");

        // A page size change rewrites everything and still reads back fine.
        fill(&path, 8192, &data);
        check(&path, &data);
    }

    #[test]
    fn leaf_breaks_contain_size_changes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("segments.db");
        let mut data = rows(100_000);
        data.sort();
        data.dedup_by(|a, b| a[..3] == b[..3]);
        let total = fill_segmented(&path, &data, 2000).pages_written;
        check(&path, &data);

        // Rows that change width inside one segment repack the rest of that
        // segment only; the segments after it are untouched. Widening every
        // row of a stretch guarantees the leaf cuts really move.
        let start = data.len() / 3 / 2000 * 2000 + 100;
        for row in &mut data[start..start + 50] {
            row[3] = i64::MIN;
        }
        let changed = fill_segmented(&path, &data, 2000);
        check(&path, &data);
        assert!(changed.pages_written > 3, "{:?}", changed);
        assert!(changed.pages_written * 20 < total, "{:?} of {}", changed, total);

        // Without the breaks, the same edit rewrites the rest of the file.
        fill(&path, 4096, &data);
        for row in &mut data[start..start + 50] {
            row[3] = 0;
        }
        let (_, unsegmented) = fill(&path, 4096, &data);
        assert!(
            unsegmented.pages_written * 3 > total,
            "{:?} of {}",
            unsegmented,
            total
        );

        // Breaks at the very start, back to back, and at every row are fine.
        for segment in [1usize, 2, 3, 250, 251] {
            fill_segmented(&path, &data[..5000], segment);
            check(&path, &data[..5000]);
        }
    }

    #[test]
    fn large_pages_and_sqlite_writes_afterwards() {
        let dir = tempfile::tempdir().unwrap();
        for page_size in [4096usize, 16384, 65536] {
            let path = dir.path().join(format!("large{}.db", page_size));
            let mut data = rows(200_000);
            data.sort();
            data.dedup_by(|a, b| a[..3] == b[..3]);
            let (stats, _) = fill(&path, page_size, &data);
            assert!(stats.depth >= 2);
            check(&path, &data);

            // sqlite must be able to keep modifying the tree it did not write:
            // update in place, grow cells, insert new keys and delete some.
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch("BEGIN").unwrap();
            let mut expected = data.clone();
            for (i, row) in expected.iter_mut().enumerate().step_by(97) {
                row[3] = if i % 2 == 0 { 0 } else { 1 << 33 };
                conn.execute(
                    "INSERT INTO tiles VALUES (?1,?2,?3,?4,?5) ON CONFLICT(x,y,plane) DO UPDATE SET walk_mask=excluded.walk_mask",
                    params![row[0], row[1], row[2], row[3], row[4]],
                )
                .unwrap();
            }
            for k in 0..500i64 {
                let row = [10_000_000 + k, k, 0, 7, 0];
                conn.execute(
                    "INSERT INTO tiles VALUES (?1,?2,?3,?4,?5)",
                    params![row[0], row[1], row[2], row[3], row[4]],
                )
                .unwrap();
                expected.push(row);
            }
            conn.execute("DELETE FROM tiles WHERE x % 11 = 0", []).unwrap();
            expected.retain(|r| r[0] % 11 != 0);
            conn.execute("INSERT INTO other (a, b) VALUES (2, 'two')", [])
                .unwrap();
            conn.execute_batch("COMMIT").unwrap();
            drop(conn);
            expected.sort();
            check(&path, &expected);
        }
    }

    #[test]
    fn rejects_bad_input() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("reject.db");
        let conn = schema(4096);
        let root = table_root_page(&conn, "tiles").unwrap();
        let image = serialize_db(&conn).unwrap();
        let sink = FileSink::create_or_replace(&path, 4096).unwrap();
        let mut tree = TreeWriter::new(&image, root, 3, sink).unwrap();
        tree.push_ints(&[1, 2, 0, 0, 0]).unwrap();
        assert!(tree.push_ints(&[1, 2, 0, 5, 0]).is_err());
        assert!(tree.push_ints(&[1, 1, 3, 5, 0]).is_err());
        tree.push_ints(&[1, 2, 1, 0, 0]).unwrap();
        tree.finish().unwrap();
        check(&path, &[[1, 2, 0, 0, 0], [1, 2, 1, 0, 0]]);

        // The table must be empty in the image.
        conn.execute("INSERT INTO tiles VALUES (5, 5, 0, 0, 0)", [])
            .unwrap();
        let image = serialize_db(&conn).unwrap();
        let sink = FileSink::create_or_replace(&path, 4096).unwrap();
        assert!(TreeWriter::new(&image, root, 3, sink).is_err());

        // Only a database with the same schema is replaced in place.
        assert!(ensure_replaceable(&path, &schema(4096)).is_ok());
        assert!(ensure_replaceable(&dir.path().join("missing.db"), &schema(4096)).is_ok());
        let other = dir.path().join("other.db");
        Connection::open(&other)
            .unwrap()
            .execute_batch("CREATE TABLE t (a);")
            .unwrap();
        assert!(ensure_replaceable(&other, &schema(4096)).is_err());
        std::fs::write(&other, b"not a database, just some bytes that are long enough").unwrap();
        assert!(ensure_replaceable(&other, &schema(4096)).is_err());
    }

    #[test]
    fn lock_byte_page_numbers() {
        // Matches sqlite's PENDING_BYTE_PAGE: the page holding offset 1 GiB.
        assert_eq!(lock_byte_page(16384), 65537);
        assert_eq!(lock_byte_page(4096), 262145);
        assert_eq!(lock_byte_page(65536), 16385);
        assert_eq!(lock_byte_page(512), 2097153);
    }

    /// Grows a database past 1 GiB so the tree has to step over the lock-byte
    /// page. Writes ~1.2 GB to the target directory, so it only runs on request:
    /// `cargo test --release -- --ignored lock_byte`.
    #[test]
    #[ignore]
    fn tree_across_the_lock_byte_page() {
        let dir = tempfile::tempdir_in(concat!(env!("CARGO_MANIFEST_DIR"), "/target")).unwrap();
        let path = dir.path().join("big.db");
        let conn = schema(65536);
        let image = serialize_db(&conn).unwrap();
        let root = table_root_page(&conn, "tiles").unwrap();
        let sink = FileSink::create_or_replace(&path, 65536).unwrap();
        let mut tree = TreeWriter::new(&image, root, 3, sink).unwrap();
        let (mut n, mut probes) = (0u64, Vec::new());
        // ~15 bytes per cell; 80M rows is ~1.2 GB.
        for x in 0..20_000i64 {
            for y in 0..1_000i64 {
                for plane in 0..4i64 {
                    let row = [x, y, plane, (x ^ y) & 0xff, ((x >> 6) << 8) + (y >> 6)];
                    tree.push_ints(&row).unwrap();
                    if n % 9_999_991 == 0 {
                        probes.push(row);
                    }
                    n += 1;
                }
            }
        }
        let (stats, _) = tree.finish().unwrap();
        assert!(stats.db_bytes > PENDING_BYTE);
        let conn = Connection::open(&path).unwrap();
        let ic: String = conn
            .query_row("PRAGMA integrity_check", [], |r| r.get(0))
            .unwrap();
        assert_eq!(ic, "ok");
        let (count, sum): (i64, i64) = conn
            .query_row("SELECT count(*), sum(walk_mask) FROM tiles", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        assert_eq!(count as u64, n);
        let expected: i64 = (0..20_000i64)
            .map(|x| (0..1_000i64).map(|y| 4 * ((x ^ y) & 0xff)).sum::<i64>())
            .sum();
        assert_eq!(sum, expected);
        for row in probes {
            let m: i64 = conn
                .query_row(
                    "SELECT walk_mask FROM tiles WHERE x=?1 AND y=?2 AND plane=?3",
                    params![row[0], row[1], row[2]],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(m, row[3]);
        }
    }

    #[test]
    fn varints_round_trip() {
        for v in [
            0u64,
            1,
            127,
            128,
            16383,
            16384,
            1 << 35,
            0x00ff_ffff_ffff_ffff,
            1 << 56,
            u64::MAX,
        ] {
            let mut buf = Vec::new();
            put_varint(&mut buf, v);
            assert_eq!(get_varint(&buf), (v, buf.len()), "{}", v);
            assert_eq!(varint_len(v), buf.len());
        }
    }
}
