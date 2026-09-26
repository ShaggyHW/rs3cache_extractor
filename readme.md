# TILE DATA EXTRACTION

One command builds everything: `tiles.db` from the cache, the spreadsheet's
teleports imported into it, and `worldReachableTiles.db`:

```sh
cargo run --release --manifest-path rust/Cargo.toml -- build -o /home/query/.local/share/bolt-launcher/Jagex/RuneScape/ --overrides override.txt --xlsx 'https://docs.google.com/spreadsheets/d/1gp1fePtecvpU1u-WhZk-uKm-wLiDcYB0LkmtaKOiPwo'

walk_mask_decode.py --encode left
```

`build` writes `tiles.db` and `worldReachableTiles.db` in the repo root (`--db`
and `--out` to move them). It produces exactly what the three separate steps
below produce, and those still work on their own:

```sh
cargo run --release --manifest-path rust/Cargo.toml -- walkflags -o /home/query/.local/share/bolt-launcher/Jagex/RuneScape/ --db tiles.db --overrides override.txt
cargo run --release --manifest-path rust/Cargo.toml -- import-xlsx --xlsx 'https://docs.google.com/spreadsheets/d/1gp1fePtecvpU1u-WhZk-uKm-wLiDcYB0LkmtaKOiPwo' --db tiles.db
cargo run --release --manifest-path rust/Cargo.toml -- tile-cleaner
```

Neither database has to be deleted first: an existing one is updated in place
(see [Rebuilding in place](#rebuilding-in-place)). Do not run a build while
another program has either database open.

| `build` flag | |
|---|---|
| `-o, --cache <dir>` | NXT cache directory holding the `js5-*.jcache` files |
| `--db <path>` | tiles db to create or update, default `tiles.db` |
| `--out <path>` | reachable-tiles db to create or update, default `worldReachableTiles.db` |
| `--overrides <path>` | `x,y,plane,walk_mask` lines; a later line for the same tile wins |
| `--xlsx <url or path>` | spreadsheet to import; without it the teleport tables stay empty |
| `--start-x/--start-y/--start-plane` | BFS start tile, default 3200, 3200, 0 |
| `--fsync` | wait until both dbs are on disk before exiting |
| `--log`, `--no-log-file`, `--startx/--startz/--sizex/--sizez` | as for `walkflags` |

`walkflags` is a port of the old `node dist/cli walkflags` script. It reads the
NXT sqlite cache directly and, with `--db`, writes tiles straight into
`tiles.db` and applies `--overrides` — so it replaces `load-tiles` as well. The
two node/`load-tiles` steps it supersedes were:

```sh
npm install && npm run build
node dist/cli walkflags -o cache:/home/query/.local/share/bolt-launcher/Jagex/RuneScape/ -s ./out --startx 0 --startz 0
cargo run --manifest-path rust/Cargo.toml -- load-tiles --json-dir out/walk --db tiles.db --overrides override.txt
```

`-s <dir>` still writes the old `<dir>/walk/<x>-<z>.json` files, byte for byte
identical to the node version's output apart from the collision the opcode fixes
below restored; it is useful for diffing but the json only ever contributed one
column (`walkMask`) to the database. `-s` and `--db` can be combined.

| `walkflags` flag | |
|---|---|
| `-o, --cache <dir>` | NXT cache directory holding the `js5-*.jcache` files |
| `--db <path>` | write tiles straight into this sqlite db; a new file, or an existing `tiles.db` to update in place |
| `-s, --save <dir>` | write `<dir>/walk/<x>-<z>.json`, as the node script did |
| `--overrides <path>` | `x,y,plane,walk_mask` lines; needs `--db` |
| `--log <path>` | full diagnostics log, default `walkflags.log` |
| `--no-log-file` | report problems on the console only |
| `--fsync` | wait until the db is on disk before exiting |
| `--startx/--startz` | first mapsquare column/row, default 0 |
| `--sizex/--sizez` | how many to cover, default 128 x 200 (the whole world) |

At least one of `--db` and `-s` is required. There is also
`dump-object -o <cachedir> <id>` for inspecting a single location definition.

## Performance

Measured on the development machine (32 threads, a SATA SSD that is 89% full and
also holds the swap, so its sustained write speed swings between ~30 and ~300
MB/s):

| | before | `build` |
|---|---|---|
| everything unchanged since the last build | ~62s | **1.1–2.7s** |
| after editing `override.txt` or the spreadsheet | ~62s | **1.1–2.7s** |
| first build, no databases yet | ~62s | 5–40s, the time the disk takes to write 1.44 GB |

The spread in the first two rows is the Google Sheets export, which takes 0.9–2.5s
by itself; it runs alongside the ~1s extraction, so whichever is slower sets the
time (with a local `.xlsx` it is 1.1–1.3s).

"Before" is `walkflags` (~53s, nearly all of it writing 1.6 GB) + `import-xlsx`
(~2s, the download) + `tile-cleaner` (~7s). Run on their own, the three commands
now take ~1s (in place) or the disk's time for 1.44 GB (fresh), the download,
and ~0.8s.

Where it comes from:

* **Nothing goes through sqlite's insert path.** `rust/src/sqlite_btree.rs`
  writes the `tiles` b-tree pages itself: rows are produced in primary key order,
  packed into full 16 KB leaves and written as whole pages, with the schema still
  created by sqlite. This removes a b-tree descent and a VDBE program per row, and
  because sqlite's page splits left every leaf ~12% empty, the file shrinks from
  1.65 GB to 1.44 GB. The row sort is gone as well: walking each column's mapsquares
  x-major yields the rows already in key order.
* **Rebuilding in place.** An existing database is compared page by page and only
  pages whose bytes changed are written; see below. Writing is the only slow part
  on this disk, and a typical rebuild writes a few hundred KB.
* **Everything overlaps.** `build` downloads the spreadsheet while the cache is
  decoded, keeps the walk masks it extracts in memory so the reachability BFS
  never reads 84.75M rows back out of `tiles.db`, and builds
  `worldReachableTiles.db` from an in-memory copy of the teleports while
  `tiles.db` is still being written. Columns are encoded and cut into pages on a
  separate thread pool, several at once.
* **The mapsquares are read into memory in one scan.** Looking them up one by one
  while a gigabyte is being written used to take up to 50s on a cold cache,
  because under memory pressure the kernel evicted the cache file to make room
  for the output and every lookup then queued behind the writes.
* **`tile-cleaner` on its own** loads the masks from `tiles.db` with 32 parallel
  range scans (~0.65s) instead of millions of point queries, and runs the BFS
  without any sql.
* **No fsync by default.** The databases are regenerable, so the run ends once
  the data is handed to the kernel, which writes it back in the background.
  `--fsync` waits for the disk; `import-xlsx` likewise commits with
  `synchronous=OFF`.

Every run says where its time went:

```
extracted 84754432 tiles from 5173 squares in 1.0s (square load 0.3s, chunk work 0.4s, waiting on the tiles.db writer 0.1s)
tiles.db: 84754432 tiles, 88018 leaf + 114 interior pages (depth 3), 1.44 GB; 22 of 88155 pages changed, 0.4 MB written (compare 0.6s, write 0.0s)
```

### Rebuilding in place

The page layout is a pure function of the rows, so rebuilding from the same input
reproduces the same bytes. `--db` (and `build`'s `--out`) may therefore point at
an existing database: the new pages are compared with the ones on disk in
parallel, only differing runs are written, and the file is truncated to the new
size. The result is byte for byte what a fresh build writes. A `--db` that holds
some other database (a different schema) is refused rather than overwritten.

For this to stay local, every mapsquare column starts a new leaf. Without that, a
tile whose record changes size (an override flipping a mask from 255 to 0 saves
two bytes) would move every leaf boundary after it and turn a one-page change
into a rewrite of the rest of the file. With it, a change only repacks its own
column, and the 128 partly filled leaves cost ~1 MB.

The page writer writes ordinary sqlite databases: sqlite reads, updates and
integrity-checks them like any other (the import after the tree is plain sqlite,
as are the tests that modify written trees). Two format rules it has to respect
and that are easy to miss: a `WITHOUT ROWID` table is a B-tree rather than a
B+tree, so the rows separating two leaves live in the interior pages rather than
in either leaf; and the page holding file offset 1 GiB (page 65537 at 16 KB
pages) is sqlite's lock-byte page, which must stay empty.

## The tiles table

`SQLDB.txt` mirrors the schema in `rust/src/db.rs`; keep them in step. Two
choices there are load-bearing and easy to undo by accident:

* `tiles` is **`WITHOUT ROWID`**. The payload is one byte per tile, so a second
  (rowid) b-tree would roughly double both the write cost and the file size.
  Every consumer looks tiles up by `(x, y, plane)` or scans the table; nothing
  uses `rowid`. Reverting this costs ~2.9x the file size and most of the runtime.
  The page writer also depends on it: it only fills `WITHOUT ROWID` tables.
* There is **no `idx_tiles_walkable`**. It was an exact duplicate of the primary
  key, which *is* the table now, so it cost a ~118s rebuild and ~1.9 GB for
  nothing.

`tile_cleaner` copies table and index DDL verbatim from its source database, so
both properties propagate into `worldReachableTiles.db` with no code change
there. (If a spreadsheet ever gets a sheet named `tiles`, `build` notices that
the import changes the table and reloads the masks from `tiles.db` before the BFS.)

## Decode diagnostics

Anything that goes wrong while decoding an object opcode stream, a mapsquare or
a loc placement is reported on the console and written in full to
`walkflags.log` (`--log <path>` to move it, `--no-log-file` for console only).
The console shows the first 25 messages of each kind and always ends with
per-kind totals, so a flood of repeats does not bury the run.

This mirrors what the node exporter printed via `console.warn`, with the same
opcode numbers and byte offsets. A current cache should report
`diagnostics: no problems reported`; anything else means the opcode table has
fallen behind the cache again (see below).

`dump-object -o <cachedir> <id>` hex dumps one definition's opcode stream next
to what the decoder made of it, which is how operand widths get worked out.

## Location definition opcodes

The loc opcode table lives in `src/opcodes/objects.jsonc` and is shared with the
node tooling; the rust decoder in `rust/src/walk/objdef.rs` implements the same
table with the `buildnr` branches already resolved.

It is worth knowing how this breaks, because it is silent. The stream is
self-describing: each opcode is followed by operands of a width only the table
knows. Get one width wrong and every byte after it is misread, so the failure
shows up as a burst of *unrelated* "unknown opcode" warnings further along —
not at the opcode that is actually wrong. Three symptoms all trace back to the
same cause:

* unknown opcodes that are not real opcodes, just misaligned data,
* definitions that overrun their buffer and are abandoned,
* loc placements silently contributing no collision, because the definition
  they need could not be decoded.

Ports of the NXT decoder are the reference for operand widths — the one in
Hoor2 (`launcher/src/payload/gs_cache_defs.c`, `gs_cache_object_def`) covers the
modern opcodes. Its primitives map onto this repo's as `big_smart` = `varuint`,
`unsigned_smart` = `varushort`, `tri_byte` = `unsigned tribyte`.

Fixed in this repo so far: `0xCD` was declared a single byte when it is really a
nested structure, and `0x6C`-`0x6E` and `0xCE` were missing outright.

| | before | after |
|---|---|---|
| unknown opcodes | 2236 | 0 |
| definitions abandoned mid-decode | 84 | 0 |
| definitions with no `0x00` terminator | 42 | 0 |
| morph targets that could not be loaded | 1 | 0 |
| loc placements dropped from collision | 3487 | 0 |
| definitions decoded | 139470 | 139555 |

That changed **39,193 tiles**, and every one of them became *more* blocked — no
tile anywhere gained walkability, which is the signature you want from a fix that
only restores missing collision. The worst single offender was loc `38731`: 918
placements of a 3x3 `maybe_blocks_movement` object that pathfinding could
previously walk straight through.

### Known remaining divergences

`objects.jsonc` still disagrees with the NXT decoder about a few *count* widths.
They agree for counts below 128, which is why nothing fails against the current
cache, but they are latent:

| opcode | this repo | NXT decoder |
|---|---|---|
| `0x28`, `0x29`, `0x2A`, `0x4F`, `0x6A`, `0xA0` | `varushort` count | `ubyte` count |
| `0xCC` | `ubyte` count | `unsigned_smart` count |
| `0xCA` | `ubyte` | `unsigned_smart` |

These were left alone deliberately: there is no evidence either way from this
cache, and changing them blind risks breaking definitions that decode correctly
today. If new unknown-opcode warnings ever appear, start here.

# RuneScape Model Viewer (.js)
A RuneScape cache downloader, decoder and model viewer implemented in TypeScript. The tool will download the cache directly from the game servers and decode parts of into usable data and models. 

## Installation
A new-ish version of node.js is needed with native build tools installed (the node.js installer will ask about this).
After that run the following commands in your systems console.

```sh
#install the dependencies
npm i
#compile the native nodejs dependencies for use in electron
npm run buildnative

#build the native/electron files
npm run build
#build the web viewer (currently broken because of local fork of sql.js)
npm run web
```

## Running the viewer
The web viewer needs a server since it uses several API's that aren't allowed on `file://`.
```sh
#use a simple http localhost server
npx http-server dist
#alternatively run a webpack dev server with HMR
npm run hot
```
Both of these options will host the app at http://localhost:8080/assets/index.html

The electron viewer can be opened with
```sh
npm start
```

## Other scripts
Cache extraction tool
```sh
#to dump item ids 0-100
node dist/cli extract  # name of the script
    -o cache           # open NXT cache at default location
    -s cache/items     # where to dump the files
    --mode items       # extraction mode, determines how ids are interpreted and the format of the output
    -i 0-100           # ids of the files to extract

#to download raw data from groups 10-20 of cache index 53 (png textures)
node dist/cli extract
    -o live            # download files directly from jagex game servers
	-s cache/textures
	--mode bin         # dumps the raw file
	-i 53.10-53.20     # some modes use tuples as id, both tuple interpretation and range interpolation depend on mode
```
The are more tools in src/scripts, most are used for testing, they are also in various degrees of completeness.

## Jagex copyrights
Jagex is generally aware of the existence of cache decoding tools like this one and they are extensively used for the runescape wiki. However, in the interest of the games integrity and the future of these tools please do not publicly share leaks or unreleased content found using this tool.

## Todo
* Rewrite RT5 anims (again) in order to convert shear anims to multiple bones
* Figure out the rest of RT7 anims
* Particles and billboards (both RT and RT7)
* Color animations
* DAE exporter? (three.js one doesn't work out of the box)
* properly implement caching, currrently doesn't clear texture/model cache

## Credits
Modern rewrite by Skillbert

Based on downloader/3d viewer by Sahima, ui by [manpaint](https://github.com/manpaint)

2d map based on code by [mejrs](https://github.com/mejrs)

Cache loader based on code by [villermen](https://github.com/villermen)
