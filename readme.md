# TILE DATA EXTRACTION

```sh
cargo run --release --manifest-path rust/Cargo.toml -- walkflags -o /home/query/.local/share/bolt-launcher/Jagex/RuneScape/ --db tiles.db --overrides override.txt --startx 0 --startz 0

cargo run --manifest-path rust/Cargo.toml -- import-xlsx --xlsx 'https://docs.google.com/spreadsheets/d/1gp1fePtecvpU1u-WhZk-uKm-wLiDcYB0LkmtaKOiPwo' --db tiles.db
cargo run --manifest-path rust/Cargo.toml -- tile-cleaner


walk_mask_decode.py --encode left


```

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
column (`walkMask`) to the database. `-s` and `--db` can be combined, and `--db`
needs a database that does not exist yet.

| flag | |
|---|---|
| `-o, --cache <dir>` | NXT cache directory holding the `js5-*.jcache` files |
| `--db <path>` | write tiles straight into this sqlite db; must not exist yet |
| `-s, --save <dir>` | write `<dir>/walk/<x>-<z>.json`, as the node script did |
| `--overrides <path>` | `x,y,plane,walk_mask` lines applied last; needs `--db` |
| `--log <path>` | full diagnostics log, default `walkflags.log` |
| `--no-log-file` | report problems on the console only |
| `--startx/--startz` | first mapsquare column/row, default 0 |
| `--sizex/--sizez` | how many to cover, default 128 x 200 (the whole world) |

At least one of `--db` and `-s` is required. There is also
`dump-object -o <cachedir> <id>` for inspecting a single location definition.

## Performance

A full world extraction is **~50s on a spinning disk, ~13s on fast storage**, and
essentially all of it is writing the database. Every run prints where its time
went:

```
phases: square load 0.4s, chunk work 0.3s, sort 1.8s, db write+flush 56.0s
  tail: overrides 0.00s, pragmas 0.00s, close 0.03s, flush 43.8s (38 MB/s)
```

* **square load** — sqlite reads + inflate for the mapsquares of one column and
  its neighbours. Cheap once the `js5-*.jcache` files are in the page cache.
* **chunk work** — decoding tiles and locs, building the grid, and deriving every
  walk mask. This is the actual extraction, and it is **~0.3s for all 84.75M
  tiles**; it is not worth optimising further.
* **sort** — ordering each column by `(x, y, plane)`.
* **db write+flush** — time the main loop spent blocked on the writer thread,
  plus the final drain and fsync.

Three things make it as fast as it is:

* **Batched inserts.** Rows go in 256 at a time through one multi-row statement
  with raw parameter binding. One statement per row costs a VDBE invocation each
  and used to be 81% of the entire runtime.
* **Bulk-load pragmas.** `page_size=16384` (set before the first table exists),
  `journal_mode=OFF`, `synchronous=OFF`. The database is built from scratch every
  run and is regenerable in under a minute, so there is nothing to recover to.
  Both are restored to `DELETE`/`FULL` before the file is handed back.
* **A streaming writer thread.** sqlite runs on its own thread behind a bounded
  channel, so decoding and collision for column N+1 overlap the insert of column
  N. The bound is two columns, so a slow disk applies backpressure instead of
  letting decoded columns pile up in memory. Columns are still handed over in
  ascending x, which is what keeps the primary key writes sequential and is the
  whole reason each column is sorted first.

### Why the end of the run looks stuck

`--overrides` is applied after the tiles are in, and the `Applying overrides...`
line is the last thing printed before the final flush. It is **not** slow — all
70 upserts measure `0.00s`. What follows it is pushing ~1.6 GB of page cache to
disk, which is why the flush now announces itself and reports its throughput.

That flush is the floor, not overhead to be tuned away. `dd conv=fsync` writes
the same 1.6 GB to this disk in 53.9s (32 MB/s); the whole extraction finishes in
~50s because kernel writeback already overlaps the decoding. **Everything except
the disk is ~2.5s.**

### The one lever left

Going faster means writing less. The `tiles` table spends ~19 bytes per row to
carry one byte of payload, and `RegionID` is pure derivation from `x`/`y`.
navpathService's `tiles_regions` layout — one row per (region, plane) holding a
512-byte presence bitmap plus 4096 walk masks — is **20,692 rows / ~95 MB instead
of 84.75M rows / 1.6 GB**, so ~3s of flushing and a ~5s total run. It already
reads that format (`rust/navpath-builder/src/build/load_sqlite.rs`) and warns and
falls back to a slow row scan without it.

It is not additive: emitting it *alongside* `tiles` writes more, not less, so the
win needs it to replace the per-tile table — which means reworking
`tile_cleaner`, whose BFS does per-tile `SELECT ... WHERE x=? AND y=? AND
plane=?`. A cheaper partial step is dropping the derivable `RegionID` column,
worth roughly 250 MB.

## The tiles table

`SQLDB.txt` mirrors the schema in `rust/src/db.rs`; keep them in step. Two
choices there are load-bearing and easy to undo by accident:

* `tiles` is **`WITHOUT ROWID`**. The payload is one byte per tile, so a second
  (rowid) b-tree would roughly double both the write cost and the file size.
  Every consumer looks tiles up by `(x, y, plane)` or scans the table; nothing
  uses `rowid`. Reverting this costs ~2.9x the file size and most of the runtime.
* There is **no `idx_tiles_walkable`**. It was an exact duplicate of the primary
  key, which *is* the table now, so it cost a ~118s rebuild and ~1.9 GB for
  nothing.

`tile_cleaner` copies table and index DDL verbatim from its source database, so
both properties propagate into `worldReachableTiles.db` with no code change
there.

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
