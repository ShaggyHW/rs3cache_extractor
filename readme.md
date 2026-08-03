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
identical to the node version's output; it is useful for diffing but the json
only ever contributed one column (`walkMask`) to the database. `-s` and `--db`
can be combined, and `--db` needs a database that does not exist yet.

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
nested structure, and `0x6C`-`0x6E` and `0xCE` were missing outright. Together
those desynced 596 definitions, produced 2236 bogus unknown-opcode warnings, and
dropped 3487 loc placements out of collision entirely.

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
