# Map tile lookup reference

## File naming
- **Chunk coordinates** The files under `cache/maptiles/` use the pattern `maptiles-<chunkX>_<chunkZ>.json`. For example, `cache/maptiles/maptiles-1_10.json` stores data for chunk `chunkX = 1`, `chunkZ = 10`.

## Tile entries
- **Array layout** Each file exposes a `tiles` array whose entries describe tile attributes (`flags`, `overlay`, `settings`, `underlay`, `height`, etc.). Positioning is implicit.
- **Chunk dimensions** Files produced by the modern RS3 cache are `64 × 64` tiles per chunk, matching the logic in `src/scripts/filetypes.ts` via `worldmapIndex()`.

## Deriving tile coordinates
- **Local tile index** Tiles are stored row-major. Index `0` corresponds to local tile `(0,0)` at the chunk’s top-left. Let `i` be the array index (starting at 0):
```ts
const tilesPerSide = 64;
const localX = i % tilesPerSide;
const localZ = Math.floor(i / tilesPerSide);
```
- **World coordinates** Combine the chunk and local coordinates to obtain world coordinates:
```ts
const worldX = chunkX * tilesPerSide + localX;
const worldZ = chunkZ * tilesPerSide + localZ;
```

## Height planes (`y`)
- **Per-plane heights** The `height` field holds per-plane elevation data. Choose the plane index you need (0–3 for floors) when interpreting heights; the JSON does not embed an explicit `y` coordinate beyond these arrays.
