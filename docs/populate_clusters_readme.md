# Populate Clusters CLI

This CLI populates HPA* cluster data in a local SQLite DB (`worldReachableTiles.db`). It discovers cluster entrances, links inter-cluster edges across chunk borders, and computes intra-cluster paths (optionally JPS-accelerated).

## Usage
```bash
python3 scripts/populate_clusters.py \
  --db-path ./worldReachableTiles.db \
  [--planes 0,1] \
  [--chunk-range 100:120,200:220] \
  [--recompute] \
  [--store-paths] \
  [--workers 8] \
  [--dry-run] \
  [--log-level INFO]
```

- **--db-path**: Path to SQLite DB (local only; no network I/O).
- **--planes**: Comma-separated planes to include; default is all present.
- **--chunk-range**: Inclusive bounds `x_min:x_max,z_min:z_max` for chunk filtering.
- **--recompute**: Clear and rebuild data in-scope for idempotent reseeding.
- **--store-paths**: Store compressed waypoint paths for intra connections.
- **--workers**: Parallelize by chunks using per-process SQLite connections.
- **--dry-run**: Open DB read-only; print summaries without mutations.
- **--log-level**: `DEBUG|INFO|WARNING|ERROR`.

## Data Model Alignment
- **Entrances (`cluster_entrances`)**: Border tiles per chunk/plane with `neighbor_dir` `N|E|S|W`. Idempotent upsert keyed by `(chunk_x, chunk_z, plane, x, y)`.
- **Interconnections (`cluster_interconnections`)**: Bidirectional edges between opposing entrances across chunk borders. Cost=1 (single-step crossing). Idempotent with `ON CONFLICT`.
- **Intraconnections (`cluster_intraconnections`)**: Least-cost paths between entrances inside a chunk. Optional `path_blob` stores compressed waypoints (turns only).
- **Movement semantics**: Derived from `movement_policy(policy_id=1)` and `tiles.walk_mask/blocked`. Neighbor order deterministic.
- **JPS (optional)**: If `jps_jump`/`jps_spans` exist, expansion uses jump points; otherwise falls back to neighbor expansion with parity of costs.

## Performance Notes
- O(perimeter) entrance scan; inter links are local checks; intra uses A* with optional JPS.
- With `--workers > 1`, chunks are partitioned across processes; each worker opens its own SQLite connection with backoff on `SQLITE_BUSY`.

## Examples
- Dry run, all chunks/planes:
```bash
python3 scripts/populate_clusters.py --db-path ./worldReachableTiles.db --dry-run --log-level INFO
```
- Recompute plane 0 over a range with 8 workers:
```bash
python3 scripts/populate_clusters.py --db-path ./worldReachableTiles.db --planes 0 --chunk-range 100:140,200:260 --recompute --workers 8 --log-level INFO
```
