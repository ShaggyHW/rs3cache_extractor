# Cluster Schema: Questions & Suggestions

This document captures open questions and proposed schema adjustments for multiple clusters per chunk and their entrances/edges. It references `docs/worldReachableTiles_schema.md`.

## Quick context (current schema)

- **`chunk_clusters`**: `(cluster_id PK, chunk_x, chunk_z, plane, label, tile_count)`.
- **`cluster_entrances`**: Entrances keyed by `entrance_id`, unique on `(chunk_x, chunk_z, plane, x, y)`, with `neighbor_dir IN ('N','S','E','W')`.
- **`cluster_interconnections`**: `(entrance_from, entrance_to, cost)` between entrances (across chunk borders).
- **`cluster_intraconnections`**: Paths within a chunk: PK includes `(chunk_x_from, chunk_z_from, plane_from, entrance_from, entrance_to)`, with optional `path_blob`.
- Entrances and intra edges currently reference the chunk/plane but not `cluster_id`.

---

## Questions for you to confirm

- **Entrances scope**
  - Should `cluster_entrances` remain strictly for chunk-border portals, or should it also cover intra-chunk cluster boundaries (e.g., door from courtyard → house)?

 

- **Cluster membership binding**
  - Do you want each entrance to explicitly reference a `cluster_id` in `chunk_clusters`? This is needed to disambiguate which connected component an entrance belongs to when multiple clusters exist in the same `(chunk_x, chunk_z, plane)`.
  - Likewise, should `cluster_intraconnections` be keyed by `cluster_id` (instead of `chunk_x_from, chunk_z_from, plane_from`) to make “intra” explicit per cluster?

- **Teleport provenance on edges**
  - Would you like explicit fields (e.g., `abstract_teleport_edge_id`) on `cluster_intraconnections` and/or `cluster_interconnections` to directly tie an edge to a teleport/door record, or will you resolve this via coordinate joins at query time?

- **Door/open-state semantics**
  - Treat doors as teleports with `teleports_requirements` gating availability? When requirements aren’t met, the edge is effectively absent—confirm this behavior.

- **Label semantics**
  - What does `chunk_clusters.label` represent and what is its uniqueness scope? (per chunk/plane vs global?) Do we need uniqueness constraints or is it purely diagnostic?

- **Uniqueness of entrances**
  - If we add `cluster_id` to `cluster_entrances`, do you want to keep the unique constraint on `(chunk_x, chunk_z, plane, x, y)` (implying each tile belongs to a single cluster) or change the uniqueness scope?

- **`path_blob` format**
  - Desired encoding for `path_blob` in `cluster_intraconnections` (e.g., compressed polyline of turning points, CBOR/MsgPack/JSON, delta-encoding)? Only turns or all waypoints?

- **Foreign key enforcement**
  - Should we enable `PRAGMA foreign_keys = ON` at connection time and add FKs in the DDL for validation?

- **Example scenario confirmation**
  - A chunk with an isolated house: two distinct clusters (exterior + interior). A door provides a (conditional) edge between them. Confirm this is the intended modeling.

---

## Suggestions (for discussion)

- **Attach entrances to clusters**
  - Add `cluster_id REAL NOT NULL REFERENCES chunk_clusters(cluster_id)` to `cluster_entrances`.
  - Keep `UNIQUE(chunk_x, chunk_z, plane, x, y)` for idempotent upsert, assuming each tile belongs to exactly one cluster.

- **Attach intra-connections to a cluster**
  - Add `cluster_id REAL NOT NULL REFERENCES chunk_clusters(cluster_id)` to `cluster_intraconnections`.
  - Prefer a PK like `(cluster_id, entrance_from, entrance_to)` (plus plane if you want redundancy) instead of chunk-based keys.

- **Keep border vs internal portals distinct (recommended)**
  - Keep `cluster_entrances` for chunk-border entrances only (retain `neighbor_dir`).
  - Introduce `cluster_portals` for intra-chunk cluster adjacency (no `neighbor_dir`). Example shape:

```sql
-- Proposed new table (if you choose to separate internal portals)
CREATE TABLE cluster_portals (
  portal_id INTEGER PRIMARY KEY,
  cluster_id_from INTEGER NOT NULL REFERENCES chunk_clusters(cluster_id),
  cluster_id_to   INTEGER NOT NULL REFERENCES chunk_clusters(cluster_id),
  src_x INTEGER NOT NULL,
  src_y INTEGER NOT NULL,
  dst_x INTEGER NOT NULL,
  dst_y INTEGER NOT NULL,
  plane INTEGER NOT NULL,
  cost INTEGER,                  -- often 1 or same scale as movement policy
  abstract_teleport_edge_id INTEGER, -- optional FK to abstract teleport
  path_blob BLOB                 -- optional if non-teleport walk path is needed
);
```

- **Alternatively: generalize `cluster_entrances`**
  - Allow intra-chunk portals by relaxing `neighbor_dir` (e.g., nullable or extended enum). This simplifies the number of tables but blurs the “border-only” definition.

- **Edge linkage to teleports**
  - If doors/objects model as teleports, add optional `abstract_teleport_edge_id` to edges (intra/inter) for explicit provenance and easier debugging.

- **Indexes**
  - Add indexes for typical lookups:
    - `cluster_entrances(plane, x, y)`
    - `cluster_intraconnections(cluster_id, entrance_from, entrance_to)`
    - `cluster_interconnections(entrance_from, entrance_to)` (composite PK already suggested)

- **Migration notes**
  - Backfill `cluster_id` for existing entrances by flood-filling connected components per `(chunk_x, chunk_z, plane)` and mapping each entrance tile to its cluster.
  - For intra edges, map existing `(chunk_x_from, chunk_z_from, plane_from)` to the resolved `cluster_id`.

---

## Decision checklist (please fill)

- [ ] Entrances are border-only vs also intra-chunk?
- [ ] Add `cluster_id` to `cluster_entrances`?
- [ ] Re-key `cluster_intraconnections` by `cluster_id`?
- [ ] Add `cluster_portals` table (separate) vs generalize `cluster_entrances`?
- [ ] Add `abstract_teleport_edge_id` to edges?
- [ ] Door availability via `teleports_requirements` gating?
- [ ] Define `label` semantics and any uniqueness constraints.
- [ ] Define `path_blob` encoding.
- [ ] Enable foreign keys at runtime and add FK constraints.

---

## Implementation alignment

- Once decisions are confirmed, I will:
  - Update `docs/worldReachableTiles_schema.md` accordingly.
  - Align DB creation code (e.g., `rust/src/db.rs`) to create/alter the tables as per the final design.
