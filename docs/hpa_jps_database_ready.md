# Making the Database Ready for HPA* + JPS

This document details the requirements, existing support, and recommended changes for making the database schema fully compatible with **Hierarchical Path-Finding A*** (HPA*) and **Jump Point Search** (JPS).

---

## 🔹 JPS Requirements (Grid-Level)

**JPS assumes a uniform-cost grid with fast neighbor checks and optional precomputed jump pointers.**

### ✅ Already Supported

- Per-tile walkability and directionality via `blocked`, `walk_mask`, `blocked_mask`, and `flag`.
- Tile coordinates and plane (`x, y, plane`) as a primary key.
- Partial index for quick walkability lookups: `idx_tiles_walkable`.

### 🟡 Recommended Additions

1. **Movement convention and costs**
   - Persist straight and diagonal movement costs (e.g., 1024/1448).
   - Add a constants table or a pragma row in a `meta` table to keep movement costs consistent across grid and abstract levels.

2. **Corner-cutting rules**
   - Either encode in `walk_mask`/`blocked_mask` generation or add a `movement_policy` table with flags such as `allow_diagonals`, `allow_corner_cut`, `unit_radius_tiles`.

3. **(Optional but optimal) JPS jump pointers**
   - Precompute the next jump point for each direction per tile:

     ```sql
     CREATE TABLE jps_jump (
       x INTEGER NOT NULL,
       y INTEGER NOT NULL,
       plane INTEGER NOT NULL,
       dir INTEGER NOT NULL,       -- 0..7 for N,NE,E,SE,S,SW,W,NW
       next_x INTEGER,
       next_y INTEGER,
       forced_mask INTEGER,        -- bitmask of forced neighbors
       PRIMARY KEY (x, y, plane, dir)
     );
     CREATE INDEX idx_jps_tile_dir ON jps_jump(plane, x, y, dir);
     ```

   - If not precomputed, JPS still works but with slower expansions.

4. **Row/column run-lengths (alternative)**
   - Store nearest obstacle or walkable span per cardinal direction:

     ```sql
     CREATE TABLE jps_spans (
       x INTEGER NOT NULL,
       y INTEGER NOT NULL,
       plane INTEGER NOT NULL,
       left_block_at INTEGER,
       right_block_at INTEGER,
       up_block_at INTEGER,
       down_block_at INTEGER,
       PRIMARY KEY (x, y, plane)
     );
     ```

---

## 🔹 HPA* Requirements (Abstract Layers)

**HPA*** requires clusters, entrances (portals), and intra/inter-cluster connectivity with consistent cost metrics.

### ✅ Already Supported

- Clusters: via `chunks` and each tile’s `chunk_x`, `chunk_z` membership.
- Entrances: `cluster_entrances` (`x, y, plane, neighbor_dir`) with unique constraints.
- Intra-cluster connections: `cluster_intraconnections` (with cost + optional path blob).
- Inter-cluster connections: `cluster_interconnections` (entrance-to-entrance connections).
- Teleports: fully integrated through `teleports_*` tables and `abstract_teleport_edges`.

### 🟡 Recommended Fixes and Additions

1. **Primary key on inter-cluster edges should be composite**
   - Current PK only on `entrance_from`—change to `(entrance_from, entrance_to)`:

     ```sql
     CREATE TABLE cluster_interconnections_new (
       entrance_from INTEGER NOT NULL,
       entrance_to   INTEGER NOT NULL,
       cost          INTEGER NOT NULL,
       PRIMARY KEY (entrance_from, entrance_to)
     );
     INSERT INTO cluster_interconnections_new
       SELECT entrance_from, entrance_to, cost FROM cluster_interconnections;
     DROP TABLE cluster_interconnections;
     ALTER TABLE cluster_interconnections_new RENAME TO cluster_interconnections;
     CREATE INDEX idx_cluster_inter_to ON cluster_interconnections(entrance_to);
     ```

2. **Add foreign keys and integrity checks**
   - Optional but valuable for validation:

     ```sql
     ALTER TABLE cluster_intraconnections
       ADD FOREIGN KEY (entrance_from) REFERENCES cluster_entrances(entrance_id);
     ALTER TABLE cluster_intraconnections
       ADD FOREIGN KEY (entrance_to) REFERENCES cluster_entrances(entrance_id);
     ALTER TABLE cluster_interconnections
       ADD FOREIGN KEY (entrance_from) REFERENCES cluster_entrances(entrance_id);
     ALTER TABLE cluster_interconnections
       ADD FOREIGN KEY (entrance_to) REFERENCES cluster_entrances(entrance_id);
     ```

3. **Entrance coverage validation**
   - Ensure each cross-chunk boundary has at least one entrance (per plane and side).
   - `neighbor_dir` field (`N, S, E, W`) supports this—add a validator or materialized view to confirm completeness.

4. **Path blobs in `cluster_intraconnections`**
   - Populate `path_blob` with a compressed polyline of optimal intra-cluster paths.
   - Add index:

     ```sql
     CREATE INDEX idx_cluster_intra_from_to
     ON cluster_intraconnections(chunk_x_from, chunk_z_from, plane_from, entrance_from, entrance_to);
     ```

5. **Multiple abstraction levels (optional)**
   - For large maps, consider level-2 overlays grouping multiple clusters:

     ```sql
     CREATE TABLE superclusters (
       supercluster_id INTEGER PRIMARY KEY,
       level INTEGER,
       bounds TEXT
     );
     ```

6. **Cost consistency**
   - Ensure tile-level and entrance-level costs share identical scaling (e.g., 1024 straight, 1448 diagonal).
   - Store these constants in a single source of truth.

7. **Heuristic metadata**
   - Optional `hints` table for per-plane bounding boxes, region restrictions, or heuristic policies (e.g., Manhattan vs. octile).

8. **Performance indexes**
   - `tiles(plane, x, y)` → `idx_tiles_xyplane`
   - `cluster_entrances(plane, x, y)` for fast snapping
   - `cluster_interconnections(entrance_from, entrance_to)` composite PK + index on `entrance_to`
   - `abstract_teleport_edges(src_x, src_y, src_plane)` and `(dst_x, dst_y, dst_plane)`

---

## 🔹 Teleport Integration (HPA* + JPS)

- Treat teleports as **zero-length abstract edges** between entrances or explicit coordinates.
- Use `teleports_all` view and `abstract_teleport_edges` table.
- Validate `teleports_requirements` during path expansion.
- Optionally create a pre-filtered materialized view `teleport_edges_active` for active teleports.

---

## ✅ Final “Go” Checklist

- [ ] Persist movement costs and policies.
- [ ] Ensure entrance coverage across all traversable boundaries.
- [ ] Fix `cluster_interconnections` primary key and add indexes.
- [ ] Fill `path_blob` in `cluster_intraconnections`.
- [ ] (Optional) Precompute JPS jump data or spans.
- [ ] (Optional) Add a higher abstraction level for massive worlds.
- [ ] (Optional) Add FKs or CHECK constraints for data consistency.

---

## ⚙️ Optional: Verifier Script Idea

A simple validator could iterate over the DB and check:

- Missing `path_blob` entries.
- Entrances without counterparts across borders.
- Cost scaling mismatches.
- Empty or inconsistent teleport edges.

---

**With these changes, the database will be 100% ready for efficient, production-grade HPA* + JPS pathfinding.**
