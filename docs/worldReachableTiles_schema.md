---

# 🗺️ Pathfinding & Teleportation Database Schema

This schema defines the data model for a **pathfinding and teleportation system** (likely for a game or simulation). It represents **world tiles**, **clusters of areas**, **entrances between clusters**, and **teleportation mechanisms** (like doors, lodestones, NPCs, and objects).

---

## 📘 Overview

The database organizes the world into *clusters of tiles* and models how entities can move between or within these clusters — either by **walking**, **jumping**, or **teleporting**.

It also tracks **requirements** and **costs** for teleports, used in route computation or graph traversal.

---

## 🧱 Core World Structure

### **`tiles`**

Stores the base terrain grid.

| Column                                         | Type      | Description                                         |
| ---------------------------------------------- | --------- | --------------------------------------------------- |
| `x`, `y`, `plane`                              | `INTEGER` | Tile coordinates and height level. Primary key.     |
| `flag`, `blocked`, `walk_mask`, `blocked_mask` | `INTEGER` | Pathfinding attributes.                             |
| `walk_data`                                    | `TEXT`    | Additional metadata or JSON about tile walkability. |

🔹 **Indexes**

* `idx_tiles_walkable`: fast lookup of unblocked tiles.
* `idx_tiles_xyplane`: generic spatial queries.

---

### **`clusters`**

Represents groups of tiles (regions or zones).

| Column       | Type         | Description                      |
| ------------ | ------------ | -------------------------------- |
| `cluster_id` | `INTEGER PK` | Unique ID for the cluster.       |
| `plane`      | `INTEGER`    | Z-level this cluster belongs to. |
| `label`      | `INTEGER`    | Optional label/classification.   |
| `tile_count` | `INTEGER`    | Number of tiles in the cluster.  |

---

### **`cluster_tiles`**

Maps individual tiles to their containing cluster.

| Column            | Type         | Description                  |
| ----------------- | ------------ | ---------------------------- |
| `cluster_id`      | `INTEGER FK` | Cluster that owns this tile. |
| `x`, `y`, `plane` | `INTEGER`    | Tile position.               |

🔹 **Index**

* `idx_cluster_tiles_xyplane` for efficient reverse lookups.

---

## 🚪 Entrances & Connections

### **`cluster_entrances`**

Defines entrances on the boundary of clusters (where movement or teleportation occurs).

| Column             | Type         | Description                                         |
| ------------------ | ------------ | --------------------------------------------------- |
| `entrance_id`      | `INTEGER PK` | Unique entrance identifier.                         |
| `cluster_id`       | `INTEGER FK` | Cluster this entrance belongs to.                   |
| `x`, `y`, `plane`  | `INTEGER`    | Entrance coordinates.                               |
| `neighbor_dir`     | `TEXT`       | Direction of neighboring area (`N`, `S`, `E`, `W`). |
| `teleport_edge_id` | `INTEGER FK` | Optional link to a teleport edge.                   |

🔹 **Indexes**

* `idx_cluster_entrances_plane_xy`
* `idx_cluster_entrances_cluster_dir`

---

### **`cluster_interconnections`**

Represents *edges between entrances of different clusters* (inter-cluster movement).

| Column                         | Type         | Description                   |
| ------------------------------ | ------------ | ----------------------------- |
| `entrance_from`, `entrance_to` | `INTEGER FK` | Entrance IDs connected.       |
| `cost`                         | `INTEGER`    | Movement cost (non-negative). |

---

### **`cluster_intraconnections`**

Represents *edges within the same cluster* (intra-cluster paths).

| Column                         | Type         | Description                             |
| ------------------------------ | ------------ | --------------------------------------- |
| `entrance_from`, `entrance_to` | `INTEGER FK` | Entrance IDs within the same cluster.   |
| `cost`                         | `INTEGER`    | Movement cost.                          |
| `path_blob`                    | `BLOB`       | Serialized path data between entrances. |

---

## 🌐 Teleportation System

Teleportation nodes represent *non-walking transitions* in the world (e.g., using a door, NPC, or object).

Each type of teleport node links to its *requirement*, *destination coordinates*, and optionally to *next nodes*.

| Table                       | Description                                                               |
| --------------------------- | ------------------------------------------------------------------------- |
| `teleports_door_nodes`      | Doors that can be opened or closed, linking “inside” and “outside” tiles. |
| `teleports_ifslot_nodes`    | Interface slots triggering teleports (e.g., UI-based actions).            |
| `teleports_item_nodes`      | Item-based teleports (e.g., scrolls, rings).                              |
| `teleports_lodestone_nodes` | Lodestone network teleports (fast travel).                                |
| `teleports_npc_nodes`       | NPC-triggered teleports (via dialog/actions).                             |
| `teleports_object_nodes`    | World object teleports (e.g., portals, stairs).                           |

Common columns across these tables include:

| Column                           | Description                         |
| -------------------------------- | ----------------------------------- |
| `id`                             | Primary key.                        |
| `cost`                           | Teleport cost (e.g., time, energy). |
| `requirement_id`                 | FK to `teleports_requirements`.     |
| `next_node_type`, `next_node_id` | Graph link to follow-up teleports.  |

---

### **`abstract_teleport_edges`**

Abstract connections between two teleport endpoints — used by the pathfinding graph.

| Column                         | Type         | Description                 |
| ------------------------------ | ------------ | --------------------------- |
| `edge_id`                      | `INTEGER PK` | Unique ID.                  |
| `src_x`, `src_y`, `src_plane`  | `INTEGER`    | Source coordinates.         |
| `dst_x`, `dst_y`, `dst_plane`  | `INTEGER`    | Destination coordinates.    |
| `cost`                         | `INTEGER`    | Travel cost (≥ 0).          |
| `requirement_id`               | `INTEGER`    | FK to requirement table.    |
| `src_entrance`, `dst_entrance` | `INTEGER`    | Optional link to entrances. |

🔹 **Indexes**

* `idx_abstract_teleport_dst`
* `idx_abstract_teleport_src`
* `idx_ate_requirement`

---

### **`teleports_requirements`**

Holds metadata about conditions needed to use teleports (e.g., quest state, skill level).

| Column                                   | Type         | Description                                                                      |
| ---------------------------------------- | ------------ | -------------------------------------------------------------------------------- |
| `id`                                     | `INTEGER PK` | Requirement ID.                                                                  |
| `metaInfo`, `key`, `value`, `comparison` | `TEXT`       | Encodes a logical condition (like `key='Agility', comparison='>=', value='20'`). |

---

### **`teleports_all` (VIEW)**

A unified view combining all teleport node types for simplified querying.

| Column                        | Description                                                             |
| ----------------------------- | ----------------------------------------------------------------------- |
| `kind`                        | Teleport type (`door`, `npc`, `object`, `item`, `lodestone`, `ifslot`). |
| `id`                          | Source node ID.                                                         |
| `src_x`, `src_y`, `src_plane` | Optional source coordinates.                                            |
| `dst_x`, `dst_y`, `dst_plane` | Destination coordinates.                                                |
| `cost`                        | Travel cost.                                                            |
| `requirement_id`              | Requirement to use teleport.                                            |

---

## 🧭 Pathfinding Optimization Tables

### **`jps_jump`**

Stores **Jump Point Search (JPS)** precomputed jump links to optimize pathfinding.

| Column             | Description                   |
| ------------------ | ----------------------------- |
| `x, y, plane, dir` | Origin tile and direction.    |
| `next_x, next_y`   | Next jump destination.        |
| `forced_mask`      | Bitmask for forced neighbors. |

---

### **`jps_spans`**

Stores *span information* for JPS to efficiently determine jump boundaries.

| Column                                                            | Description                    |
| ----------------------------------------------------------------- | ------------------------------ |
| `x, y, plane`                                                     | Tile identifier.               |
| `left_block_at`, `right_block_at`, `up_block_at`, `down_block_at` | Distance to nearest obstacles. |

---

## ⚙️ Metadata & Policy Tables

### **`meta`**

Simple key–value store for global metadata.

| Column  | Description       |
| ------- | ----------------- |
| `key`   | Setting name.     |
| `value` | Associated value. |

---

### **`movement_policy`**

Defines the global movement configuration used by the pathfinder.

| Column              | Description                                         |
| ------------------- | --------------------------------------------------- |
| `policy_id`         | Must be `1`.                                        |
| `allow_diagonals`   | Whether diagonal moves are allowed.                 |
| `allow_corner_cut`  | Whether diagonal moves through corners are allowed. |
| `unit_radius_tiles` | Size of the agent in tiles.                         |

---

## ⚡ Integrity & Indexing Highlights

* **Constraints:**

  * Costs are always non-negative.
  * `neighbor_dir` restricted to `N, S, E, W`.
  * Referential integrity across teleport edges, entrances, clusters, and requirements.

* **Indexes:**

  * Spatial lookups (`plane, x, y`).
  * Requirement-based joins for teleport tables.
  * Performance-focused indices for cluster traversal and teleport resolution.

---

## 🧩 Entity Relationships (Simplified)

```
[tiles] ─┬─> [cluster_tiles] ─┬─> [clusters]
          │                    └─> [cluster_entrances]
          │                         ├─> [cluster_interconnections]
          │                         └─> [cluster_intraconnections]
          │
          └─> [jps_jump], [jps_spans]

[abstract_teleport_edges] ──> [teleports_requirements]
[teleports_*_nodes] ─────────> [teleports_requirements]
teleports_all (VIEW) aggregates all teleport node types
```

---

## 🧠 Summary

This schema forms a **hybrid navigation and teleportation graph**, suitable for:

* Pathfinding across dynamic terrain.
* Integrating teleport mechanics seamlessly with standard movement.
* Enforcing requirement-based access rules.
* Supporting efficient spatial queries and precomputed jump optimization.

---

Would you like me to include a **diagram (ERD)** or **graph-style visualization** of the relationships in the `.md` file too? It would make this much easier to interpret visually.
