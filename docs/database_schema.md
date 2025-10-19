# 🗄️ Database Schema Overview

This database defines a **pathfinding and teleportation system** used for spatial navigation, clustering, and teleport mechanisms such as doors, lodestones, NPCs, and interactive objects.

---

## 📚 Tables

### 1. **abstract_teleport_edges**
Represents abstract teleportation links between nodes in a graph structure.

| Column | Type | Description |
|--------|------|-------------|
| edge_id | INTEGER (PK) | Unique edge identifier. |
| kind | TEXT | Type of teleport (`door`, `lodestone`, `npc`, `object`, `item`, `ifslot`). Must match allowed kinds. |
| node_id | INTEGER | Node identifier linked to this edge. |
| src_x, src_y, src_plane | INTEGER | Source coordinates (nullable). |
| dst_x, dst_y, dst_plane | INTEGER | Destination coordinates (non-null). |
| cost | INTEGER | Travel cost (must be non-negative). |
| requirement_id | INTEGER | References `teleports_requirements(id)`. |
| src_entrance, dst_entrance | INTEGER | Optional references to cluster entrances. |

---

### 2. **clusters**
Defines topological clusters or spatial regions.

| Column | Type | Description |
|--------|------|-------------|
| cluster_id | INTEGER (PK) | Cluster identifier. |
| plane | INTEGER | Plane (z-layer) identifier. |
| label | INTEGER | Optional label or category. |
| tile_count | INTEGER | Number of tiles in the cluster. |

---

### 3. **cluster_entrances**
Defines cluster entrances/exits, possibly linked to teleport edges.

| Column | Type | Description |
|--------|------|-------------|
| entrance_id | INTEGER (PK) | Unique entrance identifier. |
| cluster_id | INTEGER | References `clusters(cluster_id)`. |
| x, y, plane | INTEGER | Entrance coordinates. |
| neighbor_dir | TEXT | Direction (`N`, `S`, `E`, `W`, `TP`). |
| teleport_edge_id | INTEGER | References `abstract_teleport_edges(edge_id)`. |

---

### 4. **cluster_interconnections**
Defines **inter-cluster** connections (paths between different clusters).

| Column | Type | Description |
|--------|------|-------------|
| entrance_from | INTEGER | Origin entrance ID. |
| entrance_to | INTEGER | Destination entrance ID. |
| cost | INTEGER | Non-negative travel cost. |

---

### 5. **cluster_intraconnections**
Defines **intra-cluster** connections (paths within the same cluster).

| Column | Type | Description |
|--------|------|-------------|
| entrance_from | INTEGER | Origin entrance ID. |
| entrance_to | INTEGER | Destination entrance ID. |
| cost | INTEGER | Non-negative path cost. |
| path_blob | BLOB | Serialized path data. |

---

### 6. **cluster_tiles**
Lists all tiles associated with a cluster.

| Column | Type | Description |
|--------|------|-------------|
| cluster_id | INTEGER | References `clusters(cluster_id)`. |
| x, y, plane | INTEGER | Tile coordinates (composite PK). |

---

### 7. **meta**
Stores global metadata for the map or schema.

| Column | Type | Description |
|--------|------|-------------|
| key | TEXT (PK) | Metadata key (`schema_version`, `tileset_version`, `map_build_at`, etc.). |
| value | TEXT | Metadata value. |

---

### 8. **teleports_door_nodes**
Represents teleportation via doors (open/close state).

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER (PK) | Door node identifier. |
| direction | TEXT | Door facing direction. |
| real_id_open / real_id_closed | INTEGER | IDs of door objects (open/closed). |
| location_* | INTEGER | Coordinates for open/closed states. |
| tile_inside_* / tile_outside_* | INTEGER | Inside/outside tile coordinates. |
| open_action | TEXT | Action name for opening. |
| cost | INTEGER | Teleport or transition cost. |
| next_node_type, next_node_id | TEXT / INTEGER | Chaining information. |
| requirement_id | INTEGER | Requirement reference. |

---

### 9. **teleports_ifslot_nodes**
Teleportation triggered via **interface slot actions**.

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER (PK) | Interface slot node ID. |
| interface_id, component_id, slot_id, click_id | INTEGER | Interface identification fields. |
| dest_min_x, dest_max_x, dest_min_y, dest_max_y, dest_plane | INTEGER | Destination area bounds. |
| cost | INTEGER | Teleport cost. |
| next_node_type, next_node_id | TEXT / INTEGER | Chaining information. |
| requirement_id | INTEGER | Requirement reference. |

---

### 10. **teleports_item_nodes**
Teleportation triggered via **item usage**.

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER (PK) | Item node ID. |
| item_id | INTEGER | Item identifier. |
| action | TEXT | Action performed (e.g., "use", "rub"). |
| dest_min_x, dest_max_x, dest_min_y, dest_max_y, dest_plane | INTEGER | Destination area bounds. |
| next_node_type, next_node_id | TEXT / INTEGER | Chaining information. |
| cost | INTEGER | Teleport cost. |
| requirement_id | INTEGER | Requirement reference. |

---

### 11. **teleports_lodestone_nodes**
Defines **lodestone-based teleportation**.

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER (PK) | Lodestone node ID. |
| lodestone | TEXT | Lodestone name. |
| dest_x, dest_y, dest_plane | INTEGER | Destination coordinates. |
| cost | INTEGER | Teleport cost. |
| next_node_type, next_node_id | TEXT / INTEGER | Chaining information. |
| requirement_id | INTEGER | Requirement reference. |

---

### 12. **teleports_npc_nodes**
Teleportation via **NPC interaction**.

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER (PK) | NPC teleport node ID. |
| match_type | TEXT | Matching strategy (e.g., by ID or name). |
| npc_id | INTEGER | NPC identifier. |
| npc_name | TEXT | NPC name. |
| action | TEXT | Interaction action (e.g., "talk-to"). |
| dest_min_x, dest_max_x, dest_min_y, dest_max_y, dest_plane | INTEGER | Destination bounds. |
| search_radius | INTEGER | NPC search radius. |
| cost | INTEGER | Teleport cost. |
| orig_min_x, orig_max_x, orig_min_y, orig_max_y, orig_plane | INTEGER | Source area bounds. |
| next_node_type, next_node_id | TEXT / INTEGER | Chaining fields. |
| requirement_id | INTEGER | Requirement reference. |

---

### 13. **teleports_object_nodes**
Teleportation via **object interaction**.

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER (PK) | Object node ID. |
| match_type | TEXT | Matching strategy. |
| object_id | INTEGER | Object identifier. |
| object_name | TEXT | Object name. |
| action | TEXT | Action performed. |
| dest_min_x, dest_max_x, dest_min_y, dest_max_y, dest_plane | INTEGER | Destination bounds. |
| orig_min_x, orig_max_x, orig_min_y, orig_max_y, orig_plane | INTEGER | Source bounds. |
| search_radius | INTEGER | Object search radius. |
| cost | INTEGER | Teleport cost. |
| next_node_type, next_node_id | TEXT / INTEGER | Chaining information. |
| requirement_id | INTEGER | Requirement reference. |

---

### 14. **teleports_requirements**
Defines **teleport condition requirements**.

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER (PK) | Requirement identifier. |
| metaInfo | TEXT | Metadata or description. |
| key | TEXT | Requirement key. |
| value | TEXT | Expected value. |
| comparison | TEXT | Comparison operator. |

---

### 15. **tiles**
Defines **world tiles** and their pathfinding attributes.

| Column | Type | Description |
|--------|------|-------------|
| x, y, plane | INTEGER | Tile coordinates (PK). |
| flag | INTEGER | Tile flag bitfield. |
| blocked | INTEGER | 0 if walkable, else 1. |
| walk_mask, blocked_mask | INTEGER | Mask fields for movement constraints. |
| walk_data | TEXT | Additional walk data. |

---

## 🧩 Indexes
Key indexes that optimize spatial and teleport queries:

- **Teleportation edges**  
  - `idx_abstract_teleport_dst (dst_plane, dst_x, dst_y)`  
  - `idx_abstract_teleport_src (src_plane, src_x, src_y)`  
  - `idx_ate_kind_node (kind, node_id)`  
  - `idx_ate_requirement (requirement_id)`

- **Clusters & Entrances**  
  - `idx_cluster_entrances_plane_xy (plane, x, y)`  
  - `idx_cluster_inter_to (entrance_to)`  
  - `idx_cluster_intra_from_to (entrance_from, entrance_to)`  
  - `idx_cluster_tiles_xyplane (plane, x, y)`  
  - `idx_cluster_entrances_cluster_dir (cluster_id, neighbor_dir)`

- **Teleport Requirements**  
  - `idx_teleport_req_all (id)`  
  - `idx_tdoor_req`, `idx_tnpc_req`, `idx_tobj_req`, `idx_titem_req`, `idx_tif_req`, `idx_tlode_req`

- **Tiles**  
  - `idx_tiles_walkable` (only for non-blocked tiles)  
  - `idx_tiles_xyplane (x, y, plane)`

---

## 👁️ View: `teleports_all`
A unified **virtual view** combining all teleport node tables.

| Column | Description |
|--------|-------------|
| kind | Teleport type (`door`, `lodestone`, `npc`, `object`, `item`, `ifslot`). |
| id | Node ID. |
| src_x, src_y, src_plane | Source coordinates (may be null). |
| dst_x, dst_y, dst_plane | Destination coordinates. |
| cost | Teleport cost. |
| requirement_id | Linked requirement ID. |

This view aggregates data from all `teleports_*_nodes` tables for unified access.

---

## ⚙️ Summary
This schema defines a **comprehensive spatial navigation and teleportation system** including:
- Abstract teleport edges for unified planning.
- Cluster-based spatial organization.
- Multiple teleportation mechanisms unified under `teleports_all`.
- Strong indexing for efficient pathfinding queries.

