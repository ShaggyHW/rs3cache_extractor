# Suggested Additions for `worldReachableTiles.db` Schema Documentation

This document lists the missing or incomplete elements that could improve the clarity, completeness, and maintainability of the existing `worldReachableTiles_schema.md` documentation.

---

## 1. Context Section

Add a short introductory section describing the purpose and usage context of the database, for example:

> The `worldReachableTiles.db` database defines the tile connectivity, teleports, and pathfinding metadata extracted from RuneScape 3’s game cache. It supports world graph traversal, cluster mapping, and teleportation logic used by external pathfinding utilities.

---

## 2. Table Overview Summary

Include a summary list of all tables and their roles before the detailed DDLs:

| Table | Purpose |
|-------|----------|
| `abstract_teleport_edges` | Abstract edges connecting source/destination tiles via teleportation |
| `chunks` | Defines world chunks by (x,z) coordinates |
| `chunk_clusters` | Groups tiles into connected clusters within a chunk |
| `cluster_entrances` | Points where clusters connect to others |
| `cluster_interconnections` | Links between entrances of different clusters |
| `cluster_intraconnections` | Links between entrances within the same cluster |
| `cluster_tiles` | Tiles belonging to each cluster |
| `jps_jump` / `jps_spans` | Data structures supporting Jump Point Search optimization |
| `meta` | Stores static key-value metadata |
| `movement_policy` | Configures movement parameters (diagonals, corner cutting) |
| `teleports_*_nodes` | Represent teleport actions triggered by specific in-game entities |
| `teleports_requirements` | Stores requirement metadata for teleports |
| `tiles` | World tiles with movement and blocking information |

---

## 3. Logical Relationships (Non-enforced FKs)

Document relationships not backed by explicit foreign keys in SQLite:

- `abstract_teleport_edges.requirement_id` → `teleports_requirements.id`
- `teleports_*_nodes.requirement_id` → `teleports_requirements.id`
- `teleports_*_nodes.next_node_id` + `next_node_type` → polymorphic link to other `teleports_*_nodes` tables
- `abstract_teleport_edges.src_*` / `dst_*` → logical link to `tiles(x, y, plane)`

---

## 4. Type Normalization Notes

Explain why mixed data types are used:

> Several teleport tables use `REAL`, `INTEGER`, or `TEXT` inconsistently for conceptually similar fields (e.g., `requirement_id`, `next_node_type`). This originates from serialized JSON input from the RS3 cache and may be normalized later to consistent types.

---

## 5. Field-Level Descriptions

Add per-column descriptions for commonly used tables:

### `tiles`
| Column | Description |
|---------|-------------|
| `flag` | Raw tile flag value indicating terrain state |
| `blocked` | 1 if movement blocked, 0 otherwise |
| `walk_mask` | Bitmask defining allowed movement directions |
| `blocked_mask` | Bitmask defining restricted directions |
| `walk_data` | Serialized JSON or hex data for detailed navigation |

### `cluster_entrances`
| Column | Description |
|---------|-------------|
| `neighbor_dir` | Cardinal direction toward adjacent cluster (`N`, `S`, `E`, `W`) |

### `cluster_interconnections` / `cluster_intraconnections`
| Column | Description |
|---------|-------------|
| `cost` | Movement or teleportation cost between entrances |

---

## 6. View Dependencies

Add a dependency map showing which tables contribute to `teleports_all`:

```
teleports_all
 ├─ teleports_door_nodes
 ├─ teleports_lodestone_nodes
 ├─ teleports_npc_nodes
 ├─ teleports_object_nodes
 ├─ teleports_item_nodes
 └─ teleports_ifslot_nodes
```

---

## 7. ER Diagram Enhancement

Add logical (non-enforced) relationships to the ER diagram (dotted lines) for better visualization:
- `teleports_*_nodes.requirement_id` → `teleports_requirements.id`
- `abstract_teleport_edges.requirement_id` → `teleports_requirements.id`
- `abstract_teleport_edges.src_*` / `dst_*` → `tiles`

---

## 8. Consistency and Validation Notes

Add a final section for database integrity recommendations:

> **Recommended PRAGMAs and checks**
> - Enable `PRAGMA foreign_keys = ON;` in clients for optional consistency enforcement.
> - Run validation queries to detect orphaned `requirement_id` references.
> - Ensure `id` columns in `teleports_*_nodes` are unique even if not declared as `PRIMARY KEY`.

---

## 9. Optional Future Enhancements

- Define explicit `PRIMARY KEY(id)` for all `teleports_*_nodes` tables.
- Add `CHECK` constraints for valid direction values and teleport cost bounds.
- Introduce version tracking via a `schema_version` meta key.
