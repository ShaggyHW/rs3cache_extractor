# Plane Connection Analysis

## Summary

Investigation of the cluster connection stages shows that planes above 0 are **not** being skipped. Both the inter- and intra-connection stages create edges on planes 1 and 2, albeit at far lower volumes than plane 0 because the source data contains far fewer walkable tiles and entrances on those planes. The apparent absence stems from inspecting `cluster_interconnections` in isolation, where the table lacks an explicit `plane` column; joining back to entrance metadata reveals non-zero planes are present.

## Data Checks

| Query | Plane 0 | Plane 1 | Plane 2 |
| --- | --- | --- | --- |
| `cluster_tiles` rows | 684,215 | 13,020 | 2,652 |
| `cluster_entrances` rows | 37,085 | 426 | 95 |
| `cluster_interconnections` edges (after joining via `cluster_entrances`) | 33,026 | 364 | 80 |
| `cluster_intraconnections` edges (after joining via `clusters`) | 670,802 | 4,701 | 510 |

Queries were executed directly against `worldReachableTiles.db` to confirm that non-zero planes flow through every stage.

## Code Review

- **Inter connectors** retrieve entrances across all planes (ordered by plane) and only filter by the optional configuration scopes (`planes`, `chunk_range`). There is no hard-coded plane restriction, and walkability checks always include the caller-provided plane parameter @rust/src/commands/cluster/inter_connector.rs#23-123.
- **Intra connectors** build the cluster list grouped by `(cluster_id, plane)` and retain clusters on non-zero planes unless the optional `planes` filter excludes them @rust/src/commands/cluster/intra_connector.rs#21-120. Tile membership and pathfinding also operate with the plane-aware queries @rust/src/commands/cluster/intra_connector.rs#121-199.
- **Entrance discovery** iterates every distinct plane represented in `cluster_tiles`, so entrances exist for higher floors when cluster tiles are present @rust/src/commands/cluster/entrance_discovery.rs#21-105.

## Explanation

Because the `cluster_interconnections` table lacks a `plane` column, looking at its rows alone can give the impression that only plane 0 entries exist. Joining through `cluster_entrances` or `clusters` shows that planes 1 and 2 are populated, just at lower counts consistent with the reduced number of tiles and entrances on those floors. No code paths explicitly discard higher planes.

## Recommendations

1. When validating outputs, always join `cluster_interconnections` back to `cluster_entrances` (or `clusters`) to identify the plane.
2. If higher plane coverage still appears insufficient, compare against the source tile density for those planes to confirm expectations, or supply a `--planes` filter to focus runs on specific floors.
3. Consider adding diagnostic logging or metrics breaking down inter/intra edge counts per plane to make stage outputs easier to verify.
