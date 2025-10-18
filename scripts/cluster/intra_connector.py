import heapq
import json
import sqlite3
from typing import Callable, Dict, List, Optional, Sequence, Set, Tuple

from .neighbor_policy import NeighborPolicy
from .jps_accelerator import expand_with_jps, extract_waypoints


def _chunk_bounds(chunk_x: int, chunk_z: int, size: int) -> Tuple[int, int, int, int]:
    x0 = chunk_x * size
    y0 = chunk_z * size
    x1 = x0 + size - 1
    y1 = y0 + size - 1
    return x0, y0, x1, y1


def _in_bounds(x: int, y: int, bounds: Tuple[int, int, int, int]) -> bool:
    x0, y0, x1, y1 = bounds
    return x0 <= x <= x1 and y0 <= y <= y1


def _is_walkable(conn: sqlite3.Connection, x: int, y: int, plane: int) -> bool:
    row = conn.execute(
        "SELECT blocked, walk_mask FROM tiles WHERE x=? AND y=? AND plane=?",
        (x, y, plane),
    ).fetchone()
    if row is None:
        return False
    blocked = int(row[0]) if row[0] is not None else 1
    walk_mask = int(row[1]) if row[1] is not None else 0
    return (blocked == 0) and (walk_mask != 0)


def _build_local_walkable(
    conn: sqlite3.Connection, chunk_x: int, chunk_z: int, plane: int
) -> Set[Tuple[int, int]]:
    rows = conn.execute(
        "SELECT x, y, blocked, walk_mask FROM tiles WHERE chunk_x=? AND chunk_z=? AND plane=?",
        (chunk_x, chunk_z, plane),
    ).fetchall()
    walkable: Set[Tuple[int, int]] = set()
    for r in rows:
        x, y = int(r[0]), int(r[1])
        blocked = int(r[2]) if r[2] is not None else 1
        walk_mask = int(r[3]) if r[3] is not None else 0
        if blocked == 0 and walk_mask != 0:
            walkable.add((x, y))
    return walkable


def _make_walkable_cb(
    conn: sqlite3.Connection,
    bounds: Tuple[int, int, int, int],
    plane: int,
    local_walkable: Set[Tuple[int, int]],
) -> Callable[[int, int, int], bool]:
    def cb(x: int, y: int, p: int) -> bool:
        if p != plane:
            # Different plane: fallback to DB
            return _is_walkable(conn, x, y, p)
        if _in_bounds(x, y, bounds):
            return (x, y) in local_walkable
        # Outside this chunk: fallback to DB
        return _is_walkable(conn, x, y, p)

    return cb


def _chebyshev_cost(ax: int, ay: int, bx: int, by: int) -> int:
    return max(abs(bx - ax), abs(by - ay))


def _a_star(
    conn: sqlite3.Connection,
    policy: NeighborPolicy,
    start: Tuple[int, int, int],
    goal: Tuple[int, int, int],
    bounds: Tuple[int, int, int, int],
    use_jps: bool,
    is_walkable_cb: Optional[Callable[[int, int, int], bool]] = None,
) -> Optional[Tuple[int, List[Tuple[int, int, int]]]]:
    sx, sy, sp = start
    gx, gy, gp = goal
    if sp != gp:
        return None
    if not (_in_bounds(sx, sy, bounds) and _in_bounds(gx, gy, bounds)):
        return None
    chk = (is_walkable_cb or (lambda x, y, p: _is_walkable(conn, x, y, p)))
    if not (chk(sx, sy, sp) and chk(gx, gy, gp)):
        return None

    open_heap: List[Tuple[int, Tuple[int, int, int]]] = []
    heapq.heappush(open_heap, (0, start))

    g_score: Dict[Tuple[int, int, int], int] = {start: 0}
    f_score: Dict[Tuple[int, int, int], int] = {start: _chebyshev_cost(sx, sy, gx, gy)}
    came: Dict[Tuple[int, int, int], Tuple[int, int, int]] = {}
    closed: set[Tuple[int, int, int]] = set()

    pol = policy if is_walkable_cb is None else policy.with_walkable(is_walkable_cb)

    while open_heap:
        _, current = heapq.heappop(open_heap)
        if current in closed:
            continue
        closed.add(current)
        cx, cy, cp = current
        if current == goal:
            # reconstruct
            path: List[Tuple[int, int, int]] = [current]
            while current in came:
                current = came[current]
                path.append(current)
            path.reverse()
            return g_score[(gx, gy, gp)], path

        # neighbors
        if use_jps:
            nbrs = expand_with_jps(conn, pol, cx, cy, cp, is_walkable_cb)
        else:
            nbrs = pol.neighbors(cx, cy, cp)

        for nx, ny, np in nbrs:
            if np != cp:
                continue
            if not _in_bounds(nx, ny, bounds):
                continue
            if (nx, ny, np) in closed:
                continue
            step = _chebyshev_cost(cx, cy, nx, ny)
            if step <= 0:
                continue
            tentative = g_score[(cx, cy, cp)] + step
            if tentative < g_score.get((nx, ny, np), 1 << 60):
                came[(nx, ny, np)] = (cx, cy, cp)
                g_score[(nx, ny, np)] = tentative
                f = tentative + _chebyshev_cost(nx, ny, gx, gy)
                f_score[(nx, ny, np)] = f
                heapq.heappush(open_heap, (f, (nx, ny, np)))

    return None


def _pairs(entrances: List[Tuple[int, int, int, int]]) -> List[Tuple[int, int]]:
    # entrances: list of (entrance_id, x, y, plane)
    n = len(entrances)
    out: List[Tuple[int, int]] = []
    for i in range(n):
        for j in range(i + 1, n):
            out.append((i, j))
    return out


def _delete_existing_scope(
    conn: sqlite3.Connection,
    chunk_x: int,
    chunk_z: int,
    plane: int,
) -> None:
    conn.execute(
        "DELETE FROM cluster_intraconnections WHERE chunk_x_from=? AND chunk_z_from=? AND plane_from=?",
        (chunk_x, chunk_z, plane),
    )


def run(
    *,
    conn: sqlite3.Connection,
    planes: Optional[Sequence[int]],
    chunk_range: Tuple[Optional[int], Optional[int], Optional[int], Optional[int]],
    recompute: bool,
    dry_run: bool,
    workers: int,
    store_paths: bool,
    logger,
) -> Dict[str, int]:
    # Load policy once per phase
    policy = NeighborPolicy.from_db(conn)

    x_min, x_max, z_min, z_max = chunk_range
    conds = ["1=1"]
    params: List[int] = []
    if x_min is not None:
        conds.append("chunk_x >= ?")
        params.append(int(x_min))
    if x_max is not None:
        conds.append("chunk_x <= ?")
        params.append(int(x_max))
    if z_min is not None:
        conds.append("chunk_z >= ?")
        params.append(int(z_min))
    if z_max is not None:
        conds.append("chunk_z <= ?")
        params.append(int(z_max))

    chunks_sql = f"SELECT chunk_x, chunk_z, chunk_size FROM chunks WHERE {' AND '.join(conds)}"
    chunks = conn.execute(chunks_sql, params).fetchall()

    total_pairs = 0
    created = 0
    solved = 0
    i = 0

    began = False
    try:
        if not dry_run:
            conn.execute("BEGIN IMMEDIATE")
            began = True

        for crow in chunks:
            i += 1
            chunk_x = int(crow[0])
            chunk_z = int(crow[1])
            size = int(crow[2])
            bounds = _chunk_bounds(chunk_x, chunk_z, size)

            print(f"Processing chunk {chunk_x},{chunk_z} ({size}x{size}) {i}/ {len(chunks)} total chunks")

            # planes in this chunk scope
            if planes is None:
                p_rows = conn.execute(
                    "SELECT DISTINCT plane FROM tiles WHERE chunk_x=? AND chunk_z=?",
                    (chunk_x, chunk_z),
                ).fetchall()
                planes_local = [int(r[0]) for r in p_rows]
            else:
                planes_local = list(planes)

            for plane in planes_local:
                # entrances in this chunk+plane
                erows = conn.execute(
                    """
                    SELECT entrance_id, x, y FROM cluster_entrances
                    WHERE chunk_x=? AND chunk_z=? AND plane=?
                    ORDER BY entrance_id ASC
                    """,
                    (chunk_x, chunk_z, plane),
                ).fetchall()
                entrances = [(int(r[0]), int(r[1]), int(r[2]), plane) for r in erows]
                if not entrances:
                    continue

                # Build local walkability cache and callback
                local_walkable = _build_local_walkable(conn, chunk_x, chunk_z, plane)
                walk_cb = _make_walkable_cb(conn, bounds, plane, local_walkable)

                if recompute and not dry_run:
                    _delete_existing_scope(conn, chunk_x, chunk_z, plane)

                pairs = _pairs(entrances)
                total_pairs += len(pairs)
                for i, j in pairs:
                    e1 = entrances[i]
                    e2 = entrances[j]
                    eid1, x1, y1, p1 = e1
                    eid2, x2, y2, p2 = e2

                    # Skip if same tile
                    if x1 == x2 and y1 == y2 and p1 == p2:
                        continue

                    # Plan path (prefer JPS expansion when available)
                    use_jps = True
                    found = _a_star(
                        conn,
                        policy,
                        (x1, y1, p1),
                        (x2, y2, p2),
                        bounds,
                        use_jps,
                        walk_cb,
                    )
                    if found is None:
                        # Non-reachable pair
                        continue
                    cost, path = found
                    solved += 1

                    waypoint_blob = None
                    if store_paths:
                        waypoints = extract_waypoints(path)
                        # encode waypoints as compact JSON [[x,y],[x,y],...]
                        packed = [[px, py] for (px, py, _) in waypoints]
                        waypoint_blob = json.dumps(packed).encode("utf-8")

                    if dry_run:
                        created += 2  # both directions recorded
                        continue

                    # Insert both directions, idempotent
                    conn.execute(
                        """
                        INSERT INTO cluster_intraconnections
                          (chunk_x_from, chunk_z_from, plane_from, entrance_from, entrance_to, cost, path_blob)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(chunk_x_from, chunk_z_from, plane_from, entrance_from, entrance_to)
                        DO UPDATE SET
                          cost = MIN(cluster_intraconnections.cost, excluded.cost),
                          path_blob = CASE WHEN excluded.path_blob IS NOT NULL THEN excluded.path_blob ELSE cluster_intraconnections.path_blob END
                        """,
                        (chunk_x, chunk_z, plane, eid1, eid2, cost, waypoint_blob),
                    )
                    conn.execute(
                        """
                        INSERT INTO cluster_intraconnections
                          (chunk_x_from, chunk_z_from, plane_from, entrance_from, entrance_to, cost, path_blob)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(chunk_x_from, chunk_z_from, plane_from, entrance_from, entrance_to)
                        DO UPDATE SET
                          cost = MIN(cluster_intraconnections.cost, excluded.cost),
                          path_blob = CASE WHEN excluded.path_blob IS NOT NULL THEN excluded.path_blob ELSE cluster_intraconnections.path_blob END
                        """,
                        (chunk_x, chunk_z, plane, eid2, eid1, cost, waypoint_blob),
                    )
                    created += 2

        if began:
            conn.execute("COMMIT")
    except Exception:
        if began:
            conn.execute("ROLLBACK")
        raise

    logger.info(
        "intra_connector: pairs_total=%s pairs_solved=%s rows_created=%s dry_run=%s",
        total_pairs,
        solved,
        created,
        dry_run,
    )

    return {"created": created, "pairs_total": total_pairs, "pairs_solved": solved}
