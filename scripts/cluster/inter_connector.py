import sqlite3
import json
from typing import Dict, List, Optional, Sequence, Tuple

OPPOSITE = {"N": "S", "S": "N", "E": "W", "W": "E"}
# Convention B: vertical flipped (N means y+1; S means y-1)
DIR_DELTA = {"N": (0, 1), "E": (1, 0), "S": (0, -1), "W": (-1, 0)}


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


def _get_walk_flags(conn: sqlite3.Connection, x: int, y: int, plane: int) -> Optional[Dict[str, bool]]:
    row = conn.execute(
        "SELECT walk_data FROM tiles WHERE x=? AND y=? AND plane=?",
        (x, y, plane),
    ).fetchone()
    if not row or row[0] is None:
        return None
    try:
        data = json.loads(row[0])
        return {k: bool(v) for k, v in data.items()}
    except Exception:
        return None


def _can_cross(conn: sqlite3.Connection, x: int, y: int, plane: int, d: str) -> bool:
    dx, dy = DIR_DELTA[d]
    nx, ny = x + dx, y + dy
    a = _get_walk_flags(conn, x, y, plane) or {}
    b = _get_walk_flags(conn, nx, ny, plane) or {}
    if d == "N":
        return a.get("bottom", True) and b.get("top", True)
    if d == "S":
        return a.get("top", True) and b.get("bottom", True)
    if d == "E":
        return a.get("right", True) and b.get("left", True)
    if d == "W":
        return a.get("left", True) and b.get("right", True)
    return True


def _select_chunk_pairs(conn: sqlite3.Connection,
                         planes: Optional[Sequence[int]],
                         chunk_range: Tuple[Optional[int], Optional[int], Optional[int], Optional[int]]
                         ) -> List[Tuple[int, int, int, int, int, int, str]]:
    # We iterate entrances filtered by chunk range and optional planes
    x_min, x_max, z_min, z_max = chunk_range
    conds = ["1=1"]
    params: List[int] = []
    if x_min is not None:
        conds.append("ce.chunk_x >= ?")
        params.append(int(x_min))
    if x_max is not None:
        conds.append("ce.chunk_x <= ?")
        params.append(int(x_max))
    if z_min is not None:
        conds.append("ce.chunk_z >= ?")
        params.append(int(z_min))
    if z_max is not None:
        conds.append("ce.chunk_z <= ?")
        params.append(int(z_max))
    if planes is not None and len(planes) > 0:
        placeholders = ",".join(["?"] * len(planes))
        conds.append(f"ce.plane IN ({placeholders})")
        params.extend([int(p) for p in planes])

    sql = f"""
        SELECT ce.entrance_id, ce.chunk_x, ce.chunk_z, ce.plane, ce.x, ce.y, ce.neighbor_dir
        FROM cluster_entrances ce
        WHERE {' AND '.join(conds)}
        ORDER BY ce.chunk_x, ce.chunk_z, ce.plane, ce.entrance_id
    """
    rows = conn.execute(sql, params).fetchall()
    return [(int(r[0]), int(r[1]), int(r[2]), int(r[3]), int(r[4]), int(r[5]), str(r[6])) for r in rows]


def _neighbor_chunk(chunk_x: int, chunk_z: int, d: str) -> Tuple[int, int]:
    if d == "N":
        return (chunk_x, chunk_z + 1)
    if d == "S":
        return (chunk_x, chunk_z - 1)
    if d == "E":
        return (chunk_x + 1, chunk_z)
    if d == "W":
        return (chunk_x - 1, chunk_z)
    raise ValueError("invalid dir")


def _find_opposing_entrance(conn: sqlite3.Connection, x: int, y: int, plane: int,
                            opp_chunk_x: int, opp_chunk_z: int, opp_dir: str,
                            dx: int, dy: int) -> Optional[int]:
    # Opposing entrance is at the adjacent tile across the border (x+dx, y+dy)
    row = conn.execute(
        """
        SELECT entrance_id FROM cluster_entrances
        WHERE chunk_x=? AND chunk_z=? AND plane=? AND x=? AND y=? AND neighbor_dir=?
        """,
        (opp_chunk_x, opp_chunk_z, plane, x + dx, y + dy, opp_dir),
    ).fetchone()
    return int(row[0]) if row else None


def _delete_existing_for_scope(conn: sqlite3.Connection,
                               planes: Optional[Sequence[int]],
                               chunk_range: Tuple[Optional[int], Optional[int], Optional[int], Optional[int]]) -> None:
    # Delete interconnections where entrance_from belongs to the scope
    x_min, x_max, z_min, z_max = chunk_range
    conds = ["1=1"]
    params: List[int] = []
    if x_min is not None:
        conds.append("ce.chunk_x >= ?")
        params.append(int(x_min))
    if x_max is not None:
        conds.append("ce.chunk_x <= ?")
        params.append(int(x_max))
    if z_min is not None:
        conds.append("ce.chunk_z >= ?")
        params.append(int(z_min))
    if z_max is not None:
        conds.append("ce.chunk_z <= ?")
        params.append(int(z_max))
    if planes is not None and len(planes) > 0:
        placeholders = ",".join(["?"] * len(planes))
        conds.append(f"ce.plane IN ({placeholders})")
        params.extend([int(p) for p in planes])

    sql = f"""
        DELETE FROM cluster_interconnections
        WHERE entrance_from IN (
            SELECT ce.entrance_id FROM cluster_entrances ce WHERE {' AND '.join(conds)}
        )
    """
    conn.execute(sql, params)


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
    # Optionally clear previous edges for scope for reproducibility
    if recompute and not dry_run:
        _delete_existing_for_scope(conn, planes, chunk_range)

    entrances = _select_chunk_pairs(conn, planes, chunk_range)
    created = 0
    examined = 0

    for entrance_id, chunk_x, chunk_z, plane, x, y, d in entrances:
        examined += 1
        opp_dir = OPPOSITE.get(d)
        if not opp_dir:
            continue
        n_cx, n_cz = _neighbor_chunk(chunk_x, chunk_z, d)

        # sanity check: both tiles walkable and directional pass-through allowed
        if not _is_walkable(conn, x, y, plane):
            continue
        dx, dy = DIR_DELTA[d]
        if not _is_walkable(conn, x + dx, y + dy, plane):
            continue
        if not _can_cross(conn, x, y, plane, d):
            continue

        opp_id = _find_opposing_entrance(conn, x, y, plane, n_cx, n_cz, opp_dir, dx, dy)
        if opp_id is None:
            continue

        if dry_run:
            created += 2  # we would insert both directions
            continue

        # Cost model: adjacent crossing cost = 1 (tile step)
        cost = 1

        # Insert bidirectional, idempotent via ON CONFLICT DO UPDATE keeping lowest cost
        conn.execute(
            """
            INSERT INTO cluster_interconnections (entrance_from, entrance_to, cost)
            VALUES (?, ?, ?)
            ON CONFLICT(entrance_from, entrance_to)
            DO UPDATE SET cost = MIN(cluster_interconnections.cost, excluded.cost)
            """,
            (entrance_id, opp_id, cost),
        )
        conn.execute(
            """
            INSERT INTO cluster_interconnections (entrance_from, entrance_to, cost)
            VALUES (?, ?, ?)
            ON CONFLICT(entrance_from, entrance_to)
            DO UPDATE SET cost = MIN(cluster_interconnections.cost, excluded.cost)
            """,
            (opp_id, entrance_id, cost),
        )
        created += 2

    logger.info(
        "inter_connector: entrances_examined=%s inter_edges_created=%s dry_run=%s",
        examined,
        created,
        dry_run,
    )

    return {"created": created, "examined": examined}
