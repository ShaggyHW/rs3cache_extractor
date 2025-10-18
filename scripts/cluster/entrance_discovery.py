import sqlite3
import json
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

DIRECTIONS: List[Tuple[str, Tuple[int, int]]] = [
    ("N", (0, 1)),   # Convention B: N means y+1
    ("E", (1, 0)),
    ("S", (0, -1)),  # Convention B: S means y-1
    ("W", (-1, 0)),
]


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
    if d == "N":
        dx, dy = 0, -1
    elif d == "S":
        dx, dy = 0, 1
    elif d == "E":
        dx, dy = 1, 0
    else:
        dx, dy = -1, 0
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


def _planes_for_chunk(conn: sqlite3.Connection, chunk_x: int, chunk_z: int, planes: Optional[Sequence[int]]) -> List[int]:
    if planes is not None:
        return list(planes)
    # Discover planes present for this chunk
    rows = conn.execute(
        "SELECT DISTINCT plane FROM tiles WHERE chunk_x=? AND chunk_z=?",
        (chunk_x, chunk_z),
    ).fetchall()
    return [int(r[0]) for r in rows]


def _border_tiles(
    conn: sqlite3.Connection,
    chunk_x: int,
    chunk_z: int,
    plane: int,
    x0: int,
    y0: int,
    x1: int,
    y1: int,
) -> List[Tuple[int, int]]:
    # Only walkable tiles on the border to keep O(perimeter)
    cur = conn.execute(
        """
        SELECT x, y FROM tiles
        WHERE chunk_x=? AND chunk_z=? AND plane=?
          AND (x IN (?, ?) OR y IN (?, ?))
          AND blocked=0 AND walk_mask != 0
        """,
        (chunk_x, chunk_z, plane, x0, x1, y0, y1),
    )
    return [(int(r[0]), int(r[1])) for r in cur.fetchall()]


def _delete_existing(conn: sqlite3.Connection, chunk_x: int, chunk_z: int, plane: Optional[int]) -> None:
    if plane is None:
        conn.execute(
            "DELETE FROM cluster_entrances WHERE chunk_x=? AND chunk_z=?",
            (chunk_x, chunk_z),
        )
    else:
        conn.execute(
            "DELETE FROM cluster_entrances WHERE chunk_x=? AND chunk_z=? AND plane=?",
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
    x_min, x_max, z_min, z_max = chunk_range

    # Select chunks within range (inclusive bounds, allowing open-ended)
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

    sql_chunks = f"SELECT chunk_x, chunk_z, chunk_size FROM chunks WHERE {' AND '.join(conds)}"
    rows = conn.execute(sql_chunks, params).fetchall()

    created = 0
    examined_chunks = 0
    examined_tiles = 0

    for row in rows:
        chunk_x = int(row[0])
        chunk_z = int(row[1])
        size = int(row[2])
        x0 = chunk_x * size
        y0 = chunk_z * size
        x1 = x0 + size - 1
        y1 = y0 + size - 1

        for plane in _planes_for_chunk(conn, chunk_x, chunk_z, planes):
            examined_chunks += 1

            if recompute and not dry_run:
                _delete_existing(conn, chunk_x, chunk_z, plane)

            # Gather candidate border tiles
            border = _border_tiles(conn, chunk_x, chunk_z, plane, x0, y0, x1, y1)
            examined_tiles += len(border)

            for (x, y) in border:
                # Determine which borders this tile lies on
                dirs: List[str] = []
                if y == y0:
                    dirs.append("S")
                if x == x1:
                    dirs.append("E")
                if y == y1:
                    dirs.append("N")
                if x == x0:
                    dirs.append("W")

                chosen_dir: Optional[str] = None
                for d, (dx, dy) in DIRECTIONS:
                    if d not in dirs:
                        continue
                    nx, ny = x + dx, y + dy
                    if _is_walkable(conn, nx, ny, plane) and _can_cross(conn, x, y, plane, d):
                        chosen_dir = d
                        break

                if chosen_dir is None:
                    continue

                if dry_run:
                    created += 1
                    continue

                # Idempotent upsert: prefer ON CONFLICT DO UPDATE to keep neighbor_dir deterministic
                conn.execute(
                    """
                    INSERT INTO cluster_entrances (chunk_x, chunk_z, plane, x, y, neighbor_dir)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(chunk_x, chunk_z, plane, x, y)
                    DO UPDATE SET neighbor_dir=excluded.neighbor_dir
                    """,
                    (chunk_x, chunk_z, plane, x, y, chosen_dir),
                )
                created += 1

    logger.info(
        "entrance_discovery: chunks=%s tiles_border=%s entrances_created=%s dry_run=%s",
        examined_chunks,
        examined_tiles,
        created,
        dry_run,
    )

    return {"created": created, "chunks": examined_chunks, "tiles_border": examined_tiles}
