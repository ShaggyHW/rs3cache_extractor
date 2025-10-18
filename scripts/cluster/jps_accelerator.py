import sqlite3
from typing import Callable, Iterable, List, Optional, Sequence, Tuple

from .neighbor_policy import NeighborPolicy


_JPS_PRESENT_CACHE: Optional[bool] = None


def _tables_present(conn: sqlite3.Connection) -> bool:
    global _JPS_PRESENT_CACHE
    if _JPS_PRESENT_CACHE is not None:
        return _JPS_PRESENT_CACHE
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('jps_jump','jps_spans')"
    ).fetchall()
    _JPS_PRESENT_CACHE = len(rows) >= 1  # at least one present is enough
    return _JPS_PRESENT_CACHE


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


def _chebyshev_cost(x0: int, y0: int, x1: int, y1: int) -> int:
    return max(abs(x1 - x0), abs(y1 - y0))


def expand_with_jps(
    conn: sqlite3.Connection,
    policy: NeighborPolicy,
    x: int,
    y: int,
    plane: int,
    is_walkable: Optional[Callable[[int, int, int], bool]] = None,
) -> List[Tuple[int, int, int]]:
    """
    Return accelerated expansion set from (x,y,plane).
    - If JPS tables exist, use jps_jump to emit next jump points.
    - Fallback to policy.neighbors() if no acceleration available for this tile.
    Notes:
      Returned list is deterministic in DB order (rowid) then unique-filtered while preserving order.
      Costs are not returned here; callers compute costs with Chebyshev distance for parity.
    """
    results: List[Tuple[int, int, int]] = []
    if _tables_present(conn):
        cur = conn.execute(
            "SELECT next_x, next_y FROM jps_jump WHERE x=? AND y=? AND plane=? AND next_x IS NOT NULL AND next_y IS NOT NULL",
            (x, y, plane),
        )
        seen = set()
        for r in cur.fetchall():
            nx, ny = int(r[0]), int(r[1])
            if (nx, ny) in seen:
                continue
            if is_walkable is not None:
                if not is_walkable(nx, ny, plane):
                    continue
            else:
                if not _is_walkable(conn, nx, ny, plane):
                    continue
            seen.add((nx, ny))
            results.append((nx, ny, plane))
        if results:
            return results

    # Fallback: standard neighbor expansion (cost per step handled by caller)
    nbrs = policy.neighbors(x, y, plane)
    return nbrs


def extract_waypoints(path: Sequence[Tuple[int, int, int]]) -> List[Tuple[int, int, int]]:
    """
    Compress a tile path into waypoints by removing collinear interior points.
    Keeps the first and last points and any change in movement direction (turns).
    """
    n = len(path)
    if n <= 2:
        return list(path)

    def dir_of(a: Tuple[int, int, int], b: Tuple[int, int, int]) -> Tuple[int, int, int]:
        ax, ay, ap = a
        bx, by, bp = b
        dx = 0 if bx == ax else (1 if bx > ax else -1)
        dy = 0 if by == ay else (1 if by > ay else -1)
        dp = 0 if bp == ap else (1 if bp > ap else -1)
        return (dx, dy, dp)

    out: List[Tuple[int, int, int]] = [path[0]]
    prev_dir = dir_of(path[0], path[1])
    for i in range(1, n - 1):
        d = dir_of(path[i], path[i + 1])
        if d != prev_dir:
            out.append(path[i])
            prev_dir = d
    out.append(path[-1])
    return out


__all__ = ["expand_with_jps", "extract_waypoints"]
