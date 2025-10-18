import sqlite3
from dataclasses import dataclass
from typing import Callable, Iterable, List, Optional, Sequence, Tuple


@dataclass(frozen=True)
class NeighborPolicy:
    allow_diagonals: int
    allow_corner_cut: int
    unit_radius_tiles: int
    _conn: sqlite3.Connection
    _walkable_cb: Optional[Callable[[int, int, int], bool]] = None

    @classmethod
    def from_db(cls, conn: sqlite3.Connection) -> "NeighborPolicy":
        row = conn.execute(
            "SELECT allow_diagonals, allow_corner_cut, unit_radius_tiles FROM movement_policy WHERE policy_id = 1"
        ).fetchone()
        if row is None:
            raise RuntimeError("movement_policy row with policy_id=1 not found")
        return cls(
            int(row[0]),
            int(row[1]),
            int(row[2]),
            conn,
            None,
        )

    def with_walkable(self, cb: Optional[Callable[[int, int, int], bool]]) -> "NeighborPolicy":
        return NeighborPolicy(
            self.allow_diagonals,
            self.allow_corner_cut,
            self.unit_radius_tiles,
            self._conn,
            cb,
        )

    def neighbors(self, x: int, y: int, plane: int) -> List[Tuple[int, int, int]]:
        res: List[Tuple[int, int, int]] = []
        # Deterministic order: N, E, S, W, then diagonals NE, SE, SW, NW
        cardinals: Sequence[Tuple[int, int]] = ((0, -1), (1, 0), (0, 1), (-1, 0))
        diagonals: Sequence[Tuple[int, int]] = ((1, -1), (1, 1), (-1, 1), (-1, -1))

        for dx, dy in cardinals:
            nx, ny = x + dx, y + dy
            if self._can_step(x, y, nx, ny, plane, corner=False):
                res.append((nx, ny, plane))

        if self.allow_diagonals:
            for dx, dy in diagonals:
                nx, ny = x + dx, y + dy
                if not self.allow_corner_cut:
                    # Require both adjacent cardinals to be walkable to avoid corner cutting
                    if not (self._is_walkable(x + dx, y, plane) and self._is_walkable(x, y + dy, plane)):
                        continue
                if self._can_step(x, y, nx, ny, plane, corner=True):
                    res.append((nx, ny, plane))

        return res

    # ---- Helpers ----
    def _can_step(self, x: int, y: int, nx: int, ny: int, plane: int, corner: bool) -> bool:
        if not self._is_walkable(nx, ny, plane):
            return False
        r = self.unit_radius_tiles
        if r <= 0:
            return True
        # Ensure a square of radius r centered at target is walkable (Chebyshev radius)
        for ox in range(-r, r + 1):
            for oy in range(-r, r + 1):
                tx, ty = nx + ox, ny + oy
                if not self._is_walkable(tx, ty, plane):
                    return False
        return True

    def _is_walkable(self, x: int, y: int, plane: int) -> bool:
        if self._walkable_cb is not None:
            return self._walkable_cb(x, y, plane)
        row = self._conn.execute(
            "SELECT blocked, walk_mask FROM tiles WHERE x=? AND y=? AND plane=?",
            (x, y, plane),
        ).fetchone()
        if row is None:
            return False
        blocked = int(row[0]) if row[0] is not None else 1
        walk_mask = int(row[1]) if row[1] is not None else 0
        if blocked:
            return False
        # Consider walkable if any walkable capability present
        return walk_mask != 0
