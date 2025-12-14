#!/usr/bin/env python3

import json
import sys
from typing import Dict, List, Tuple


DIRECTION_BITS: List[Tuple[int, str]] = [
    (0, "left"),
    (1, "bottom"),
    (2, "right"),
    (3, "top"),
    (4, "topleft"),
    (5, "bottomleft"),
    (6, "bottomright"),
    (7, "topright"),
]


def parse_int(value: str) -> int:
    value = value.strip()
    if not value:
        raise ValueError("empty value")
    return int(value, 0)


def decode_walk_mask(walk_mask: int) -> Dict[str, bool]:
    return {name: bool(walk_mask & (1 << bit)) for bit, name in DIRECTION_BITS}


def walkable_directions(walk_mask: int) -> List[str]:
    return [name for bit, name in DIRECTION_BITS if (walk_mask & (1 << bit))]


def main(argv: List[str]) -> int:
    if len(argv) >= 2 and argv[1] in {"-h", "--help"}:
        print("Usage: walk_mask_decode.py <walk_mask> [--json]")
        print("  <walk_mask> can be decimal (e.g. 13) or hex (e.g. 0x0d)")
        return 0

    emit_json = "--json" in argv[2:]

    if len(argv) >= 2:
        raw = argv[1]
    else:
        raw = sys.stdin.readline().strip() or input("walk_mask: ").strip()

    try:
        mask = parse_int(raw)
    except ValueError as e:
        print(f"Invalid walk_mask: {raw!r} ({e})", file=sys.stderr)
        return 2

    dirs = walkable_directions(mask)

    if emit_json:
        print(json.dumps({"walk_mask": mask, "walkable": decode_walk_mask(mask), "directions": dirs}))
        return 0

    if dirs:
        print("\n".join(dirs))
    else:
        print("none")

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
