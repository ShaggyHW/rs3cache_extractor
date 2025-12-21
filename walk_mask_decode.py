#!/usr/bin/env python3

import json
import sys
from typing import Any, Dict, List, Tuple


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


def encode_walk_mask(walkable: Dict[str, bool]) -> int:
    mask = 0
    for bit, name in DIRECTION_BITS:
        if walkable.get(name, False):
            mask |= 1 << bit
    return mask


def encode_walk_mask_from_directions(directions: List[str]) -> int:
    valid = {name for _, name in DIRECTION_BITS}
    unknown = sorted({d for d in directions if d not in valid})
    if unknown:
        raise ValueError(f"unknown direction(s): {', '.join(unknown)}")
    return encode_walk_mask({d: True for d in directions})


def _parse_positions_as_directions(value: Any) -> List[str]:
    offset_to_dir = {
        (-1, 0): "left",
        (0, 1): "bottom",
        (1, 0): "right",
        (0, -1): "top",
        (-1, -1): "topleft",
        (-1, 1): "bottomleft",
        (1, 1): "bottomright",
        (1, -1): "topright",
    }

    if not isinstance(value, list):
        raise ValueError("positions must be a list")

    directions: List[str] = []
    for item in value:
        dx: Any
        dy: Any
        if isinstance(item, (list, tuple)) and len(item) == 2:
            dx, dy = item
        elif isinstance(item, dict) and "dx" in item and "dy" in item:
            dx, dy = item["dx"], item["dy"]
        else:
            raise ValueError("positions items must be [dx, dy] or {dx, dy}")

        try:
            dx_i = int(dx)
            dy_i = int(dy)
        except (TypeError, ValueError) as e:
            raise ValueError(f"invalid position offset: {item!r}") from e

        name = offset_to_dir.get((dx_i, dy_i))
        if name is None:
            raise ValueError(f"unsupported offset: {(dx_i, dy_i)!r}")
        directions.append(name)

    return directions


def parse_walkable_json(raw: str) -> List[str]:
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"invalid json: {e}") from e

    if isinstance(data, list):
        if all(isinstance(x, str) for x in data):
            return list(data)
        return _parse_positions_as_directions(data)

    if not isinstance(data, dict):
        raise ValueError("json must be an object or array")

    if "directions" in data:
        directions = data["directions"]
        if not isinstance(directions, list) or not all(isinstance(x, str) for x in directions):
            raise ValueError("directions must be a list of strings")
        return list(directions)

    if "walkable" in data:
        walkable = data["walkable"]
        if not isinstance(walkable, dict):
            raise ValueError("walkable must be an object")
        directions = [k for k, v in walkable.items() if bool(v)]
        return directions

    if "positions" in data:
        return _parse_positions_as_directions(data["positions"])

    if all(isinstance(k, str) for k in data.keys()):
        directions = [k for k, v in data.items() if bool(v)]
        return directions

    raise ValueError("unrecognized json shape")


def walkable_directions(walk_mask: int) -> List[str]:
    return [name for bit, name in DIRECTION_BITS if (walk_mask & (1 << bit))]


def main(argv: List[str]) -> int:
    if len(argv) >= 2 and argv[1] in {"-h", "--help"}:
        print("Usage: walk_mask_decode.py <walk_mask> [--json]")
        print("       walk_mask_decode.py --encode <json|directions> [--json]")
        print("  <walk_mask> can be decimal (e.g. 13) or hex (e.g. 0x0d)")
        print("  --encode input accepts JSON (directions/walkable/positions) or a comma-separated list")
        return 0

    encode_mode = "--encode" in argv[1:]
    emit_json = "--json" in argv[2:]

    positional = [a for a in argv[1:] if not a.startswith("--")]

    if positional:
        raw = positional[0]
    else:
        prompt = "walkable json/directions: " if encode_mode else "walk_mask: "
        raw = sys.stdin.readline().strip() or input(prompt).strip()

    if encode_mode:
        try:
            if raw.lstrip().startswith(("{", "[")):
                directions = parse_walkable_json(raw)
            else:
                directions = [p.strip() for p in raw.split(",") if p.strip()]
            mask = encode_walk_mask_from_directions(directions)
        except ValueError as e:
            print(f"Invalid encode input: {e}", file=sys.stderr)
            return 2

        if emit_json:
            print(json.dumps({"walk_mask": mask, "directions": directions, "walkable": decode_walk_mask(mask)}))
            return 0

        print(mask)
        return 0

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
