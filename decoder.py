#!/usr/bin/env python3 
# python decoder.py input.txt -o output.csv
import argparse
import base64
import binascii
import json
import re
import sys
from typing import List, Tuple

def _maybe_hex_text_to_bytes(b: bytes) -> bytes:
    """
    Accepts:
      - raw binary (returns as-is)
      - SQLite X'..' literal
      - \\x.. escaped hex
      - bare hex string
      - base64 (fallback)
    """
    # If already binary-like (non-printable bytes), return as-is
    if any(ch > 0x7E or ch < 0x20 for ch in b if ch not in (9, 10, 13)):
        return b

    s = b.decode("utf-8", errors="replace").strip()

    # SQLite X'...'
    m = re.fullmatch(r"[xX]'([0-9A-Fa-f\s]+)'", s)
    if m:
        hexpart = re.sub(r"\s+", "", m.group(1))
        try:
            return binascii.unhexlify(hexpart)
        except binascii.Error:
            pass

    # \x.. escaped hex
    if "\\x" in s:
        hexpart = re.sub(r"\\x", "", s)
        hexpart = re.sub(r"\s+", "", hexpart)
        if re.fullmatch(r"[0-9A-Fa-f]+", hexpart or "0"):
            try:
                return binascii.unhexlify(hexpart)
            except binascii.Error:
                pass

    # Bare hex
    bare = re.sub(r"\s+", "", s)
    if len(bare) >= 2 and re.fullmatch(r"[0-9A-Fa-f]+", bare) and len(bare) % 2 == 0:
        try:
            return binascii.unhexlify(bare)
        except binascii.Error:
            pass

    # Base64 fallback
    try:
        return base64.b64decode(s, validate=True)
    except Exception:
        pass

    # Default: return raw bytes
    return b


def decode_triplets_le_i32(buf: bytes) -> List[Tuple[int, int, int]]:
    if len(buf) % 12 != 0:
        raise ValueError(f"Blob length {len(buf)} is not a multiple of 12 bytes.")
    pts = []
    for i in range(0, len(buf), 12):
        x = int.from_bytes(buf[i:i+4], "little", signed=True)
        y = int.from_bytes(buf[i+4:i+8], "little", signed=True)
        plane = int.from_bytes(buf[i+8:i+12], "little", signed=True)
        pts.append((x, y, plane))
    return pts


def main():
    ap = argparse.ArgumentParser(
        description="Decode intra path blob (little-endian i32 triplets x,y,plane) from SQL dump."
    )
    ap.add_argument("input", help="Path to blob file (raw or hex).")
    ap.add_argument("-o", "--output", help="Output file path (default: stdout).")
    ap.add_argument("-f", "--format", choices=["csv", "json"], default="csv",
                    help="Output format (default: csv).")
    args = ap.parse_args()

    with open(args.input, "rb") as f:
        raw = f.read()

    blob = _maybe_hex_text_to_bytes(raw)
    points = decode_triplets_le_i32(blob)

    if args.format == "json":
        out_text = json.dumps([{"x": x, "y": y, "plane": p} for x, y, p in points], indent=2)
    else:
        lines = ["x,y,plane", *[f"{x},{y},{p}" for x, y, p in points]]
        out_text = "\n".join(lines)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(out_text)
    else:
        print(out_text)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
