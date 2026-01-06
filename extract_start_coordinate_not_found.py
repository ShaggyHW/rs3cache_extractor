#!/usr/bin/env python3

import argparse
import csv
import json
import re
import sys
from dataclasses import dataclass
from typing import IO, Any, Dict, Iterable, List, Optional, Tuple


_OPTIONS_START_RE = re.compile(
    r"start=\[\s*X:\s*(?P<x>-?\d+),\s*Y:\s*(?P<y>-?\d+),\s*Z:\s*(?P<z>-?\d+)\s*\]"
)
_OPTIONS_GOAL_RE = re.compile(
    r"goal=\[\s*X:\s*(?P<x>-?\d+),\s*Y:\s*(?P<y>-?\d+),\s*Z:\s*(?P<z>-?\d+)\s*\]"
)
_TIMESTAMP_RE = re.compile(r"^\[(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})\]\s*")


@dataclass
class RequestContext:
    ts: str
    start: Tuple[int, int, int]
    goal: Optional[Tuple[int, int, int]]
    line_no: int


@dataclass
class ExtractedFailure:
    response_ts: Optional[str]
    start: Tuple[int, int, int]
    request_ts: Optional[str]
    goal: Optional[Tuple[int, int, int]]
    request_line_no: Optional[int]
    response_line_no: int


def _parse_timestamp(line: str) -> Optional[str]:
    m = _TIMESTAMP_RE.match(line)
    if not m:
        return None
    return m.group("ts")


def _parse_options_line(line: str) -> Optional[Tuple[Tuple[int, int, int], Optional[Tuple[int, int, int]]]]:
    start_m = _OPTIONS_START_RE.search(line)
    if not start_m:
        return None

    start = (int(start_m.group("x")), int(start_m.group("y")), int(start_m.group("z")))

    goal_m = _OPTIONS_GOAL_RE.search(line)
    goal: Optional[Tuple[int, int, int]]
    if goal_m:
        goal = (int(goal_m.group("x")), int(goal_m.group("y")), int(goal_m.group("z")))
    else:
        goal = None

    return start, goal


def _extract_json_payload(line: str) -> Optional[str]:
    idx = line.find("{")
    if idx == -1:
        return None
    return line[idx:].strip()


def _iter_actions_with_reason(obj: Any, reason: str) -> Iterable[Dict[str, Any]]:
    if not isinstance(obj, dict):
        return

    actions = obj.get("actions")
    if not isinstance(actions, list):
        return

    for action in actions:
        if not isinstance(action, dict):
            continue
        metadata = action.get("metadata")
        if isinstance(metadata, dict) and metadata.get("reason") == reason:
            yield action


def _parse_start_from_action(action: Dict[str, Any]) -> Optional[Tuple[int, int, int]]:
    from_obj = action.get("from")
    if not isinstance(from_obj, dict):
        return None

    max_v = from_obj.get("max")
    if isinstance(max_v, list) and len(max_v) == 3:
        try:
            return int(max_v[0]), int(max_v[1]), int(max_v[2])
        except Exception:
            return None

    min_v = from_obj.get("min")
    if isinstance(min_v, list) and len(min_v) == 3:
        try:
            return int(min_v[0]), int(min_v[1]), int(min_v[2])
        except Exception:
            return None

    return None


def extract_failures(log_file: IO[str]) -> List[ExtractedFailure]:
    last_request: Optional[RequestContext] = None
    out: List[ExtractedFailure] = []

    for idx, raw_line in enumerate(log_file, start=1):
        line = raw_line.rstrip("\n")

        if "Options(command=python3" in line:
            parsed = _parse_options_line(line)
            if parsed is not None:
                ts = _parse_timestamp(line) or ""
                start, goal = parsed
                last_request = RequestContext(ts=ts, start=start, goal=goal, line_no=idx)

        if "start_coordinate_not_found" not in line:
            continue

        payload = _extract_json_payload(line)
        if not payload:
            continue

        parsed_json: Optional[Dict[str, Any]] = None
        try:
            candidate = json.loads(payload)
            if isinstance(candidate, dict):
                parsed_json = candidate
        except Exception:
            parsed_json = None

        if not parsed_json:
            continue

        response_ts = _parse_timestamp(line)

        for action in _iter_actions_with_reason(parsed_json, "start_coordinate_not_found"):
            start = _parse_start_from_action(action)
            if start is None:
                continue

            out.append(
                ExtractedFailure(
                    response_ts=response_ts,
                    start=start,
                    request_ts=last_request.ts if last_request else None,
                    goal=last_request.goal if last_request else None,
                    request_line_no=last_request.line_no if last_request else None,
                    response_line_no=idx,
                )
            )

    return out


def _write_csv(rows: List[ExtractedFailure], fp: IO[str]) -> None:
    writer = csv.DictWriter(
        fp,
        fieldnames=[
            "response_ts",
            "start_x",
            "start_y",
            "start_z",
            "request_ts",
            "goal_x",
            "goal_y",
            "goal_z",
            "request_line_no",
            "response_line_no",
        ],
    )
    writer.writeheader()
    for r in rows:
        goal_x = goal_y = goal_z = None
        if r.goal is not None:
            goal_x, goal_y, goal_z = r.goal

        writer.writerow(
            {
                "response_ts": r.response_ts,
                "start_x": r.start[0],
                "start_y": r.start[1],
                "start_z": r.start[2],
                "request_ts": r.request_ts,
                "goal_x": goal_x,
                "goal_y": goal_y,
                "goal_z": goal_z,
                "request_line_no": r.request_line_no,
                "response_line_no": r.response_line_no,
            }
        )


def _write_json(rows: List[ExtractedFailure], fp: IO[str]) -> None:
    payload: List[Dict[str, Any]] = []
    for r in rows:
        payload.append(
            {
                "response_ts": r.response_ts,
                "start": list(r.start),
                "request_ts": r.request_ts,
                "goal": list(r.goal) if r.goal is not None else None,
                "request_line_no": r.request_line_no,
                "response_line_no": r.response_line_no,
            }
        )
    json.dump(payload, fp, indent=2)
    fp.write("\n")


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Extract start coordinates for requests that resulted in reason=start_coordinate_not_found"
    )
    ap.add_argument("log", help="Path to the undercut log file")
    ap.add_argument("-o", "--output", help="Write output to this file (default: stdout)")
    ap.add_argument("--format", choices=["csv", "json"], default="csv")
    args = ap.parse_args()

    with open(args.log, "r", encoding="utf-8", errors="replace") as f:
        rows = extract_failures(f)

    if args.output:
        with open(args.output, "w", encoding="utf-8", newline="") as out_fp:
            if args.format == "csv":
                _write_csv(rows, out_fp)
            else:
                _write_json(rows, out_fp)
    else:
        if args.format == "csv":
            _write_csv(rows, sys.stdout)
        else:
            _write_json(rows, sys.stdout)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
