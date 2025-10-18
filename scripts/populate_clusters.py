#!/usr/bin/env python3
import argparse
import importlib
import logging
import os
import sqlite3
import sys
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, List

# Ensure package imports work when executed as a script via 'python3 scripts/populate_clusters.py'
if __package__ is None or __package__ == "":
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))



@dataclass
class ChunkRange:
    x_min: Optional[int] = None
    x_max: Optional[int] = None
    z_min: Optional[int] = None
    z_max: Optional[int] = None

    @classmethod
    def parse(cls, s: Optional[str]) -> "ChunkRange":
        if not s:
            return cls()
        # Format: x_min:x_max,z_min:z_max (any part can be omitted but keep colons/commas)
        try:
            xpart, zpart = s.split(",")
            x_min, x_max = (part.strip() or None for part in xpart.split(":"))
            z_min, z_max = (part.strip() or None for part in zpart.split(":"))
            def to_int(v: Optional[str]) -> Optional[int]:
                return int(v) if v not in (None, "") else None
            return cls(to_int(x_min), to_int(x_max), to_int(z_min), to_int(z_max))
        except Exception as e:
            raise argparse.ArgumentTypeError(
                f"Invalid --chunk-range '{s}'. Expected 'x_min:x_max,z_min:z_max'"
            ) from e

    def to_tuple(self) -> Tuple[Optional[int], Optional[int], Optional[int], Optional[int]]:
        return (self.x_min, self.x_max, self.z_min, self.z_max)


def setup_logging(level: str) -> logging.Logger:
    lvl = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=lvl,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        datefmt="%H:%M:%S",
    )
    logger = logging.getLogger("populate_clusters")
    return logger


def connect_sqlite(db_path: str, dry_run: bool, logger: logging.Logger) -> sqlite3.Connection:
    if dry_run:
        # Open read-only to ensure no mutation
        uri = f"file:{os.path.abspath(db_path)}?mode=ro"
        logger.debug("Opening SQLite in read-only mode (dry-run): %s", uri)
        conn = sqlite3.connect(uri, uri=True, isolation_level=None)
    else:
        logger.debug("Opening SQLite read-write: %s", db_path)
        conn = sqlite3.connect(db_path)
        conn.isolation_level = None  # manage transactions explicitly if phases need it
    # Pragmas: we do not force foreign_keys ON here because schema doc says disabled; phases may manage.
    conn.row_factory = sqlite3.Row
    return conn


def import_phase(module_name: str, logger: logging.Logger):
    try:
        return importlib.import_module(module_name)
    except ModuleNotFoundError:
        logger.warning("Phase module '%s' not found - skipping.", module_name)
        return None
    except Exception as e:
        logger.error("Failed importing %s: %s", module_name, e)
        return None


def call_phase(mod: Any, func_name: str, logger: logging.Logger, **kwargs) -> Dict[str, Any]:
    if mod is None:
        return {"skipped": True}
    fn = getattr(mod, func_name, None)
    if fn is None:
        logger.warning("Function %s not found in %s - skipping.", func_name, mod.__name__)
        return {"skipped": True}
    try:
        params = dict(kwargs)
        params["logger"] = logger
        result = fn(**params)
        if isinstance(result, dict):
            return result
        return {"result": result}
    except sqlite3.OperationalError as e:
        logger.error("SQLite operational error in %s.%s: %s", getattr(mod, "__name__", mod), func_name, e)
        raise
    except Exception as e:
        logger.exception("Error in %s.%s", getattr(mod, "__name__", mod), func_name)
        raise


def parse_planes(arg: Optional[str]) -> Optional[List[int]]:
    if arg is None or arg.strip() == "":
        return None
    try:
        return [int(p.strip()) for p in arg.split(",") if p.strip() != ""]
    except Exception as e:
        raise argparse.ArgumentTypeError("--planes must be a comma-separated list of ints") from e


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Populate cluster entrances and connections (HPA*)")
    p.add_argument("--db-path", required=True, help="Path to local SQLite DB (worldReachableTiles.db)")
    p.add_argument("--planes", type=parse_planes, default=None, help="Comma-separated plane list (default: all)")
    p.add_argument(
        "--chunk-range",
        type=ChunkRange.parse,
        default=None,
        help="Chunk range filter 'x_min:x_max,z_min:z_max' (inclusive). Omit bounds to leave open.",
    )
    p.add_argument(
        "--recompute",
        action="store_true",
        help="Recompute and overwrite existing rows where applicable",
    )
    p.add_argument(
        "--store-paths",
        action="store_true",
        help="Store intra-connection path blobs when available",
    )
    p.add_argument("--dry-run", action="store_true", help="Do not mutate DB; open in read-only and print summaries")
    p.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level",
    )
    return p


def main(argv: Optional[List[str]] = None) -> int:
    args = build_arg_parser().parse_args(argv)
    logger = setup_logging(args.log_level)

    planes = args.planes  # None means all
    chunk_range = args.chunk_range or ChunkRange()

    logger.info("Starting populate-clusters pipeline")
    logger.info("DB: %s | dry_run=%s | planes=%s | chunk_range=%s | recompute=%s | store_paths=%s",
                args.db_path, args.dry_run, planes, chunk_range.to_tuple(), args.recompute, args.store_paths)

    # Connect
    conn = connect_sqlite(args.db_path, args.dry_run, logger)

    # Phase modules
    entrances_mod = import_phase("scripts.cluster.entrance_discovery", logger)
    inter_mod = import_phase("scripts.cluster.inter_connector", logger)
    intra_mod = import_phase("scripts.cluster.intra_connector", logger)

    summary: Dict[str, Any] = {"entrances": {}, "inter": {}, "intra": {}}

    # Single-process path only
    common_kwargs = dict(
        conn=conn,
        planes=planes,
        chunk_range=chunk_range.to_tuple(),
        recompute=args.recompute,
        dry_run=args.dry_run,
        workers=1,
        store_paths=args.store_paths,
    )

    logger.info("Phase: entrances")
    summary["entrances"] = call_phase(entrances_mod, "run", logger, **common_kwargs)

    logger.info("Phase: interconnections")
    summary["inter"] = call_phase(inter_mod, "run", logger, **common_kwargs)

    logger.info("Phase: intraconnections")
    summary["intra"] = call_phase(intra_mod, "run", logger, **common_kwargs)

    # Print summaries
    def safe_get(d: Dict[str, Any], key: str, default: Any = 0) -> Any:
        if not isinstance(d, dict):
            return default
        return d.get(key, default)

    entrances_created = safe_get(summary["entrances"], "created")
    inter_created = safe_get(summary["inter"], "created")
    intra_created = safe_get(summary["intra"], "created")

    logger.info("Summary: entrances created=%s, interconnections created=%s, intraconnections created=%s",
                entrances_created, inter_created, intra_created)

    if args.dry_run:
        logger.info("Dry-run mode: no database changes were made.")

    logger.info("Pipeline completed successfully")
    return 0


if __name__ == "__main__":
    try:
        code = main()
    except KeyboardInterrupt:
        logging.getLogger("populate_clusters").error("Interrupted by user")
        code = 130
    except Exception as e:
        logging.getLogger("populate_clusters").exception("Fatal error: %s", e)
        code = 1
    sys.exit(code)
