import importlib
import logging
import math
import sqlite3
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from time import sleep
from typing import Dict, List, Optional, Sequence, Tuple


@dataclass
class Chunk:
    x: int
    z: int
    size: int


def _list_chunks(db_path: str, chunk_range: Tuple[Optional[int], Optional[int], Optional[int], Optional[int]]) -> List[Chunk]:
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
    sql = f"SELECT chunk_x, chunk_z, chunk_size FROM chunks WHERE {' AND '.join(conds)} ORDER BY chunk_x, chunk_z"
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(sql, params).fetchall()
        return [Chunk(int(r[0]), int(r[1]), int(r[2])) for r in rows]
    finally:
        conn.close()


def _partition(items: List[Chunk], workers: int) -> List[List[Chunk]]:
    if workers <= 1 or len(items) == 0:
        return [items]
    n = len(items)
    base = n // workers
    rem = n % workers
    parts: List[List[Chunk]] = []
    start = 0
    for i in range(workers):
        size = base + (1 if i < rem else 0)
        parts.append(items[start:start+size])
        start += size
    return parts


def _call_phase_for_chunk(db_path: str,
                          module_name: str,
                          planes: Optional[Sequence[int]],
                          recompute: bool,
                          dry_run: bool,
                          workers: int,
                          store_paths: bool,
                          chunk: Chunk,
                          log_level: str,
                          max_retries: int = 5) -> Dict[str, int]:
    logger = logging.getLogger(f"executor.worker.{module_name}")
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    backoff = 0.1
    for attempt in range(max_retries + 1):
        try:
            # Each worker opens its own connection (no shared connections)
            uri = f"file:{db_path}?mode=ro" if dry_run else db_path
            conn = sqlite3.connect(uri, uri=dry_run, timeout=5.0)
            conn.row_factory = sqlite3.Row
            # Use autocommit to mirror CLI single-process behavior; phases may still manage their own transactions
            conn.isolation_level = None
            try:
                mod = importlib.import_module(module_name)
                fn = getattr(mod, "run")
                # Narrow scope to exactly one chunk by passing a tight chunk_range
                cr = (chunk.x, chunk.x, chunk.z, chunk.z)
                res = fn(
                    conn=conn,
                    planes=planes,
                    chunk_range=cr,
                    recompute=recompute,
                    dry_run=dry_run,
                    workers=1,  # within a worker, we run single-threaded
                    store_paths=store_paths,
                    logger=logger,
                )
                # Ensure changes are flushed before closing in worker path
                if not dry_run:
                    try:
                        conn.commit()
                    except Exception:
                        pass
                if isinstance(res, dict):
                    return res
                return {"result": 1}
            finally:
                conn.close()
        except sqlite3.OperationalError as e:
            msg = str(e)
            if "database is locked" in msg or "database schema is locked" in msg:
                if attempt < max_retries:
                    sleep(backoff)
                    backoff *= 2
                    continue
            raise


def execute_phase(
    *,
    db_path: str,
    module_name: str,
    planes: Optional[Sequence[int]],
    chunk_range: Tuple[Optional[int], Optional[int], Optional[int], Optional[int]],
    recompute: bool,
    dry_run: bool,
    workers: int,
    store_paths: bool,
    logger: logging.Logger,
) -> Dict[str, int]:
    # List and partition chunks
    chunks = _list_chunks(db_path, chunk_range)
    parts = _partition(chunks, workers)
    if workers <= 1 or len(parts) == 1:
        # Run sequentially in current process for simplicity
        total: Dict[str, int] = {}
        for ch in chunks:
            res = _call_phase_for_chunk(db_path, module_name, planes, recompute, dry_run, workers, store_paths, ch, logger.level_name if hasattr(logger, 'level_name') else 'INFO')
            for k, v in (res or {}).items():
                total[k] = total.get(k, 0) + (v if isinstance(v, int) else 0)
        return total

    total: Dict[str, int] = {}
    # Process pool: one task per chunk (fine grained for load balance)
    with ProcessPoolExecutor(max_workers=workers) as pool:
        futs = [
            pool.submit(
                _call_phase_for_chunk,
                db_path,
                module_name,
                planes,
                recompute,
                dry_run,
                workers,
                store_paths,
                ch,
                logging.getLevelName(logger.level),
            )
            for ch in chunks
        ]
        for fut in as_completed(futs):
            res = fut.result()
            for k, v in (res or {}).items():
                total[k] = total.get(k, 0) + (v if isinstance(v, int) else 0)
    return total
