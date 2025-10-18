import logging
import os
import sqlite3
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Iterator, Optional, Sequence, Tuple


_SQLITE_BUSY_ERRS = (sqlite3.OperationalError, sqlite3.DatabaseError)


@dataclass
class RetryPolicy:
    max_retries: int = 6
    initial_sleep: float = 0.05
    backoff: float = 2.0


class DbContext:
    """
    SQLite DbContext with explicit transaction control and deterministic retry on SQLITE_BUSY.

    - Uses parameterized queries only (callers pass params as sequences/mappings).
    - Optional read-only mode (opens URI with mode=ro) to enforce --dry-run.
    - Deterministic retry: fixed backoff (no jitter) to keep reproducible behavior.
    """

    def __init__(
        self,
        db_path: str,
        *,
        read_only: bool = False,
        timeout: float = 5.0,
        retry: Optional[RetryPolicy] = None,
        pragmas: Optional[Dict[str, Any]] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self._db_path = db_path
        self._read_only = read_only
        self._timeout = timeout
        self._retry = retry or RetryPolicy()
        self._pragmas = pragmas or {}
        self._logger = logger or logging.getLogger(__name__)
        self._conn: Optional[sqlite3.Connection] = None
        self._in_txn = False
        self._lock = threading.RLock()

    # ---- Connection management ----
    def connect(self) -> None:
        if self._conn is not None:
            return
        if self._read_only:
            uri = f"file:{os.path.abspath(self._db_path)}?mode=ro"
            self._conn = sqlite3.connect(uri, uri=True, timeout=self._timeout)
        else:
            self._conn = sqlite3.connect(self._db_path, timeout=self._timeout)
        self._conn.row_factory = sqlite3.Row
        # We manage transactions explicitly
        self._conn.isolation_level = None
        # Apply pragmas deterministically in a single batch
        if self._pragmas:
            for key, value in self._pragmas.items():
                self._conn.execute(f"PRAGMA {key} = {value}")

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            finally:
                self._conn = None
                self._in_txn = False

    # ---- Context manager ----
    def __enter__(self) -> "DbContext":
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        try:
            if exc_type is not None and self._in_txn:
                self.rollback()
            elif self._in_txn:
                self.commit()
        finally:
            self.close()

    # ---- Transaction control ----
    def begin(self) -> None:
        with self._lock:
            self._ensure_conn()
            if not self._in_txn:
                self._execute_raw("BEGIN IMMEDIATE")
                self._in_txn = True

    def commit(self) -> None:
        with self._lock:
            if self._in_txn:
                self._execute_raw("COMMIT")
                self._in_txn = False

    def rollback(self) -> None:
        with self._lock:
            if self._in_txn:
                self._execute_raw("ROLLBACK")
                self._in_txn = False

    @contextmanager
    def transaction(self) -> Iterator[None]:
        self.begin()
        try:
            yield
        except Exception:
            self.rollback()
            raise
        else:
            self.commit()

    # ---- Execution helpers (prepared statements only) ----
    def execute(self, sql: str, params: Sequence[Any] | Dict[str, Any] = ()) -> sqlite3.Cursor:
        self._ensure_conn()
        self._assert_safe(sql)
        return self._with_retry(lambda: self._conn.execute(sql, params))

    def executemany(self, sql: str, seq_of_params: Iterable[Sequence[Any] | Dict[str, Any]]) -> sqlite3.Cursor:
        self._ensure_conn()
        self._assert_safe(sql)
        return self._with_retry(lambda: self._conn.executemany(sql, list(seq_of_params)))

    def query_all(self, sql: str, params: Sequence[Any] | Dict[str, Any] = ()) -> list[sqlite3.Row]:
        cur = self.execute(sql, params)
        return cur.fetchall()

    def query_one(self, sql: str, params: Sequence[Any] | Dict[str, Any] = ()) -> Optional[sqlite3.Row]:
        cur = self.execute(sql, params)
        return cur.fetchone()

    # ---- Internal utilities ----
    def _ensure_conn(self) -> None:
        if self._conn is None:
            self.connect()

    def _execute_raw(self, sql: str) -> None:
        # For BEGIN/COMMIT/ROLLBACK only
        self._with_retry(lambda: self._conn.execute(sql))

    def _with_retry(self, fn):
        attempts = 0
        delay = self._retry.initial_sleep
        while True:
            try:
                return fn()
            except _SQLITE_BUSY_ERRS as e:
                msg = str(e)
                if "database is locked" not in msg and "database schema is locked" not in msg:
                    raise
                if attempts >= self._retry.max_retries:
                    self._logger.error("SQLITE_BUSY after %d retries", attempts)
                    raise
                self._logger.debug("SQLITE_BUSY, retry %d/%d after %.3fs", attempts + 1, self._retry.max_retries, delay)
                time.sleep(delay)
                attempts += 1
                delay *= self._retry.backoff

    def _assert_safe(self, sql: str) -> None:
        if not self._read_only:
            return
        # naive check to prevent writes in read-only mode; SQLite will also enforce at connection level
        leading = sql.lstrip().upper()
        if leading.startswith(("INSERT", "UPDATE", "DELETE", "REPLACE", "CREATE", "DROP", "ALTER", "VACUUM", "PRAGMA ")):
            raise PermissionError("Write operation attempted in read-only DbContext")


__all__ = ["DbContext", "RetryPolicy"]
