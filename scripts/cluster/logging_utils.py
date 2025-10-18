import logging
import time
from contextlib import contextmanager
from typing import Any, Dict


@contextmanager
def timer(logger: logging.Logger, label: str):
    start = time.perf_counter()
    try:
        yield
    finally:
        dur = time.perf_counter() - start
        logger.info("timer: %s duration_sec=%.3f", label, dur)


def emit_summary(logger: logging.Logger, phase: str, **metrics: Any) -> None:
    parts = [f"{k}={v}" for k, v in metrics.items()]
    logger.info("summary: phase=%s %s", phase, " ".join(parts))
