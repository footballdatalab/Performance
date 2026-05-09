"""
Structured logging setup for the performance data ingestion pipeline.

Uses Python's built-in ``logging`` module.  The log level is driven by the
``LOG_LEVEL`` environment variable (default ``INFO``).
"""

from __future__ import annotations

import logging
import sys

from ingestion.common.config import get_env

_LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

_CONFIGURED = False


def _configure_root() -> None:
    """Apply the shared format/level to the root logger exactly once."""
    global _CONFIGURED
    if _CONFIGURED:
        return

    level_name = get_env("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt=_DATE_FORMAT))

    root = logging.getLogger()
    root.setLevel(level)
    # Avoid adding duplicate handlers if get_logger is called many times.
    if not root.handlers:
        root.addHandler(handler)

    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    """Return a named logger pre-configured with the project format.

    Args:
        name: Dot-separated logger name (e.g. ``"ingestion.catapult.activities"``).

    Returns:
        A :class:`logging.Logger` instance.
    """
    _configure_root()
    return logging.getLogger(name)
