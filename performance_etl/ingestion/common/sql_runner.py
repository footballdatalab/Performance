"""
SQL script execution helpers.

Provides utilities to discover and execute the repository DDL files in a
stable order for schema bootstrap.
"""

from __future__ import annotations

from pathlib import Path
import re
from typing import Iterable

from ingestion.common.db import DatabaseManager
from ingestion.common.logging import get_logger

logger = get_logger(__name__)
_BLOCK_COMMENT_RE = re.compile(r"/\*.*?\*/", re.DOTALL)
_LINE_COMMENT_RE = re.compile(r"--.*$", re.MULTILINE)


def discover_sql_files(root: Path) -> list[Path]:
    """Return DDL SQL files in bootstrap order."""
    ordered_dirs = [
        root,
        root / "raw",
        root / "bronze",
        root / "silver",
        root / "gold",
    ]
    sql_files: list[Path] = []
    for directory in ordered_dirs:
        if not directory.exists():
            continue
        sql_files.extend(sorted(directory.glob("*.sql")))
    return sql_files


def execute_sql_files(
    db: DatabaseManager,
    files: Iterable[Path],
) -> list[str]:
    """Execute each SQL file in order and return the executed file names."""
    executed: list[str] = []
    for path in files:
        sql = path.read_text(encoding="utf-8")
        if _is_effectively_empty_sql(sql):
            logger.info("Skipping empty SQL bootstrap file: %s", path)
            continue
        logger.info("Executing SQL bootstrap file: %s", path)
        db.execute(sql)
        executed.append(str(path))
    return executed


def _is_effectively_empty_sql(sql: str) -> bool:
    """Return True when *sql* contains only comments and whitespace."""
    without_block_comments = _BLOCK_COMMENT_RE.sub("", sql)
    without_comments = _LINE_COMMENT_RE.sub("", without_block_comments)
    return not without_comments.strip()
