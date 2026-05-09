"""
Platform-wide warehouse bootstrap.
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import Any

from ingestion.catapult.bootstrap import bootstrap_catapult_foundation
from ingestion.common.config import get_db_config, get_env
from ingestion.common.db import DatabaseManager
from ingestion.common.logging import get_logger
from ingestion.common.sql_runner import discover_sql_files, execute_sql_files

logger = get_logger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_SQL_DDL_ROOT = _PROJECT_ROOT / "sql" / "ddl"
_LOCK_NOT_AVAILABLE_SQLSTATE = "55P03"
_DEFAULT_BOOTSTRAP_LOCK_RETRY_ATTEMPTS = 6
_DEFAULT_BOOTSTRAP_LOCK_RETRY_SLEEP_SECONDS = 10
_DEFAULT_BOOTSTRAP_LOCK_TIMEOUT_MS = 600_000
_DEFAULT_BOOTSTRAP_STATEMENT_TIMEOUT_MS = 3_600_000


def bootstrap_database() -> dict[str, Any]:
    """Create or reconcile the warehouse schema from the repository DDL."""
    attempts = _resolve_positive_int_env(
        "POSTGRES_BOOTSTRAP_LOCK_RETRY_ATTEMPTS",
        _DEFAULT_BOOTSTRAP_LOCK_RETRY_ATTEMPTS,
    )
    sleep_seconds = _resolve_positive_int_env(
        "POSTGRES_BOOTSTRAP_LOCK_RETRY_SLEEP_SECONDS",
        _DEFAULT_BOOTSTRAP_LOCK_RETRY_SLEEP_SECONDS,
    )
    ddl_files = discover_sql_files(_SQL_DDL_ROOT)

    for attempt in range(1, attempts + 1):
        db = DatabaseManager(_get_bootstrap_db_config())
        try:
            logger.info("Starting warehouse bootstrap attempt %d/%d.", attempt, attempts)
            executed = execute_sql_files(db, ddl_files)
            catapult_summary = bootstrap_catapult_foundation(db)
            summary = {
                "executed_files": executed,
                "ddl_file_count": len(executed),
                "catapult": catapult_summary,
            }
            logger.info("Platform bootstrap complete: %s", summary)
            return summary
        except Exception as exc:
            if not _is_retryable_lock_timeout(exc) or attempt >= attempts:
                raise
            logger.warning(
                "Bootstrap hit a PostgreSQL lock timeout on attempt %d/%d. Retrying in %d seconds.",
                attempt,
                attempts,
                sleep_seconds,
            )
            time.sleep(sleep_seconds)
        finally:
            db.close()

    raise RuntimeError("Bootstrap retry loop exhausted without returning or raising.")


def _is_retryable_lock_timeout(exc: Exception) -> bool:
    """Return True when *exc* represents a PostgreSQL lock-timeout style failure."""
    if getattr(exc, "pgcode", None) == _LOCK_NOT_AVAILABLE_SQLSTATE:
        return True
    if exc.__class__.__name__ == "LockNotAvailable":
        return True
    message = str(exc).lower()
    return "lock timeout" in message or "locknotavailable" in message


def _get_bootstrap_db_config() -> dict[str, Any]:
    """Return DB config tuned for schema bootstrap DDL."""
    config = get_db_config()
    config["lock_timeout_ms"] = _resolve_non_negative_int_env(
        "POSTGRES_BOOTSTRAP_LOCK_TIMEOUT_MS",
        _DEFAULT_BOOTSTRAP_LOCK_TIMEOUT_MS,
    )
    config["statement_timeout_ms"] = _resolve_non_negative_int_env(
        "POSTGRES_BOOTSTRAP_STATEMENT_TIMEOUT_MS",
        _DEFAULT_BOOTSTRAP_STATEMENT_TIMEOUT_MS,
    )
    return config


def _resolve_positive_int_env(env_var: str, default: int) -> int:
    raw_value = get_env(env_var, str(default))
    try:
        parsed = int(raw_value or default)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _resolve_non_negative_int_env(env_var: str, default: int) -> int:
    raw_value = get_env(env_var, str(default))
    try:
        parsed = int(raw_value or default)
    except (TypeError, ValueError):
        return default
    return parsed if parsed >= 0 else default


def main_bootstrap_database(argv: list[str] | None = None) -> int:
    """CLI entrypoint for the canonical platform bootstrap command."""
    parser = argparse.ArgumentParser(description="Bootstrap the performance warehouse schema.")
    parser.parse_args(argv)
    bootstrap_database()
    return 0
