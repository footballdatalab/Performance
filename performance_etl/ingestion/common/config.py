"""
Configuration loader for the performance data ingestion pipeline.

Loads environment variables from .env and provider-specific YAML configs
from config/providers/. Provides helpers to retrieve DB connection settings
and per-provider configuration dictionaries.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_PROJECT_ROOT = Path(__file__).resolve().parents[2]  # performance_hub/
_ENV_PATH = _PROJECT_ROOT / ".env"
_DEFAULT_POSTGRES_APPLICATION_NAME = "performance_etl"
_DEFAULT_POSTGRES_CONNECT_TIMEOUT_SECONDS = 15
_DEFAULT_POSTGRES_LOCK_TIMEOUT_MS = 30_000
_DEFAULT_POSTGRES_STATEMENT_TIMEOUT_MS = 1_800_000
_DEFAULT_POSTGRES_IDLE_TX_TIMEOUT_MS = 300_000
_DEFAULT_POSTGRES_POOL_MIN_CONNECTIONS = 1
_DEFAULT_POSTGRES_POOL_MAX_CONNECTIONS = 32


def _ensure_env_loaded() -> None:
    """Load the .env file once.  Subsequent calls are idempotent."""
    load_dotenv(_ENV_PATH, override=False)


def _get_int_env(key: str, default: int | None = None) -> int | None:
    """Return an integer environment variable or the provided default."""
    raw_value = os.environ.get(key)
    if raw_value in (None, ""):
        return default
    return int(raw_value)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def load_config() -> dict[str, str]:
    """Return a snapshot of all environment variables after loading .env.

    Returns:
        dict mapping every ``os.environ`` key to its value.
    """
    _ensure_env_loaded()
    return dict(os.environ)


def get_db_config() -> dict[str, Any]:
    """Return PostgreSQL connection parameters sourced from environment.

    Returns:
        dict with core connection keys plus optional session settings used by
        the ETL connection pool.
    """
    _ensure_env_loaded()
    return {
        "host": os.environ.get("POSTGRES_HOST", "localhost"),
        "port": int(os.environ.get("POSTGRES_PORT", 5432)),
        "dbname": os.environ.get("POSTGRES_DB", "performance_data_lakehouse"),
        "user": os.environ.get("POSTGRES_USER", "AdminETL"),
        "password": os.environ.get("POSTGRES_PASSWORD", ""),
        "application_name": os.environ.get(
            "POSTGRES_APPLICATION_NAME",
            _DEFAULT_POSTGRES_APPLICATION_NAME,
        ),
        "connect_timeout_seconds": _get_int_env(
            "POSTGRES_CONNECT_TIMEOUT_SECONDS",
            _DEFAULT_POSTGRES_CONNECT_TIMEOUT_SECONDS,
        ),
        "lock_timeout_ms": _get_int_env(
            "POSTGRES_LOCK_TIMEOUT_MS",
            _DEFAULT_POSTGRES_LOCK_TIMEOUT_MS,
        ),
        "statement_timeout_ms": _get_int_env(
            "POSTGRES_STATEMENT_TIMEOUT_MS",
            _DEFAULT_POSTGRES_STATEMENT_TIMEOUT_MS,
        ),
        "idle_in_transaction_session_timeout_ms": _get_int_env(
            "POSTGRES_IDLE_IN_TRANSACTION_SESSION_TIMEOUT_MS",
            _DEFAULT_POSTGRES_IDLE_TX_TIMEOUT_MS,
        ),
        "min_connections": _get_int_env(
            "POSTGRES_POOL_MIN_CONNECTIONS",
            _DEFAULT_POSTGRES_POOL_MIN_CONNECTIONS,
        ),
        "max_connections": _get_int_env(
            "POSTGRES_POOL_MAX_CONNECTIONS",
            _DEFAULT_POSTGRES_POOL_MAX_CONNECTIONS,
        ),
    }


def get_db_connection_string() -> str:
    """Build a ``psycopg2``-compatible DSN string.

    Returns:
        Connection string in the format
        ``host=... port=... dbname=... user=... password=...``
    """
    cfg = get_db_config()
    return (
        f"host={cfg['host']} port={cfg['port']} dbname={cfg['dbname']} "
        f"user={cfg['user']} password={cfg['password']}"
    )


def load_provider_config(provider: str) -> dict[str, Any]:
    """Load and return the YAML configuration for a specific provider.

    The file is resolved relative to the ``PROVIDERS_CONFIG_ROOT`` env var
    (default ``config/providers``).

    Args:
        provider: Provider name (e.g. ``"catapult"``, ``"vald"``).

    Returns:
        Parsed YAML content as a dictionary.

    Raises:
        FileNotFoundError: If the YAML file does not exist.
        yaml.YAMLError: If the file contains invalid YAML.
    """
    _ensure_env_loaded()
    config_root = os.environ.get(
        "PROVIDERS_CONFIG_ROOT",
        str(_PROJECT_ROOT / "config" / "providers"),
    )
    config_root_path = Path(config_root)
    if not config_root_path.is_absolute():
        config_root_path = (_PROJECT_ROOT / config_root_path).resolve()
    yaml_path = config_root_path / f"{provider}.yml"

    if not yaml_path.exists():
        raise FileNotFoundError(
            f"Provider config not found: {yaml_path}"
        )

    with open(yaml_path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def get_env(key: str, default: str | None = None) -> str | None:
    """Convenience wrapper around ``os.environ.get`` that ensures .env is loaded.

    Args:
        key: Environment variable name.
        default: Fallback value if the variable is not set.

    Returns:
        The variable value, or *default*.
    """
    _ensure_env_loaded()
    return os.environ.get(key, default)
