from __future__ import annotations

from ingestion.common import config as common_config


def test_get_db_config_includes_postgres_session_safety_settings(monkeypatch) -> None:
    monkeypatch.setattr(common_config, "_ensure_env_loaded", lambda: None)
    monkeypatch.setenv("POSTGRES_HOST", "db.internal")
    monkeypatch.setenv("POSTGRES_PORT", "5433")
    monkeypatch.setenv("POSTGRES_DB", "warehouse")
    monkeypatch.setenv("POSTGRES_USER", "etl_user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "secret")
    monkeypatch.setenv("POSTGRES_APPLICATION_NAME", "custom_etl")
    monkeypatch.setenv("POSTGRES_CONNECT_TIMEOUT_SECONDS", "12")
    monkeypatch.setenv("POSTGRES_LOCK_TIMEOUT_MS", "45000")
    monkeypatch.setenv("POSTGRES_STATEMENT_TIMEOUT_MS", "900000")
    monkeypatch.setenv(
        "POSTGRES_IDLE_IN_TRANSACTION_SESSION_TIMEOUT_MS",
        "120000",
    )
    monkeypatch.setenv("POSTGRES_POOL_MIN_CONNECTIONS", "2")
    monkeypatch.setenv("POSTGRES_POOL_MAX_CONNECTIONS", "16")

    cfg = common_config.get_db_config()

    assert cfg == {
        "host": "db.internal",
        "port": 5433,
        "dbname": "warehouse",
        "user": "etl_user",
        "password": "secret",
        "application_name": "custom_etl",
        "connect_timeout_seconds": 12,
        "lock_timeout_ms": 45000,
        "statement_timeout_ms": 900000,
        "idle_in_transaction_session_timeout_ms": 120000,
        "min_connections": 2,
        "max_connections": 16,
    }
