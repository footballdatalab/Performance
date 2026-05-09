from __future__ import annotations

import sys
import types
from pathlib import Path

if "psycopg2" not in sys.modules:
    psycopg2_stub = types.ModuleType("psycopg2")
    psycopg2_stub.extensions = types.ModuleType("psycopg2.extensions")
    psycopg2_stub.extensions.connection = object
    psycopg2_stub.extensions.cursor = object
    psycopg2_stub.extras = types.ModuleType("psycopg2.extras")
    psycopg2_stub.extras.execute_values = lambda *args, **kwargs: None
    psycopg2_stub.extras.RealDictCursor = object
    psycopg2_pool_stub = types.ModuleType("psycopg2.pool")
    psycopg2_pool_stub.ThreadedConnectionPool = object
    sys.modules["psycopg2"] = psycopg2_stub
    sys.modules["psycopg2.extensions"] = psycopg2_stub.extensions
    sys.modules["psycopg2.extras"] = psycopg2_stub.extras
    sys.modules["psycopg2.pool"] = psycopg2_pool_stub

from ingestion import bootstrap


def test_platform_bootstrap_executes_ddl_and_catapult_foundation(monkeypatch) -> None:
    executed_groups: list[list[Path]] = []
    foundation_calls: list[object] = []

    class _FakeDatabase:
        def close(self) -> None:
            return None

    monkeypatch.setattr(bootstrap, "DatabaseManager", lambda config: _FakeDatabase())
    monkeypatch.setattr(
        bootstrap,
        "get_db_config",
        lambda: {"host": "x", "port": 1, "dbname": "x", "user": "x", "password": "x"},
    )
    monkeypatch.setattr(
        bootstrap,
        "discover_sql_files",
        lambda root: [
            Path("sql/ddl/00_schemas.sql"),
            Path("sql/ddl/raw/15_raw_catapult_tables.sql"),
            Path("sql/ddl/bronze/38_bronze_catapult_tables.sql"),
        ],
    )

    def fake_execute_sql_files(db, files):
        file_group = list(files)
        executed_groups.append(file_group)
        return [str(path) for path in file_group]

    monkeypatch.setattr(bootstrap, "execute_sql_files", fake_execute_sql_files)
    monkeypatch.setattr(
        bootstrap,
        "bootstrap_catapult_foundation",
        lambda db: foundation_calls.append(db) or {"partitions": {"partition_count": 2}},
    )

    summary = bootstrap.bootstrap_database()

    assert summary["ddl_file_count"] == 3
    assert summary["catapult"] == {"partitions": {"partition_count": 2}}
    assert executed_groups == [[
        Path("sql/ddl/00_schemas.sql"),
        Path("sql/ddl/raw/15_raw_catapult_tables.sql"),
        Path("sql/ddl/bronze/38_bronze_catapult_tables.sql"),
    ]]
    assert len(foundation_calls) == 1


def test_platform_bootstrap_retries_on_lock_timeout(monkeypatch) -> None:
    attempts: list[int] = []
    sleeps: list[int] = []

    class _FakeDatabase:
        def close(self) -> None:
            return None

    class _FakeLockTimeoutError(Exception):
        pgcode = "55P03"

    monkeypatch.setattr(bootstrap, "DatabaseManager", lambda config: _FakeDatabase())
    monkeypatch.setattr(
        bootstrap,
        "get_db_config",
        lambda: {"host": "x", "port": 1, "dbname": "x", "user": "x", "password": "x"},
    )
    monkeypatch.setattr(
        bootstrap,
        "discover_sql_files",
        lambda root: [Path("sql/ddl/00_schemas.sql")],
    )
    monkeypatch.setattr(
        bootstrap,
        "get_env",
        lambda key, default=None: "3" if key == "POSTGRES_BOOTSTRAP_LOCK_RETRY_ATTEMPTS" else "1",
    )
    monkeypatch.setattr(bootstrap.time, "sleep", lambda seconds: sleeps.append(seconds))

    def fake_execute_sql_files(db, files):
        attempts.append(1)
        if len(attempts) < 3:
            raise _FakeLockTimeoutError("canceling statement due to lock timeout")
        return [str(path) for path in files]

    monkeypatch.setattr(bootstrap, "execute_sql_files", fake_execute_sql_files)
    monkeypatch.setattr(
        bootstrap,
        "bootstrap_catapult_foundation",
        lambda db: {"partitions": {"partition_count": 2}},
    )

    summary = bootstrap.bootstrap_database()

    assert summary["ddl_file_count"] == 1
    assert len(attempts) == 3
    assert sleeps == [1, 1]


def test_platform_bootstrap_uses_bootstrap_timeout_overrides(monkeypatch) -> None:
    captured_configs: list[dict[str, object]] = []

    class _FakeDatabase:
        def __init__(self, config):
            captured_configs.append(config)

        def close(self) -> None:
            return None

    monkeypatch.setattr(bootstrap, "DatabaseManager", _FakeDatabase)
    monkeypatch.setattr(
        bootstrap,
        "get_db_config",
        lambda: {
            "host": "x",
            "port": 1,
            "dbname": "x",
            "user": "x",
            "password": "x",
            "lock_timeout_ms": 30_000,
            "statement_timeout_ms": 1_800_000,
        },
    )
    monkeypatch.setattr(
        bootstrap,
        "get_env",
        lambda key, default=None: {
            "POSTGRES_BOOTSTRAP_LOCK_TIMEOUT_MS": "900000",
            "POSTGRES_BOOTSTRAP_STATEMENT_TIMEOUT_MS": "7200000",
        }.get(key, default),
    )
    monkeypatch.setattr(bootstrap, "discover_sql_files", lambda root: [Path("sql/ddl/00_schemas.sql")])
    monkeypatch.setattr(bootstrap, "execute_sql_files", lambda db, files: [str(path) for path in files])
    monkeypatch.setattr(bootstrap, "bootstrap_catapult_foundation", lambda db: {})

    bootstrap.bootstrap_database()

    assert captured_configs[0]["lock_timeout_ms"] == 900000
    assert captured_configs[0]["statement_timeout_ms"] == 7200000


def test_platform_bootstrap_does_not_retry_non_lock_errors(monkeypatch) -> None:
    attempts: list[int] = []

    class _FakeDatabase:
        def close(self) -> None:
            return None

    monkeypatch.setattr(bootstrap, "DatabaseManager", lambda config: _FakeDatabase())
    monkeypatch.setattr(
        bootstrap,
        "get_db_config",
        lambda: {"host": "x", "port": 1, "dbname": "x", "user": "x", "password": "x"},
    )
    monkeypatch.setattr(
        bootstrap,
        "discover_sql_files",
        lambda root: [Path("sql/ddl/00_schemas.sql")],
    )
    monkeypatch.setattr(
        bootstrap,
        "get_env",
        lambda key, default=None: "5" if key == "POSTGRES_BOOTSTRAP_LOCK_RETRY_ATTEMPTS" else "1",
    )

    def fake_execute_sql_files(db, files):
        attempts.append(1)
        raise ValueError("broken sql")

    monkeypatch.setattr(bootstrap, "execute_sql_files", fake_execute_sql_files)

    try:
        bootstrap.bootstrap_database()
    except ValueError as exc:
        assert str(exc) == "broken sql"
    else:
        raise AssertionError("Expected bootstrap to raise the original non-lock error.")

    assert len(attempts) == 1
