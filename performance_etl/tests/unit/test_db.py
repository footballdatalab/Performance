from __future__ import annotations

import sys
import types

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

from ingestion.common import db as common_db


def test_database_manager_sets_application_name_and_timeouts(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class _FakePool:
        def closeall(self) -> None:
            captured["closed"] = True

    def fake_pool(*, minconn: int, maxconn: int, **kwargs):
        captured["minconn"] = minconn
        captured["maxconn"] = maxconn
        captured["kwargs"] = kwargs
        return _FakePool()

    monkeypatch.setattr(common_db, "ThreadedConnectionPool", fake_pool)
    monkeypatch.setenv("AIRFLOW_CTX_DAG_ID", "vald_intraday")
    monkeypatch.setenv("AIRFLOW_CTX_TASK_ID", "silver")
    monkeypatch.setattr(common_db.socket, "gethostname", lambda: "worker-host.domain")
    monkeypatch.setattr(common_db.os, "getpid", lambda: 4321)

    manager = common_db.DatabaseManager(
        {
            "host": "db.internal",
            "port": 5432,
            "dbname": "warehouse",
            "user": "etl_user",
            "password": "secret",
            "application_name": "performance_etl",
            "connect_timeout_seconds": 15,
            "lock_timeout_ms": 30000,
            "statement_timeout_ms": 1800000,
            "idle_in_transaction_session_timeout_ms": 300000,
            "min_connections": 2,
            "max_connections": 9,
        }
    )

    assert captured["minconn"] == 2
    assert captured["maxconn"] == 9
    assert captured["kwargs"] == {
        "host": "db.internal",
        "port": 5432,
        "dbname": "warehouse",
        "user": "etl_user",
        "password": "secret",
        "application_name": "performance_etl:vald_intraday.silver:worker-host:4321",
        "connect_timeout": 15,
        "options": (
            "-c lock_timeout=30000 "
            "-c statement_timeout=1800000 "
            "-c idle_in_transaction_session_timeout=300000"
        ),
    }

    manager.close()

    assert captured["closed"] is True
