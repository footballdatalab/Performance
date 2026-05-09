from __future__ import annotations

import sys
import types

try:
    import psycopg2.extras  # noqa: F401
except ModuleNotFoundError:
    psycopg2 = types.ModuleType("psycopg2")
    psycopg2.extras = types.ModuleType("psycopg2.extras")
    psycopg2.extras.execute_values = lambda *args, **kwargs: None
    psycopg2.extras.RealDictCursor = object
    psycopg2.pool = types.ModuleType("psycopg2.pool")
    psycopg2.pool.ThreadedConnectionPool = object
    psycopg2.extensions = types.SimpleNamespace(connection=object, cursor=object)
    sys.modules["psycopg2"] = psycopg2
    sys.modules["psycopg2.extras"] = psycopg2.extras
    sys.modules["psycopg2.pool"] = psycopg2.pool

from script.clean_catapult_tables import discover_catapult_tables, drop_catapult_tables


class _FakeDatabase:
    def __init__(self, rows: list[tuple[str, str]], counts: dict[str, int]) -> None:
        self.rows = rows
        self.counts = counts
        self.executed: list[str] = []

    def fetch_all(
        self,
        sql: str,
        params: tuple[object, ...] | None = None,
    ) -> list[tuple[str, str]]:
        assert "information_schema.tables" in sql
        return self.rows

    def fetch_one(
        self,
        sql: str,
        params: tuple[object, ...] | None = None,
    ) -> tuple[int] | None:
        assert sql.startswith("SELECT COUNT(*) FROM ")
        table = sql.split("FROM ", 1)[1]
        return (self.counts[table],)

    def execute(
        self,
        sql: str,
        params: tuple[object, ...] | None = None,
    ) -> None:
        self.executed.append(sql)


def test_discover_catapult_tables_returns_fully_qualified_names() -> None:
    db = _FakeDatabase(
        rows=[
            ("bronze", "catapult_stats_2026_03"),
            ("raw", "catapult_stats"),
            ("silver", "catapult_athlete_profile"),
        ],
        counts={},
    )

    assert discover_catapult_tables(db) == [
        "bronze.catapult_stats_2026_03",
        "raw.catapult_stats",
        "silver.catapult_athlete_profile",
    ]


def test_drop_catapult_tables_executes_drop_statements() -> None:
    db = _FakeDatabase(
        rows=[
            ("silver", "catapult_athlete_profile"),
            ("bronze", "catapult_stats"),
            ("raw", "catapult_stats"),
        ],
        counts={
            "silver.catapult_athlete_profile": 1,
            "bronze.catapult_stats": 2,
            "raw.catapult_stats": 3,
        },
    )

    results = drop_catapult_tables(db, dry_run=False)

    assert results == {
        "silver.catapult_athlete_profile": 1,
        "bronze.catapult_stats": 2,
        "raw.catapult_stats": 3,
    }
    assert db.executed == [
        "DROP TABLE IF EXISTS silver.catapult_athlete_profile CASCADE",
        "DROP TABLE IF EXISTS bronze.catapult_stats CASCADE",
        "DROP TABLE IF EXISTS raw.catapult_stats CASCADE",
    ]
