"""Phase 8.7.A — unit tests for ingestion.common.atomic_publish.

Uses a fake DatabaseManager that records every executed SQL string so we
can assert the rename dance happens in the right order, and that the
archived-table drop is best-effort (failures don't propagate).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from ingestion.common import atomic_publish as ap
from ingestion.common.atomic_publish import (
    atomic_publish_table,
    build_stage_table_like,
    cleanup_stale_archived_tables,
)


# ---------------------------------------------------------------------------
# Fake DB
# ---------------------------------------------------------------------------


class _FakeCursor:
    def __init__(self, statements: list[str], scripted_rows: list[Any]) -> None:
        self._statements = statements
        self._scripted_rows = scripted_rows
        self._last_row: Any = None

    def execute(self, sql: str, params: Any = None) -> None:
        self._statements.append(sql)
        # Pop the next scripted row only when SELECT-ish
        if "SELECT" in sql.upper():
            self._last_row = self._scripted_rows.pop(0) if self._scripted_rows else None
        else:
            self._last_row = None

    def fetchone(self):
        return self._last_row

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *exc: Any) -> None:
        return None


class _FakeConn:
    def __init__(self, statements: list[str], scripted_rows: list[Any]) -> None:
        self._statements = statements
        self._scripted_rows = scripted_rows
        self.committed = False

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self._statements, self._scripted_rows)

    def commit(self) -> None:
        self.committed = True

    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, *exc: Any) -> None:
        return None


class _FakeDB:
    """Records every SQL statement issued through .execute() and .connection()."""

    def __init__(self, scripted_rows: list[Any] | None = None, drop_raises: bool = False) -> None:
        self.statements: list[str] = []
        self._scripted_rows = list(scripted_rows or [])
        self._drop_raises = drop_raises

    def connection(self) -> _FakeConn:
        return _FakeConn(self.statements, self._scripted_rows)

    def execute(self, sql: str, params: Any = None) -> None:
        self.statements.append(sql)
        if self._drop_raises and "DROP TABLE" in sql.upper():
            raise RuntimeError("simulated drop failure")

    def fetch_all_dict(self, sql: str, params: tuple[Any, ...] | None = None) -> list[dict[str, Any]]:
        # only used by cleanup_stale_archived_tables; default empty
        return []


# ---------------------------------------------------------------------------
# _split_qualified
# ---------------------------------------------------------------------------


def test_split_qualified_valid() -> None:
    assert ap._split_qualified("silver.vald_athlete_profile") == ("silver", "vald_athlete_profile")


@pytest.mark.parametrize(
    "name",
    [
        "no_schema",                                    # no dot
        "silver.too.many.dots",                        # too many dots
        "1bad.name",                                    # starts with digit
        ".missing_schema",                              # empty schema
        "silver.",                                      # empty table
        "silver.bad-name",                              # hyphen not allowed
        "schema name.table",                            # space
    ],
)
def test_split_qualified_rejects_bad_names(name: str) -> None:
    with pytest.raises(ValueError):
        ap._split_qualified(name)


# ---------------------------------------------------------------------------
# atomic_publish_table — happy path
# ---------------------------------------------------------------------------


def test_atomic_publish_executes_rename_dance_in_order() -> None:
    db = _FakeDB(scripted_rows=[(42,)])
    summary = atomic_publish_table(
        db,
        live_table="silver.foo",
        stage_table="etl_staging.foo_stage_x",
    )
    # Find indices of each step
    sqls = db.statements
    assert any("RENAME TO" in s and "silver.foo" in s and "foo_old_" in s for s in sqls), (
        "first step must rename live to live_old_<ts>"
    )
    set_schema_idx = next(i for i, s in enumerate(sqls) if "SET SCHEMA" in s)
    rename_to_live_idx = next(i for i, s in enumerate(sqls) if "RENAME TO foo" in s and "_stage_" in s.lower() or ("RENAME TO foo" in s and "old" not in s))
    assert set_schema_idx < rename_to_live_idx, "SET SCHEMA must precede the final RENAME TO live_name"
    assert summary["rows_in_new_live"] == 42
    assert summary["live_table"] == "silver.foo"
    assert summary["stage_table"] == "etl_staging.foo_stage_x"
    assert summary["archived_table"].startswith("silver.foo_old_")


def test_atomic_publish_drops_archived_table_after_swap() -> None:
    db = _FakeDB(scripted_rows=[(7,)])
    summary = atomic_publish_table(db, live_table="silver.foo", stage_table="etl_staging.foo_stage")
    # The DROP TABLE should be the LAST statement
    assert "DROP TABLE IF EXISTS" in db.statements[-1]
    assert summary["archived_table"] in db.statements[-1]
    assert summary["archived_dropped"] is True


def test_atomic_publish_handles_drop_failure_gracefully() -> None:
    """A failure during the post-commit drop must not propagate; archived table lingers."""
    db = _FakeDB(scripted_rows=[(7,)], drop_raises=True)
    summary = atomic_publish_table(db, live_table="silver.foo", stage_table="etl_staging.foo_stage")
    assert summary["archived_dropped"] is False  # but no exception raised
    assert summary["rows_in_new_live"] == 7


def test_atomic_publish_same_schema_skips_set_schema() -> None:
    """If stage is already in the live schema, no SET SCHEMA is issued."""
    db = _FakeDB(scripted_rows=[(0,)])
    atomic_publish_table(db, live_table="silver.foo", stage_table="silver.foo_stage")
    sqls = " | ".join(db.statements)
    assert "SET SCHEMA" not in sqls


def test_atomic_publish_same_name_skips_final_rename() -> None:
    """If stage already has the live's name (and is in stage schema), only SET SCHEMA is needed."""
    db = _FakeDB(scripted_rows=[(0,)])
    atomic_publish_table(db, live_table="silver.foo", stage_table="etl_staging.foo")
    # We expect: rename live → live_old_<ts>, then SET SCHEMA, no second RENAME TO foo.
    set_schema_count = sum(1 for s in db.statements if "SET SCHEMA" in s)
    assert set_schema_count == 1


def test_atomic_publish_rejects_self_swap() -> None:
    db = _FakeDB(scripted_rows=[])
    with pytest.raises(ValueError, match="differ"):
        atomic_publish_table(db, live_table="silver.foo", stage_table="silver.foo")


def test_atomic_publish_rejects_unqualified_names() -> None:
    db = _FakeDB(scripted_rows=[])
    with pytest.raises(ValueError):
        atomic_publish_table(db, live_table="foo", stage_table="etl_staging.foo")


def test_atomic_publish_does_not_truncate_or_delete_live() -> None:
    """Locked decision #7 invariant: the helper must NEVER use TRUNCATE or DELETE on the live table."""
    db = _FakeDB(scripted_rows=[(0,)])
    atomic_publish_table(db, live_table="silver.foo", stage_table="etl_staging.foo_s")
    for sql in db.statements:
        # The post-commit DROP IF EXISTS targets the *archived* table, not live — that's OK.
        if "DROP TABLE" in sql.upper():
            assert "_old_" in sql, (
                "DROP TABLE may only target the archived table, never live"
            )
        assert "TRUNCATE" not in sql.upper(), "atomic_publish must never TRUNCATE"
        # Allow DELETE-equivalent only inside DROP TABLE; standalone DELETE is forbidden.
        assert not (
            "DELETE FROM" in sql.upper()
        ), "atomic_publish must never DELETE FROM"


# ---------------------------------------------------------------------------
# build_stage_table_like
# ---------------------------------------------------------------------------


def test_build_stage_table_like_creates_in_etl_staging_with_timestamp() -> None:
    db = _FakeDB(scripted_rows=[])
    name = build_stage_table_like(db, live_table="silver.foo")
    assert name.startswith("etl_staging.foo_stage_")
    create_sql = next(s for s in db.statements if "CREATE TABLE" in s)
    assert "LIKE silver.foo INCLUDING ALL" in create_sql


def test_build_stage_table_like_supports_custom_schema() -> None:
    db = _FakeDB(scripted_rows=[])
    name = build_stage_table_like(db, live_table="silver.foo", stage_schema="my_temp")
    assert name.startswith("my_temp.foo_stage_")


# ---------------------------------------------------------------------------
# cleanup_stale_archived_tables
# ---------------------------------------------------------------------------


class _FakeDBWithArchives(_FakeDB):
    def __init__(self, archive_names: list[str]) -> None:
        super().__init__()
        self._archive_names = archive_names

    def fetch_all_dict(self, sql: str, params: tuple[Any, ...] | None = None) -> list[dict[str, Any]]:
        return [{"table_name": n} for n in self._archive_names]


def test_cleanup_stale_archived_tables_drops_old_only() -> None:
    fresh = (datetime.now(timezone.utc) - timedelta(hours=2)).strftime("%Y%m%d_%H%M%S_000000")
    stale = (datetime.now(timezone.utc) - timedelta(hours=48)).strftime("%Y%m%d_%H%M%S_000000")
    db = _FakeDBWithArchives([
        f"foo_old_{fresh}",
        f"foo_old_{stale}",
        "foo_old_garbage",   # malformed suffix — should be skipped silently
    ])
    dropped = cleanup_stale_archived_tables(db, schema="silver", live_name="foo", older_than_hours=24)
    assert dropped == 1
    drop_stmts = [s for s in db.statements if "DROP TABLE" in s]
    assert len(drop_stmts) == 1
    assert stale in drop_stmts[0]


def test_cleanup_no_archives_returns_zero() -> None:
    db = _FakeDBWithArchives([])
    assert cleanup_stale_archived_tables(db, schema="silver", live_name="foo") == 0