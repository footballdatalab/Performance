"""Unit tests for ingestion.common.perf (Phase 8.8.B)."""

from __future__ import annotations

import io
import sys
import types
from contextlib import contextmanager

import pytest

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

from ingestion.common.perf import (
    _encode_copy_row,
    analyze_table,
    bulk_copy_into,
    default_workers,
    deferred_indexes,
    unsafe_fast_session,
)


class _FakeCursor:
    """Records every execute() and copy_expert() call."""

    def __init__(self, owner: "_FakeDb") -> None:
        self.owner = owner
        self.scripted_responses: list = list(owner.scripted_responses)

    def execute(self, sql: str, params: tuple = None) -> None:
        self.owner.executed.append((sql, params))

    def copy_expert(self, sql: str, stream) -> None:
        text = stream.read() if hasattr(stream, "read") else str(stream)
        self.owner.copy_calls.append((sql, text))

    def fetchone(self):
        return self.scripted_responses.pop(0) if self.scripted_responses else None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None


class _FakeConn:
    isolation_level = 1  # READ COMMITTED

    def __init__(self, owner: "_FakeDb") -> None:
        self.owner = owner

    def cursor(self):
        return _FakeCursor(self.owner)

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None

    def set_isolation_level(self, level: int) -> None:
        self.owner.isolation_levels.append(level)


class _FakeDb:
    def __init__(self, scripted_responses=None) -> None:
        self.executed: list[tuple[str, tuple]] = []
        self.copy_calls: list[tuple[str, str]] = []
        self.scripted_responses = scripted_responses or []
        self.isolation_levels: list[int] = []
        self._conn_count = 0

    @contextmanager
    def connection(self):
        self._conn_count += 1
        yield _FakeConn(self)

    def get_connection(self) -> _FakeConn:
        return _FakeConn(self)

    def put_connection(self, conn) -> None:
        return None


# ---------------------------------------------------------------------------
# bulk_copy_into
# ---------------------------------------------------------------------------

def test_encode_copy_row_handles_nulls_tabs_newlines() -> None:
    assert _encode_copy_row(["abc", None, 42]) == "abc\t\\N\t42\n"
    assert _encode_copy_row(["a\tb"]) == "a\\tb\n"
    assert _encode_copy_row(["a\nb"]) == "a\\nb\n"
    assert _encode_copy_row(["a\\b"]) == "a\\\\b\n"
    assert _encode_copy_row([True, False]) == "t\tf\n"


def test_bulk_copy_into_emits_copy_sql_and_streams_data() -> None:
    db = _FakeDb()
    rows = [(1, "alpha"), (2, "beta"), (3, "gamma")]
    total = bulk_copy_into(db, "bronze.test_table", ("id", "name"), rows)
    assert total == 3
    assert len(db.copy_calls) == 1
    sql, body = db.copy_calls[0]
    assert "COPY bronze.test_table (id, name) FROM STDIN" in sql
    assert "FORMAT TEXT, NULL '\\N'" in sql
    assert "1\talpha" in body
    assert "2\tbeta" in body
    assert "3\tgamma" in body


def test_bulk_copy_into_chunks_at_chunk_size() -> None:
    db = _FakeDb()
    rows = [(i, f"row{i}") for i in range(11)]
    bulk_copy_into(db, "bronze.t", ("id", "name"), rows, chunk_size=5)
    # chunk_size=5 → flush at 5, 10, then trailing 1 = 3 copy_expert calls
    assert len(db.copy_calls) == 3


def test_bulk_copy_into_rejects_misaligned_rows() -> None:
    db = _FakeDb()
    with pytest.raises(ValueError, match="row width"):
        bulk_copy_into(db, "bronze.t", ("id", "name"), [(1,)])


def test_bulk_copy_into_zero_rows_emits_no_copy() -> None:
    db = _FakeDb()
    total = bulk_copy_into(db, "bronze.t", ("id",), [])
    assert total == 0
    assert db.copy_calls == []


# ---------------------------------------------------------------------------
# unsafe_fast_session
# ---------------------------------------------------------------------------

def test_unsafe_fast_session_sets_local_synchronous_commit() -> None:
    db = _FakeDb()
    with unsafe_fast_session(db):
        pass
    set_calls = [s for s, _ in db.executed if "SET LOCAL synchronous_commit = off" in s]
    assert len(set_calls) == 1
    # And a defensive RESET on exit.
    reset_calls = [s for s, _ in db.executed if "RESET synchronous_commit" in s]
    assert len(reset_calls) == 1


def test_unsafe_fast_session_resets_on_exception() -> None:
    db = _FakeDb()
    with pytest.raises(RuntimeError):
        with unsafe_fast_session(db):
            raise RuntimeError("boom")
    # The defensive RESET still ran.
    reset_calls = [s for s, _ in db.executed if "RESET synchronous_commit" in s]
    assert len(reset_calls) == 1


# ---------------------------------------------------------------------------
# analyze_table
# ---------------------------------------------------------------------------

def test_analyze_table_runs_analyze_and_restores_isolation() -> None:
    db = _FakeDb()
    analyze_table(db, "silver.vald_assessment_metric")
    assert any(
        s.strip().startswith("ANALYZE silver.vald_assessment_metric")
        for s, _ in db.executed
    )
    # We bumped to AUTOCOMMIT (0) and back.
    assert 0 in db.isolation_levels


def test_analyze_table_with_columns_clause() -> None:
    db = _FakeDb()
    analyze_table(db, "silver.t", columns=("a", "b"))
    sql = next(
        s for s, _ in db.executed if s.strip().upper().startswith("ANALYZE")
    )
    assert "(a, b)" in sql


def test_analyze_table_swallows_errors_when_skip_locked() -> None:
    """Analyze failures must not break the wrapped ETL stage."""

    class _RaisingDb(_FakeDb):
        def get_connection(self):
            raise RuntimeError("pool exhausted")

    db = _RaisingDb()
    # Should not raise.
    analyze_table(db, "silver.t", skip_locked=True)


# ---------------------------------------------------------------------------
# deferred_indexes
# ---------------------------------------------------------------------------

def test_deferred_indexes_drops_then_recreates() -> None:
    db = _FakeDb(scripted_responses=[
        ("CREATE INDEX idx_test_table_a ON bronze.test_table (a)",),
    ])
    with deferred_indexes(db, "bronze.test_table", ["idx_test_table_a"]):
        pass

    # Expect: SELECT lookup, DROP INDEX, recreate via the captured DDL
    sqls = [s for s, _ in db.executed]
    assert any("FROM pg_indexes" in s for s in sqls)
    assert any("DROP INDEX IF EXISTS idx_test_table_a" in s for s in sqls)
    assert any("CREATE INDEX idx_test_table_a" in s for s in sqls)


def test_deferred_indexes_recreates_on_exception() -> None:
    db = _FakeDb(scripted_responses=[
        ("CREATE INDEX idx_test_table_a ON bronze.test_table (a)",),
    ])
    with pytest.raises(ValueError, match="boom"):
        with deferred_indexes(db, "bronze.test_table", ["idx_test_table_a"]):
            raise ValueError("boom")
    # CREATE INDEX still ran.
    sqls = [s for s, _ in db.executed]
    assert any("CREATE INDEX idx_test_table_a" in s for s in sqls)


def test_deferred_indexes_with_empty_list_is_a_noop() -> None:
    db = _FakeDb()
    with deferred_indexes(db, "bronze.t", []):
        pass
    assert db.executed == []


def test_deferred_indexes_skips_unknown_indexes() -> None:
    db = _FakeDb(scripted_responses=[None])  # SELECT returns nothing
    with deferred_indexes(db, "bronze.t", ["idx_does_not_exist"]):
        pass
    sqls = [s for s, _ in db.executed]
    # No DROP issued when SELECT returned nothing.
    assert not any("DROP INDEX" in s for s in sqls)


# ---------------------------------------------------------------------------
# default_workers
# ---------------------------------------------------------------------------

def test_default_workers_floor() -> None:
    # Force a 1-core box; we still respect the floor.
    import os
    real = os.cpu_count
    os.cpu_count = lambda: 1
    try:
        assert default_workers(floor=2, divisor=2) == 2
    finally:
        os.cpu_count = real


def test_default_workers_typical_16_core() -> None:
    import os
    real = os.cpu_count
    os.cpu_count = lambda: 16
    try:
        assert default_workers(floor=2, divisor=2) == 8
    finally:
        os.cpu_count = real


def test_default_workers_ceiling_caps_value() -> None:
    import os
    real = os.cpu_count
    os.cpu_count = lambda: 64
    try:
        assert default_workers(floor=2, divisor=2, ceiling=12) == 12
    finally:
        os.cpu_count = real
