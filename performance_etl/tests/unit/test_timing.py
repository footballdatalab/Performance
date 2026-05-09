"""Unit tests for ingestion.common.timing (Phase 8.8.A)."""

from __future__ import annotations

import sys
import types
from uuid import UUID

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

from ingestion.common.timing import (
    current_run_id,
    make_run_id,
    pipeline_run,
    recent_pipeline_summary,
    summarize_run,
    track_stage,
)


class _CapturingDb:
    """Minimal db stub: records every execute() call as (sql, params) tuples."""

    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple]] = []
        self.fetch_responses: list[list[dict]] = []

    def execute(self, sql: str, params: tuple = None) -> None:
        self.executed.append((sql, params))

    def fetch_all_dict(self, sql: str, params: tuple = None) -> list[dict]:
        self.executed.append((sql, params))
        if self.fetch_responses:
            return self.fetch_responses.pop(0)
        return []


class _RaisingDb:
    """Db stub whose execute() raises — verifies instrumentation never breaks ETL."""

    def execute(self, sql: str, params: tuple = None) -> None:
        raise RuntimeError("simulated DB outage during timing write")


def test_make_run_id_returns_uuid() -> None:
    run_id = make_run_id()
    assert isinstance(run_id, UUID)
    # And every call returns a fresh one.
    assert make_run_id() != run_id


def test_track_stage_persists_row_on_success() -> None:
    db = _CapturingDb()
    run_id = make_run_id()

    with track_stage("vald", "silver.assessment_metric",
                     sub_stage="forcedecks", db=db, run_id=run_id) as metrics:
        metrics["rows_read"] = 1000
        metrics["rows_written"] = 800

    assert len(db.executed) == 1
    sql, params = db.executed[0]
    assert "INSERT INTO silver.etl_run_timings" in sql
    # params order: run_id, pipeline, stage, sub_stage, started_at,
    # finished_at, elapsed_ms, status, rows_read, rows_written, ...
    assert params[0] == str(run_id)
    assert params[1] == "vald"
    assert params[2] == "silver.assessment_metric"
    assert params[3] == "forcedecks"
    # elapsed_ms is index 6
    assert isinstance(params[6], int)
    assert params[6] >= 0
    assert params[7] == "success"
    assert params[8] == 1000
    assert params[9] == 800
    # error_message (index 12) should be None on success
    assert params[12] is None


def test_track_stage_persists_row_on_failure_and_reraises() -> None:
    db = _CapturingDb()
    run_id = make_run_id()

    with pytest.raises(ValueError, match="boom"):
        with track_stage("vald", "bronze.replay",
                         db=db, run_id=run_id) as metrics:
            metrics["rows_read"] = 50
            raise ValueError("boom")

    assert len(db.executed) == 1
    sql, params = db.executed[0]
    assert params[1] == "vald"
    assert params[2] == "bronze.replay"
    assert params[7] == "failed"
    assert params[8] == 50  # rows_read still recorded
    # error_message (index 12)
    assert "ValueError" in params[12]
    assert "boom" in params[12]


def test_track_stage_swallows_db_failures_silently() -> None:
    """Instrumentation must NEVER break the wrapped ETL stage.

    If the DB write fails (network blip, DDL not applied, whatever),
    the stage must complete successfully.
    """
    db = _RaisingDb()

    with track_stage("common", "smoke_test", db=db) as metrics:
        metrics["rows_read"] = 1
        # Pretend ETL work completes successfully

    # No exception raised — we got here.


def test_track_stage_with_no_db_only_logs(caplog: pytest.LogCaptureFixture) -> None:
    """When db is None we still time + log, but don't try to persist."""
    import logging
    caplog.set_level(logging.INFO, logger="ingestion.common.timing")

    with track_stage("common", "no_db_test", db=None) as metrics:
        metrics["rows_read"] = 42

    # Find the etl_timing log line
    timing_lines = [r for r in caplog.records if "etl_timing" in r.getMessage()]
    assert timing_lines, "expected at least one etl_timing log line"
    assert any("no_db_test" in r.getMessage() for r in timing_lines)


def test_track_stage_records_elapsed_ms_at_least_zero() -> None:
    """The elapsed_ms must be non-negative even for instant blocks."""
    db = _CapturingDb()
    with track_stage("vald", "instant_stage", db=db):
        pass
    sql, params = db.executed[0]
    assert params[6] >= 0


def test_track_stage_extra_dict_persisted_as_json() -> None:
    db = _CapturingDb()
    with track_stage(
        "vald", "configured_stage",
        db=db, extra={"shard_count": 8, "worker_count": 4},
    ) as metrics:
        # The wrapped block can also add to extra
        metrics["extra"]["batch_size"] = 5000

    sql, params = db.executed[0]
    # extra is the last column (index 13)
    extra_json = params[13]
    assert "shard_count" in extra_json
    assert "worker_count" in extra_json
    assert "batch_size" in extra_json


def test_summarize_run_queries_by_run_id() -> None:
    db = _CapturingDb()
    db.fetch_responses.append([
        {"pipeline": "vald", "stage": "silver.assessment_metric",
         "sub_stage": "forcedecks", "elapsed_ms": 3600000, "status": "success",
         "rows_read": 1000, "rows_written": 800,
         "started_at": None, "finished_at": None,
         "error_message": None, "extra": None},
    ])
    run_id = make_run_id()

    rows = summarize_run(db, run_id)

    assert len(rows) == 1
    assert rows[0]["stage"] == "silver.assessment_metric"
    sql, params = db.executed[0]
    assert "WHERE run_id = %s" in sql
    assert "ORDER BY elapsed_ms DESC" in sql
    assert params == (str(run_id),)


def test_pipeline_run_sets_contextvar_and_records_top_row() -> None:
    """pipeline_run yields run_id, sets contextvar, persists 'pipeline.run' row."""
    db = _CapturingDb()

    captured_run_id = None
    captured_inside = None

    with pipeline_run("vald", db=db) as run_id:
        captured_run_id = run_id
        captured_inside = current_run_id()
        # Child stage with run_id=None falls back to the contextvar
        with track_stage("vald", "silver.assessment_metric", db=db) as metrics:
            metrics["rows_written"] = 100

    # Outside the context manager the contextvar is reset.
    assert current_run_id() is None
    assert captured_inside == captured_run_id

    # Two INSERTs persisted: child first (closes from inside out), parent second.
    assert len(db.executed) == 2
    child_sql, child_params = db.executed[0]
    parent_sql, parent_params = db.executed[1]

    # Both share the same run_id.
    assert child_params[0] == str(captured_run_id)
    assert parent_params[0] == str(captured_run_id)

    # Parent has stage='pipeline.run'.
    assert parent_params[2] == "pipeline.run"
    assert child_params[2] == "silver.assessment_metric"


def test_track_stage_outside_pipeline_run_generates_fresh_run_id() -> None:
    """When called outside pipeline_run, track_stage generates its own id."""
    db = _CapturingDb()

    assert current_run_id() is None
    with track_stage("vald", "orphan_stage", db=db):
        pass

    sql, params = db.executed[0]
    # A fresh UUID was generated; verify it's a valid uuid string.
    UUID(params[0])  # raises if not a valid UUID


def test_recent_pipeline_summary_returns_percentiles() -> None:
    db = _CapturingDb()
    db.fetch_responses.append([
        {"stage": "bronze.replay", "sub_stage": "forcedecks_trial_results",
         "run_count": 10, "p50_ms": 4200000, "p95_ms": 5400000,
         "max_ms": 6000000, "total_rows_written": 39600000},
    ])

    rows = recent_pipeline_summary(db, "vald", limit_runs=10)

    assert len(rows) == 1
    assert rows[0]["p50_ms"] == 4200000
    sql, params = db.executed[0]
    assert "PERCENTILE_DISC(0.5)" in sql
    assert "PERCENTILE_DISC(0.95)" in sql
    # WITH recent_runs CTE references pipeline twice
    assert params == ("vald", 10, "vald")
