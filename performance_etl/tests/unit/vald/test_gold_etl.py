from __future__ import annotations

import sys
import types
from datetime import datetime, timezone

if "psycopg2" not in sys.modules:
    psycopg2_stub = types.ModuleType("psycopg2")
    psycopg2_stub.extensions = types.SimpleNamespace(
        connection=object,
        cursor=object,
    )
    psycopg2_stub.extras = types.ModuleType("psycopg2.extras")
    psycopg2_pool_stub = types.ModuleType("psycopg2.pool")
    psycopg2_pool_stub.ThreadedConnectionPool = object
    sys.modules["psycopg2"] = psycopg2_stub
    sys.modules["psycopg2.extras"] = psycopg2_stub.extras
    sys.modules["psycopg2.pool"] = psycopg2_pool_stub

from ingestion.vald import gold_etl
from ingestion.vald.gold_etl import (
    DEFAULT_REFERENCE_METRIC_COVERAGE_TABLE,
    GOLD_COLUMNS_BY_FAMILY,
    _build_gold_insert_sql,
    _build_gold_publish_sql,
    _build_reference_metric_coverage_rows,
    _refresh_reference_metric_coverage,
    _resolve_gold_family_workers,
    _select_reference_metric_name,
    is_above_gold_threshold,
    is_below_gold_threshold,
    run_gold_etl,
)


class _CoverageDb:
    """Minimal DatabaseManager fake for coverage refresh.

    Phase 8.7.A added atomic_publish_table calls into the coverage path; that
    helper uses ``.connection()`` to issue the rename-swap inside one txn and
    then ``.execute()`` for the post-commit DROP. We mock both, recording the
    SQL so tests can still assert publish behaviour without a real Postgres.
    """

    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[object, ...] | None]] = []
        self.batch_inserts: list[tuple[str, list[dict[str, object]]]] = []

    def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
        self.executed.append((sql, params))

    def insert_batch_raw(self, table: str, rows: list[dict[str, object]]) -> list[int]:
        self.batch_inserts.append((table, rows))
        return list(range(1, len(rows) + 1))

    # --- Phase 8.7.A: atomic_publish_table needs a transactional context. ---
    def connection(self) -> "_CoverageConn":
        return _CoverageConn(self)


class _CoverageConn:
    def __init__(self, db: "_CoverageDb") -> None:
        self._db = db

    def cursor(self) -> "_CoverageCursor":
        return _CoverageCursor(self._db)

    def commit(self) -> None:
        return None

    def __enter__(self) -> "_CoverageConn":
        return self

    def __exit__(self, *exc: object) -> None:
        return None


class _CoverageCursor:
    def __init__(self, db: "_CoverageDb") -> None:
        self._db = db
        # Scripted return for the SELECT COUNT(*) inside atomic_publish_table.
        self._scripted_rows: list[tuple[int]] = [(0,)]

    def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
        self._db.executed.append((sql, params))

    def fetchone(self) -> tuple[int] | None:
        return self._scripted_rows.pop(0) if self._scripted_rows else None

    def __enter__(self) -> "_CoverageCursor":
        return self

    def __exit__(self, *exc: object) -> None:
        return None


def test_gold_threshold_helpers_use_strict_bounds() -> None:
    assert not is_above_gold_threshold(10.0, 12.0)
    assert not is_above_gold_threshold(12.0, 12.0)
    assert is_above_gold_threshold(12.01, 12.0)
    assert not is_below_gold_threshold(12.0, 12.0)
    assert is_below_gold_threshold(11.99, 12.0)
    assert not is_below_gold_threshold(None, 12.0)


def test_select_reference_metric_name_for_speed_uses_highest_split() -> None:
    metric_name = _select_reference_metric_name(
        source_module="smartspeed",
        assessment_family="speed",
        metric_rows=[
            {"metric_name": "split_1_cumulative_time", "max_metric_value": 1.91},
            {"metric_name": "split_3_cumulative_time", "max_metric_value": 4.88},
            {"metric_name": "split_2_cumulative_time", "max_metric_value": 3.11},
        ],
    )

    assert metric_name == "split_3_cumulative_time"


def test_select_reference_metric_name_for_forcedecks_uses_priority_then_value() -> None:
    metric_name = _select_reference_metric_name(
        source_module="forcedecks",
        assessment_family="forcedecks",
        metric_rows=[
            {"metric_name": "performance_peak_vertical_force", "max_metric_value": 1550},
            {"metric_name": "performance_concentric_peak_force", "max_metric_value": 1610},
            {"metric_name": "takeoff_jump_height_flight_time", "max_metric_value": 42.0},
            {"metric_name": "takeoff_jump_height_imp_mom", "max_metric_value": 40.0},
        ],
    )

    assert metric_name == "takeoff_jump_height_imp_mom"


def test_select_reference_metric_name_for_fixed_metric_modules() -> None:
    assert _select_reference_metric_name(
        source_module="forceframe",
        assessment_family="forceframe",
        metric_rows=[{"metric_name": "max_force", "max_metric_value": 510}],
    ) == "max_force"
    assert _select_reference_metric_name(
        source_module="nordbord",
        assessment_family="nordics",
        metric_rows=[{"metric_name": "max_force", "max_metric_value": 430}],
    ) == "max_force"
    assert _select_reference_metric_name(
        source_module="dynamo",
        assessment_family="dynamo",
        metric_rows=[{"metric_name": "max_force_newtons", "max_metric_value": 860}],
    ) == "max_force_newtons"


def test_build_reference_metric_coverage_rows_marks_covered_and_unmapped() -> None:
    rows = _build_reference_metric_coverage_rows(
        [
            {
                "source_table": "bronze.vald_forceframe_tests",
                "source_module": "forceframe",
                "assessment_family": "forceframe",
                "test_name": "Adduction - Standing",
                "source_test_count": 3,
                "latest_test_date": datetime(2026, 3, 29, tzinfo=timezone.utc),
            },
            {
                "source_table": "bronze.vald_smartspeed_test_summaries",
                "source_module": "smartspeed",
                "assessment_family": "speed",
                "test_name": "10 m Sprint",
                "source_test_count": 2,
                "latest_test_date": datetime(2026, 3, 29, tzinfo=timezone.utc),
            },
        ],
        reference_catalog={
            ("forceframe", "forceframe", "Adduction - Standing"): "max_force",
        },
    )

    assert rows == [
        {
            "source_table": "bronze.vald_forceframe_tests",
            "source_module": "forceframe",
            "assessment_family": "forceframe",
            "test_name": "Adduction - Standing",
            "reference_metric_name": "max_force",
            "coverage_status": "covered",
            "source_test_count": 3,
            "latest_test_date": datetime(2026, 3, 29, tzinfo=timezone.utc),
        },
        {
            "source_table": "bronze.vald_smartspeed_test_summaries",
            "source_module": "smartspeed",
            "assessment_family": "speed",
            "test_name": "10 m Sprint",
            "reference_metric_name": None,
            "coverage_status": "unmapped",
            "source_test_count": 2,
            "latest_test_date": datetime(2026, 3, 29, tzinfo=timezone.utc),
        },
    ]


def test_refresh_reference_metric_coverage_writes_rows_and_reports_unmapped(monkeypatch) -> None:
    db = _CoverageDb()

    monkeypatch.setattr(
        gold_etl,
        "_fetch_reference_metric_source_rows",
        lambda _db: [
            {
                "source_table": "bronze.vald_forceframe_tests",
                "source_module": "forceframe",
                "assessment_family": "forceframe",
                "test_name": "Adduction - Standing",
                "source_test_count": 3,
                "latest_test_date": datetime(2026, 3, 29, tzinfo=timezone.utc),
            },
            {
                "source_table": "bronze.vald_smartspeed_test_summaries",
                "source_module": "smartspeed",
                "assessment_family": "speed",
                "test_name": "10 m Sprint",
                "source_test_count": 2,
                "latest_test_date": datetime(2026, 3, 29, tzinfo=timezone.utc),
            },
        ],
    )
    monkeypatch.setattr(
        gold_etl,
        "_fetch_reference_metric_candidate_rows",
        lambda _db, assessment_source_table: [
            {
                "source_module": "forceframe",
                "assessment_family": "forceframe",
                "test_name": "Adduction - Standing",
                "metric_name": "max_force",
                "max_metric_value": 510,
            }
        ],
    )

    summary = _refresh_reference_metric_coverage(
        db,
        assessment_source_table="silver.vald_assessment_metric",
        coverage_table=DEFAULT_REFERENCE_METRIC_COVERAGE_TABLE,
    )

    # Phase 8.7.A: the coverage replace now uses atomic_publish_table:
    #   1. CREATE stage table mirroring live
    #   2. INSERT batch into stage
    #   3. RENAME live → live_old_<ts>, SET SCHEMA, RENAME stage → live (one txn)
    #   4. SELECT COUNT(*) for the summary
    #   5. DROP archived live_old_<ts>
    # The live table is never empty during the operation.
    executed_sqls = [sql for sql, _ in db.executed]
    assert any(
        "CREATE TABLE etl_staging." in sql
        and f"LIKE {DEFAULT_REFERENCE_METRIC_COVERAGE_TABLE} INCLUDING ALL" in sql
        for sql in executed_sqls
    ), "Phase 8.7.A: must build a stage table mirroring the live coverage table"
    assert any("ALTER TABLE" in sql and "RENAME TO" in sql and "_old_" in sql for sql in executed_sqls), (
        "Phase 8.7.A: must rename live → live_old_<ts> as the first txn step"
    )
    assert any("SET SCHEMA" in sql for sql in executed_sqls), (
        "Phase 8.7.A: must move stage into the live schema"
    )
    assert any(
        "DROP TABLE IF EXISTS" in sql and "_old_" in sql and "CASCADE" in sql
        for sql in executed_sqls
    ), "Phase 8.7.A: must drop the archived live table after the swap"
    # Locked decision #7 invariant: never TRUNCATE the live coverage table.
    assert not any(
        "TRUNCATE" in sql and "_stage_" not in sql and "_old_" not in sql
        for sql in executed_sqls
    ), "Phase 8.7.A: TRUNCATE must not appear against the live coverage table"
    # The batch insert now targets the stage table (auto-named with timestamp).
    assert db.batch_inserts[0][0].startswith("etl_staging.vald_reference_metric_coverage_stage_")
    assert len(db.batch_inserts[0][1]) == 2
    assert summary["rows_written"] == 2
    assert summary["covered_count"] == 1
    assert summary["unmapped_count"] == 1
    assert summary["unmapped_test_names"][0]["test_name"] == "10 m Sprint"


def test_build_gold_publish_sql_references_coverage_table_and_thresholds() -> None:
    sql = _build_gold_publish_sql("gold.vald_forcedecks")

    assert "PERCENTILE_CONT(0.99)" in sql
    assert "PERCENTILE_CONT(0.25)" in sql
    assert DEFAULT_REFERENCE_METRIC_COVERAGE_TABLE in sql
    assert "winner_candidates AS MATERIALIZED" in sql
    assert "test_side_modes AS MATERIALIZED" in sql
    assert "selection_candidates AS MATERIALIZED" in sql
    assert "selected_partitions AS MATERIALIZED" in sql
    assert "candidate.metric_value DESC" in sql
    assert "COUNT(*)::BIGINT FROM inserted_rows" in sql
    assert "WHEN candidate.side = 'trial' THEN 0" not in sql
    assert "PARTITION BY candidate.test_id, candidate.selection_side" in sql
    assert "p.side IS NOT DISTINCT FROM s.side" in sql


def test_build_gold_insert_sql_speed_uses_reference_selection_and_day_window() -> None:
    sql = _build_gold_insert_sql(
        "gold.vald_speed",
        day_start_utc=datetime(2026, 3, 29, 0, 0, tzinfo=timezone.utc),
        day_end_utc=datetime(2026, 3, 30, 0, 0, tzinfo=timezone.utc),
    )

    assert (
        f"INSERT INTO gold.vald_speed ({', '.join(GOLD_COLUMNS_BY_FAMILY['speed'])})"
        in sql
    )
    assert "candidate.metric_value ASC" in sql
    assert "AND s.test_date >= %s" in sql
    assert "AND s.test_date < %s" in sql
    assert "rep_number" in GOLD_COLUMNS_BY_FAMILY["speed"]
    assert "side" not in GOLD_COLUMNS_BY_FAMILY["speed"]


def test_run_gold_etl_includes_coverage_summary_and_aggregates_family_stats(monkeypatch) -> None:
    monkeypatch.setattr(
        gold_etl,
        "_refresh_reference_metric_coverage",
        lambda db, assessment_source_table, coverage_table: {
            "rows_written": 3,
            "covered_count": 2,
            "unmapped_count": 1,
            "unmapped_test_names": [{"test_name": "New Test"}],
        },
    )
    monkeypatch.setattr(
        gold_etl,
        "_publish_gold_family",
        lambda db, family, table_name, assessment_source_table, coverage_table, day_start_utc, day_end_utc, scoped_test_ids: {
            "source_rows": 5 if family == "forcedecks" else 2,
            "excluded_above_threshold_rows": 1 if family == "forcedecks" else 0,
            "excluded_below_threshold_rows": 1 if family == "forcedecks" else 0,
            "excluded_outside_threshold_rows": 2 if family == "forcedecks" else 0,
            "inserted_rows": 3 if family == "forcedecks" else 2,
        },
    )

    summary = run_gold_etl(
        object(),
        family_workers=1,
        target_tables={
            "forcedecks": "gold.vald_forcedecks",
            "speed": "gold.vald_speed",
        },
    )

    assert summary["coverage"]["rows_written"] == 3
    assert summary["coverage"]["unmapped_count"] == 1
    assert summary["coverage"]["unmapped_test_names"][0]["test_name"] == "New Test"
    assert summary["total_source_rows"] == 7
    assert summary["total_rows"] == 5
    assert summary["total_excluded_outside_threshold_rows"] == 2
    assert summary["tables"]["gold.vald_forcedecks"]["inserted_rows"] == 3


def test_resolve_gold_family_workers_reads_env(monkeypatch) -> None:
    monkeypatch.setenv("VALD_GOLD_FAMILY_WORKERS", "4")

    assert _resolve_gold_family_workers() == 4
