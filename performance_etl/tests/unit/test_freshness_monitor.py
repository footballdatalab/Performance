"""Phase 8.5 — unit tests for ingestion.common.freshness_monitor.

Uses a fake DatabaseManager that returns scripted rows from
``fetch_one_dict`` so we can exercise threshold-crossing logic without a
live Postgres. The persistence path (``emit_flags``) is also tested with a
fake that records its SQL calls.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from ingestion.common import freshness_monitor as fm
from ingestion.common.freshness_monitor import (
    DEFAULT_HEARTBEAT_SILENCE_HOURS,
    DEFAULT_LAG_HOURS,
    DEFAULT_LAG_HOURS_BY_TABLE,
    FreshnessFlag,
    check_dag_heartbeat,
    check_raw_to_bronze_lag,
    emit_flags,
    run_freshness_audit,
)


# ---------------------------------------------------------------------------
# Fake DB
# ---------------------------------------------------------------------------


class _FakeDB:
    """Minimal DatabaseManager stand-in.

    Returns scripted dicts from ``fetch_one_dict`` based on call order, and
    records every ``execute`` call for inspection.
    """

    def __init__(self, scripted_rows: list[dict[str, Any] | None]) -> None:
        self._rows = list(scripted_rows)
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []

    def fetch_one_dict(self, sql: str, params: tuple[Any, ...] | None = None) -> dict[str, Any] | None:
        if not self._rows:
            return None
        return self._rows.pop(0)

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> None:
        self.executed.append((sql, params))

    def close(self) -> None:
        return None


def _row(hours: float | None, last: datetime | None) -> dict[str, Any]:
    return {"hours": hours, "last_updated": last}


# ---------------------------------------------------------------------------
# Threshold lookup
# ---------------------------------------------------------------------------


def test_default_threshold_for_unknown_table() -> None:
    assert fm._table_threshold_hours("bronze.catapult_stats", None) == DEFAULT_LAG_HOURS


def test_default_threshold_for_sensor_data() -> None:
    assert fm._table_threshold_hours("bronze.catapult_sensor_data", None) == DEFAULT_LAG_HOURS_BY_TABLE["sensor_data"]


def test_default_threshold_for_events() -> None:
    assert fm._table_threshold_hours("bronze.catapult_events", None) == DEFAULT_LAG_HOURS_BY_TABLE["events"]


def test_threshold_overrides_take_precedence() -> None:
    overrides = {"sensor_data": 24}
    assert fm._table_threshold_hours("bronze.catapult_sensor_data", overrides) == 24


# ---------------------------------------------------------------------------
# check_raw_to_bronze_lag
# ---------------------------------------------------------------------------


def test_lag_check_unknown_provider_raises() -> None:
    db = _FakeDB([])
    with pytest.raises(ValueError):
        check_raw_to_bronze_lag(db, provider="hawkeye")


def test_lag_check_within_threshold_returns_no_flags() -> None:
    fresh = datetime.now(timezone.utc) - timedelta(hours=1)
    rows = [_row(1.0, fresh) for _ in fm.PROVIDER_TABLE_PAIRS["catapult"]]
    db = _FakeDB(rows)
    flags = check_raw_to_bronze_lag(db, provider="catapult")
    assert flags == []


def test_lag_check_emits_warning_when_just_over_threshold() -> None:
    """A table at 7h lag (default threshold = 6h) → warning, not critical."""
    rows = []
    for raw, bronze in fm.PROVIDER_TABLE_PAIRS["catapult"]:
        if "stats" in bronze:
            rows.append(_row(7.0, datetime.now(timezone.utc) - timedelta(hours=7)))
        else:
            rows.append(_row(0.5, datetime.now(timezone.utc)))
    db = _FakeDB(rows)
    flags = check_raw_to_bronze_lag(db, provider="catapult")
    assert len(flags) == 1
    flag = flags[0]
    assert flag.severity == "warning"
    assert flag.flag_subtype == "raw_to_bronze_lag"
    assert flag.source_table == "bronze.catapult_stats"
    assert flag.metric_value == 7.0


def test_lag_check_emits_critical_at_2x_threshold() -> None:
    """At 12h on a 6h-threshold table → critical."""
    rows = []
    for raw, bronze in fm.PROVIDER_TABLE_PAIRS["catapult"]:
        if "stats" in bronze:
            rows.append(_row(12.5, datetime.now(timezone.utc) - timedelta(hours=12, minutes=30)))
        else:
            rows.append(_row(0.5, datetime.now(timezone.utc)))
    db = _FakeDB(rows)
    flags = check_raw_to_bronze_lag(db, provider="catapult")
    assert len(flags) == 1
    assert flags[0].severity == "critical"


def test_lag_check_skips_empty_table() -> None:
    """Empty bronze table (no MAX(updated_at)) is silently skipped."""
    rows = [_row(None, None) for _ in fm.PROVIDER_TABLE_PAIRS["catapult"]]
    db = _FakeDB(rows)
    flags = check_raw_to_bronze_lag(db, provider="catapult")
    assert flags == []


def test_sensor_data_uses_relaxed_threshold() -> None:
    """Sensor data threshold is 12h; an 11h lag must NOT fire."""
    rows = []
    for raw, bronze in fm.PROVIDER_TABLE_PAIRS["catapult"]:
        if "sensor_data" in bronze:
            rows.append(_row(11.0, datetime.now(timezone.utc) - timedelta(hours=11)))
        else:
            rows.append(_row(0.5, datetime.now(timezone.utc)))
    db = _FakeDB(rows)
    flags = check_raw_to_bronze_lag(db, provider="catapult")
    assert flags == []


# ---------------------------------------------------------------------------
# check_dag_heartbeat
# ---------------------------------------------------------------------------


def test_heartbeat_within_window_returns_no_flags() -> None:
    db = _FakeDB([_row(2.0, datetime.now(timezone.utc) - timedelta(hours=2))])
    # We're reusing _row's shape; field name doesn't matter to the function
    # because it picks `hours` and `last_started` keys via .get(...).
    db._rows = [{"last_started": datetime.now(timezone.utc), "hours": 2.0}]
    flags = check_dag_heartbeat(db, provider="catapult")
    assert flags == []


def test_heartbeat_just_past_threshold_warns() -> None:
    db = _FakeDB([])
    db._rows = [{"last_started": datetime.now(timezone.utc) - timedelta(hours=26), "hours": 26.0}]
    flags = check_dag_heartbeat(db, provider="catapult")
    assert len(flags) == 1
    assert flags[0].severity == "warning"
    assert flags[0].flag_subtype == "dag_heartbeat"
    assert flags[0].metric_value == 26.0


def test_heartbeat_at_2x_threshold_critical() -> None:
    db = _FakeDB([])
    db._rows = [{"last_started": datetime.now(timezone.utc) - timedelta(hours=51), "hours": 51.0}]
    flags = check_dag_heartbeat(db, provider="catapult", max_silence_hours=25)
    assert len(flags) == 1
    assert flags[0].severity == "critical"


def test_heartbeat_no_batches_ever_emits_critical() -> None:
    """Provider with no batch_log entries → critical first-run alert."""
    db = _FakeDB([])
    db._rows = [{"last_started": None, "hours": None}]
    flags = check_dag_heartbeat(db, provider="catapult")
    assert len(flags) == 1
    assert flags[0].severity == "critical"
    assert flags[0].metric_value is None
    assert "no batches recorded" in flags[0].details["reason"]


# ---------------------------------------------------------------------------
# FreshnessFlag → row
# ---------------------------------------------------------------------------


def test_freshness_flag_row_shape() -> None:
    flag = FreshnessFlag(
        flag_subtype="raw_to_bronze_lag",
        provider="catapult",
        source_table="bronze.catapult_stats",
        metric_name="hours_since_updated_at",
        metric_value=7.5,
        severity="warning",
        details={"raw_table": "raw.catapult_stats", "threshold_hours": 6},
    )
    row = flag.as_quality_flag_row()
    assert row["flag_type"] == "etl_freshness"
    assert row["source_table"] == "bronze.catapult_stats"
    assert row["record_id"] == "raw_to_bronze_lag:catapult:bronze.catapult_stats"
    assert row["severity"] == "warning"


# ---------------------------------------------------------------------------
# emit_flags persistence
# ---------------------------------------------------------------------------


def test_emit_flags_executes_one_upsert_per_flag() -> None:
    db = _FakeDB([])
    flags = [
        FreshnessFlag(
            flag_subtype="raw_to_bronze_lag",
            provider="catapult",
            source_table="bronze.catapult_stats",
            metric_name="hours_since_updated_at",
            metric_value=7.0,
            severity="warning",
            details={"raw_table": "raw.catapult_stats"},
        ),
        FreshnessFlag(
            flag_subtype="dag_heartbeat",
            provider="catapult",
            source_table="raw.ingestion_batch_log",
            metric_name="hours_since_last_batch",
            metric_value=27.0,
            severity="warning",
            details={"max_silence_hours": 25},
        ),
    ]
    count = emit_flags(db, flags)
    assert count == 2
    assert len(db.executed) == 2
    # Confirm ON CONFLICT clause present (idempotent upsert).
    for sql, _params in db.executed:
        assert "ON CONFLICT" in sql
        assert "etl_freshness" in sql


def test_emit_flags_empty_iterable_is_noop() -> None:
    db = _FakeDB([])
    assert emit_flags(db, []) == 0
    assert db.executed == []