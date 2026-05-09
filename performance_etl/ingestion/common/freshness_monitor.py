"""Phase 8.5 — ETL freshness monitoring.

Continuously checks two things and emits `silver.data_quality_flag` rows
with `flag_type='etl_freshness'` when the system drifts:

1. **raw → bronze lag**: for each (raw, bronze) pair, alert when
   ``now() - MAX(bronze.<table>.updated_at) > threshold``. Catches the case
   surfaced on 2026-05-08 where Catapult bronze replay was stuck for 3 days.

2. **DAG heartbeat**: alert when no `raw.ingestion_batch_log` row exists
   for the provider within ``max_silence_hours``. Catches the case where
   the DAG silently doesn't fire (2026-05-07 — zero Catapult batches).

Per locked decision #7, this module is **read-only against providers** and
**only inserts** into `silver.data_quality_flag` (no DELETE / TRUNCATE).
The CHECK constraint on `flag_type` was extended in
`sql/ddl/silver/47_silver_data_quality_etl_freshness.sql`.

Per Phase 8.6, all freshness checks use ``MAX(updated_at)``, never
``MAX(ingested_at)``, because ``ingested_at`` is only set on INSERT and
``ON CONFLICT DO UPDATE`` does not bump it.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping

from ingestion.common.db import DatabaseManager
from ingestion.common.logging import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Thresholds — keyed by table type. Override per-call if you need tighter SLAs.
# ---------------------------------------------------------------------------

DEFAULT_LAG_HOURS_BY_TABLE: dict[str, int] = {
    # High-volume streams: a longer threshold avoids false positives during
    # slow bulk loads (sensor_data is 10Hz GPS).
    "sensor_data": 12,
    "events": 8,
    # Everything else: 6h.
}
DEFAULT_LAG_HOURS = 6
DEFAULT_HEARTBEAT_SILENCE_HOURS = 25  # Daily DAGs run every ~24h; allow 1h slack.


# ---------------------------------------------------------------------------
# Per-provider raw↔bronze pair tables. Adding a provider = add a row here.
# ---------------------------------------------------------------------------

PROVIDER_TABLE_PAIRS: dict[str, list[tuple[str, str]]] = {
    "catapult": [
        ("raw.catapult_activities", "bronze.catapult_activities"),
        ("raw.catapult_periods", "bronze.catapult_periods"),
        ("raw.catapult_annotations", "bronze.catapult_annotations"),
        ("raw.catapult_stats", "bronze.catapult_stats"),
        ("raw.catapult_efforts", "bronze.catapult_efforts"),
        ("raw.catapult_events", "bronze.catapult_events"),
        ("raw.catapult_sensor_data", "bronze.catapult_sensor_data"),
    ],
    "vald": [
        ("raw.vald_forcedecks_tests", "bronze.vald_forcedecks_tests"),
        ("raw.vald_forceframe_tests", "bronze.vald_forceframe_tests"),
        ("raw.vald_nordbord_tests", "bronze.vald_nordbord_tests"),
        ("raw.vald_smartspeed_test_summaries", "bronze.vald_smartspeed_test_summaries"),
        ("raw.vald_dynamo_tests", "bronze.vald_dynamo_tests"),
    ],
}


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------


@dataclass
class FreshnessFlag:
    """One detected freshness violation, ready to be persisted as a quality flag."""

    flag_subtype: str  # 'raw_to_bronze_lag' | 'dag_heartbeat'
    provider: str
    source_table: str
    metric_name: str
    metric_value: float | None  # hours of lag/silence
    severity: str  # 'warning' | 'critical'
    details: dict[str, Any] = field(default_factory=dict)

    def as_quality_flag_row(self) -> dict[str, Any]:
        """Shape this flag into a `silver.data_quality_flag` row payload."""
        record_id = f"{self.flag_subtype}:{self.provider}:{self.source_table}"
        return {
            "source_table": self.source_table,
            "record_id": record_id,
            "metric_name": self.metric_name,
            "metric_value": self.metric_value,
            "flag_type": "etl_freshness",
            "severity": self.severity,
            "details": self.details,
        }


# ---------------------------------------------------------------------------
# Check 1: raw → bronze lag
# ---------------------------------------------------------------------------


def _table_threshold_hours(
    bronze_table: str,
    overrides: Mapping[str, int] | None,
) -> int:
    """Return the lag threshold (hours) for a given bronze table."""
    if overrides:
        for keyword, hours in overrides.items():
            if keyword in bronze_table:
                return hours
    for keyword, hours in DEFAULT_LAG_HOURS_BY_TABLE.items():
        if keyword in bronze_table:
            return hours
    return DEFAULT_LAG_HOURS


def check_raw_to_bronze_lag(
    db: DatabaseManager,
    *,
    provider: str,
    threshold_overrides: Mapping[str, int] | None = None,
) -> list[FreshnessFlag]:
    """Return one ``FreshnessFlag`` per bronze table whose lag exceeds threshold.

    ``threshold_overrides`` is a {keyword: hours} map applied if the bronze
    table name contains the keyword (e.g. ``{"sensor_data": 24}``).
    """
    pairs = PROVIDER_TABLE_PAIRS.get(provider)
    if not pairs:
        raise ValueError(f"Unknown provider for freshness monitor: {provider!r}")

    flags: list[FreshnessFlag] = []
    for raw_table, bronze_table in pairs:
        threshold_hours = _table_threshold_hours(bronze_table, threshold_overrides)
        # Use updated_at (Phase 8.6) — ingested_at doesn't bump on UPDATE.
        sql = f"""
            SELECT
                EXTRACT(EPOCH FROM (now() - MAX(updated_at))) / 3600.0 AS hours,
                MAX(updated_at) AS last_updated
            FROM {bronze_table}
        """
        try:
            row = db.fetch_one_dict(sql)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Skipping %s: %s", bronze_table, exc)
            continue

        hours = row.get("hours") if row else None
        last_updated = row.get("last_updated") if row else None

        if hours is None:
            # Empty table — skip silently. A separate 'never seen' alert is
            # out of scope here (handled by the DAG-heartbeat check + by a
            # human noticing that an account never gained any data).
            continue

        hours_float = float(hours)
        if hours_float <= threshold_hours:
            continue

        severity = "critical" if hours_float >= 2 * threshold_hours else "warning"
        flags.append(
            FreshnessFlag(
                flag_subtype="raw_to_bronze_lag",
                provider=provider,
                source_table=bronze_table,
                metric_name="hours_since_updated_at",
                metric_value=round(hours_float, 2),
                severity=severity,
                details={
                    "raw_table": raw_table,
                    "threshold_hours": threshold_hours,
                    "last_updated": last_updated.isoformat() if last_updated else None,
                },
            )
        )
    return flags


# ---------------------------------------------------------------------------
# Check 2: DAG heartbeat
# ---------------------------------------------------------------------------


def check_dag_heartbeat(
    db: DatabaseManager,
    *,
    provider: str,
    max_silence_hours: int = DEFAULT_HEARTBEAT_SILENCE_HOURS,
) -> list[FreshnessFlag]:
    """Return a single ``FreshnessFlag`` if the provider's ETL hasn't logged
    a batch in ``max_silence_hours``. Empty list otherwise.
    """
    sql = """
        SELECT
            MAX(started_at) AS last_started,
            EXTRACT(EPOCH FROM (now() - MAX(started_at))) / 3600.0 AS hours
        FROM raw.ingestion_batch_log
        WHERE provider = %s
    """
    row = db.fetch_one_dict(sql, (provider,))
    last_started = row.get("last_started") if row else None
    hours = row.get("hours") if row else None

    if hours is None:
        # No batches at all for this provider — this is itself a problem
        # (probably first-run / not yet wired) but we surface it so the
        # operator notices.
        return [
            FreshnessFlag(
                flag_subtype="dag_heartbeat",
                provider=provider,
                source_table="raw.ingestion_batch_log",
                metric_name="hours_since_last_batch",
                metric_value=None,
                severity="critical",
                details={
                    "max_silence_hours": max_silence_hours,
                    "reason": "no batches recorded for this provider yet",
                },
            )
        ]

    hours_float = float(hours)
    if hours_float <= max_silence_hours:
        return []

    severity = "critical" if hours_float >= 2 * max_silence_hours else "warning"
    return [
        FreshnessFlag(
            flag_subtype="dag_heartbeat",
            provider=provider,
            source_table="raw.ingestion_batch_log",
            metric_name="hours_since_last_batch",
            metric_value=round(hours_float, 2),
            severity=severity,
            details={
                "max_silence_hours": max_silence_hours,
                "last_started": last_started.isoformat() if last_started else None,
            },
        )
    ]


# ---------------------------------------------------------------------------
# Persistence — UPSERT into silver.data_quality_flag
# ---------------------------------------------------------------------------


_UPSERT_FLAG_SQL = """
INSERT INTO silver.data_quality_flag (
    source_table, record_id, metric_name, metric_value,
    flag_type, severity, details, resolution_status, created_at
) VALUES (
    %s, %s, %s, %s, 'etl_freshness', %s, %s::jsonb, 'open', now()
)
ON CONFLICT (source_table, record_id, metric_name, flag_type)
DO UPDATE SET
    metric_value = EXCLUDED.metric_value,
    severity = EXCLUDED.severity,
    details = EXCLUDED.details,
    resolution_status = CASE
        -- Re-open if the flag had been suppressed/resolved but the issue is back.
        WHEN silver.data_quality_flag.resolution_status IN ('reviewed','valid','invalid','suppressed')
             AND silver.data_quality_flag.metric_value IS DISTINCT FROM EXCLUDED.metric_value
            THEN 'open'
        ELSE silver.data_quality_flag.resolution_status
    END
"""


def emit_flags(
    db: DatabaseManager,
    flags: Iterable[FreshnessFlag],
) -> int:
    """UPSERT each flag into silver.data_quality_flag. Returns the count.

    Idempotent: re-emitting an unchanged flag is a no-op.
    """
    count = 0
    for flag in flags:
        row = flag.as_quality_flag_row()
        db.execute(
            _UPSERT_FLAG_SQL,
            (
                row["source_table"],
                row["record_id"],
                row["metric_name"],
                row["metric_value"],
                row["severity"],
                json.dumps(row["details"]),
            ),
        )
        count += 1
        logger.warning(
            "ETL freshness flag emitted: %s %s %s severity=%s metric=%s",
            flag.provider,
            flag.flag_subtype,
            flag.source_table,
            flag.severity,
            flag.metric_value,
        )
    return count


# ---------------------------------------------------------------------------
# High-level entry point used by the freshness_monitor Airflow DAG
# ---------------------------------------------------------------------------


def run_freshness_audit(
    db: DatabaseManager,
    *,
    providers: Iterable[str] = ("catapult", "vald"),
) -> dict[str, Any]:
    """Run both checks across the listed providers and persist any flags.

    Returns a summary dict shaped for Airflow XCom and structured logging.
    """
    summary: dict[str, Any] = {
        "providers": {},
        "flags_emitted": 0,
        "lag_violations": 0,
        "heartbeat_violations": 0,
    }
    for provider in providers:
        lag_flags = check_raw_to_bronze_lag(db, provider=provider)
        heartbeat_flags = check_dag_heartbeat(db, provider=provider)
        all_flags = [*lag_flags, *heartbeat_flags]
        emitted = emit_flags(db, all_flags)
        summary["providers"][provider] = {
            "lag_violations": len(lag_flags),
            "heartbeat_violations": len(heartbeat_flags),
            "flags_emitted": emitted,
        }
        summary["lag_violations"] += len(lag_flags)
        summary["heartbeat_violations"] += len(heartbeat_flags)
        summary["flags_emitted"] += emitted

    logger.info(
        "Freshness audit complete: emitted=%s lag=%s heartbeat=%s",
        summary["flags_emitted"],
        summary["lag_violations"],
        summary["heartbeat_violations"],
    )
    return summary