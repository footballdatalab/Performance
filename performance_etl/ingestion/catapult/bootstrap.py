"""
Catapult schema bootstrap helpers.
"""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING, Any

from ingestion.common.logging import get_logger

if TYPE_CHECKING:
    from ingestion.common.db import DatabaseManager

logger = get_logger(__name__)

_PARTITION_START_MONTH = date(2024, 1, 1)
_DEFAULT_MONTHS_AHEAD = 18


def bootstrap_catapult_foundation(
    db: "DatabaseManager",
    *,
    reference_date: date | None = None,
    months_ahead: int = _DEFAULT_MONTHS_AHEAD,
) -> dict[str, Any]:
    """Create the Catapult partition horizon required by the bronze schema."""
    partition_summary = ensure_partition_horizon(
        db,
        reference_date=reference_date,
        months_ahead=months_ahead,
    )
    summary = {
        "partition_start_month": _PARTITION_START_MONTH.isoformat(),
        "months_ahead": months_ahead,
        "partitions": partition_summary,
    }
    logger.info("Catapult bootstrap foundation complete: %s", summary)
    return summary


def build_partition_plan(
    *,
    reference_date: date | None = None,
    months_ahead: int = _DEFAULT_MONTHS_AHEAD,
) -> list[dict[str, str]]:
    """Return the Catapult monthly partition plan for stats and sensor data."""
    if months_ahead < 0:
        raise ValueError("months_ahead must be zero or greater.")

    today = reference_date or date.today()
    current_month = date(today.year, today.month, 1)
    last_month = _add_months(current_month, months_ahead)

    plan: list[dict[str, str]] = []
    month_cursor = _PARTITION_START_MONTH
    while month_cursor <= last_month:
        next_month = _add_months(month_cursor, 1)
        suffix = month_cursor.strftime("%Y_%m")
        from_ts = f"{month_cursor.isoformat()} 00:00:00+00"
        to_ts = f"{next_month.isoformat()} 00:00:00+00"
        plan.append(
            {
                "parent_table": "bronze.catapult_stats",
                "partition_table": f"bronze.catapult_stats_{suffix}",
                "from_ts": from_ts,
                "to_ts": to_ts,
                "suffix": suffix,
            }
        )
        plan.append(
            {
                "parent_table": "bronze.catapult_sensor_data",
                "partition_table": f"bronze.catapult_sensor_data_{suffix}",
                "from_ts": from_ts,
                "to_ts": to_ts,
                "suffix": suffix,
            }
        )
        month_cursor = next_month

    return plan


def ensure_partition_horizon(
    db: "DatabaseManager",
    *,
    reference_date: date | None = None,
    months_ahead: int = _DEFAULT_MONTHS_AHEAD,
) -> dict[str, Any]:
    """Create Catapult monthly partitions and their partition-local indexes."""
    plan = build_partition_plan(reference_date=reference_date, months_ahead=months_ahead)
    created_partitions: list[str] = []

    for partition in plan:
        table_name = partition["partition_table"]
        parent_table = partition["parent_table"]
        db.execute(
            (
                f"CREATE TABLE IF NOT EXISTS {table_name} "
                f"PARTITION OF {parent_table} "
                f"FOR VALUES FROM ('{partition['from_ts']}') TO ('{partition['to_ts']}')"
            )
        )
        created_partitions.append(table_name)
        for sql in _build_partition_index_sql(
            table_name=table_name,
            suffix=partition["suffix"],
            parent_table=parent_table,
        ):
            db.execute(sql)

    return {
        "partition_count": len(created_partitions),
        "partitions": created_partitions,
        "first_partition": created_partitions[0] if created_partitions else None,
        "last_partition": created_partitions[-1] if created_partitions else None,
    }


def _build_partition_index_sql(
    *,
    table_name: str,
    suffix: str,
    parent_table: str,
) -> list[str]:
    if parent_table == "bronze.catapult_stats":
        return [
            (
                f"CREATE INDEX IF NOT EXISTS ix_catapult_stats_{suffix}_activity_time "
                f"ON {table_name} (source_account, activity_id, start_time DESC)"
            ),
            (
                f"CREATE INDEX IF NOT EXISTS ix_catapult_stats_{suffix}_athlete_time "
                f"ON {table_name} (source_account, athlete_id, start_time DESC)"
            ),
            (
                f"CREATE INDEX IF NOT EXISTS ix_catapult_stats_{suffix}_start_time "
                f"ON {table_name} (start_time DESC)"
            ),
            (
                f"CREATE INDEX IF NOT EXISTS ix_catapult_stats_{suffix}_all_parameters "
                f"ON {table_name} USING gin (all_parameters)"
            ),
        ]

    return [
        (
            f"CREATE INDEX IF NOT EXISTS ix_catapult_sensor_data_{suffix}_activity_time "
            f"ON {table_name} (source_account, activity_id, recorded_at DESC)"
        ),
        (
            f"CREATE INDEX IF NOT EXISTS ix_catapult_sensor_data_{suffix}_athlete_time "
            f"ON {table_name} (source_account, athlete_id, recorded_at DESC)"
        ),
        (
            f"CREATE INDEX IF NOT EXISTS ix_catapult_sensor_data_{suffix}_recorded_at "
            f"ON {table_name} (recorded_at DESC)"
        ),
    ]


def _add_months(value: date, months: int) -> date:
    month_index = (value.year * 12 + value.month - 1) + months
    year = month_index // 12
    month = month_index % 12 + 1
    return date(year, month, 1)
