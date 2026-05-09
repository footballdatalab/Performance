"""Phase 8.5 — freshness monitor DAG.

Runs hourly (06:00–22:00 Europe/Lisbon) and emits `silver.data_quality_flag`
rows with `flag_type='etl_freshness'` when:

- Catapult or VALD raw→bronze lag exceeds the per-table threshold, or
- A provider hasn't logged any `raw.ingestion_batch_log` entry in
  ``DEFAULT_HEARTBEAT_SILENCE_HOURS`` (default 25h).

The two failure modes that motivated this DAG (both observed 2026-05-08):

1. Catapult bronze raw→bronze replay stuck for 3 days → lag check catches it.
2. Catapult intraday DAG didn't fire 2026-05-07 → heartbeat check catches it.

This DAG is deliberately read-mostly: it only inserts/updates rows in
``silver.data_quality_flag``. No provider calls, no DELETE / TRUNCATE. Locked
decisions #7 and #8 still hold.
"""

from __future__ import annotations

from typing import Any

import pendulum
from airflow.decorators import dag, task

from ingestion.common.config import get_db_config
from ingestion.common.db import DatabaseManager
from ingestion.common.freshness_monitor import run_freshness_audit


_TZ = pendulum.timezone("Europe/Lisbon")
_DAG_ID = "freshness_monitor"


@dag(
    dag_id=_DAG_ID,
    schedule="0 6-22 * * *",   # hourly, 06:00–22:00 Europe/Lisbon
    start_date=pendulum.datetime(2026, 5, 8, tz=_TZ),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["monitoring", "freshness", "etl", "phase-8"],
    default_args={
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
    },
)
def freshness_monitor():
    @task
    def Audit() -> dict[str, Any]:
        db = DatabaseManager(get_db_config())
        try:
            return run_freshness_audit(db, providers=("catapult", "vald"))
        finally:
            db.close()

    Audit()


freshness_monitor()