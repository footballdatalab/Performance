"""
Airflow DAGs for the Catapult raw/bronze pipeline.
"""

from __future__ import annotations

from typing import Any

import pendulum
from airflow.decorators import dag, task, task_group
from airflow.models.dagrun import DagRun
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState

from ingestion.bootstrap import bootstrap_database
from ingestion.catapult.pipeline import (
    run_extract_raw,
    run_full_refresh_raw_to_bronze_stage,
    run_historical_day_raw_to_bronze,
    run_intraday_raw_to_bronze_stage,
)
from ingestion.catapult.replay_scope import build_batch_ids_by_source_table, merge_batch_ids_by_source_table

_TZ = pendulum.timezone("Europe/Lisbon")
_FULL_REFRESH_DAG_ID = "catapult_daily_full_refresh"
_INTRADAY_DAG_ID = "catapult_intraday_incremental"
_HISTORICAL_DAG_ID = "catapult_historical_day_reprocess"
_ACTIVE_DAG_RUN_STATES = (DagRunState.QUEUED, DagRunState.RUNNING)
_CATAPULT_ACCOUNT_CODES = ("A", "B", "U15", "U16", "SUB17", "U19", "FEMININO")
_REFERENCE_ENDPOINT_TASKS = (
    ("Teams", ("teams",)),
    ("Players", ("athletes",)),
    ("Positions", ("positions",)),
    ("Parameters", ("parameters",)),
    ("Venues", ("venues",)),
    ("TagTypes", ("tag_types",)),
    ("Tags", ("tags",)),
)
_ACTIVITY_PERFORMANCE_ENDPOINTS = (
    "activities",
    "periods",
    "annotations",
    "stats",
    "efforts",
    "events",
    "sensor_data",
)


def _dag_has_active_runs(dag_id: str) -> bool:
    with create_session() as session:
        return (
            session.query(DagRun.id)
            .filter(
                DagRun.dag_id == dag_id,
                DagRun.state.in_(_ACTIVE_DAG_RUN_STATES),
            )
            .limit(1)
            .first()
            is not None
        )


def _run_extract_raw_or_skip(
    *,
    account_code: str,
    endpoint_names: tuple[str, ...],
    full_refresh: bool,
    include_reference: bool,
    include_sensor_data: bool,
) -> dict[str, Any]:
    try:
        return run_extract_raw(
            accounts=account_code,
            full_refresh=full_refresh,
            include_reference=include_reference,
            include_sensor_data=include_sensor_data,
            endpoints=set(endpoint_names),
        )
    except ValueError as exc:
        if str(exc).startswith("No Catapult accounts matched"):
            return {
                "account": account_code,
                "endpoints": list(endpoint_names),
                "skipped": True,
                "skip_reason": str(exc),
            }
        raise


def _build_account_extraction_group(
    *,
    account_code: str,
    full_refresh: bool,
):
    @task_group(group_id=account_code)
    def AccountExtraction(start_after):
        endpoint_results = []

        for task_id, endpoint_names in _REFERENCE_ENDPOINT_TASKS:
            @task(task_id=task_id)
            def ReferenceEndpoint(
                _account_code: str = account_code,
                _endpoint_names: tuple[str, ...] = endpoint_names,
            ):
                return _run_extract_raw_or_skip(
                    account_code=_account_code,
                    endpoint_names=_endpoint_names,
                    full_refresh=False,
                    include_reference=True,
                    include_sensor_data=False,
                )

            endpoint_result = ReferenceEndpoint()
            start_after >> endpoint_result
            endpoint_results.append(endpoint_result)

        @task(task_id="ActivitiesPerformance", execution_timeout=pendulum.duration(hours=6 if full_refresh else 1))
        def ActivitiesPerformance(_account_code: str = account_code):
            return _run_extract_raw_or_skip(
                account_code=_account_code,
                endpoint_names=_ACTIVITY_PERFORMANCE_ENDPOINTS,
                full_refresh=full_refresh,
                include_reference=False,
                include_sensor_data=True,
            )

        activity_result = ActivitiesPerformance()
        start_after >> activity_result
        endpoint_results.append(activity_result)

        @task(task_id="Summary")
        def Summary(*summaries: dict[str, Any]):
            batch_ids_by_source_table = merge_batch_ids_by_source_table(
                *(build_batch_ids_by_source_table(summary) for summary in summaries if summary)
            )
            return {
                "account": account_code,
                "endpoint_count": len(summaries),
                "has_new_data": any(bool(summary.get("has_new_data")) for summary in summaries if summary),
                "skipped_count": sum(1 for summary in summaries if summary and summary.get("skipped")),
                "total_loaded": sum(int(summary.get("total_loaded", 0) or 0) for summary in summaries if summary),
                "batch_ids_by_source_table": batch_ids_by_source_table,
                "errors": [
                    error
                    for summary in summaries
                    for error in (summary.get("errors", []) if summary else [])
                ],
            }

        return Summary(*endpoint_results)

    return AccountExtraction


def _merge_account_extraction_summaries(*summaries: dict[str, Any]) -> dict[str, Any]:
    batch_ids_by_source_table = merge_batch_ids_by_source_table(
        *(
            dict(summary.get("batch_ids_by_source_table", {}))
            for summary in summaries
            if summary
        )
    )
    return {
        "accounts": {
            str(summary.get("account")): summary
            for summary in summaries
            if summary and summary.get("account")
        },
        "has_new_data": any(bool(summary.get("has_new_data")) for summary in summaries if summary),
        "total_loaded": sum(int(summary.get("total_loaded", 0) or 0) for summary in summaries if summary),
        "skipped_count": sum(int(summary.get("skipped_count", 0) or 0) for summary in summaries if summary),
        "batch_ids_by_source_table": batch_ids_by_source_table,
        "errors": [
            error
            for summary in summaries
            for error in (summary.get("errors", []) if summary else [])
        ],
    }


@dag(
    dag_id=_FULL_REFRESH_DAG_ID,
    schedule="30 1 * * *",
    start_date=pendulum.datetime(2026, 4, 30, tz=_TZ),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["catapult", "daily", "full-refresh", "raw", "bronze"],
    default_args={
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=10),
    },
)
def catapult_daily_full_refresh():
    @task_group(group_id="Recovery")
    def Recovery():
        @task
        def BootstrapWarehouse():
            return bootstrap_database()

        return BootstrapWarehouse()

    @task_group(group_id="Extraction")
    def Extraction(start_after):
        account_summaries = []
        for account_code in _CATAPULT_ACCOUNT_CODES:
            account_summaries.append(
                _build_account_extraction_group(
                    account_code=account_code,
                    full_refresh=True,
                )(start_after)
            )

        @task(task_id="Summary")
        def Summary(*summaries: dict[str, Any]):
            return _merge_account_extraction_summaries(*summaries)

        return Summary(*account_summaries)

    @task_group(group_id="Bronze")
    def Bronze(start_after):
        @task(execution_timeout=pendulum.duration(hours=4))
        def ReplayAll():
            return run_full_refresh_raw_to_bronze_stage()

        replay = ReplayAll()
        start_after >> replay
        return replay

    recovered = Recovery()
    extracted = Extraction(recovered)
    Bronze(extracted)


@dag(
    dag_id=_INTRADAY_DAG_ID,
    schedule="*/30 6-23 * * *",
    start_date=pendulum.datetime(2026, 4, 30, tz=_TZ),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["catapult", "intraday", "incremental", "raw", "bronze"],
)
def catapult_intraday_incremental():
    @task_group(group_id="Recovery")
    def Recovery():
        @task.short_circuit
        def FullRefreshIdle():
            context = get_current_context()
            dag_run = context.get("dag_run")
            run_type = str(getattr(dag_run, "run_type", "") or "").lower()
            if run_type and run_type != "scheduled":
                return True
            return not (
                _dag_has_active_runs(_FULL_REFRESH_DAG_ID)
                or _dag_has_active_runs(_HISTORICAL_DAG_ID)
            )

        return FullRefreshIdle()

    @task_group(group_id="Extraction")
    def Extraction(start_after):
        account_summaries = []
        for account_code in _CATAPULT_ACCOUNT_CODES:
            account_summaries.append(
                _build_account_extraction_group(
                    account_code=account_code,
                    full_refresh=False,
                )(start_after)
            )

        @task(task_id="Summary")
        def Summary(*summaries: dict[str, Any]):
            return _merge_account_extraction_summaries(*summaries)

        return Summary(*account_summaries)

    @task_group(group_id="Raw")
    def Raw(extraction_summary: dict[str, Any]):
        @task.short_circuit
        def HasNewData(summary: dict[str, Any]):
            return bool(summary.get("has_new_data"))

        return HasNewData(extraction_summary)

    @task_group(group_id="Bronze")
    def Bronze(start_after, extraction_summary: dict[str, Any]):
        @task(execution_timeout=pendulum.duration(minutes=45))
        def ReplayAll(summary: dict[str, Any]):
            return run_intraday_raw_to_bronze_stage(
                batch_ids_by_source_table=dict(summary.get("batch_ids_by_source_table", {}))
            )

        replay = ReplayAll(extraction_summary)
        start_after >> replay
        return replay

    recovered = Recovery()
    extracted = Extraction(recovered)
    raw_gate = Raw(extracted)
    Bronze(raw_gate, extracted)


@dag(
    dag_id=_HISTORICAL_DAG_ID,
    schedule=None,
    start_date=pendulum.datetime(2026, 4, 30, tz=_TZ),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["catapult", "historical", "reprocess", "bronze"],
    params={
        "replay_date": Param(
            default="",
            type="string",
            description="Lisbon calendar day whose already-ingested raw rows should be replayed (YYYY-MM-DD).",
        ),
    },
)
def catapult_historical_day_reprocess():
    @task_group(group_id="Bronze")
    def Bronze():
        @task(execution_timeout=pendulum.duration(hours=4))
        def ReplayDay():
            context = get_current_context()
            replay_date = str(context["params"]["replay_date"])
            return run_historical_day_raw_to_bronze(replay_date)

        return ReplayDay()

    Bronze()


catapult_daily_full_refresh()
catapult_intraday_incremental()
catapult_historical_day_reprocess()
