"""
Airflow DAGs for the stage-based VALD pipeline.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

import pendulum
from airflow.decorators import dag, task, task_group
from airflow.models.dagrun import DagRun
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState

from ingestion.vald.pipeline import (
    bootstrap_database,
    prepare_full_refresh_bronze_stage_tables,
    run_extract_raw,
    run_full_refresh_bronze_to_silver_stage,
    run_full_refresh_raw_to_bronze_stage,
    run_full_refresh_silver_to_gold_stage,
    run_historical_day_bronze_to_silver_stage,
    run_historical_day_raw_to_bronze,
    run_historical_day_silver_to_gold_stage,
    run_intraday_bronze_to_silver_stage,
    run_intraday_deferred_raw_to_bronze_stage,
    run_intraday_raw_to_bronze_stage,
    run_intraday_silver_to_gold_stage,
)

_TZ = pendulum.timezone("Europe/Lisbon")
_FULL_REFRESH_DAG_ID = "vald_midnight_full_refresh"
_INTRADAY_DAG_ID = "vald_intraday_incremental"
_HISTORICAL_DAG_ID = "vald_historical_day_reprocess"
_ACTIVE_DAG_RUN_STATES = (DagRunState.QUEUED, DagRunState.RUNNING)


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


def _merge_raw_extraction_summaries(*summaries: dict[str, Any]) -> dict[str, Any]:
    """Merge module/reference raw extraction summaries into a single Airflow payload."""
    merged: dict[str, Any] = {
        "reference": {},
        "modules": {},
        "tenant_ids": [],
        "total_extracted": 0,
        "total_loaded": 0,
        "has_new_data": False,
        "errors": [],
    }
    tenant_ids: set[str] = set()

    for summary in summaries:
        if not summary:
            continue
        if summary.get("reference"):
            merged["reference"] = dict(summary["reference"])
        for module_name, module_summary in summary.get("modules", {}).items():
            merged["modules"][module_name] = module_summary
        tenant_ids.update(str(tenant_id) for tenant_id in summary.get("tenant_ids", []))
        merged["total_extracted"] += int(summary.get("total_extracted", 0) or 0)
        merged["total_loaded"] += int(summary.get("total_loaded", 0) or 0)
        merged["errors"].extend(summary.get("errors", []))

    merged["tenant_ids"] = sorted(tenant_ids)
    merged["has_new_data"] = merged["total_loaded"] > 0
    return merged


def _merge_intraday_replay_summaries(*summaries: dict[str, Any]) -> dict[str, Any]:
    """Merge per-module replay summaries into one payload for downstream intraday stages."""
    by_family: dict[str, set[str]] = defaultdict(set)
    counts_by_family: dict[str, int] = {}
    source_tables: dict[str, dict[str, Any]] = {}
    deferred_source_tables: set[str] = set()
    tables: dict[str, Any] = {}
    errors: list[str] = []
    processed_raw_rows = 0
    loaded_rows = 0

    for summary in summaries:
        if not summary:
            continue
        processed_raw_rows += int(summary.get("processed_raw_rows", 0) or 0)
        loaded_rows += int(summary.get("loaded_rows", 0) or 0)
        errors.extend(summary.get("errors", []))
        deferred_source_tables.update(summary.get("deferred_source_tables", []))
        tables.update(summary.get("tables", {}))

        incremental_scope = summary.get("incremental_scope") or {}
        for family, test_ids in (incremental_scope.get("by_family") or {}).items():
            by_family[str(family)].update(str(test_id) for test_id in test_ids)
        for family, count in (incremental_scope.get("counts_by_family") or {}).items():
            counts_by_family[str(family)] = counts_by_family.get(str(family), 0) + int(count or 0)
        source_tables.update(incremental_scope.get("source_tables") or {})

    normalized_by_family = {
        family: sorted(test_ids)
        for family, test_ids in by_family.items()
    }
    total_test_ids = sum(len(test_ids) for test_ids in normalized_by_family.values())
    normalized_counts = {
        family: len(test_ids)
        for family, test_ids in normalized_by_family.items()
    }
    normalized_counts.update(
        {
            family: normalized_counts.get(family, int(count or 0))
            for family, count in counts_by_family.items()
        }
    )

    return {
        "processed_raw_rows": processed_raw_rows,
        "loaded_rows": loaded_rows,
        "tables": tables,
        "errors": errors,
        "deferred_source_tables": sorted(deferred_source_tables),
        "incremental_scope": {
            "by_family": normalized_by_family,
            "counts_by_family": normalized_counts,
            "source_tables": source_tables,
            "total_test_ids": total_test_ids,
            "has_impacted_tests": total_test_ids > 0,
        },
        "has_new_data": loaded_rows > 0,
    }


@dag(
    dag_id=_FULL_REFRESH_DAG_ID,
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2026, 3, 28, tz=_TZ),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["vald", "daily", "full-refresh"],
    default_args={
        "retries": 3,
        # The Bronze/Silver tasks are now idempotent (cursor + raw_id keyset
        # checkpointing for replay; TRUNCATE+sharded re-insert for the Silver
        # ForceDecks backfill), so failed retries are cheap. Tighter delay
        # keeps the full refresh inside its overnight window.
        "retry_delay": pendulum.duration(minutes=5),
    },
)
def vald_midnight_full_refresh():
    @task_group(group_id="Recovery")
    def Recovery():
        @task
        def BootstrapWarehouse():
            return bootstrap_database()

        return BootstrapWarehouse()

    @task_group(group_id="Extraction")
    def Extraction(start_after):
        @task
        def Reference():
            return run_extract_raw(modules="", full_refresh=False, include_reference=True)

        @task
        def ForceDecks():
            return run_extract_raw(modules="forcedecks", full_refresh=True, include_reference=False)

        @task
        def ForceFrame():
            return run_extract_raw(modules="forceframe", full_refresh=True, include_reference=False)

        @task
        def Nordics():
            return run_extract_raw(modules="nordbord", full_refresh=True, include_reference=False)

        @task
        def Speed():
            return run_extract_raw(modules="smartspeed", full_refresh=True, include_reference=False)

        @task
        def Dynamo():
            return run_extract_raw(modules="dynamo", full_refresh=True, include_reference=False)

        @task
        def Summary(
            reference_summary: dict[str, Any],
            forcedecks_summary: dict[str, Any],
            forceframe_summary: dict[str, Any],
            nordics_summary: dict[str, Any],
            speed_summary: dict[str, Any],
            dynamo_summary: dict[str, Any],
        ):
            return _merge_raw_extraction_summaries(
                reference_summary,
                forcedecks_summary,
                forceframe_summary,
                nordics_summary,
                speed_summary,
                dynamo_summary,
            )

        reference = Reference()
        forcedecks = ForceDecks()
        forceframe = ForceFrame()
        nordics = Nordics()
        speed = Speed()
        dynamo = Dynamo()

        # All six API extractions are independent of each other and run in
        # parallel, but every one of them must wait for Bootstrap to finish
        # so the raw tables exist before any INSERT is attempted.
        # Note: cross-task-group fan-out via `start_after >> [list]` does not
        # register individual edges in Airflow — use per-task edges instead.
        for _ext in [reference, forcedecks, forceframe, nordics, speed, dynamo]:
            start_after >> _ext

        return Summary(reference, forcedecks, forceframe, nordics, speed, dynamo)

    @task_group(group_id="Raw")
    def Raw(start_after):
        @task
        def PrepareBronzeStageTables():
            return prepare_full_refresh_bronze_stage_tables()

        prepare = PrepareBronzeStageTables()
        # Must not run until ALL extraction tasks have finished writing to raw
        start_after >> prepare
        return prepare

    @task_group(group_id="Bronze")
    def Bronze(start_after):
        @task
        def Reference():
            return run_full_refresh_raw_to_bronze_stage(modules="", include_reference=True)

        @task
        def ForceDecks():
            return run_full_refresh_raw_to_bronze_stage(modules="forcedecks", include_reference=False)

        @task
        def ForceFrame():
            return run_full_refresh_raw_to_bronze_stage(modules="forceframe", include_reference=False)

        @task
        def Nordics():
            return run_full_refresh_raw_to_bronze_stage(modules="nordbord", include_reference=False)

        @task
        def Speed():
            return run_full_refresh_raw_to_bronze_stage(modules="smartspeed", include_reference=False)

        @task
        def Dynamo():
            return run_full_refresh_raw_to_bronze_stage(modules="dynamo", include_reference=False)

        @task
        def BronzeSummary(
            reference_summary: dict[str, Any],
            forcedecks_summary: dict[str, Any],
            forceframe_summary: dict[str, Any],
            nordics_summary: dict[str, Any],
            speed_summary: dict[str, Any],
            dynamo_summary: dict[str, Any],
        ) -> dict[str, Any]:
            """Fan-in gate: waits for all bronze tasks before Silver can start."""
            return {}

        reference = Reference()
        forcedecks = ForceDecks()
        forceframe = ForceFrame()
        nordics = Nordics()
        speed = Speed()
        dynamo = Dynamo()

        # Reference (profiles) must load first because module bronze tables
        # contain foreign references to profiles. Modules are independent of
        # each other and run in parallel once Reference is done.
        # Note: returning a list from @task_group chains those tasks sequentially
        # in Airflow — use a fan-in summary task as the single exit point instead.
        start_after >> reference
        reference >> [forcedecks, forceframe, nordics, speed, dynamo]
        return BronzeSummary(reference, forcedecks, forceframe, nordics, speed, dynamo)

    @task_group(group_id="Silver")
    def Silver(start_after):
        @task
        def AuthoritativeBronzeToSilver():
            return run_full_refresh_bronze_to_silver_stage()

        silver = AuthoritativeBronzeToSilver()
        # Must not run until ALL bronze module tasks have finished
        start_after >> silver
        return silver

    @task_group(group_id="Gold")
    def Gold(start_after):
        @task
        def AuthoritativeSilverToGold():
            return run_full_refresh_silver_to_gold_stage(runtime_validation=True)

        gold = AuthoritativeSilverToGold()
        start_after >> gold
        return gold

    recovered = Recovery()
    extracted = Extraction(recovered)
    raw_prepared = Raw(extracted)
    bronzed = Bronze(raw_prepared)
    silvered = Silver(bronzed)
    golded = Gold(silvered)


@dag(
    dag_id=_INTRADAY_DAG_ID,
    schedule="*/30 6-23 * * *",
    start_date=pendulum.datetime(2026, 3, 28, tz=_TZ),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["vald", "intraday", "incremental"],
)
def vald_intraday_incremental():
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
        @task
        def Reference():
            return run_extract_raw(
                modules="",
                full_refresh=False,
                include_reference=True,
                intraday_current_day_only=True,
            )

        @task
        def ForceDecks():
            return run_extract_raw(
                modules="forcedecks",
                full_refresh=False,
                include_reference=False,
                intraday_current_day_only=True,
            )

        @task
        def ForceFrame():
            return run_extract_raw(
                modules="forceframe",
                full_refresh=False,
                include_reference=False,
                intraday_current_day_only=True,
            )

        @task
        def Nordics():
            return run_extract_raw(
                modules="nordbord",
                full_refresh=False,
                include_reference=False,
                intraday_current_day_only=True,
            )

        @task
        def Speed():
            return run_extract_raw(
                modules="smartspeed",
                full_refresh=False,
                include_reference=False,
                intraday_current_day_only=True,
            )

        @task
        def Dynamo():
            return run_extract_raw(
                modules="dynamo",
                full_refresh=False,
                include_reference=False,
                intraday_current_day_only=True,
            )

        @task
        def Summary(
            reference_summary: dict[str, Any],
            forcedecks_summary: dict[str, Any],
            forceframe_summary: dict[str, Any],
            nordics_summary: dict[str, Any],
            speed_summary: dict[str, Any],
            dynamo_summary: dict[str, Any],
        ):
            return _merge_raw_extraction_summaries(
                reference_summary,
                forcedecks_summary,
                forceframe_summary,
                nordics_summary,
                speed_summary,
                dynamo_summary,
            )

        reference = Reference()
        forcedecks = ForceDecks()
        forceframe = ForceFrame()
        nordics = Nordics()
        speed = Speed()
        dynamo = Dynamo()

        for extraction in [reference, forcedecks, forceframe, nordics, speed, dynamo]:
            start_after >> extraction

        return Summary(
            reference,
            forcedecks,
            forceframe,
            nordics,
            speed,
            dynamo,
        )

    @task_group(group_id="Raw")
    def Raw(extraction_summary: dict[str, Any]):
        @task.short_circuit
        def HasNewData(summary: dict[str, Any]):
            return bool(summary.get("has_new_data"))

        return HasNewData(extraction_summary)

    @task_group(group_id="Bronze")
    def Bronze(start_after):
        @task
        def Reference():
            return run_intraday_raw_to_bronze_stage(modules="", include_reference=True)

        @task
        def ForceDecks():
            return run_intraday_raw_to_bronze_stage(modules="forcedecks", include_reference=False)

        @task
        def ForceFrame():
            return run_intraday_raw_to_bronze_stage(modules="forceframe", include_reference=False)

        @task
        def Nordics():
            return run_intraday_raw_to_bronze_stage(modules="nordbord", include_reference=False)

        @task
        def Speed():
            return run_intraday_raw_to_bronze_stage(modules="smartspeed", include_reference=False)

        @task
        def Dynamo():
            return run_intraday_raw_to_bronze_stage(modules="dynamo", include_reference=False)

        @task
        def ScopeSummary(
            reference_summary: dict[str, Any],
            forcedecks_summary: dict[str, Any],
            forceframe_summary: dict[str, Any],
            nordics_summary: dict[str, Any],
            speed_summary: dict[str, Any],
            dynamo_summary: dict[str, Any],
        ):
            merged = _merge_intraday_replay_summaries(
                reference_summary,
                forcedecks_summary,
                forceframe_summary,
                nordics_summary,
                speed_summary,
                dynamo_summary,
            )
            merged["reference_has_new_data"] = bool(reference_summary.get("loaded_rows"))
            return merged

        reference = Reference()
        forcedecks = ForceDecks()
        forceframe = ForceFrame()
        nordics = Nordics()
        speed = Speed()
        dynamo = Dynamo()

        for replay in [reference, forcedecks, forceframe, nordics, speed, dynamo]:
            start_after >> replay

        return ScopeSummary(
            reference,
            forcedecks,
            forceframe,
            nordics,
            speed,
            dynamo,
        )

    @task(execution_timeout=pendulum.duration(minutes=30))
    def BronzeDeferredForceFrameTraces():
        if _dag_has_active_runs(_FULL_REFRESH_DAG_ID):
            return {"skipped": True, "skip_reason": "full refresh is running", "deferred_mode": True}
        return run_intraday_deferred_raw_to_bronze_stage(modules="forceframe")

    @task_group(group_id="Silver")
    def Silver(replay_summary: dict[str, Any]):
        @task
        def AssessmentAndEntities(summary: dict[str, Any]):
            return run_intraday_bronze_to_silver_stage(
                incremental_scope=summary.get("incremental_scope"),
                refresh_reference_entities=bool(summary.get("reference_has_new_data")),
            )

        return AssessmentAndEntities(replay_summary)

    @task_group(group_id="Gold")
    def Gold(silver_summary: dict[str, Any]):
        @task
        def ReferenceMetrics(summary: dict[str, Any]):
            return run_intraday_silver_to_gold_stage(
                incremental_scope=summary.get("incremental_scope"),
            )

        return ReferenceMetrics(silver_summary)

    recovered = Recovery()
    extracted = Extraction(recovered)
    raw_gate = Raw(extracted)
    bronzed = Bronze(raw_gate)
    deferred_traces = BronzeDeferredForceFrameTraces()
    silvered = Silver(bronzed)
    golded = Gold(silvered)

    raw_gate >> deferred_traces


@dag(
    dag_id=_HISTORICAL_DAG_ID,
    schedule=None,
    start_date=pendulum.datetime(2026, 3, 28, tz=_TZ),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["vald", "historical", "reprocess"],
    params={
        "replay_date": Param(
            default="",
            type="string",
            description="Lisbon calendar day to reprocess (YYYY-MM-DD, e.g. 2026-04-09)",
        ),
    },
)
def vald_historical_day_reprocess():
    @task_group(group_id="Bronze")
    def Bronze():
        @task
        def ReplayDay():
            context = get_current_context()
            replay_date = str(context["params"]["replay_date"])
            return run_historical_day_raw_to_bronze(replay_date)

        return ReplayDay()

    @task_group(group_id="Silver")
    def Silver(replay_summary: dict[str, Any]):
        @task
        def AssessmentAndEntities(summary: dict[str, Any]):
            return run_historical_day_bronze_to_silver_stage(
                incremental_scope=summary.get("incremental_scope"),
            )

        return AssessmentAndEntities(replay_summary)

    @task_group(group_id="Gold")
    def Gold(silver_summary: dict[str, Any]):
        @task
        def ReferenceMetrics(summary: dict[str, Any]):
            return run_historical_day_silver_to_gold_stage(
                incremental_scope=summary.get("incremental_scope"),
            )

        return ReferenceMetrics(silver_summary)

    bronzed = Bronze()
    silvered = Silver(bronzed)
    Gold(silvered)


vald_midnight_full_refresh()
vald_intraday_incremental()
vald_historical_day_reprocess()
