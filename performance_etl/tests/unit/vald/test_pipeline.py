from __future__ import annotations

import sys
import types
from contextlib import contextmanager
from datetime import datetime, timezone

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

from ingestion.vald import pipeline


@contextmanager
def _noop_live_write_lock(*, owner: str, db_config=None, wait: bool = False):
    yield


def test_resolve_modules_filters_to_enabled_modules() -> None:
    provider_config = {
        "modules": [
            {"name": "forcedecks", "enabled": True},
            {"name": "forceframe", "enabled": False},
            {"name": "nordbord", "enabled": True},
        ]
    }

    assert pipeline.resolve_modules("all", provider_config) == ["forcedecks", "nordbord"]
    assert pipeline.resolve_modules("forceframe,nordbord", provider_config) == ["nordbord"]


def test_bootstrap_database_delegates_to_platform_bootstrap(monkeypatch) -> None:
    monkeypatch.setattr(
        pipeline,
        "platform_bootstrap_database",
        lambda: {"executed_files": ["sql/ddl/00_schemas.sql"], "ddl_file_count": 1},
    )

    summary = pipeline.bootstrap_database()

    assert summary == {
        "executed_files": ["sql/ddl/00_schemas.sql"],
        "ddl_file_count": 1,
    }


def test_main_run_ingestion_passes_stage_flags(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_run_end_to_end(**kwargs):
        captured.update(kwargs)
        return {"errors": []}

    monkeypatch.setattr(pipeline, "run_end_to_end", fake_run_end_to_end)
    monkeypatch.setattr(pipeline, "_print_pipeline_summary", lambda summary: None)

    exit_code = pipeline.main_run_ingestion(
        [
            "--modules",
            "forcedecks,dynamo",
            "--full-refresh",
            "--skip-reference",
            "--runtime-validate",
            "--skip-quality",
        ]
    )

    assert exit_code == 0
    assert captured == {
        "modules": "forcedecks,dynamo",
        "full_refresh": True,
        "include_reference": False,
        "runtime_validation": True,
    }


def test_run_extract_raw_passes_intraday_current_day_only_to_module_extractors(monkeypatch) -> None:
    captured_flags: list[bool] = []

    class _FakeDatabase:
        def __init__(self, _config) -> None:
            return None

        def close(self) -> None:
            return None

    class _FakeBatchManager:
        def __init__(self, _db) -> None:
            return None

        def start_batch(self, **_kwargs):
            return "batch-1"

        def complete_batch(self, **_kwargs) -> None:
            return None

        def fail_batch(self, *_args, **_kwargs) -> None:
            return None

    class _FakeClient:
        def close(self) -> None:
            return None

    class _FakeExtractor:
        def extract(self, tenant_id: str):
            return {
                "tenant_id": tenant_id,
                "records_extracted": 1,
                "records_loaded": 1,
            }

    monkeypatch.setattr(
        pipeline,
        "load_provider_config",
        lambda _provider: {"modules": [{"name": "forcedecks", "enabled": True}]},
    )
    monkeypatch.setattr(pipeline, "DatabaseManager", _FakeDatabase)
    monkeypatch.setattr(pipeline, "get_db_config", lambda: {})
    monkeypatch.setattr(pipeline, "WatermarkManager", lambda _db: object())
    monkeypatch.setattr(pipeline, "BatchManager", _FakeBatchManager)
    monkeypatch.setattr(pipeline, "ValdClient", lambda _cfg: _FakeClient())
    monkeypatch.setattr(pipeline, "discover_tenant_ids", lambda _client: ["tenant-1"])
    monkeypatch.setattr(pipeline, "ValdRawLoader", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(pipeline, "_validate_batch_integrity", lambda _db, _batch_ids: None)

    def fake_build_module_extractor(**kwargs):
        captured_flags.append(bool(kwargs["intraday_current_day_only"]))
        return _FakeExtractor()

    monkeypatch.setattr(pipeline, "_build_module_extractor", fake_build_module_extractor)

    summary = pipeline.run_extract_raw(
        modules="forcedecks",
        include_reference=False,
        intraday_current_day_only=True,
    )

    assert summary["total_loaded"] == 1
    assert captured_flags == [True]


def test_run_extract_raw_disables_intraday_current_day_only_for_full_refresh(monkeypatch) -> None:
    captured_flags: list[bool] = []

    class _FakeDatabase:
        def __init__(self, _config) -> None:
            return None

        def close(self) -> None:
            return None

    class _FakeBatchManager:
        def __init__(self, _db) -> None:
            return None

        def start_batch(self, **_kwargs):
            return "batch-1"

        def complete_batch(self, **_kwargs) -> None:
            return None

        def fail_batch(self, *_args, **_kwargs) -> None:
            return None

    class _FakeClient:
        def close(self) -> None:
            return None

    class _FakeExtractor:
        def extract(self, tenant_id: str):
            return {
                "tenant_id": tenant_id,
                "records_extracted": 1,
                "records_loaded": 1,
            }

    monkeypatch.setattr(
        pipeline,
        "load_provider_config",
        lambda _provider: {"modules": [{"name": "forcedecks", "enabled": True}]},
    )
    monkeypatch.setattr(pipeline, "DatabaseManager", _FakeDatabase)
    monkeypatch.setattr(pipeline, "get_db_config", lambda: {})
    monkeypatch.setattr(pipeline, "WatermarkManager", lambda _db: object())
    monkeypatch.setattr(pipeline, "BatchManager", _FakeBatchManager)
    monkeypatch.setattr(pipeline, "ValdClient", lambda _cfg: _FakeClient())
    monkeypatch.setattr(pipeline, "discover_tenant_ids", lambda _client: ["tenant-1"])
    monkeypatch.setattr(pipeline, "ValdRawLoader", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(pipeline, "_validate_batch_integrity", lambda _db, _batch_ids: None)
    monkeypatch.setattr(pipeline, "_reset_module_watermarks", lambda **_kwargs: None)

    def fake_build_module_extractor(**kwargs):
        captured_flags.append(bool(kwargs["intraday_current_day_only"]))
        return _FakeExtractor()

    monkeypatch.setattr(pipeline, "_build_module_extractor", fake_build_module_extractor)

    summary = pipeline.run_extract_raw(
        modules="forcedecks",
        full_refresh=True,
        include_reference=False,
        intraday_current_day_only=True,
    )

    assert summary["total_loaded"] == 1
    assert captured_flags == [False]


def test_main_validate_pipeline_honors_runtime_only(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_run_validation(runtime_only: bool, run_pytest_suite: bool):
        captured["runtime_only"] = runtime_only
        captured["run_pytest_suite"] = run_pytest_suite
        return {"ok": True, "errors": [], "runtime_only": runtime_only, "pytest_ran": False}

    monkeypatch.setattr(pipeline, "run_validation", fake_run_validation)
    monkeypatch.setattr(pipeline, "_log_stage_summary", lambda stage_name, summary: None)

    exit_code = pipeline.main_validate_pipeline(["--runtime-only"])

    assert exit_code == 0
    assert captured == {
        "runtime_only": True,
        "run_pytest_suite": True,
    }


def test_main_run_reset_rebuild_passes_runtime_flag(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_run_reset_rebuild(runtime_validation: bool):
        captured["runtime_validation"] = runtime_validation
        return {"errors": []}

    monkeypatch.setattr(pipeline, "run_reset_rebuild", fake_run_reset_rebuild)
    monkeypatch.setattr(pipeline, "_print_pipeline_summary", lambda summary: None)

    exit_code = pipeline.main_run_reset_rebuild(["--runtime-validate"])

    assert exit_code == 0
    assert captured == {"runtime_validation": True}


def test_validate_schema_state_checks_required_and_removed_columns(monkeypatch) -> None:
    class _ValidationDb:
        def fetch_all(self, sql: str):
            if "table_schema = 'silver'" in sql:
                return [(table_name.split(".", 1)[1],) for table_name in pipeline.ACTIVE_SILVER_TABLES]
            if "table_schema = 'gold'" in sql:
                return [(table_name.split(".", 1)[1],) for table_name in pipeline.ACTIVE_GOLD_TABLES]
            raise AssertionError(f"Unexpected SQL: {sql}")

    required_missing = ("raw.vald_dynamo_tests", "page_number")
    forbidden_present = ("gold.vald_speed", "side")

    monkeypatch.setattr(
        pipeline,
        "_table_exists",
        lambda db, table_name: (
            table_name not in pipeline.OBSOLETE_VALD_TABLES
            and table_name not in pipeline.UNSUPPORTED_CATAPULT_TABLES
        ),
    )
    monkeypatch.setattr(pipeline, "_count_partitions", lambda db, table_name: 1)

    def fake_column_exists(db, table_name: str, column_name: str) -> bool:
        pair = (table_name, column_name)
        if pair == required_missing:
            return False
        if pair == forbidden_present:
            return True
        if pair in pipeline._REQUIRED_VALD_COLUMNS:
            return True
        if pair in pipeline._FORBIDDEN_VALD_COLUMNS:
            return False
        raise AssertionError(f"Unexpected column check: {pair}")

    monkeypatch.setattr(pipeline, "_column_exists", fake_column_exists)

    errors: list[str] = []
    pipeline._validate_schema_state(_ValidationDb(), errors)

    assert "Missing required column: raw.vald_dynamo_tests.page_number" in errors
    assert "Removed VALD column still exists: gold.vald_speed.side" in errors


def test_validate_schema_state_allows_supported_focus_gold_tables(monkeypatch) -> None:
    class _ValidationDb:
        def fetch_all(self, sql: str):
            if "table_schema = 'silver'" in sql:
                return [(table_name.split(".", 1)[1],) for table_name in pipeline.ACTIVE_SILVER_TABLES]
            if "table_schema = 'gold'" in sql:
                allowed = [
                    *pipeline.ACTIVE_GOLD_TABLES,
                    *pipeline._SUPPORTED_NON_VALD_GOLD_TABLES,
                ]
                return [(table_name.split(".", 1)[1],) for table_name in allowed]
            raise AssertionError(f"Unexpected SQL: {sql}")

    monkeypatch.setattr(
        pipeline,
        "_table_exists",
        lambda db, table_name: (
            table_name not in pipeline.OBSOLETE_VALD_TABLES
            and table_name not in pipeline.UNSUPPORTED_CATAPULT_TABLES
        ),
    )
    monkeypatch.setattr(pipeline, "_count_partitions", lambda db, table_name: 1)
    monkeypatch.setattr(
        pipeline,
        "_column_exists",
        lambda db, table_name, column_name: (
            (table_name, column_name) in pipeline._REQUIRED_VALD_COLUMNS
        ),
    )

    errors: list[str] = []
    pipeline._validate_schema_state(_ValidationDb(), errors)

    assert errors == []


def test_run_reset_rebuild_uses_staged_publish_and_preserves_raw(monkeypatch) -> None:
    captured: dict[str, object] = {
        "db_configs": [],
    }

    class _FakeDatabase:
        def __init__(self, config: dict[str, object]) -> None:
            captured["db_configs"].append(config)

        def close(self) -> None:
            return None

    monkeypatch.setattr(pipeline, "bootstrap_database", lambda: {"ok": True})

    def fake_run_extract_raw(**kwargs):
        captured["extract_kwargs"] = kwargs
        return {"errors": [], "modules": {}}

    def fake_run_raw_to_bronze_stage(**kwargs):
        captured["replay_kwargs"] = kwargs
        return {"processed_raw_rows": 1, "loaded_rows": 1}

    def fake_run_silver_etl(db, **kwargs):
        captured["silver_kwargs"] = kwargs
        return {"assessment_metrics": {"total_inserted": 1}}

    def fake_run_gold_etl(db, **kwargs):
        captured["gold_kwargs"] = kwargs
        return {"total_rows": 1, "total_excluded_outside_threshold_rows": 0}

    def fake_publish_stage_tables(db, specs):
        captured["published_specs"] = specs
        return {"total_published_rows": 2, "tables": {}}

    def fake_publish_bronze_stage_tables(db, specs):
        captured["published_bronze_specs"] = specs
        return {"total_inserted_rows": 4, "total_deleted_rows": 1, "tables": {}}

    monkeypatch.setattr(
        pipeline,
        "run_extract_raw",
        fake_run_extract_raw,
    )
    monkeypatch.setattr(
        pipeline,
        "prepare_full_refresh_bronze_stage_tables",
        lambda: captured.setdefault("prepared_bronze_stage_tables_called", True) or {},
    )
    monkeypatch.setattr(
        pipeline,
        "run_raw_to_bronze_stage",
        fake_run_raw_to_bronze_stage,
    )
    monkeypatch.setattr(
        pipeline,
        "run_silver_etl",
        fake_run_silver_etl,
    )
    monkeypatch.setattr(
        pipeline,
        "run_gold_etl",
        fake_run_gold_etl,
    )
    monkeypatch.setattr(
        pipeline,
        "_prepare_stage_tables",
        lambda db, specs: captured.setdefault("prepared_specs", []).append(specs),
    )
    monkeypatch.setattr(
        pipeline,
        "_publish_bronze_stage_tables",
        fake_publish_bronze_stage_tables,
    )
    monkeypatch.setattr(
        pipeline,
        "_publish_stage_tables",
        fake_publish_stage_tables,
    )
    monkeypatch.setattr(
        pipeline,
        "rebuild_overlap_quality_flags",
        lambda db: {"flags_written": 3, "ambiguous_profiles": 2},
    )
    monkeypatch.setattr(
        pipeline,
        "run_validation",
        lambda runtime_only, run_pytest_suite: {"ok": True, "errors": [], "runtime_only": runtime_only, "pytest_ran": False},
    )
    monkeypatch.setattr(pipeline, "DatabaseManager", _FakeDatabase)
    monkeypatch.setattr(
        pipeline,
        "get_db_config",
        lambda: {
            "host": "x",
            "port": 1,
            "dbname": "x",
            "user": "x",
            "password": "x",
            "statement_timeout_ms": 1800000,
        },
    )
    monkeypatch.setattr(
        pipeline,
        "get_env",
        lambda key, default=None: "5400000" if key == "POSTGRES_GOLD_STATEMENT_TIMEOUT_MS" else default,
    )
    monkeypatch.setattr(
        pipeline,
        "_hold_vald_live_write_lock",
        _noop_live_write_lock,
    )

    summary = pipeline.run_reset_rebuild(runtime_validation=True)

    assert captured["extract_kwargs"] == {
        "modules": "all",
        "full_refresh": True,
        "include_reference": True,
    }
    assert captured["replay_kwargs"] == {
        "modules": "all",
        "include_reference": True,
        "table_overrides": {
            "bronze.vald_profiles": "etl_staging.bronze_vald_profiles",
            "bronze.vald_profile_categories": "etl_staging.bronze_vald_profile_categories",
            "bronze.vald_forcedecks_result_definitions": "etl_staging.bronze_vald_forcedecks_result_definitions",
            "bronze.vald_forcedecks_tests": "etl_staging.bronze_vald_forcedecks_tests",
            "bronze.vald_forcedecks_trials": "etl_staging.bronze_vald_forcedecks_trials",
            "bronze.vald_forcedecks_trial_results": "etl_staging.bronze_vald_forcedecks_trial_results",
            "bronze.vald_forceframe_tests": "etl_staging.bronze_vald_forceframe_tests",
            "bronze.vald_forceframe_test_metrics": "etl_staging.bronze_vald_forceframe_test_metrics",
            "bronze.vald_forceframe_force_traces": "etl_staging.bronze_vald_forceframe_force_traces",
            "bronze.vald_nordbord_tests": "etl_staging.bronze_vald_nordbord_tests",
            "bronze.vald_nordbord_test_metrics": "etl_staging.bronze_vald_nordbord_test_metrics",
            "bronze.vald_nordbord_ecc_exercises": "etl_staging.bronze_vald_nordbord_ecc_exercises",
            "bronze.vald_nordbord_ecc_repetitions": "etl_staging.bronze_vald_nordbord_ecc_repetitions",
            "bronze.vald_smartspeed_test_summaries": "etl_staging.bronze_vald_smartspeed_test_summaries",
            "bronze.vald_smartspeed_test_details": "etl_staging.bronze_vald_smartspeed_test_details",
            "bronze.vald_smartspeed_rep_results": "etl_staging.bronze_vald_smartspeed_rep_results",
            "bronze.vald_dynamo_tests": "etl_staging.bronze_vald_dynamo_tests",
            "bronze.vald_dynamo_rep_summaries": "etl_staging.bronze_vald_dynamo_rep_summaries",
            "bronze.vald_dynamo_repetitions": "etl_staging.bronze_vald_dynamo_repetitions",
            "bronze.vald_dynamo_traces": "etl_staging.bronze_vald_dynamo_traces",
        },
        "full_replay": True,
        "replay_cursor_table": None,
        "exclude_source_tables": ("raw.vald_forceframe_force_traces",),
    }
    assert captured["silver_kwargs"]["sync_quality_flags"] is False
    assert captured["silver_kwargs"]["table_overrides"] == {
        "membership": "etl_staging.silver_vald_target_group_membership",
        "profile": "etl_staging.silver_vald_athlete_profile",
        "assessment": "etl_staging.silver_vald_assessment_metric",
    }
    assert captured["gold_kwargs"]["assessment_source_table"] == "etl_staging.silver_vald_assessment_metric"
    assert captured["gold_kwargs"]["coverage_table"] == "etl_staging.silver_vald_reference_metric_coverage"
    assert captured["gold_kwargs"]["target_tables"]["forcedecks"] == "etl_staging.gold_vald_forcedecks"
    assert captured["prepared_bronze_stage_tables_called"] is True
    assert len(captured["prepared_specs"]) == 2
    assert len(captured["published_bronze_specs"]) == len(pipeline.ACTIVE_BRONZE_TABLES)
    assert len(captured["published_specs"]) == 9
    assert captured["db_configs"][2]["statement_timeout_ms"] == 1800000
    assert summary["publish"]["bronze"]["total_inserted_rows"] == 4
    assert summary["publish"]["silver_gold"]["total_published_rows"] == 2
    assert summary["quality_refresh"] == {
        "flags_written": 3,
        "ambiguous_profiles": 2,
    }


def test_main_run_resume_pipeline_passes_resume_flags(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_run_resume_pipeline(
        from_stage: str,
        *,
        modules: str,
        full_refresh: bool,
        include_reference: bool,
        runtime_validation: bool,
    ):
        captured["from_stage"] = from_stage
        captured["modules"] = modules
        captured["full_refresh"] = full_refresh
        captured["include_reference"] = include_reference
        captured["runtime_validation"] = runtime_validation
        return {"errors": []}

    monkeypatch.setattr(pipeline, "run_resume_pipeline", fake_run_resume_pipeline)
    monkeypatch.setattr(pipeline, "_print_pipeline_summary", lambda summary: None)

    exit_code = pipeline.main_run_resume_pipeline(
        [
            "--from-stage",
            "silver_to_gold",
            "--modules",
            "forcedecks,speed",
            "--skip-reference",
            "--runtime-validate",
        ]
    )

    assert exit_code == 0
    assert captured == {
        "from_stage": "silver_to_gold",
        "modules": "forcedecks,speed",
        "full_refresh": False,
        "include_reference": False,
        "runtime_validation": True,
    }


def test_run_resume_pipeline_runs_requested_tail_only(monkeypatch) -> None:
    call_order: list[str] = []

    monkeypatch.setattr(
        pipeline,
        "run_extract_raw",
        lambda **kwargs: call_order.append("raw") or {"errors": [], "modules": {}},
    )
    monkeypatch.setattr(
        pipeline,
        "run_raw_to_bronze_stage",
        lambda **kwargs: call_order.append("raw_to_bronze") or {"processed_raw_rows": 1, "loaded_rows": 1},
    )
    monkeypatch.setattr(
        pipeline,
        "_run_bronze_to_silver_stage_unlocked",
        lambda: call_order.append("bronze_to_silver") or {"assessment_metrics": {"total_inserted": 1}},
    )
    monkeypatch.setattr(
        pipeline,
        "_run_silver_to_gold_stage_unlocked",
        lambda: call_order.append("silver_to_gold") or {"total_rows": 1},
    )
    monkeypatch.setattr(
        pipeline,
        "run_validation",
        lambda runtime_only, run_pytest_suite: call_order.append("validation")
        or {"ok": True, "errors": [], "runtime_only": runtime_only, "pytest_ran": False},
    )
    monkeypatch.setattr(
        pipeline,
        "_hold_vald_live_write_lock",
        _noop_live_write_lock,
    )

    summary = pipeline.run_resume_pipeline(
        "bronze_to_silver",
        modules="forcedecks",
        full_refresh=True,
        include_reference=False,
        runtime_validation=True,
    )

    assert call_order == ["bronze_to_silver", "silver_to_gold", "validation"]
    assert summary["resumed_from"] == "bronze_to_silver"
    assert summary["silver"] == {"assessment_metrics": {"total_inserted": 1}}
    assert summary["gold"] == {"total_rows": 1}
    assert summary["validation"]["ok"] is True


def test_run_silver_to_gold_stage_uses_gold_statement_timeout_override(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class _FakeDatabase:
        def __init__(self, config: dict[str, object]) -> None:
            captured["config"] = config

        def close(self) -> None:
            return None

    monkeypatch.setattr(pipeline, "DatabaseManager", _FakeDatabase)
    monkeypatch.setattr(
        pipeline,
        "get_db_config",
        lambda: {
            "host": "x",
            "port": 1,
            "dbname": "x",
            "user": "x",
            "password": "x",
            "statement_timeout_ms": 1800000,
        },
    )
    monkeypatch.setattr(
        pipeline,
        "get_env",
        lambda key, default=None: "5400000" if key == "POSTGRES_GOLD_STATEMENT_TIMEOUT_MS" else default,
    )
    monkeypatch.setattr(pipeline, "_require_tables", lambda db, tables: None)
    monkeypatch.setattr(
        pipeline,
        "run_gold_etl",
        lambda db, day_start_utc=None, day_end_utc=None: {"total_excluded_outside_threshold_rows": 0},
    )
    monkeypatch.setattr(
        pipeline,
        "_hold_vald_live_write_lock",
        _noop_live_write_lock,
    )

    summary = pipeline.run_silver_to_gold_stage()

    assert summary["total_excluded_outside_threshold_rows"] == 0
    assert captured["config"]["statement_timeout_ms"] == 5400000


def test_run_intraday_bronze_to_silver_stage_uses_today_window(monkeypatch) -> None:
    captured: dict[str, object] = {}
    day_start = datetime(2026, 3, 29, 0, 0, tzinfo=timezone.utc)
    day_end = datetime(2026, 3, 30, 0, 0, tzinfo=timezone.utc)

    class _FakeDatabase:
        def close(self) -> None:
            return None

    monkeypatch.setattr(pipeline, "DatabaseManager", lambda config: _FakeDatabase())
    monkeypatch.setattr(
        pipeline,
        "get_db_config",
        lambda: {
            "host": "x",
            "port": 1,
            "dbname": "x",
            "user": "x",
            "password": "x",
        },
    )
    monkeypatch.setattr(pipeline, "_require_tables", lambda db, tables: None)
    monkeypatch.setattr(
        pipeline,
        "resolve_lisbon_day_window_utc",
        lambda reference_time=None: types.SimpleNamespace(
            day_start_utc=day_start,
            day_end_utc=day_end,
            as_summary=lambda: {
                "day_start_utc": "2026-03-29T00:00:00Z",
                "day_end_utc": "2026-03-30T00:00:00Z",
            },
        ),
    )

    def fake_run_silver_etl(
        db,
        day_start_utc=None,
        day_end_utc=None,
        scoped_test_ids_by_family=None,
        refresh_reference_entities=True,
    ):
        captured["day_start_utc"] = day_start_utc
        captured["day_end_utc"] = day_end_utc
        captured["scoped_test_ids_by_family"] = scoped_test_ids_by_family
        captured["refresh_reference_entities"] = refresh_reference_entities
        return {"assessment_metrics": {"total_inserted": 1}}

    monkeypatch.setattr(pipeline, "run_silver_etl", fake_run_silver_etl)
    monkeypatch.setattr(
        pipeline,
        "_hold_vald_live_write_lock",
        _noop_live_write_lock,
    )

    summary = pipeline.run_intraday_bronze_to_silver_stage()

    assert captured == {
        "day_start_utc": day_start,
        "day_end_utc": day_end,
        "scoped_test_ids_by_family": None,
        "refresh_reference_entities": True,
    }
    assert summary["day_window"] == {
        "day_start_utc": "2026-03-29T00:00:00Z",
        "day_end_utc": "2026-03-30T00:00:00Z",
    }


def test_run_intraday_bronze_to_silver_stage_skips_without_lock_when_no_changes(monkeypatch) -> None:
    day_start = datetime(2026, 3, 29, 0, 0, tzinfo=timezone.utc)
    day_end = datetime(2026, 3, 30, 0, 0, tzinfo=timezone.utc)

    monkeypatch.setattr(
        pipeline,
        "resolve_lisbon_day_window_utc",
        lambda reference_time=None: types.SimpleNamespace(
            day_start_utc=day_start,
            day_end_utc=day_end,
            as_summary=lambda: {
                "day_start_utc": "2026-03-29T00:00:00Z",
                "day_end_utc": "2026-03-30T00:00:00Z",
            },
        ),
    )

    def fail_if_lock_used(*args, **kwargs):
        raise AssertionError("live write lock should not be used when intraday scope is empty")

    monkeypatch.setattr(pipeline, "_hold_vald_live_write_lock", fail_if_lock_used)

    summary = pipeline.run_intraday_bronze_to_silver_stage(
        incremental_scope={
            "by_family": {},
            "counts_by_family": {},
            "source_tables": {},
            "total_test_ids": 0,
            "has_impacted_tests": False,
        },
        refresh_reference_entities=False,
    )

    assert summary == {
        "assessment_metrics": {"total_inserted": 0},
        "day_window": {
            "day_start_utc": "2026-03-29T00:00:00Z",
            "day_end_utc": "2026-03-30T00:00:00Z",
        },
        "incremental_scope": {
            "by_family": {},
            "counts_by_family": {},
            "source_tables": {},
            "total_test_ids": 0,
            "has_impacted_tests": False,
        },
        "reference_entities_refreshed": False,
        "skipped": True,
        "skip_reason": pipeline._NO_INTRADAY_CHANGES_SKIP_REASON,
    }


def test_run_intraday_raw_to_bronze_stage_defers_heavy_tables(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_run_raw_to_bronze_stage(**kwargs):
        captured.update(kwargs)
        return {"processed_raw_rows": 3, "loaded_rows": 30}

    monkeypatch.setattr(pipeline, "run_raw_to_bronze_stage", fake_run_raw_to_bronze_stage)
    monkeypatch.setattr(
        pipeline,
        "DatabaseManager",
        lambda config: types.SimpleNamespace(close=lambda: None),
    )
    monkeypatch.setattr(
        pipeline,
        "get_db_config",
        lambda: {"host": "x", "port": 1, "dbname": "x", "user": "x", "password": "x"},
    )
    monkeypatch.setattr(
        pipeline,
        "_build_intraday_incremental_scope",
        lambda db, summary: {
            "by_family": {"forcedecks": ["test-1"]},
            "counts_by_family": {"forcedecks": 1},
            "source_tables": {},
            "total_test_ids": 1,
            "has_impacted_tests": True,
        },
    )

    summary = pipeline.run_intraday_raw_to_bronze_stage()

    assert captured == {
        "modules": "all",
        "include_reference": True,
        "exclude_source_tables": tuple(pipeline.INTRADAY_DEFERRED_RAW_TABLES),
    }
    assert summary["deferred_source_tables"] == pipeline.INTRADAY_DEFERRED_RAW_TABLES
    assert summary["incremental_scope"]["total_test_ids"] == 1


def test_run_intraday_deferred_raw_to_bronze_stage_replays_only_heavy_tables(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_run_raw_to_bronze_stage(**kwargs):
        captured.update(kwargs)
        return {"processed_raw_rows": 2, "loaded_rows": 200}

    monkeypatch.setattr(pipeline, "run_raw_to_bronze_stage", fake_run_raw_to_bronze_stage)

    summary = pipeline.run_intraday_deferred_raw_to_bronze_stage()

    assert captured["modules"] == "all"
    assert captured["include_reference"] is False
    assert captured["include_only_source_tables"] == tuple(pipeline.INTRADAY_DEFERRED_RAW_TABLES)
    assert isinstance(captured["deadline"], float)
    assert summary["deferred_mode"] is True
    assert summary["source_tables"] == pipeline.INTRADAY_DEFERRED_RAW_TABLES


def test_run_intraday_silver_to_gold_stage_uses_today_window(monkeypatch) -> None:
    captured: dict[str, object] = {}
    day_start = datetime(2026, 3, 29, 0, 0, tzinfo=timezone.utc)
    day_end = datetime(2026, 3, 30, 0, 0, tzinfo=timezone.utc)

    class _FakeDatabase:
        def close(self) -> None:
            return None

    monkeypatch.setattr(pipeline, "DatabaseManager", lambda config: _FakeDatabase())
    monkeypatch.setattr(
        pipeline,
        "get_db_config",
        lambda: {
            "host": "x",
            "port": 1,
            "dbname": "x",
            "user": "x",
            "password": "x",
        },
    )
    monkeypatch.setattr(pipeline, "_require_tables", lambda db, tables: None)
    monkeypatch.setattr(
        pipeline,
        "resolve_lisbon_day_window_utc",
        lambda reference_time=None: types.SimpleNamespace(
            day_start_utc=day_start,
            day_end_utc=day_end,
            as_summary=lambda: {
                "day_start_utc": "2026-03-29T00:00:00Z",
                "day_end_utc": "2026-03-30T00:00:00Z",
            },
        ),
    )

    def fake_run_gold_etl(db, day_start_utc=None, day_end_utc=None, scoped_test_ids_by_family=None):
        captured["day_start_utc"] = day_start_utc
        captured["day_end_utc"] = day_end_utc
        captured["scoped_test_ids_by_family"] = scoped_test_ids_by_family
        return {"total_excluded_outside_threshold_rows": 0}

    monkeypatch.setattr(pipeline, "run_gold_etl", fake_run_gold_etl)
    monkeypatch.setattr(
        pipeline,
        "_hold_vald_live_write_lock",
        _noop_live_write_lock,
    )

    summary = pipeline.run_intraday_silver_to_gold_stage()

    assert captured == {
        "day_start_utc": day_start,
        "day_end_utc": day_end,
        "scoped_test_ids_by_family": None,
    }
    assert summary["day_window"] == {
        "day_start_utc": "2026-03-29T00:00:00Z",
        "day_end_utc": "2026-03-30T00:00:00Z",
    }


def test_run_intraday_silver_to_gold_stage_skips_without_lock_when_no_changes(monkeypatch) -> None:
    day_start = datetime(2026, 3, 29, 0, 0, tzinfo=timezone.utc)
    day_end = datetime(2026, 3, 30, 0, 0, tzinfo=timezone.utc)

    monkeypatch.setattr(
        pipeline,
        "resolve_lisbon_day_window_utc",
        lambda reference_time=None: types.SimpleNamespace(
            day_start_utc=day_start,
            day_end_utc=day_end,
            as_summary=lambda: {
                "day_start_utc": "2026-03-29T00:00:00Z",
                "day_end_utc": "2026-03-30T00:00:00Z",
            },
        ),
    )

    def fail_if_lock_used(*args, **kwargs):
        raise AssertionError("live write lock should not be used when intraday scope is empty")

    monkeypatch.setattr(pipeline, "_hold_vald_live_write_lock", fail_if_lock_used)

    summary = pipeline.run_intraday_silver_to_gold_stage(
        incremental_scope={
            "by_family": {},
            "counts_by_family": {},
            "source_tables": {},
            "total_test_ids": 0,
            "has_impacted_tests": False,
        }
    )

    assert summary == {
        "day_window": {
            "day_start_utc": "2026-03-29T00:00:00Z",
            "day_end_utc": "2026-03-30T00:00:00Z",
        },
        "coverage": {
            "rows_written": 0,
            "covered_count": 0,
            "unmapped_count": 0,
            "unmapped_test_names": [],
        },
        "total_rows": 0,
        "total_source_rows": 0,
        "total_excluded_above_threshold_rows": 0,
        "total_excluded_below_threshold_rows": 0,
        "total_excluded_outside_threshold_rows": 0,
        "incremental_scope": {
            "by_family": {},
            "counts_by_family": {},
            "source_tables": {},
            "total_test_ids": 0,
            "has_impacted_tests": False,
        },
        "skipped": True,
        "skip_reason": pipeline._NO_INTRADAY_CHANGES_SKIP_REASON,
    }


def test_main_run_raw_to_bronze_supports_fast_and_deferred_modes(monkeypatch) -> None:
    captured_calls: list[dict[str, object]] = []

    monkeypatch.setattr(
        pipeline,
        "run_raw_to_bronze_stage",
        lambda **kwargs: captured_calls.append(kwargs) or {"processed_raw_rows": 0, "loaded_rows": 0},
    )
    monkeypatch.setattr(pipeline, "_log_stage_summary", lambda *_args, **_kwargs: None)

    exit_code = pipeline.main_run_raw_to_bronze(["--defer-heavy-tables"])
    assert exit_code == 0
    assert captured_calls[0] == {
        "modules": "all",
        "include_reference": True,
        "exclude_source_tables": tuple(pipeline.INTRADAY_DEFERRED_RAW_TABLES),
    }

    exit_code = pipeline.main_run_raw_to_bronze(["--heavy-tables-only"])
    assert exit_code == 0
    assert captured_calls[1] == {
        "modules": "all",
        "include_reference": False,
        "include_only_source_tables": tuple(pipeline.INTRADAY_DEFERRED_RAW_TABLES),
    }


def test_run_resume_pipeline_holds_live_write_lock_once_for_tail(monkeypatch) -> None:
    call_order: list[str] = []

    @contextmanager
    def fake_lock(*, owner: str, db_config=None, wait: bool = False):
        call_order.append(f"lock:{owner}")
        yield
        call_order.append(f"unlock:{owner}")

    monkeypatch.setattr(
        pipeline,
        "_hold_vald_live_write_lock",
        fake_lock,
    )
    monkeypatch.setattr(
        pipeline,
        "_run_bronze_to_silver_stage_unlocked",
        lambda: call_order.append("bronze_to_silver") or {"assessment_metrics": {"total_inserted": 1}},
    )
    monkeypatch.setattr(
        pipeline,
        "_run_silver_to_gold_stage_unlocked",
        lambda: call_order.append("silver_to_gold") or {"total_rows": 1},
    )

    summary = pipeline.run_resume_pipeline("bronze_to_silver")

    assert call_order == [
        "lock:resume_pipeline:bronze_to_silver",
        "bronze_to_silver",
        "silver_to_gold",
        "unlock:resume_pipeline:bronze_to_silver",
    ]
    assert summary["silver"]["assessment_metrics"]["total_inserted"] == 1
    assert summary["gold"]["total_rows"] == 1


def test_run_intraday_bronze_to_silver_stage_skips_when_lock_is_busy(monkeypatch) -> None:
    day_start = datetime(2026, 3, 29, 0, 0, tzinfo=timezone.utc)
    day_end = datetime(2026, 3, 30, 0, 0, tzinfo=timezone.utc)

    @contextmanager
    def busy_lock(*, owner: str, db_config=None, wait: bool = False):
        raise pipeline.ValdPipelineBusyError("lock busy")
        yield

    monkeypatch.setattr(
        pipeline,
        "_hold_vald_live_write_lock",
        busy_lock,
    )
    monkeypatch.setattr(
        pipeline,
        "resolve_lisbon_day_window_utc",
        lambda reference_time=None: types.SimpleNamespace(
            day_start_utc=day_start,
            day_end_utc=day_end,
            as_summary=lambda: {
                "day_start_utc": "2026-03-29T00:00:00Z",
                "day_end_utc": "2026-03-30T00:00:00Z",
            },
        ),
    )

    summary = pipeline.run_intraday_bronze_to_silver_stage()

    assert summary == {
        "assessment_metrics": {"total_inserted": 0},
        "day_window": {
            "day_start_utc": "2026-03-29T00:00:00Z",
            "day_end_utc": "2026-03-30T00:00:00Z",
        },
        "skipped": True,
        "skip_reason": "lock busy",
    }


def test_run_end_to_end_holds_live_write_lock_for_silver_and_gold(monkeypatch) -> None:
    call_order: list[str] = []

    @contextmanager
    def fake_lock(*, owner: str, db_config=None, wait: bool = False):
        call_order.append(f"lock:{owner}")
        yield
        call_order.append(f"unlock:{owner}")

    monkeypatch.setattr(
        pipeline,
        "run_extract_raw",
        lambda **kwargs: call_order.append("raw") or {"errors": [], "modules": {}},
    )
    monkeypatch.setattr(
        pipeline,
        "run_raw_to_bronze_stage",
        lambda **kwargs: call_order.append("raw_to_bronze") or {"processed_raw_rows": 1, "loaded_rows": 1},
    )
    monkeypatch.setattr(
        pipeline,
        "_run_bronze_to_silver_stage_unlocked",
        lambda: call_order.append("bronze_to_silver") or {"assessment_metrics": {"total_inserted": 1}},
    )
    monkeypatch.setattr(
        pipeline,
        "_run_silver_to_gold_stage_unlocked",
        lambda: call_order.append("silver_to_gold") or {"total_rows": 1},
    )
    monkeypatch.setattr(
        pipeline,
        "_hold_vald_live_write_lock",
        fake_lock,
    )

    summary = pipeline.run_end_to_end()

    assert call_order == [
        "raw",
        "raw_to_bronze",
        "lock:end_to_end",
        "bronze_to_silver",
        "silver_to_gold",
        "unlock:end_to_end",
    ]
    assert summary["silver"]["assessment_metrics"]["total_inserted"] == 1
    assert summary["gold"]["total_rows"] == 1
