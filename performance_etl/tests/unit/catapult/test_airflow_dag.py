from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path


def _resolve(value):
    return value.value if isinstance(value, _FakeTaskResult) else value


class _FakeTaskResult:
    def __init__(self, value):
        self.value = value

    def __rshift__(self, other):
        return other


class _FakeTaskDecorator:
    def __call__(self, func=None, **_kwargs):
        if func is None:
            def decorator(inner_func):
                return self(inner_func)

            return decorator

        def wrapper(*args, **kwargs):
            resolved_args = [_resolve(arg) for arg in args]
            resolved_kwargs = {
                key: _resolve(value)
                for key, value in kwargs.items()
            }
            return _FakeTaskResult(func(*resolved_args, **resolved_kwargs))

        return wrapper

    def short_circuit(self, func):
        return self(func)


class _FakeTaskGroupDecorator:
    def __call__(self, *args, **kwargs):
        def decorator(func):
            def wrapper(*wrapper_args, **wrapper_kwargs):
                return func(*wrapper_args, **wrapper_kwargs)

            return wrapper

        return decorator


class _FakeDagRunColumn:
    def __init__(self, name: str) -> None:
        self.name = name

    def __eq__(self, other):
        return (self.name, "eq", other)

    def in_(self, values):
        return (self.name, "in", tuple(values))


class _FakeSession:
    def query(self, *_args, **_kwargs):
        return self

    def filter(self, *_args, **_kwargs):
        return self

    def limit(self, *_args, **_kwargs):
        return self

    def first(self):
        return None


class _FakeSessionContext:
    def __enter__(self):
        return _FakeSession()

    def __exit__(self, exc_type, exc, tb):
        return None


def test_catapult_airflow_dag_routes_full_incremental_and_historical_work(monkeypatch) -> None:
    call_log: list[object] = []

    fake_bootstrap = types.ModuleType("ingestion.bootstrap")
    fake_bootstrap.bootstrap_database = lambda: call_log.append("bootstrap_database") or {}

    def _merge_batch_maps(*maps):
        merged: dict[str, set[str]] = {}
        for batch_map in maps:
            if not batch_map:
                continue
            for source_table, batch_ids in batch_map.items():
                merged.setdefault(source_table, set()).update(batch_ids)
        return {source_table: sorted(batch_ids) for source_table, batch_ids in sorted(merged.items())}

    def _batch_map_for_endpoints(endpoints: set[str]) -> dict[str, list[str]]:
        mapping = {
            "teams": "raw.catapult_teams",
            "athletes": "raw.catapult_athletes",
            "positions": "raw.catapult_positions",
            "parameters": "raw.catapult_parameters",
            "venues": "raw.catapult_venues",
            "tag_types": "raw.catapult_tag_types",
            "tags": "raw.catapult_tags",
            "activities": "raw.catapult_activities",
            "periods": "raw.catapult_periods",
            "annotations": "raw.catapult_annotations",
            "stats": "raw.catapult_stats",
            "efforts": "raw.catapult_efforts",
            "events": "raw.catapult_events",
            "sensor_data": "raw.catapult_sensor_data",
        }
        return {
            source_table: [f"batch-{endpoint_name}"]
            for endpoint_name, source_table in mapping.items()
            if endpoint_name in endpoints
        }

    def _fake_run_extract_raw(**kwargs):
        call_log.append(("run_extract_raw", kwargs))
        return {
            "has_new_data": True,
            "total_loaded": 1,
            "batch_ids_by_source_table": _batch_map_for_endpoints(set(kwargs["endpoints"])),
        }

    fake_pipeline = types.ModuleType("ingestion.catapult.pipeline")
    fake_pipeline.run_extract_raw = _fake_run_extract_raw
    fake_pipeline.run_full_refresh_raw_to_bronze_stage = (
        lambda **kwargs: call_log.append(("run_full_refresh_raw_to_bronze_stage", kwargs))
        or {"loaded_rows": 1}
    )
    fake_pipeline.run_intraday_raw_to_bronze_stage = (
        lambda **kwargs: call_log.append(("run_intraday_raw_to_bronze_stage", kwargs))
        or {"loaded_rows": 1}
    )
    fake_pipeline.run_historical_day_raw_to_bronze = (
        lambda replay_date: call_log.append(("run_historical_day_raw_to_bronze", replay_date))
        or {"loaded_rows": 1}
    )
    fake_replay_scope = types.ModuleType("ingestion.catapult.replay_scope")
    fake_replay_scope.build_batch_ids_by_source_table = (
        lambda summary: dict(summary.get("batch_ids_by_source_table", {}))
    )
    fake_replay_scope.merge_batch_ids_by_source_table = _merge_batch_maps

    fake_pendulum = types.ModuleType("pendulum")
    fake_pendulum.timezone = lambda name: name
    fake_pendulum.datetime = lambda *args, **kwargs: (args, kwargs)
    fake_pendulum.duration = lambda **kwargs: kwargs

    fake_airflow = types.ModuleType("airflow")
    fake_airflow_decorators = types.ModuleType("airflow.decorators")
    fake_airflow_decorators.dag = lambda **_kwargs: (lambda func: func)
    fake_airflow_decorators.task = _FakeTaskDecorator()
    fake_airflow_decorators.task_group = _FakeTaskGroupDecorator()

    fake_airflow_models = types.ModuleType("airflow.models")
    fake_airflow_models_dagrun = types.ModuleType("airflow.models.dagrun")
    fake_airflow_models_param = types.ModuleType("airflow.models.param")
    fake_airflow_models_dagrun.DagRun = types.SimpleNamespace(
        id=_FakeDagRunColumn("id"),
        dag_id=_FakeDagRunColumn("dag_id"),
        state=_FakeDagRunColumn("state"),
    )
    fake_airflow_models_param.Param = lambda **kwargs: kwargs

    fake_airflow_utils = types.ModuleType("airflow.utils")
    fake_airflow_utils_session = types.ModuleType("airflow.utils.session")
    fake_airflow_utils_session.create_session = lambda: _FakeSessionContext()
    fake_airflow_utils_state = types.ModuleType("airflow.utils.state")
    fake_airflow_utils_state.DagRunState = types.SimpleNamespace(
        QUEUED="queued",
        RUNNING="running",
    )
    fake_airflow_operators = types.ModuleType("airflow.operators")
    fake_airflow_operators_python = types.ModuleType("airflow.operators.python")
    fake_airflow_operators_python.get_current_context = lambda: {
        "dag_run": types.SimpleNamespace(run_type="scheduled"),
        "params": {"replay_date": "2026-04-09"},
    }

    monkeypatch.setitem(sys.modules, "ingestion.bootstrap", fake_bootstrap)
    monkeypatch.setitem(sys.modules, "ingestion.catapult.pipeline", fake_pipeline)
    monkeypatch.setitem(sys.modules, "ingestion.catapult.replay_scope", fake_replay_scope)
    monkeypatch.setitem(sys.modules, "pendulum", fake_pendulum)
    monkeypatch.setitem(sys.modules, "airflow", fake_airflow)
    monkeypatch.setitem(sys.modules, "airflow.decorators", fake_airflow_decorators)
    monkeypatch.setitem(sys.modules, "airflow.models", fake_airflow_models)
    monkeypatch.setitem(sys.modules, "airflow.models.dagrun", fake_airflow_models_dagrun)
    monkeypatch.setitem(sys.modules, "airflow.models.param", fake_airflow_models_param)
    monkeypatch.setitem(sys.modules, "airflow.operators", fake_airflow_operators)
    monkeypatch.setitem(sys.modules, "airflow.operators.python", fake_airflow_operators_python)
    monkeypatch.setitem(sys.modules, "airflow.utils", fake_airflow_utils)
    monkeypatch.setitem(sys.modules, "airflow.utils.session", fake_airflow_utils_session)
    monkeypatch.setitem(sys.modules, "airflow.utils.state", fake_airflow_utils_state)

    dag_path = (
        Path(__file__).resolve().parents[3]
        / "airflow"
        / "dags"
        / "catapult_pipeline.py"
    )
    spec = importlib.util.spec_from_file_location("test_catapult_pipeline_dag_module", dag_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert "bootstrap_database" in call_log
    assert (
        "run_extract_raw",
        {
            "accounts": "A",
            "full_refresh": False,
            "include_reference": True,
            "include_sensor_data": False,
            "endpoints": {"teams"},
        },
    ) in call_log
    assert (
        "run_extract_raw",
        {
            "accounts": "A",
            "full_refresh": False,
            "include_reference": True,
            "include_sensor_data": False,
            "endpoints": {"athletes"},
        },
    ) in call_log
    assert (
        "run_extract_raw",
        {
            "accounts": "B",
            "full_refresh": False,
            "include_reference": True,
            "include_sensor_data": False,
            "endpoints": {"teams"},
        },
    ) in call_log
    assert (
        "run_extract_raw",
        {
            "accounts": "A",
            "full_refresh": True,
            "include_reference": False,
            "include_sensor_data": True,
            "endpoints": {
                "activities",
                "periods",
                "annotations",
                "stats",
                "efforts",
                "events",
                "sensor_data",
            },
        },
    ) in call_log
    assert ("run_full_refresh_raw_to_bronze_stage", {}) in call_log
    assert (
        "run_extract_raw",
        {
            "accounts": "A",
            "full_refresh": False,
            "include_reference": False,
            "include_sensor_data": True,
            "endpoints": {
                "activities",
                "periods",
                "annotations",
                "stats",
                "efforts",
                "events",
                "sensor_data",
            },
        },
    ) in call_log
    assert (
        "run_intraday_raw_to_bronze_stage",
        {
            "batch_ids_by_source_table": {
                "raw.catapult_activities": ["batch-activities"],
                "raw.catapult_annotations": ["batch-annotations"],
                "raw.catapult_athletes": ["batch-athletes"],
                "raw.catapult_efforts": ["batch-efforts"],
                "raw.catapult_events": ["batch-events"],
                "raw.catapult_parameters": ["batch-parameters"],
                "raw.catapult_periods": ["batch-periods"],
                "raw.catapult_positions": ["batch-positions"],
                "raw.catapult_sensor_data": ["batch-sensor_data"],
                "raw.catapult_stats": ["batch-stats"],
                "raw.catapult_tag_types": ["batch-tag_types"],
                "raw.catapult_tags": ["batch-tags"],
                "raw.catapult_teams": ["batch-teams"],
                "raw.catapult_venues": ["batch-venues"],
            }
        },
    ) in call_log
    assert ("run_historical_day_raw_to_bronze", "2026-04-09") in call_log
