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


def test_airflow_dag_routes_intraday_and_midnight_work_to_named_stages(monkeypatch) -> None:
    call_log: list[object] = []

    def fake_extract_raw(**kwargs):
        call_log.append(("run_extract_raw", kwargs))
        module_name = kwargs.get("modules") or "reference"
        module_key = "reference" if module_name == "" else str(module_name)
        summary: dict[str, object] = {
            "tenant_ids": ["tenant-1"],
            "total_extracted": 1,
            "total_loaded": 1,
            "has_new_data": True,
            "errors": [],
        }
        if kwargs.get("include_reference"):
            summary["reference"] = {"profiles_seen": 1, "snapshots_written": 1}
        if module_name:
            summary["modules"] = {
                module_key: {"total_extracted": 1, "total_loaded": 1, "errors": []}
            }
        else:
            summary["modules"] = {}
        return summary

    def fake_intraday_replay(**kwargs):
        call_log.append(("run_intraday_raw_to_bronze_stage", kwargs))
        module_name = kwargs.get("modules") or "reference"
        return {
            "processed_raw_rows": 1,
            "loaded_rows": 1,
            "tables": {},
            "errors": [],
            "incremental_scope": {
                "by_family": {str(module_name): [f"{module_name}-test-1"]} if module_name else {},
                "counts_by_family": {str(module_name): 1} if module_name else {},
                "source_tables": {},
                "total_test_ids": 1 if module_name else 0,
                "has_impacted_tests": bool(module_name),
            },
        }

    fake_pipeline = types.ModuleType("ingestion.vald.pipeline")
    fake_pipeline.bootstrap_database = lambda: call_log.append("bootstrap_database") or {}
    fake_pipeline.prepare_full_refresh_bronze_stage_tables = (
        lambda: call_log.append("prepare_full_refresh_bronze_stage_tables") or {}
    )
    fake_pipeline.run_extract_raw = fake_extract_raw
    fake_pipeline.run_full_refresh_raw_to_bronze_stage = (
        lambda **kwargs: call_log.append(("run_full_refresh_raw_to_bronze_stage", kwargs)) or {}
    )
    fake_pipeline.run_full_refresh_bronze_to_silver_stage = (
        lambda: call_log.append("run_full_refresh_bronze_to_silver_stage") or {}
    )
    fake_pipeline.run_full_refresh_silver_to_gold_stage = (
        lambda runtime_validation=True: call_log.append(
            ("run_full_refresh_silver_to_gold_stage", {"runtime_validation": runtime_validation})
        )
        or {}
    )
    fake_pipeline.run_intraday_raw_to_bronze_stage = fake_intraday_replay
    fake_pipeline.run_intraday_deferred_raw_to_bronze_stage = (
        lambda **kwargs: call_log.append(("run_intraday_deferred_raw_to_bronze_stage", kwargs))
        or {}
    )
    fake_pipeline.run_intraday_bronze_to_silver_stage = (
        lambda incremental_scope=None, refresh_reference_entities=True: call_log.append(
            (
                "run_intraday_bronze_to_silver_stage",
                {
                    "incremental_scope": incremental_scope,
                    "refresh_reference_entities": refresh_reference_entities,
                },
            )
        ) or {"incremental_scope": incremental_scope}
    )
    fake_pipeline.run_intraday_silver_to_gold_stage = (
        lambda incremental_scope=None: call_log.append(
            ("run_intraday_silver_to_gold_stage", {"incremental_scope": incremental_scope})
        ) or {}
    )
    fake_pipeline.run_historical_day_raw_to_bronze = lambda replay_date: {}
    fake_pipeline.run_historical_day_bronze_to_silver_stage = lambda incremental_scope=None: {}
    fake_pipeline.run_historical_day_silver_to_gold_stage = lambda incremental_scope=None: {}
    # Phase 1 (2026-05-09): VALD IQR audit entry point.
    fake_pipeline.run_vald_quality_audit = (
        lambda family=None, incremental=True, limit=None: call_log.append(
            ("run_vald_quality_audit", {"family": family, "incremental": incremental})
        ) or {"families": {}, "total_flags": 0}
    )

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

    monkeypatch.setitem(sys.modules, "ingestion.vald.pipeline", fake_pipeline)
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
        / "vald_pipeline.py"
    )
    spec = importlib.util.spec_from_file_location("test_vald_pipeline_dag_module", dag_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert "bootstrap_database" in call_log
    assert "prepare_full_refresh_bronze_stage_tables" in call_log
    assert "run_full_refresh_bronze_to_silver_stage" in call_log
    assert (
        "run_full_refresh_silver_to_gold_stage",
        {"runtime_validation": True},
    ) in call_log
    assert (
        "run_full_refresh_raw_to_bronze_stage",
        {"modules": "", "include_reference": True},
    ) in call_log
    assert (
        "run_full_refresh_raw_to_bronze_stage",
        {"modules": "dynamo", "include_reference": False},
    ) in call_log
    assert (
        "run_extract_raw",
        {
            "modules": "",
            "full_refresh": False,
            "include_reference": True,
            "intraday_current_day_only": True,
        },
    ) in call_log
    assert (
        "run_extract_raw",
        {
            "modules": "forcedecks",
            "full_refresh": False,
            "include_reference": False,
            "intraday_current_day_only": True,
        },
    ) in call_log
    assert (
        "run_extract_raw",
        {"modules": "nordbord", "full_refresh": True, "include_reference": False},
    ) in call_log
    assert (
        "run_intraday_raw_to_bronze_stage",
        {"modules": "", "include_reference": True},
    ) in call_log
    assert (
        "run_intraday_raw_to_bronze_stage",
        {"modules": "forceframe", "include_reference": False},
    ) in call_log
    assert (
        "run_intraday_deferred_raw_to_bronze_stage",
        {"modules": "forceframe"},
    ) in call_log

    silver_calls = [
        entry
        for entry in call_log
        if isinstance(entry, tuple) and entry[0] == "run_intraday_bronze_to_silver_stage"
    ]
    assert len(silver_calls) == 1
    silver_scope = silver_calls[0][1]["incremental_scope"]
    assert silver_scope is not None
    assert silver_scope["has_impacted_tests"] is True
    assert silver_calls[0][1]["refresh_reference_entities"] is True

    gold_calls = [
        entry
        for entry in call_log
        if isinstance(entry, tuple) and entry[0] == "run_intraday_silver_to_gold_stage"
    ]
    assert len(gold_calls) == 1
    assert gold_calls[0][1]["incremental_scope"] == silver_scope


def test_airflow_dag_allows_manual_intraday_runs_even_when_full_refresh_is_active(monkeypatch) -> None:
    call_log: list[object] = []

    def fake_extract_raw(**kwargs):
        call_log.append(("run_extract_raw", kwargs))
        module_name = kwargs.get("modules") or "reference"
        module_key = "reference" if module_name == "" else str(module_name)
        summary: dict[str, object] = {
            "tenant_ids": ["tenant-1"],
            "total_extracted": 1,
            "total_loaded": 1,
            "has_new_data": True,
            "errors": [],
            "modules": {},
        }
        if kwargs.get("include_reference"):
            summary["reference"] = {"profiles_seen": 1, "snapshots_written": 1}
        if module_name:
            summary["modules"] = {
                module_key: {"total_extracted": 1, "total_loaded": 1, "errors": []}
            }
        return summary

    def fake_intraday_replay(**kwargs):
        call_log.append(("run_intraday_raw_to_bronze_stage", kwargs))
        module_name = kwargs.get("modules") or "reference"
        return {
            "processed_raw_rows": 1,
            "loaded_rows": 1,
            "tables": {},
            "errors": [],
            "incremental_scope": {
                "by_family": {str(module_name): [f"{module_name}-test-1"]} if module_name else {},
                "counts_by_family": {str(module_name): 1} if module_name else {},
                "source_tables": {},
                "total_test_ids": 1 if module_name else 0,
                "has_impacted_tests": bool(module_name),
            },
        }

    class _ActiveRunSession:
        def query(self, *_args, **_kwargs):
            return self

        def filter(self, *_args, **_kwargs):
            return self

        def limit(self, *_args, **_kwargs):
            return self

        def first(self):
            return object()

    class _ActiveRunSessionContext:
        def __enter__(self):
            return _ActiveRunSession()

        def __exit__(self, exc_type, exc, tb):
            return None

    fake_pipeline = types.ModuleType("ingestion.vald.pipeline")
    fake_pipeline.bootstrap_database = lambda: {}
    fake_pipeline.prepare_full_refresh_bronze_stage_tables = lambda: {}
    fake_pipeline.run_extract_raw = fake_extract_raw
    fake_pipeline.run_full_refresh_raw_to_bronze_stage = lambda **kwargs: {}
    fake_pipeline.run_full_refresh_bronze_to_silver_stage = lambda: {}
    fake_pipeline.run_full_refresh_silver_to_gold_stage = lambda runtime_validation=True: {}
    fake_pipeline.run_intraday_raw_to_bronze_stage = fake_intraday_replay
    fake_pipeline.run_intraday_deferred_raw_to_bronze_stage = lambda **kwargs: {}
    fake_pipeline.run_intraday_bronze_to_silver_stage = (
        lambda incremental_scope=None, refresh_reference_entities=True: {
            "incremental_scope": incremental_scope,
            "refresh_reference_entities": refresh_reference_entities,
        }
    )
    fake_pipeline.run_intraday_silver_to_gold_stage = lambda incremental_scope=None: {}
    fake_pipeline.run_vald_quality_audit = (  # Phase 1
        lambda family=None, incremental=True, limit=None: {"families": {}, "total_flags": 0}
    )
    fake_pipeline.run_historical_day_raw_to_bronze = lambda replay_date: {}
    fake_pipeline.run_historical_day_bronze_to_silver_stage = lambda incremental_scope=None: {}
    fake_pipeline.run_historical_day_silver_to_gold_stage = lambda incremental_scope=None: {}

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

    fake_airflow_operators = types.ModuleType("airflow.operators")
    fake_airflow_operators_python = types.ModuleType("airflow.operators.python")
    fake_airflow_operators_python.get_current_context = lambda: {
        "dag_run": types.SimpleNamespace(run_type="manual"),
        "params": {"replay_date": "2026-04-09"},
    }

    fake_airflow_utils = types.ModuleType("airflow.utils")
    fake_airflow_utils_session = types.ModuleType("airflow.utils.session")
    fake_airflow_utils_session.create_session = lambda: _ActiveRunSessionContext()
    fake_airflow_utils_state = types.ModuleType("airflow.utils.state")
    fake_airflow_utils_state.DagRunState = types.SimpleNamespace(
        QUEUED="queued",
        RUNNING="running",
    )

    monkeypatch.setitem(sys.modules, "ingestion.vald.pipeline", fake_pipeline)
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
        / "vald_pipeline.py"
    )
    spec = importlib.util.spec_from_file_location("test_vald_pipeline_dag_manual_module", dag_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert (
        "run_extract_raw",
        {
            "modules": "forcedecks",
            "full_refresh": False,
            "include_reference": False,
            "intraday_current_day_only": True,
        },
    ) in call_log
    assert (
        "run_intraday_raw_to_bronze_stage",
        {"modules": "forcedecks", "include_reference": False},
    ) in call_log
