from __future__ import annotations

from contextlib import contextmanager
from datetime import timezone

from ingestion.catapult import pipeline
from ingestion.catapult.client import CatapultAccountConfig, CatapultRuntimeConfig


class _FakeDatabaseManager:
    def __init__(self, config):
        self.config = config

    def close(self) -> None:
        return None


class _FakeBatchManager:
    def __init__(self, db):
        self.db = db


class _FakeWatermarkManager:
    def __init__(self, db):
        self.db = db


class _FakeClient:
    def __init__(self, runtime_config, account):
        self.runtime_config = runtime_config
        self.account = account

    def close(self) -> None:
        return None


@contextmanager
def _noop_bronze_replay_lock(*, owner: str, db_config=None, wait: bool = False):
    yield


def test_run_extract_raw_aggregates_account_summaries(monkeypatch) -> None:
    runtime_config = CatapultRuntimeConfig(
        provider="catapult",
        api_version="v6",
        base_url="https://connect-eu.catapultsports.com/api/v6",
        default_page_size=100,
        rate_limit_ms=200,
        max_retries=3,
        accounts=(
            CatapultAccountConfig(
                name="CATAPULT_A",
                api_key_env="CATAPULT_A_API_KEY",
                api_key="token-a",
                team_code="A",
                team_level="senior",
            ),
        ),
    )
    monkeypatch.setattr(pipeline, "build_catapult_runtime_config", lambda: runtime_config)
    monkeypatch.setattr(pipeline, "DatabaseManager", _FakeDatabaseManager)
    monkeypatch.setattr(pipeline, "BatchManager", _FakeBatchManager)
    monkeypatch.setattr(pipeline, "WatermarkManager", _FakeWatermarkManager)
    monkeypatch.setattr(pipeline, "CatapultClient", _FakeClient)
    monkeypatch.setattr(pipeline, "get_db_config", lambda: {"host": "localhost"})
    monkeypatch.setattr(
        pipeline,
        "_extract_reference_endpoints",
        lambda **kwargs: {"records_extracted": 10, "raw_rows_written": 2},
    )
    monkeypatch.setattr(
        pipeline,
        "_extract_activities",
        lambda **kwargs: {
            "summary": {"records_extracted": 5, "raw_rows_written": 1},
            "activities": [{"id": 123}],
        },
    )
    monkeypatch.setattr(
        pipeline,
        "_extract_activity_children",
        lambda **kwargs: {"records_extracted": 4, "raw_rows_written": 1},
    )
    monkeypatch.setattr(
        pipeline,
        "_enumerate_activity_devices",
        lambda **kwargs: {
            "summary": {"records_extracted": 1, "raw_rows_written": 0},
            "pairs": [("athlete-1", "activity-1")],
        },
    )
    monkeypatch.setattr(
        pipeline,
        "_extract_stats",
        lambda **kwargs: {
            "summary": {"records_extracted": 6, "raw_rows_written": 1},
            "athlete_activity_pairs": [(7, 123)],
        },
    )
    monkeypatch.setattr(
        pipeline,
        "_extract_athlete_activity_details",
        lambda **kwargs: {"records_extracted": 3, "raw_rows_written": 1},
    )

    summary = pipeline.run_extract_raw(include_sensor_data=True)

    assert summary["total_extracted"] == 38
    assert summary["total_loaded"] == 9
    assert summary["has_new_data"] is True
    assert summary["accounts"]["CATAPULT_A"]["errors"] == []


def test_select_accounts_accepts_team_code_alias() -> None:
    runtime_config = CatapultRuntimeConfig(
        provider="catapult",
        api_version="v6",
        base_url="https://connect-eu.catapultsports.com/api/v6",
        default_page_size=100,
        rate_limit_ms=200,
        max_retries=3,
        accounts=(
            CatapultAccountConfig(
                name="CATAPULT_A",
                api_key_env="CATAPULT_A_API_KEY",
                api_key="token-a",
                team_code="A",
                team_level="senior",
            ),
        ),
    )

    selected = pipeline._select_accounts("A", runtime_config)

    assert [account.name for account in selected] == ["CATAPULT_A"]


def test_parse_watermark_accepts_unix_second_strings() -> None:
    parsed = pipeline._parse_watermark("1734001023")

    assert parsed.tzinfo is not None
    assert parsed.year == 2024


def test_normalize_stats_dimension_identifier_treats_zero_as_missing() -> None:
    row = {"activity_id_id": 0, "athlete_id_id": "0", "period_id_id": "period-1"}

    assert pipeline._normalize_stats_dimension_identifier(row, "activity_id") is None
    assert pipeline._normalize_stats_dimension_identifier(row, "athlete_id") is None
    assert pipeline._normalize_stats_dimension_identifier(row, "period_id") == "period-1"


def test_run_extract_raw_can_use_activity_athlete_enumeration_for_detail_endpoints(monkeypatch) -> None:
    runtime_config = CatapultRuntimeConfig(
        provider="catapult",
        api_version="v6",
        base_url="https://connect-eu.catapultsports.com/api/v6",
        default_page_size=100,
        rate_limit_ms=200,
        max_retries=3,
        accounts=(
            CatapultAccountConfig(
                name="CATAPULT_U15",
                api_key_env="CATAPULT_U15_API_KEY",
                api_key="token-u15",
                team_code="U15",
                team_level="academy",
            ),
        ),
    )
    detail_calls: list[tuple[str, list[tuple[str, str]]]] = []

    monkeypatch.setattr(pipeline, "build_catapult_runtime_config", lambda: runtime_config)
    monkeypatch.setattr(pipeline, "DatabaseManager", _FakeDatabaseManager)
    monkeypatch.setattr(pipeline, "BatchManager", _FakeBatchManager)
    monkeypatch.setattr(pipeline, "WatermarkManager", _FakeWatermarkManager)
    monkeypatch.setattr(pipeline, "CatapultClient", _FakeClient)
    monkeypatch.setattr(pipeline, "get_db_config", lambda: {"host": "localhost"})
    monkeypatch.setattr(
        pipeline,
        "_extract_reference_endpoints",
        lambda **kwargs: {"records_extracted": 0, "raw_rows_written": 0},
    )
    monkeypatch.setattr(
        pipeline,
        "_extract_activities",
        lambda **kwargs: {
            "summary": {"records_extracted": 2, "raw_rows_written": 1},
            "activities": [{"id": "activity-1"}],
        },
    )
    monkeypatch.setattr(
        pipeline,
        "_enumerate_activity_athletes",
        lambda **kwargs: {
            "summary": {"records_extracted": 1, "raw_rows_written": 0, "batch_id": "batch-enum"},
            "pairs": [("athlete-1", "activity-1")],
        },
    )
    monkeypatch.setattr(
        pipeline,
        "_enumerate_activity_devices",
        lambda **kwargs: {
            "summary": {"records_extracted": 1, "raw_rows_written": 0, "batch_id": "batch-devices"},
            "pairs": [("athlete-device-1", "activity-1")],
        },
    )
    monkeypatch.setattr(
        pipeline,
        "_extract_activity_children",
        lambda **kwargs: {"records_extracted": 0, "raw_rows_written": 0},
    )
    monkeypatch.setattr(
        pipeline,
        "_extract_stats",
        lambda **kwargs: {
            "summary": {"records_extracted": 0, "raw_rows_written": 0},
            "athlete_activity_pairs": [],
        },
    )

    def _fake_detail(**kwargs):
        detail_calls.append((kwargs["detail_name"], kwargs["athlete_activity_pairs"]))
        return {"records_extracted": 1, "raw_rows_written": 1}

    monkeypatch.setattr(pipeline, "_extract_athlete_activity_details", _fake_detail)

    summary = pipeline.run_extract_raw(include_sensor_data=True, pair_source="activity_athletes")

    assert detail_calls == [
        ("efforts", [("athlete-device-1", "activity-1")]),
        ("events", [("athlete-device-1", "activity-1")]),
        ("sensor_data", [("athlete-device-1", "activity-1")]),
    ]
    assert summary["accounts"]["CATAPULT_U15"]["activity_athlete_enumeration"]["pairs"] == [
        ("athlete-1", "activity-1")
    ]
    assert summary["accounts"]["CATAPULT_U15"]["activity_devices"]["pairs"] == [
        ("athlete-device-1", "activity-1")
    ]


def test_run_extract_raw_uses_activity_devices_when_stats_have_no_pairs(monkeypatch) -> None:
    runtime_config = CatapultRuntimeConfig(
        provider="catapult",
        api_version="v6",
        base_url="https://connect-eu.catapultsports.com/api/v6",
        default_page_size=100,
        rate_limit_ms=200,
        max_retries=3,
        accounts=(
            CatapultAccountConfig(
                name="CATAPULT_U15",
                api_key_env="CATAPULT_U15_API_KEY",
                api_key="token-u15",
                team_code="U15",
                team_level="academy",
            ),
        ),
    )
    detail_calls: list[tuple[str, list[tuple[str, str]]]] = []

    monkeypatch.setattr(pipeline, "build_catapult_runtime_config", lambda: runtime_config)
    monkeypatch.setattr(pipeline, "DatabaseManager", _FakeDatabaseManager)
    monkeypatch.setattr(pipeline, "BatchManager", _FakeBatchManager)
    monkeypatch.setattr(pipeline, "WatermarkManager", _FakeWatermarkManager)
    monkeypatch.setattr(pipeline, "CatapultClient", _FakeClient)
    monkeypatch.setattr(pipeline, "get_db_config", lambda: {"host": "localhost"})
    monkeypatch.setattr(
        pipeline,
        "_extract_reference_endpoints",
        lambda **kwargs: {"records_extracted": 0, "raw_rows_written": 0},
    )
    monkeypatch.setattr(
        pipeline,
        "_extract_activities",
        lambda **kwargs: {
            "summary": {"records_extracted": 2, "raw_rows_written": 1},
            "activities": [{"id": "activity-1"}],
        },
    )
    monkeypatch.setattr(
        pipeline,
        "_enumerate_activity_athletes",
        lambda **kwargs: {
            "summary": {"records_extracted": 1, "raw_rows_written": 0, "batch_id": "batch-enum"},
            "pairs": [("athlete-1", "activity-1")],
        },
    )
    monkeypatch.setattr(
        pipeline,
        "_enumerate_activity_devices",
        lambda **kwargs: {
            "summary": {"records_extracted": 1, "raw_rows_written": 0, "batch_id": "batch-devices"},
            "pairs": [("athlete-device-1", "activity-1")],
        },
    )
    monkeypatch.setattr(
        pipeline,
        "_extract_activity_children",
        lambda **kwargs: {"records_extracted": 0, "raw_rows_written": 0},
    )
    monkeypatch.setattr(
        pipeline,
        "_extract_stats",
        lambda **kwargs: {
            "summary": {"records_extracted": 1, "raw_rows_written": 1},
            "athlete_activity_pairs": [],
        },
    )

    def _fake_detail(**kwargs):
        detail_calls.append((kwargs["detail_name"], kwargs["athlete_activity_pairs"]))
        return {"records_extracted": 1, "raw_rows_written": 1}

    monkeypatch.setattr(pipeline, "_extract_athlete_activity_details", _fake_detail)

    summary = pipeline.run_extract_raw(
        include_sensor_data=True,
        include_activity_athlete_enumeration=True,
        pair_source="stats",
    )

    assert detail_calls == [
        ("efforts", [("athlete-device-1", "activity-1")]),
        ("events", [("athlete-device-1", "activity-1")]),
        ("sensor_data", [("athlete-device-1", "activity-1")]),
    ]
    assert summary["accounts"]["CATAPULT_U15"]["stats"]["detail_pair_source"] == "activity_devices"


def test_run_raw_to_bronze_stage_holds_bronze_replay_lock(monkeypatch) -> None:
    call_log: list[tuple[str, object]] = []

    class _LockedDatabaseManager:
        def __init__(self, config):
            call_log.append(("db_init", config))

        def close(self) -> None:
            call_log.append(("db_close", None))

    def fake_replay_raw_to_bronze(
        db,
        *,
        batch_ids_by_source_table=None,
        endpoints=None,
        full_replay=False,
        ingested_at_start=None,
        ingested_at_end=None,
    ):
        call_log.append(
            (
                "replay",
                batch_ids_by_source_table,
                endpoints,
                full_replay,
                ingested_at_start,
                ingested_at_end,
            )
        )
        return {"processed_raw_rows": 1}

    @contextmanager
    def fake_lock(*, owner: str, db_config=None, wait: bool = False):
        call_log.append(("lock_enter", owner))
        try:
            yield
        finally:
            call_log.append(("lock_exit", owner))

    monkeypatch.setattr(pipeline, "DatabaseManager", _LockedDatabaseManager)
    monkeypatch.setattr(pipeline, "get_db_config", lambda: {"host": "localhost"})
    monkeypatch.setattr(pipeline, "replay_raw_to_bronze", fake_replay_raw_to_bronze)
    monkeypatch.setattr(pipeline, "_hold_catapult_bronze_replay_lock", fake_lock)

    summary = pipeline.run_raw_to_bronze_stage(
        batch_ids_by_source_table={"raw.catapult_efforts": ["batch-1"]}
    )

    assert summary == {"processed_raw_rows": 1}
    assert call_log == [
        ("lock_enter", "raw_to_bronze"),
        ("db_init", {"host": "localhost"}),
        ("replay", {"raw.catapult_efforts": ["batch-1"]}, None, False, None, None),
        ("db_close", None),
        ("lock_exit", "raw_to_bronze"),
    ]


def test_run_raw_to_bronze_stage_raises_when_lock_is_busy(monkeypatch) -> None:
    @contextmanager
    def busy_lock(*, owner: str, db_config=None, wait: bool = False):
        raise pipeline.CatapultPipelineBusyError("busy")
        yield

    monkeypatch.setattr(pipeline, "_hold_catapult_bronze_replay_lock", busy_lock)

    try:
        pipeline.run_raw_to_bronze_stage()
    except pipeline.CatapultPipelineBusyError as exc:
        assert str(exc) == "busy"
    else:
        raise AssertionError("Expected CatapultPipelineBusyError")


def test_run_intraday_raw_to_bronze_stage_forwards_batch_scope(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_run_raw_to_bronze_stage(**kwargs):
        captured.update(kwargs)
        return {"processed_raw_rows": 1, "loaded_rows": 1}

    monkeypatch.setattr(pipeline, "run_raw_to_bronze_stage", fake_run_raw_to_bronze_stage)

    summary = pipeline.run_intraday_raw_to_bronze_stage(
        batch_ids_by_source_table={"raw.catapult_teams": ["batch-1"]},
        endpoints={"teams"},
    )

    assert captured == {
        "batch_ids_by_source_table": {"raw.catapult_teams": ["batch-1"]},
        "endpoints": {"teams"},
    }
    assert summary["processed_raw_rows"] == 1


def test_run_historical_day_raw_to_bronze_replays_lisbon_day_window(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_run_raw_to_bronze_stage(**kwargs):
        captured.update(kwargs)
        return {"processed_raw_rows": 2, "loaded_rows": 2}

    monkeypatch.setattr(pipeline, "run_raw_to_bronze_stage", fake_run_raw_to_bronze_stage)

    summary = pipeline.run_historical_day_raw_to_bronze("2026-04-09")

    assert captured["full_replay"] is True
    assert captured["ingested_at_start"].tzinfo == timezone.utc
    assert captured["ingested_at_end"].tzinfo == timezone.utc
    assert summary["processed_raw_rows"] == 2
    assert summary["replay_date"] == "2026-04-09"
    assert summary["day_window"]["day_start_utc"].endswith("Z")
