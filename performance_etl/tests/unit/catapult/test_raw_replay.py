from __future__ import annotations

from datetime import datetime, timezone

import pytest

from ingestion.catapult import raw_replay
from ingestion.catapult.raw_replay import _replay_annotations, _replay_efforts, _replay_periods


class _FakeLoader:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple, dict]] = []

    def load_annotations(self, *args, **kwargs):
        self.calls.append(("annotations", args, kwargs))
        return 5

    def load_periods(self, *args, **kwargs):
        self.calls.append(("periods", args, kwargs))
        return 4

    def load_efforts(self, *args, **kwargs):
        self.calls.append(("efforts", args, kwargs))
        return 3


def test_replay_annotations_uses_scope_and_target_from_request_params() -> None:
    loader = _FakeLoader()

    loaded = _replay_annotations(
        loader,
        {
            "raw_id": 11,
            "request_params": {"annotation_scope": "activity", "target_id": 22},
            "response_payload": [{"id": 1, "name": "Kick-off"}],
        },
    )

    assert loaded == 5
    _, args, kwargs = loader.calls[0]
    assert args[1] == 11
    assert kwargs["annotation_scope"] == "activity"
    assert kwargs["target_id"] == "22"


def test_replay_periods_passes_activity_id_from_request_params() -> None:
    loader = _FakeLoader()

    loaded = _replay_periods(
        loader,
        {
            "raw_id": 9,
            "request_params": {"activity_id": 44},
            "response_payload": [{"id": 1, "name": "Warm-up"}],
        },
    )

    assert loaded == 4
    _, args, kwargs = loader.calls[0]
    assert args[1] == 9
    assert kwargs["activity_id"] == "44"


def test_replay_efforts_preserves_list_payload_shape() -> None:
    loader = _FakeLoader()
    payload = [{"athlete_id": "athlete-1", "data": {"velocity_efforts": []}}]

    loaded = _replay_efforts(
        loader,
        {
            "raw_id": 12,
            "request_params": {"activity_id": "activity-1", "athlete_id": "athlete-1"},
            "response_payload": payload,
        },
    )

    assert loaded == 3
    _, args, kwargs = loader.calls[0]
    assert args[0] == payload
    assert args[1] == 12
    assert kwargs["activity_id"] == "activity-1"


def test_replay_raw_to_bronze_full_replay_filters_by_ingested_window(monkeypatch) -> None:
    calls: list[tuple[str, object]] = []

    class _ReplayDb:
        def fetch_one(self, sql: str):
            calls.append(("fetch_one", sql))
            return (99,)

        def fetch_all_dict(self, sql: str, params: tuple):
            calls.append(("fetch_all_dict", sql, params))
            if "SELECT DISTINCT raw_row.source_account" in sql:
                return [{"source_account": "CATAPULT_A"}] if "raw.catapult_teams" in sql else []
            if "SELECT raw_row.raw_id" not in sql or "raw.catapult_teams" not in sql:
                return []
            if params[0] != 0:
                return []
            return [
                {
                    "raw_id": 7,
                    "source_account": "CATAPULT_A",
                    "batch_id": "batch-1",
                    "request_params": {},
                    "response_payload": [{"id": "team-1", "name": "A"}],
                }
            ]

        def connection(self):
            class _ConnectionContext:
                def __enter__(self):
                    return object()

                def __exit__(self, exc_type, exc, tb):
                    return None

            return _ConnectionContext()

    class _BatchManager:
        def __init__(self, db):
            self.db = db

        def start_batch(self, **kwargs):
            calls.append(("start_batch", kwargs))
            return "replay-batch"

        def complete_batch(self, *args, **kwargs):
            calls.append(("complete_batch", args, kwargs))

        def fail_batch(self, *args, **kwargs):
            raise AssertionError("fail_batch should not be called")

    class _BronzeLoader:
        def __init__(self, **kwargs):
            self.last_load_stats = {"skipped_rows": 0, "skip_reasons": {}}

        def load_teams(self, payload, raw_id):
            calls.append(("load_teams", payload, raw_id))
            return 1

    monkeypatch.setattr(raw_replay, "BatchManager", _BatchManager)
    monkeypatch.setattr(raw_replay, "CatapultBronzeLoader", _BronzeLoader)

    start = datetime(2026, 4, 9, tzinfo=timezone.utc)
    end = datetime(2026, 4, 10, tzinfo=timezone.utc)
    summary = raw_replay.replay_raw_to_bronze(
        _ReplayDb(),
        endpoints={"teams"},
        full_replay=True,
        ingested_at_start=start,
        ingested_at_end=end,
    )

    fetch_call = next(call for call in calls if call[0] == "fetch_all_dict" and "SELECT raw_row.raw_id" in str(call[1]))
    sql = str(fetch_call[1])
    params = fetch_call[2]
    assert "raw_row.raw_id > %s" in sql
    assert "raw_row.source_account = %s" in sql
    assert "ingested_at >= %s" in sql
    assert "ingested_at < %s" in sql
    assert "LIMIT %s" in sql
    assert params == (0, "CATAPULT_A", start, end, raw_replay._REPLAY_CHUNK_SIZE)
    assert summary["processed_raw_rows"] == 1
    assert summary["loaded_rows"] == 1
    assert summary["full_replay"] is True


def test_replay_raw_to_bronze_rejects_ingested_window_without_full_replay() -> None:
    with pytest.raises(ValueError, match="ingested_at filters require full_replay=True"):
        raw_replay.replay_raw_to_bronze(
            object(),
            ingested_at_start=datetime(2026, 4, 9, tzinfo=timezone.utc),
        )


def test_replay_raw_to_bronze_fetches_source_account_rows_in_keyset_chunks(monkeypatch) -> None:
    calls: list[tuple[str, object]] = []

    class _ReplayDb:
        def fetch_one(self, sql: str):
            calls.append(("fetch_one", sql))
            return (3,)

        def fetch_all_dict(self, sql: str, params: tuple):
            calls.append(("fetch_all_dict", sql, params))
            if "SELECT source_account, COALESCE(MAX(raw_id), 0) AS last_raw_id" in sql:
                return [{"source_account": "CATAPULT_A", "last_raw_id": 0}]
            if "SELECT DISTINCT raw_row.source_account" in sql:
                return [{"source_account": "CATAPULT_A"}]
            if "SELECT raw_row.raw_id" not in sql:
                return []
            rows_by_cursor = {
                0: [
                    {
                        "raw_id": 1,
                        "source_account": "CATAPULT_A",
                        "batch_id": "batch-1",
                        "request_params": {},
                        "response_payload": [{"id": "team-1", "name": "A"}],
                    },
                    {
                        "raw_id": 2,
                        "source_account": "CATAPULT_A",
                        "batch_id": "batch-1",
                        "request_params": {},
                        "response_payload": [{"id": "team-2", "name": "B"}],
                    },
                ],
                2: [
                    {
                        "raw_id": 3,
                        "source_account": "CATAPULT_A",
                        "batch_id": "batch-2",
                        "request_params": {},
                        "response_payload": [{"id": "team-3", "name": "C"}],
                    }
                ],
            }
            return rows_by_cursor.get(int(params[0]), [])

        def connection(self):
            class _ConnectionContext:
                def __enter__(self):
                    return object()

                def __exit__(self, exc_type, exc, tb):
                    return None

            return _ConnectionContext()

    class _BatchManager:
        def __init__(self, db):
            self.db = db

        def start_batch(self, **kwargs):
            calls.append(("start_batch", kwargs))
            return "replay-batch"

        def complete_batch(self, *args, **kwargs):
            calls.append(("complete_batch", args, kwargs))

        def fail_batch(self, *args, **kwargs):
            raise AssertionError("fail_batch should not be called")

    class _BronzeLoader:
        def __init__(self, **kwargs):
            self.last_load_stats = {"skipped_rows": 0, "skip_reasons": {}}

        def load_teams(self, payload, raw_id):
            calls.append(("load_teams", payload, raw_id))
            return 1

    monkeypatch.setattr(raw_replay, "_REPLAY_CHUNK_SIZE", 2)
    monkeypatch.setattr(raw_replay, "BatchManager", _BatchManager)
    monkeypatch.setattr(raw_replay, "CatapultBronzeLoader", _BronzeLoader)

    summary = raw_replay.replay_raw_to_bronze(_ReplayDb(), endpoints={"teams"})

    raw_fetches = [
        call
        for call in calls
        if call[0] == "fetch_all_dict" and "SELECT raw_row.raw_id" in str(call[1])
    ]
    assert [call[2][0] for call in raw_fetches] == [0, 2, 3]
    assert all(call[2][1] == "CATAPULT_A" for call in raw_fetches)
    assert all(call[2][-1] == 2 for call in raw_fetches)
    assert all("LIMIT %s" in str(call[1]) for call in raw_fetches)
    assert summary["processed_raw_rows"] == 3
    assert summary["loaded_rows"] == 3
    assert summary["tables"]["raw.catapult_teams"]["raw_id_min"] == 1
    assert summary["tables"]["raw.catapult_teams"]["raw_id_max"] == 3


def test_replay_raw_to_bronze_batch_scope_skips_watermarks_and_uses_uuid_batch_filter(
    monkeypatch,
) -> None:
    calls: list[tuple[str, object]] = []

    class _ReplayDb:
        def fetch_one(self, sql: str):
            raise AssertionError(f"Unexpected scalar query during batch-scoped replay: {sql}")

        def fetch_all_dict(self, sql: str, params: tuple):
            calls.append(("fetch_all_dict", sql, params))
            if "SELECT source_account, COALESCE(MAX(raw_id), 0) AS last_raw_id" in sql:
                raise AssertionError("Batch-scoped replay should not read bronze watermarks")
            if "SELECT DISTINCT raw_row.source_account" in sql:
                assert "raw_row.batch_id = ANY(%s::uuid[])" in sql
                assert params == (["batch-1"],)
                return [{"source_account": "CATAPULT_A"}]
            if "SELECT raw_row.raw_id" not in sql:
                return []
            assert "WITH marker_watermarks AS" not in sql
            assert "raw_row.batch_id = ANY(%s::uuid[])" in sql
            assert params[1] == "CATAPULT_A"
            assert params[2] == ["batch-1"]
            if int(params[0]) != 0:
                return []
            return [
                {
                    "raw_id": 10,
                    "source_account": "CATAPULT_A",
                    "batch_id": "batch-1",
                    "request_params": {},
                    "response_payload": [{"id": "team-1", "name": "A"}],
                }
            ]

        def connection(self):
            class _ConnectionContext:
                def __enter__(self):
                    return object()

                def __exit__(self, exc_type, exc, tb):
                    return None

            return _ConnectionContext()

    class _BatchManager:
        def __init__(self, db):
            self.db = db

        def start_batch(self, **kwargs):
            calls.append(("start_batch", kwargs))
            return "replay-batch"

        def complete_batch(self, *args, **kwargs):
            calls.append(("complete_batch", args, kwargs))

        def fail_batch(self, *args, **kwargs):
            raise AssertionError("fail_batch should not be called")

    class _BronzeLoader:
        def __init__(self, **kwargs):
            self.last_load_stats = {"skipped_rows": 0, "skip_reasons": {}}

        def load_teams(self, payload, raw_id):
            calls.append(("load_teams", payload, raw_id))
            return 1

    monkeypatch.setattr(raw_replay, "BatchManager", _BatchManager)
    monkeypatch.setattr(raw_replay, "CatapultBronzeLoader", _BronzeLoader)

    summary = raw_replay.replay_raw_to_bronze(
        _ReplayDb(),
        batch_ids_by_source_table={"raw.catapult_teams": ["batch-1"]},
        endpoints={"teams"},
    )

    raw_fetches = [
        call for call in calls if call[0] == "fetch_all_dict" and "SELECT raw_row.raw_id" in str(call[1])
    ]
    assert [call[2][0] for call in raw_fetches] == [0, 10]
    assert summary["processed_raw_rows"] == 1
    assert summary["tables"]["raw.catapult_teams"]["last_raw_id"] == 10


def test_fetch_raw_rows_scopes_incremental_marker_by_source_account() -> None:
    calls: list[tuple[str, tuple]] = []

    class _ReplayDb:
        def fetch_all_dict(self, sql: str, params: tuple):
            calls.append((sql, params))
            return []

    raw_replay._fetch_raw_rows(
        _ReplayDb(),
        source_table="raw.catapult_tags",
        marker_table="bronze.catapult_tags",
        batch_ids=["batch-1"],
        ingested_at_start=None,
        ingested_at_end=None,
    )

    sql, params = calls[0]
    assert "FROM raw.catapult_tags AS raw_row" in sql
    assert "WITH marker_watermarks AS" in sql
    assert "FROM bronze.catapult_tags" in sql
    assert "LEFT JOIN marker_watermarks AS marker" in sql
    assert "ON marker.source_account = raw_row.source_account" in sql
    assert "raw_row.raw_id > COALESCE" in sql
    assert "raw_row.batch_id = ANY(%s::uuid[])" in sql
    assert params == (0, ["batch-1"])
