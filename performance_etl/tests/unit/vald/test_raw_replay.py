from __future__ import annotations

from contextlib import contextmanager
import sys
import types

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

from ingestion.vald import raw_replay


class _FakeDatabase:
    def __init__(self, rows_by_table: dict[str, list[dict]], marker_raw_ids: dict[str, int] | None = None) -> None:
        self.rows_by_table = rows_by_table
        self.marker_raw_ids = marker_raw_ids or {}
        self.executed: list[tuple[str, tuple[object, ...] | None]] = []
        self.fetch_limits: list[tuple[str, int]] = []
        self.borrowed_connections = 0
        self.returned_connections = 0
        self.commits = 0
        self.rollbacks = 0

    class _FakeConnection:
        def __init__(self, owner: "_FakeDatabase") -> None:
            self.owner = owner

        def commit(self) -> None:
            self.owner.commits += 1

        def rollback(self) -> None:
            self.owner.rollbacks += 1

    def fetch_all_dict(self, sql: str, params: tuple[object, ...]) -> list[dict]:
        table_name = sql.split("FROM", 1)[1].split()[0]
        last_raw_id = int(params[0])
        limit = int(params[-1])
        self.fetch_limits.append((table_name, limit))
        rows = [
            row
            for row in self.rows_by_table.get(table_name, [])
            if int(row["raw_id"]) > last_raw_id
        ]
        return rows[:limit]

    def fetch_one(self, sql: str, params: tuple[object, ...] | None = None) -> tuple[int] | None:
        if sql.startswith("SELECT last_raw_id FROM raw.vald_replay_cursor"):
            assert params is not None
            return (self.marker_raw_ids.get(str(params[0]), 0),)
        if sql.startswith("SELECT 1 FROM "):
            assert params is not None
            table_name = sql.split("FROM", 1)[1].split()[0]
            last_raw_id = int(params[0])
            has_rows = any(
                int(row["raw_id"]) > last_raw_id
                for row in self.rows_by_table.get(table_name, [])
            )
            return (1,) if has_rows else None
        table_name = sql.split("FROM", 1)[1].strip()
        return (self.marker_raw_ids.get(table_name, 0),)

    def execute(
        self,
        sql: str,
        params: tuple[object, ...] | None = None,
    ) -> None:
        self.executed.append((sql, params))
        if sql.strip().startswith("INSERT INTO raw.vald_replay_cursor"):
            assert params is not None
            self.marker_raw_ids[str(params[0])] = int(params[1])

    def get_connection(self):
        self.borrowed_connections += 1
        return self._FakeConnection(self)

    def put_connection(self, conn) -> None:
        self.returned_connections += 1

    @contextmanager
    def connection(self):
        conn = self.get_connection()
        try:
            yield conn
        finally:
            self.put_connection(conn)


class _FakeBatchManager:
    started: list[str] = []
    completed: list[tuple[str, int, int]] = []
    failed: list[tuple[str, str]] = []

    def __init__(self, db) -> None:
        self.db = db

    def start_batch(self, provider: str, source_account: str, api_name: str) -> str:
        batch_id = f"batch-{len(self.started) + 1}"
        self.started.append(batch_id)
        return batch_id

    def complete_batch(self, batch_id: str, records_extracted: int, records_loaded: int) -> None:
        self.completed.append((batch_id, records_extracted, records_loaded))

    def fail_batch(self, batch_id: str, error_message: str) -> None:
        self.failed.append((batch_id, error_message))


class _FakeBronzeLoader:
    calls: list[tuple[str, object]] = []
    prefetch_calls: list[list[str]] = []
    dynamo_prefetch_calls: list[list[str]] = []

    def __init__(self, db, batch_id: str, conn=None, table_overrides=None) -> None:
        self.db = db
        self.batch_id = batch_id
        self.conn = conn
        self.table_overrides = table_overrides or {}

    def prefetch_forceframe_profile_ids(self, test_ids: list[str]) -> None:
        self.prefetch_calls.append(list(test_ids))

    def prefetch_dynamo_test_context(self, test_ids: list[str]) -> None:
        self.dynamo_prefetch_calls.append(list(test_ids))

    def load_profiles(self, profiles, raw_id: int, tenant_id: str | None = None) -> int:
        self.calls.append(("profiles", (profiles, raw_id, tenant_id)))
        self.db.marker_raw_ids["bronze.vald_profiles"] = raw_id
        return len(profiles)

    def load_forcedecks_tests(self, tests, raw_id: int, tenant_id: str | None = None) -> int:
        self.calls.append(("forcedecks_tests", (tests, raw_id, tenant_id)))
        self.db.marker_raw_ids["bronze.vald_forcedecks_tests"] = raw_id
        return len(tests)

    def load_forceframe_force_traces(self, test_id: str, trace_data, raw_id: int) -> int:
        self.calls.append(("forceframe_traces", (test_id, trace_data, raw_id)))
        payload = trace_data.get("forces", []) if isinstance(trace_data, dict) else list(trace_data)
        self.db.marker_raw_ids["bronze.vald_forceframe_force_traces"] = raw_id
        return len(payload)

    def load_dynamo_traces(self, test_id: str, tenant_id: str, trace_data, raw_id: int) -> int:
        self.calls.append(("dynamo_traces", (test_id, tenant_id, trace_data, raw_id)))
        self.db.marker_raw_ids["bronze.vald_dynamo_traces"] = raw_id
        return 1 if trace_data else 0

    def load_smartspeed_test_details(self, details, raw_id: int, tenant_id: str | None = None) -> int:
        self.calls.append(("smartspeed_details", (details, raw_id, tenant_id)))
        return len(details)


def _reset_fakes() -> None:
    _FakeBatchManager.started = []
    _FakeBatchManager.completed = []
    _FakeBatchManager.failed = []
    _FakeBronzeLoader.calls = []
    _FakeBronzeLoader.prefetch_calls = []
    _FakeBronzeLoader.dynamo_prefetch_calls = []


def test_replay_raw_to_bronze_uses_marker_tables_and_replays_rows(monkeypatch) -> None:
    _reset_fakes()

    db = _FakeDatabase(
        {
            "raw.vald_profiles": [
                {
                    "raw_id": 2,
                    "request_params": {"teamId": "tenant-1"},
                    "response_payload": [{"id": "profile-1"}],
                }
            ],
            "raw.vald_forcedecks_tests": [
                {
                    "raw_id": 5,
                    "request_params": {"tenantId": "tenant-1"},
                    "response_payload": [{"id": "test-1"}, {"id": "test-2"}],
                }
            ],
        }
    )

    monkeypatch.setattr(raw_replay, "BatchManager", _FakeBatchManager)
    monkeypatch.setattr(raw_replay, "ValdBronzeLoader", _FakeBronzeLoader)

    summary = raw_replay.replay_raw_to_bronze(
        db=db,
        modules=["forcedecks"],
        include_reference=True,
    )

    assert summary["processed_raw_rows"] == 2
    assert summary["loaded_rows"] == 3
    assert summary["has_new_data"] is True
    assert summary["tables"]["raw.vald_profiles"]["last_raw_id"] == 2
    assert summary["tables"]["raw.vald_forcedecks_tests"]["last_raw_id"] == 5
    assert db.marker_raw_ids["raw.vald_profiles"] == 2
    assert db.marker_raw_ids["raw.vald_forcedecks_tests"] == 5
    assert db.borrowed_connections == 2
    assert db.returned_connections == 2
    assert db.commits == 2
    assert _FakeBatchManager.failed == []


def test_replay_raw_to_bronze_skips_already_materialized_raw_ids(monkeypatch) -> None:
    _reset_fakes()

    db = _FakeDatabase(
        {
            "raw.vald_profiles": [
                {
                    "raw_id": 2,
                    "request_params": {"teamId": "tenant-1"},
                    "response_payload": [{"id": "profile-1"}],
                },
                {
                    "raw_id": 3,
                    "request_params": {"teamId": "tenant-1"},
                    "response_payload": [{"id": "profile-2"}],
                },
            ]
        },
        marker_raw_ids={"raw.vald_profiles": 2},
    )

    monkeypatch.setattr(raw_replay, "BatchManager", _FakeBatchManager)
    monkeypatch.setattr(raw_replay, "ValdBronzeLoader", _FakeBronzeLoader)

    summary = raw_replay.replay_raw_to_bronze(
        db=db,
        modules=[],
        include_reference=True,
    )

    assert summary["processed_raw_rows"] == 1
    assert summary["tables"]["raw.vald_profiles"]["last_raw_id"] == 3
    assert _FakeBronzeLoader.calls[0][1][1] == 3


def test_replay_raw_to_bronze_full_replay_ignores_cursor_and_skips_cursor_update(monkeypatch) -> None:
    _reset_fakes()

    db = _FakeDatabase(
        {
            "raw.vald_profiles": [
                {
                    "raw_id": 2,
                    "request_params": {"teamId": "tenant-1"},
                    "response_payload": [{"id": "profile-1"}],
                }
            ]
        },
        marker_raw_ids={"raw.vald_profiles": 99},
    )

    monkeypatch.setattr(raw_replay, "BatchManager", _FakeBatchManager)
    monkeypatch.setattr(raw_replay, "ValdBronzeLoader", _FakeBronzeLoader)

    summary = raw_replay.replay_raw_to_bronze(
        db=db,
        modules=[],
        include_reference=True,
        full_replay=True,
        replay_cursor_table=None,
    )

    assert summary["full_replay"] is True
    assert summary["processed_raw_rows"] == 1
    assert db.marker_raw_ids["raw.vald_profiles"] == 99


def test_replay_raw_to_bronze_can_filter_source_tables(monkeypatch) -> None:
    _reset_fakes()

    db = _FakeDatabase(
        {
            "raw.vald_profiles": [
                {
                    "raw_id": 2,
                    "request_params": {"teamId": "tenant-1"},
                    "response_payload": [{"id": "profile-1"}],
                }
            ],
            "raw.vald_forcedecks_tests": [
                {
                    "raw_id": 5,
                    "request_params": {"tenantId": "tenant-1"},
                    "response_payload": [{"id": "test-1"}],
                }
            ],
        }
    )

    monkeypatch.setattr(raw_replay, "BatchManager", _FakeBatchManager)
    monkeypatch.setattr(raw_replay, "ValdBronzeLoader", _FakeBronzeLoader)

    summary = raw_replay.replay_raw_to_bronze(
        db=db,
        modules=["forcedecks"],
        include_reference=True,
        exclude_source_tables=("raw.vald_profiles",),
    )

    assert summary["processed_raw_rows"] == 1
    assert "raw.vald_profiles" not in summary["tables"]
    assert summary["tables"]["raw.vald_forcedecks_tests"]["last_raw_id"] == 5


def test_replay_smartspeed_details_injects_test_id_from_request_params() -> None:
    _reset_fakes()
    loader = _FakeBronzeLoader(db=_FakeDatabase({}), batch_id="batch-1")

    loaded = raw_replay._replay_smartspeed_details(
        loader,
        {
            "raw_id": 33,
            "request_params": {"teamId": "tenant-1", "testId": "test-9"},
            "response_payload": {"profileId": "profile-1"},
        },
    )

    assert loaded == 1
    call_name, call_args = _FakeBronzeLoader.calls[0]
    assert call_name == "smartspeed_details"
    details, raw_id, tenant_id = call_args
    assert raw_id == 33
    assert tenant_id == "tenant-1"
    assert details[0]["testId"] == "test-9"


def test_replay_raw_to_bronze_uses_forceframe_specific_batch_settings(monkeypatch) -> None:
    _reset_fakes()
    monkeypatch.setattr(raw_replay, "BatchManager", _FakeBatchManager)
    monkeypatch.setattr(raw_replay, "ValdBronzeLoader", _FakeBronzeLoader)
    monkeypatch.setattr(raw_replay, "_REPLAY_CHUNK_SIZE", 3)
    monkeypatch.setattr(raw_replay, "_REPLAY_COMMIT_BATCH_SIZE", 3)
    monkeypatch.setattr(raw_replay, "_FORCEFRAME_TRACE_REPLAY_CHUNK_SIZE", 2)
    monkeypatch.setattr(raw_replay, "_FORCEFRAME_TRACE_REPLAY_COMMIT_BATCH_SIZE", 1)

    forceframe_db = _FakeDatabase(
        {
            raw_replay._FORCEFRAME_TRACE_SOURCE_TABLE: [
                {
                    "raw_id": 1,
                    "request_params": {"testId": "test-1"},
                    "response_payload": {"forces": [{"tick": 1}]},
                },
                {
                    "raw_id": 2,
                    "request_params": {"testId": "test-2"},
                    "response_payload": {"forces": [{"tick": 2}]},
                },
                {
                    "raw_id": 3,
                    "request_params": {"testId": "test-3"},
                    "response_payload": {"forces": [{"tick": 3}]},
                },
            ]
        }
    )

    forceframe_summary = raw_replay.replay_raw_to_bronze(
        db=forceframe_db,
        modules=["forceframe"],
        include_reference=False,
        include_only_source_tables=(raw_replay._FORCEFRAME_TRACE_SOURCE_TABLE,),
    )

    assert forceframe_summary["processed_raw_rows"] == 3
    assert forceframe_db.commits == 3
    assert [
        limit
        for table_name, limit in forceframe_db.fetch_limits
        if table_name == raw_replay._FORCEFRAME_TRACE_SOURCE_TABLE
    ] == [2, 2, 2]
    assert _FakeBronzeLoader.prefetch_calls == [["test-1", "test-2"], ["test-3"]]

    _reset_fakes()
    monkeypatch.setattr(raw_replay, "BatchManager", _FakeBatchManager)
    monkeypatch.setattr(raw_replay, "ValdBronzeLoader", _FakeBronzeLoader)

    profiles_db = _FakeDatabase(
        {
            "raw.vald_profiles": [
                {
                    "raw_id": 1,
                    "request_params": {"teamId": "tenant-1"},
                    "response_payload": [{"id": "profile-1"}],
                },
                {
                    "raw_id": 2,
                    "request_params": {"teamId": "tenant-1"},
                    "response_payload": [{"id": "profile-2"}],
                },
                {
                    "raw_id": 3,
                    "request_params": {"teamId": "tenant-1"},
                    "response_payload": [{"id": "profile-3"}],
                },
                {
                    "raw_id": 4,
                    "request_params": {"teamId": "tenant-1"},
                    "response_payload": [{"id": "profile-4"}],
                },
            ]
        }
    )

    profiles_summary = raw_replay.replay_raw_to_bronze(
        db=profiles_db,
        modules=[],
        include_reference=True,
    )

    assert profiles_summary["processed_raw_rows"] == 4
    assert profiles_db.commits == 2
    assert [
        limit
        for table_name, limit in profiles_db.fetch_limits
        if table_name == "raw.vald_profiles"
    ] == [3, 3, 3]


def test_replay_raw_to_bronze_uses_dynamo_trace_specific_batch_settings(monkeypatch) -> None:
    _reset_fakes()
    monkeypatch.setattr(raw_replay, "BatchManager", _FakeBatchManager)
    monkeypatch.setattr(raw_replay, "ValdBronzeLoader", _FakeBronzeLoader)
    monkeypatch.setattr(raw_replay, "_REPLAY_CHUNK_SIZE", 3)
    monkeypatch.setattr(raw_replay, "_REPLAY_COMMIT_BATCH_SIZE", 3)
    monkeypatch.setattr(raw_replay, "_DYNAMO_TRACE_REPLAY_CHUNK_SIZE", 2)
    monkeypatch.setattr(raw_replay, "_DYNAMO_TRACE_REPLAY_COMMIT_BATCH_SIZE", 1)

    db = _FakeDatabase(
        {
            raw_replay._DYNAMO_TRACE_SOURCE_TABLE: [
                {
                    "raw_id": 1,
                    "request_params": {"tenantId": "tenant-1", "testId": "test-1"},
                    "response_payload": {"forceTrace": [{"tick": 1}]},
                },
                {
                    "raw_id": 2,
                    "request_params": {"tenantId": "tenant-1", "testId": "test-2"},
                    "response_payload": {"forceTrace": [{"tick": 2}]},
                },
                {
                    "raw_id": 3,
                    "request_params": {"tenantId": "tenant-1", "testId": "test-3"},
                    "response_payload": {"forceTrace": [{"tick": 3}]},
                },
            ]
        }
    )

    summary = raw_replay.replay_raw_to_bronze(
        db=db,
        modules=["dynamo"],
        include_reference=False,
        include_only_source_tables=(raw_replay._DYNAMO_TRACE_SOURCE_TABLE,),
    )

    assert summary["processed_raw_rows"] == 3
    assert db.commits == 3
    assert [
        limit
        for table_name, limit in db.fetch_limits
        if table_name == raw_replay._DYNAMO_TRACE_SOURCE_TABLE
    ] == [2, 2, 2]
    assert _FakeBronzeLoader.dynamo_prefetch_calls == [
        ["test-1", "test-2"],
        ["test-3"],
    ]
    assert _FakeBronzeLoader.prefetch_calls == []


def test_replay_raw_to_bronze_commits_progress_when_deadline_hits_mid_chunk(monkeypatch) -> None:
    _reset_fakes()
    monkeypatch.setattr(raw_replay, "BatchManager", _FakeBatchManager)
    monkeypatch.setattr(raw_replay, "ValdBronzeLoader", _FakeBronzeLoader)
    monkeypatch.setattr(raw_replay, "_FORCEFRAME_TRACE_REPLAY_CHUNK_SIZE", 5)
    monkeypatch.setattr(raw_replay, "_FORCEFRAME_TRACE_REPLAY_COMMIT_BATCH_SIZE", 2)

    monotonic_values = iter([0.0, 0.0, 1.0, 1.0])
    monkeypatch.setattr(raw_replay.time, "monotonic", lambda: next(monotonic_values))

    db = _FakeDatabase(
        {
            raw_replay._FORCEFRAME_TRACE_SOURCE_TABLE: [
                {
                    "raw_id": 1,
                    "request_params": {"testId": "test-1"},
                    "response_payload": {"forces": [{"tick": 1}]},
                },
                {
                    "raw_id": 2,
                    "request_params": {"testId": "test-2"},
                    "response_payload": {"forces": [{"tick": 2}]},
                },
                {
                    "raw_id": 3,
                    "request_params": {"testId": "test-3"},
                    "response_payload": {"forces": [{"tick": 3}]},
                },
            ]
        }
    )

    summary = raw_replay.replay_raw_to_bronze(
        db=db,
        modules=["forceframe"],
        include_reference=False,
        include_only_source_tables=(raw_replay._FORCEFRAME_TRACE_SOURCE_TABLE,),
        deadline=0.5,
    )

    assert summary["deadline_reached"] is True
    assert summary["processed_raw_rows"] == 2
    assert summary["loaded_rows"] == 2
    assert summary["tables"][raw_replay._FORCEFRAME_TRACE_SOURCE_TABLE]["last_raw_id"] == 2
    assert db.marker_raw_ids[raw_replay._FORCEFRAME_TRACE_SOURCE_TABLE] == 2
    assert db.commits == 1
    assert _FakeBatchManager.failed == []
    assert _FakeBatchManager.completed == [("batch-1", 2, 2)]
