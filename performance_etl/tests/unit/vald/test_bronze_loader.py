from __future__ import annotations

from contextlib import contextmanager
import sys
import types

try:
    import psycopg2.extras
except ModuleNotFoundError:
    psycopg2 = types.ModuleType("psycopg2")
    psycopg2.extensions = types.ModuleType("psycopg2.extensions")
    psycopg2.extensions.connection = object
    psycopg2.extensions.cursor = object
    psycopg2.extras = types.ModuleType("psycopg2.extras")
    psycopg2.extras.execute_values = lambda *args, **kwargs: None
    psycopg2.extras.RealDictCursor = object
    psycopg2.pool = types.ModuleType("psycopg2.pool")
    psycopg2.pool.ThreadedConnectionPool = object
    sys.modules["psycopg2"] = psycopg2
    sys.modules["psycopg2.extensions"] = psycopg2.extensions
    sys.modules["psycopg2.extras"] = psycopg2.extras
    sys.modules["psycopg2.pool"] = psycopg2.pool

import psycopg2.extras

from ingestion.vald.loaders import bronze_loader as bronze_loader_module
from ingestion.vald.loaders.bronze_loader import ValdBronzeLoader


class _FakeCursor:
    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[object, ...] | None]] = []

    def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
        self.executed.append((sql, params))

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> _FakeCursor:
        return self._cursor


class _FakeDatabase:
    def __init__(
        self,
        profile_id: str | None = "profile-1",
        start_time_utc: str | None = "2026-04-20T00:00:00Z",
    ) -> None:
        self.profile_id = profile_id
        self.start_time_utc = start_time_utc
        self.cursor = _FakeCursor()
        self.batch_upserts: list[dict[str, object]] = []
        self.fetch_one_calls: list[tuple[str, tuple[object, ...] | None]] = []
        self.fetch_all_dict_calls: list[tuple[str, tuple[object, ...]]] = []

    def fetch_one(
        self,
        sql: str,
        params: tuple[object, ...] | None = None,
    ) -> tuple[object | None, ...]:
        self.fetch_one_calls.append((sql, params))
        if "start_time_utc" in sql:
            return (self.profile_id, self.start_time_utc)
        return (self.profile_id,)

    def fetch_all_dict(
        self,
        sql: str,
        params: tuple[object, ...],
    ) -> list[dict[str, object | None]]:
        self.fetch_all_dict_calls.append((sql, params))
        test_ids = set(params[0])
        if "vald_dynamo_tests" in sql:
            return [
                {
                    "test_id": test_id,
                    "profile_id": self.profile_id,
                    "start_time_utc": self.start_time_utc,
                }
                for test_id in test_ids
                if self.profile_id is not None
            ]
        return [
            {"test_id": test_id, "profile_id": self.profile_id}
            for test_id in test_ids
            if self.profile_id is not None
        ]

    def upsert_batch_bronze(
        self,
        table: str,
        records: list[dict[str, object]],
        conflict_columns: list[str],
        update_columns: list[str],
        conn=None,
    ) -> None:
        self.batch_upserts.append(
            {
                "table": table,
                "records": records,
                "conflict_columns": conflict_columns,
                "update_columns": update_columns,
            }
        )

    @contextmanager
    def connection(self):
        yield _FakeConnection(self.cursor)


def test_load_forceframe_force_traces_normalises_missing_ticks(monkeypatch) -> None:
    db = _FakeDatabase()
    loader = ValdBronzeLoader(db=db, batch_id="batch-1")
    captured: dict[str, object] = {}

    def fake_execute_values(cursor, sql, values, template=None, page_size=100):
        captured["sql"] = sql
        captured["values"] = list(values)

    monkeypatch.setattr(psycopg2.extras, "execute_values", fake_execute_values)

    count = loader.load_forceframe_force_traces(
        test_id="test-1",
        raw_id=42,
        trace_data={
            "forces": [
                {"tick": 10, "innerLeftForce": 1.0},
                {"tick": None, "innerLeftForce": 2.0},
                {"sampleIndex": "12", "innerLeftForce": 3.0},
            ],
        },
    )

    assert count == 3
    # Phase 8.7.B (2026-05-09): no DELETE on the live table — UPSERT replaces
    # the previous DELETE-then-INSERT pattern. cursor.executed should be empty;
    # all SQL is issued via psycopg2.extras.execute_values (captured above).
    assert db.cursor.executed == []
    assert captured["sql"] == (
        "INSERT INTO bronze.vald_forceframe_force_traces "
        "(test_id, profile_id, tick, inner_left_force, inner_right_force, "
        "outer_left_force, outer_right_force, raw_id, batch_id) VALUES %s "
        "ON CONFLICT (test_id, tick) DO UPDATE SET "
        "profile_id = EXCLUDED.profile_id, "
        "inner_left_force = EXCLUDED.inner_left_force, "
        "inner_right_force = EXCLUDED.inner_right_force, "
        "outer_left_force = EXCLUDED.outer_left_force, "
        "outer_right_force = EXCLUDED.outer_right_force, "
        "raw_id = EXCLUDED.raw_id, "
        "batch_id = EXCLUDED.batch_id"
    )
    assert captured["values"] == [
        ("test-1", "profile-1", 10, 1.0, None, None, None, 42, "batch-1"),
        ("test-1", "profile-1", 11, 2.0, None, None, None, 42, "batch-1"),
        ("test-1", "profile-1", 12, 3.0, None, None, None, 42, "batch-1"),
    ]


def test_load_forceframe_force_traces_requires_profile_id() -> None:
    loader = ValdBronzeLoader(db=_FakeDatabase(profile_id=None), batch_id="batch-1")

    try:
        loader.load_forceframe_force_traces(
            test_id="test-1",
            raw_id=42,
            trace_data={"forces": [{"tick": 1}]},
        )
    except ValueError as exc:
        assert "missing profile_id" in str(exc)
    else:
        raise AssertionError("expected ValueError when profile_id is unavailable")


def test_load_dynamo_traces_uses_prefetched_test_context() -> None:
    db = _FakeDatabase(
        profile_id="profile-1",
        start_time_utc="2026-04-20T01:00:00Z",
    )
    loader = ValdBronzeLoader(db=db, batch_id="batch-1")

    loader.prefetch_dynamo_test_context(["test-1"])
    db.fetch_one_calls = []

    count = loader.load_dynamo_traces(
        test_id="test-1",
        tenant_id="tenant-1",
        trace_data={
            "forceTrace": [{"tick": 1, "force": 10.5}],
            "imuTrace": [{"tick": 1, "x": 0.1}],
        },
        raw_id=42,
    )

    assert count == 1
    assert db.fetch_one_calls == []
    assert len(db.fetch_all_dict_calls) == 1
    # Phase 8.7.B: single UPSERT statement replaces the DELETE-then-INSERT pair.
    assert len(db.cursor.executed) == 1
    upsert_call = db.cursor.executed[0]
    assert upsert_call[0] == (
        "INSERT INTO bronze.vald_dynamo_traces "
        "(test_id, profile_id, tenant_id, start_time_utc, trace_type, "
        "force_trace, imu_trace, raw_id, batch_id) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
        "ON CONFLICT (test_id) DO UPDATE SET "
        "profile_id = EXCLUDED.profile_id, "
        "tenant_id = EXCLUDED.tenant_id, "
        "start_time_utc = EXCLUDED.start_time_utc, "
        "trace_type = EXCLUDED.trace_type, "
        "force_trace = EXCLUDED.force_trace, "
        "imu_trace = EXCLUDED.imu_trace, "
        "raw_id = EXCLUDED.raw_id, "
        "batch_id = EXCLUDED.batch_id"
    )
    assert upsert_call[1] == (
        "test-1",
        "profile-1",
        "tenant-1",
        "2026-04-20T01:00:00Z",
        "force",
        '[{"tick": 1, "force": 10.5}]',
        '[{"tick": 1, "x": 0.1}]',
        42,
        "batch-1",
    )


def test_load_profiles_prunes_zero_value_fields_from_bronze_upsert() -> None:
    db = _FakeDatabase()
    loader = ValdBronzeLoader(db=db, batch_id="batch-1")

    count = loader.load_profiles(
        [
            {
                "id": "profile-1",
                "tenantId": "tenant-1",
                "givenName": "Martim",
                "familyName": "Fernandes",
                "externalId": "ext-1",
            }
        ],
        raw_id=7,
    )

    assert count == 1
    upsert = db.batch_upserts[0]
    assert upsert["table"] == "bronze.vald_profiles"
    assert upsert["update_columns"] == [
        "tenant_id",
        "given_name",
        "family_name",
        "external_id",
        "raw_id",
        "batch_id",
        "updated_at",
    ]
    assert upsert["records"] == [
        {
            "vald_profile_id": "profile-1",
            "tenant_id": "tenant-1",
            "given_name": "Martim",
            "family_name": "Fernandes",
            "external_id": "ext-1",
            "raw_id": 7,
            "batch_id": "batch-1",
        }
    ]


def test_load_smartspeed_summaries_prunes_empty_summary_json_fields() -> None:
    db = _FakeDatabase()
    loader = ValdBronzeLoader(db=db, batch_id="batch-1")

    count = loader.load_smartspeed_summaries(
        [
            {
                "id": "test-1",
                "tenantId": "tenant-1",
                "profileId": "profile-1",
                "testName": "Sprint",
                "testTypeName": "Sprint",
                "repCount": 2,
                "deviceCount": 1,
                "testDateUtc": "2026-03-29T10:00:00Z",
                "isValid": True,
                "allGroups": [{"name": "A"}],
                "additionalOptions": {"ignored": True},
                "runningSummary": {"ignored": True},
                "jumpingSummary": {"ignored": True},
            }
        ],
        raw_id=11,
    )

    assert count == 1
    upsert = db.batch_upserts[0]
    assert upsert["table"] == "bronze.vald_smartspeed_test_summaries"
    record = upsert["records"][0]
    assert "additional_options" not in record
    assert "running_summary" not in record
    assert "jumping_summary" not in record


def test_load_dynamo_paths_prune_zero_value_columns() -> None:
    db = _FakeDatabase()
    loader = ValdBronzeLoader(db=db, batch_id="batch-1")

    loader.load_dynamo_tests(
        [
            {
                "id": "test-1",
                "tenantId": "tenant-1",
                "profileId": "profile-1",
                "movement": "Knee extension",
                "modifiedDateUtc": "2026-03-29T10:00:00Z",
            }
        ],
        raw_id=21,
    )
    loader.load_dynamo_repetitions(
        "test-1",
        [{"repetitionNumber": 1, "side": "left", "forceNewtons": 123.4, "impulseNs": 5.6}],
        raw_id=22,
    )

    tests_upsert = db.batch_upserts[0]
    repetitions_upsert = db.batch_upserts[1]
    assert tests_upsert["table"] == "bronze.vald_dynamo_tests"
    assert "modified_date_utc" not in tests_upsert["records"][0]
    assert "modified_date_utc" not in tests_upsert["update_columns"]
    assert repetitions_upsert["table"] == "bronze.vald_dynamo_repetitions"
    assert "force_newtons" not in repetitions_upsert["records"][0]
    assert "force_newtons" not in repetitions_upsert["update_columns"]


def test_load_forcedecks_trials_rebuilds_trial_results_in_chunks(monkeypatch) -> None:
    db = _FakeDatabase()
    loader = ValdBronzeLoader(db=db, batch_id="batch-1")
    captured_calls: list[list[dict[str, object]]] = []

    monkeypatch.setattr(
        bronze_loader_module,
        "_FORCEDECKS_TRIAL_RESULT_CHUNK_SIZE",
        2,
    )

    def fake_execute_values(cursor, sql, values, template=None, page_size=100):
        captured_calls.append(list(values))

    monkeypatch.setattr(psycopg2.extras, "execute_values", fake_execute_values)

    count = loader.load_forcedecks_trials(
        [
            {
                "id": "00000000-0000-0000-0000-000000000001",
                "testId": "test-1",
                "profileId": "profile-1",
                "recordedUtc": "2026-04-07T10:00:00Z",
                "results": [{"resultId": 1, "value": 100.0}],
            },
            {
                "id": "00000000-0000-0000-0000-000000000002",
                "testId": "test-1",
                "profileId": "profile-1",
                "recordedUtc": "2026-04-07T10:00:05Z",
                "results": [{"resultId": 2, "value": 101.0}],
            },
            {
                "id": "00000000-0000-0000-0000-000000000003",
                "testId": "test-1",
                "profileId": "profile-1",
                "recordedUtc": "2026-04-07T10:00:10Z",
                "results": [{"resultId": 3, "value": 102.0}],
            },
        ],
        raw_id=99,
    )

    assert count == 3
    # Phase 8.7.B: single UPSERT per chunk via psycopg2.extras.execute_values;
    # no DELETE on the live trial_results table (locked decision #7).
    # 3 rows / chunk_size=2 → 2 chunks captured by execute_values.
    assert len(captured_calls) == 2
    assert [row["result_id"] for row in captured_calls[0]] == [1, 2]
    assert [row["result_id"] for row in captured_calls[1]] == [3]
    # cursor.executed should be empty — all SQL went via execute_values.
    assert db.cursor.executed == []
