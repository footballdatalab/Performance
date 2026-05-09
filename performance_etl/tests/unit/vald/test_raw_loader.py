from __future__ import annotations

import sys
import types

try:
    import psycopg2.extras
except ModuleNotFoundError:
    psycopg2 = types.ModuleType("psycopg2")
    psycopg2.extras = types.ModuleType("psycopg2.extras")
    psycopg2.extras.execute_values = lambda *args, **kwargs: None
    psycopg2.extras.RealDictCursor = object
    psycopg2.pool = types.ModuleType("psycopg2.pool")
    psycopg2.pool.ThreadedConnectionPool = object
    psycopg2.extensions = types.SimpleNamespace(connection=object, cursor=object)
    sys.modules["psycopg2"] = psycopg2
    sys.modules["psycopg2.extras"] = psycopg2.extras
    sys.modules["psycopg2.pool"] = psycopg2.pool

from ingestion.vald.loaders.raw_loader import ValdRawLoader


class _FakeDatabase:
    def __init__(self, latest_raw: tuple[int] | None = None) -> None:
        self.latest_raw = latest_raw
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []
        self.insert_calls: list[tuple[str, dict[str, object]]] = []

    def fetch_one(
        self,
        sql: str,
        params: tuple[object, ...] | None = None,
    ) -> tuple[int] | None:
        self.fetch_calls.append((sql, params or ()))
        return self.latest_raw

    def insert_raw(self, table: str, data: dict[str, object]) -> int:
        self.insert_calls.append((table, data))
        return 999


def test_load_raw_if_changed_reuses_latest_identical_snapshot() -> None:
    db = _FakeDatabase(latest_raw=(123,))
    loader = ValdRawLoader(db=db, batch_id="batch-1", source_account="vald_default")

    raw_id = loader.load_raw_if_changed(
        table_name="vald_groups",
        api_endpoint="/groups",
        response_payload=[{"id": "group-1"}],
        request_params={"tenantId": "tenant-1"},
        api_version="v1",
    )

    assert raw_id == 123
    assert len(db.fetch_calls) == 1
    assert db.insert_calls == []


def test_load_raw_if_changed_inserts_when_snapshot_differs() -> None:
    db = _FakeDatabase(latest_raw=None)
    loader = ValdRawLoader(db=db, batch_id="batch-1", source_account="vald_default")

    raw_id = loader.load_raw_if_changed(
        table_name="vald_groups",
        api_endpoint="/groups",
        response_payload=[{"id": "group-1"}],
        request_params={"tenantId": "tenant-1"},
        api_version="v1",
    )

    assert raw_id == 999
    assert len(db.fetch_calls) == 1
    assert len(db.insert_calls) == 1
    assert db.insert_calls[0][0] == "raw.vald_groups"


def test_load_raw_if_changed_with_status_reports_insert_state() -> None:
    unchanged_db = _FakeDatabase(latest_raw=(456,))
    loader = ValdRawLoader(db=unchanged_db, batch_id="batch-1", source_account="vald_default")
    raw_id, inserted = loader.load_raw_if_changed_with_status(
        table_name="vald_profiles",
        api_endpoint="/profiles",
        response_payload=[{"id": "profile-1"}],
        request_params={"tenantId": "tenant-1"},
        api_version="v1",
    )
    assert raw_id == 456
    assert inserted is False

    changed_db = _FakeDatabase(latest_raw=None)
    loader = ValdRawLoader(db=changed_db, batch_id="batch-1", source_account="vald_default")
    raw_id, inserted = loader.load_raw_if_changed_with_status(
        table_name="vald_profiles",
        api_endpoint="/profiles",
        response_payload=[{"id": "profile-1"}],
        request_params={"tenantId": "tenant-1"},
        api_version="v1",
    )
    assert raw_id == 999
    assert inserted is True


def test_load_raw_omits_page_number_for_pruned_tables() -> None:
    db = _FakeDatabase(latest_raw=None)
    loader = ValdRawLoader(db=db, batch_id="batch-1", source_account="vald_default")

    raw_id = loader.load_raw(
        table_name="vald_profiles",
        api_endpoint="/profiles",
        response_payload=[{"id": "profile-1"}],
        page_number=3,
    )

    assert raw_id == 999
    assert db.insert_calls[0][0] == "raw.vald_profiles"
    assert "page_number" not in db.insert_calls[0][1]


def test_load_raw_keeps_page_number_for_supported_tables() -> None:
    db = _FakeDatabase(latest_raw=None)
    loader = ValdRawLoader(db=db, batch_id="batch-1", source_account="vald_default")

    raw_id = loader.load_raw(
        table_name="vald_smartspeed_test_summaries",
        api_endpoint="/tests",
        response_payload=[{"id": "test-1"}],
        page_number=2,
    )

    assert raw_id == 999
    assert db.insert_calls[0][0] == "raw.vald_smartspeed_test_summaries"
    assert db.insert_calls[0][1]["page_number"] == 2
