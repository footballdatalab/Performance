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

from ingestion.catapult.loaders.raw_loader import CatapultRawLoader


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
        return 321


def test_load_raw_if_changed_reuses_latest_identical_snapshot() -> None:
    db = _FakeDatabase(latest_raw=(77,))
    loader = CatapultRawLoader(db=db, batch_id="batch-1", source_account="CATAPULT_A")

    raw_id = loader.load_raw_if_changed(
        table_name="catapult_teams",
        api_endpoint="/teams",
        response_payload=[{"id": 1, "name": "A Team"}],
        api_version="v6",
    )

    assert raw_id == 77
    assert len(db.fetch_calls) == 1
    assert db.insert_calls == []


def test_load_raw_if_changed_inserts_when_snapshot_differs() -> None:
    db = _FakeDatabase(latest_raw=None)
    loader = CatapultRawLoader(db=db, batch_id="batch-1", source_account="CATAPULT_A")

    raw_id, inserted = loader.load_raw_if_changed_with_status(
        table_name="catapult_activities",
        api_endpoint="/activities",
        response_payload=[{"id": 10, "name": "Training"}],
        request_params={"page": 1, "page_size": 100},
        page_number=1,
        api_version="v6",
    )

    assert raw_id == 321
    assert inserted is True
    assert db.insert_calls[0][0] == "raw.catapult_activities"
    assert db.insert_calls[0][1]["source_account"] == "CATAPULT_A"
