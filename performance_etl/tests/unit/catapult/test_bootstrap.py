from __future__ import annotations

from datetime import date

from ingestion.catapult.bootstrap import build_partition_plan, ensure_partition_horizon


class _FakeDatabase:
    def __init__(self) -> None:
        self.executed: list[str] = []

    def execute(self, sql: str, params=None) -> None:
        self.executed.append(sql)


def test_build_partition_plan_covers_expected_window() -> None:
    plan = build_partition_plan(reference_date=date(2026, 3, 29), months_ahead=18)

    assert plan[0]["partition_table"] == "bronze.catapult_stats_2024_01"
    assert plan[1]["partition_table"] == "bronze.catapult_sensor_data_2024_01"
    assert plan[-2]["partition_table"] == "bronze.catapult_stats_2027_09"
    assert plan[-1]["partition_table"] == "bronze.catapult_sensor_data_2027_09"


def test_ensure_partition_horizon_creates_partitions_and_indexes() -> None:
    db = _FakeDatabase()

    summary = ensure_partition_horizon(
        db,
        reference_date=date(2024, 1, 10),
        months_ahead=0,
    )

    assert summary["partition_count"] == 2
    assert summary["first_partition"] == "bronze.catapult_stats_2024_01"
    assert any("CREATE TABLE IF NOT EXISTS bronze.catapult_stats_2024_01" in sql for sql in db.executed)
    assert any("ix_catapult_stats_2024_01_all_parameters" in sql for sql in db.executed)
    assert any("ix_catapult_sensor_data_2024_01_athlete_time" in sql for sql in db.executed)
