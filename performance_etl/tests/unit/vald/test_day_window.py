from __future__ import annotations

from datetime import datetime, timezone

import pytest

from ingestion.vald.day_window import format_utc_timestamp, resolve_lisbon_day_window_utc


def test_resolve_lisbon_day_window_utc_for_winter_day() -> None:
    window = resolve_lisbon_day_window_utc(
        datetime(2026, 1, 15, 12, 0, tzinfo=timezone.utc)
    )

    assert format_utc_timestamp(window.day_start_utc) == "2026-01-15T00:00:00Z"
    assert format_utc_timestamp(window.day_end_utc) == "2026-01-16T00:00:00Z"


def test_resolve_lisbon_day_window_utc_for_summer_day() -> None:
    window = resolve_lisbon_day_window_utc(
        datetime(2026, 7, 10, 12, 0, tzinfo=timezone.utc)
    )

    assert format_utc_timestamp(window.day_start_utc) == "2026-07-09T23:00:00Z"
    assert format_utc_timestamp(window.day_end_utc) == "2026-07-10T23:00:00Z"


def test_resolve_lisbon_day_window_rejects_naive_reference_time() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        resolve_lisbon_day_window_utc(datetime(2026, 3, 29, 12, 0))
