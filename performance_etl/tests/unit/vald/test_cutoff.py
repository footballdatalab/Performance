from __future__ import annotations

from datetime import datetime, timezone

from ingestion.vald.cutoff import (
    VALD_CUTOFF_UTC,
    clamp_vald_watermark,
    effective_timestamp_at_or_after_cutoff,
    is_on_or_after_vald_cutoff,
    resolve_vald_modified_from_utc,
)


def test_clamp_vald_watermark_never_returns_pre_cutoff_timestamp() -> None:
    assert clamp_vald_watermark(None) == VALD_CUTOFF_UTC
    assert clamp_vald_watermark("1970-01-01T00:00:00Z") == VALD_CUTOFF_UTC
    assert clamp_vald_watermark("2024-06-30T22:59:59Z") == VALD_CUTOFF_UTC
    assert clamp_vald_watermark("2024-06-30T23:00:00Z") == "2024-06-30T23:00:00Z"


def test_effective_timestamp_honors_field_priority_for_cutoff_filtering() -> None:
    payload = {
        "recordedDateUtc": "2024-06-30T22:00:00Z",
        "modifiedDateUtc": "2024-07-05T12:00:00Z",
    }

    assert (
        effective_timestamp_at_or_after_cutoff(
            payload,
            ("recordedDateUtc", "modifiedDateUtc"),
        )
        is None
    )


def test_is_on_or_after_vald_cutoff_accepts_exact_cutoff() -> None:
    assert is_on_or_after_vald_cutoff("2024-06-30T23:00:00Z")
    assert not is_on_or_after_vald_cutoff("2024-06-30T22:59:59Z")


def test_resolve_vald_modified_from_utc_can_clamp_to_current_lisbon_day() -> None:
    reference_time = datetime(2026, 4, 7, 12, 0, tzinfo=timezone.utc)

    assert (
        resolve_vald_modified_from_utc(
            "2026-04-06T10:00:00Z",
            intraday_current_day_only=True,
            reference_time=reference_time,
        )
        == "2026-04-06T23:00:00Z"
    )
    assert (
        resolve_vald_modified_from_utc(
            "2026-04-07T10:00:00Z",
            intraday_current_day_only=True,
            reference_time=reference_time,
        )
        == "2026-04-07T10:00:00Z"
    )
