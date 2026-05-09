"""
Shared VALD cutoff helpers.

VALD assessment-bearing data before July 1, 2024 Europe/Lisbon is treated as
corrupted and must not be extracted or published downstream.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import datetime
from typing import Any

from ingestion.common.utils import parse_timestamp
from ingestion.vald.day_window import format_utc_timestamp, resolve_lisbon_day_window_utc

VALD_CUTOFF_LOCAL_DATE = "2024-07-01"
VALD_CUTOFF_UTC = "2024-06-30T23:00:00Z"
VALD_CUTOFF_DT = parse_timestamp(VALD_CUTOFF_UTC)

# Minimum-test-count gate: new tests on or after this date require ≥2 per family.
VALD_NEW_ENTRY_MIN_TESTS_LOCAL_DATE = "2026-04-06"  # Europe/Lisbon
VALD_NEW_ENTRY_MIN_TESTS_UTC = "2026-04-05T23:00:00Z"  # UTC (Lisbon UTC+1 in April)
VALD_NEW_ENTRY_MIN_TESTS_DT = parse_timestamp(VALD_NEW_ENTRY_MIN_TESTS_UTC)


def clamp_vald_watermark(value: str | None) -> str:
    """Return a watermark that never starts before the VALD cutoff."""
    cutoff_dt = _require_cutoff_datetime()
    parsed = parse_timestamp(value)
    if parsed is None or parsed < cutoff_dt:
        return VALD_CUTOFF_UTC
    return str(value)


def resolve_vald_modified_from_utc(
    value: str | None,
    *,
    intraday_current_day_only: bool = False,
    reference_time: datetime | None = None,
) -> str:
    """Return the ``modifiedFromUtc`` timestamp to send to VALD endpoints.

    The returned value always respects the global VALD cutoff. When
    ``intraday_current_day_only`` is enabled, the request watermark is also
    clamped to the start of the current Europe/Lisbon day in UTC so intraday
    extraction avoids scanning older modified tests.
    """
    watermark = clamp_vald_watermark(value)
    if not intraday_current_day_only:
        return watermark

    day_start_utc = format_utc_timestamp(
        resolve_lisbon_day_window_utc(reference_time).day_start_utc
    )
    watermark_dt = parse_timestamp(watermark)
    day_start_dt = parse_timestamp(day_start_utc)
    if day_start_dt is None:  # pragma: no cover - derived constant/runtime invariant
        raise ValueError("Failed to resolve the Lisbon day-start UTC timestamp.")
    if watermark_dt is None or watermark_dt < day_start_dt:
        return day_start_utc
    return watermark


def is_on_or_after_vald_cutoff(value: Any) -> bool:
    """Return True when *value* is an ISO timestamp on or after the cutoff."""
    if value is None:
        return False
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = parse_timestamp(str(value))
    cutoff_dt = _require_cutoff_datetime()
    return parsed is not None and parsed >= cutoff_dt


def is_on_or_after_new_entry_min_tests_date(value: Any) -> bool:
    """Return True when *value* is on or after the new-entry minimum-tests gate date."""
    if value is None:
        return False
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = parse_timestamp(str(value))
    if VALD_NEW_ENTRY_MIN_TESTS_DT is None:  # pragma: no cover
        raise ValueError("Invalid VALD new-entry min-tests date constant.")
    return parsed is not None and parsed >= VALD_NEW_ENTRY_MIN_TESTS_DT


def first_parseable_timestamp(
    payload: Mapping[str, Any],
    field_names: Iterable[str],
) -> str | None:
    """Return the first parseable timestamp from *payload* using the given priority."""
    for field_name in field_names:
        value = payload.get(field_name)
        if value and parse_timestamp(str(value)) is not None:
            return str(value)
    return None


def effective_timestamp_at_or_after_cutoff(
    payload: Mapping[str, Any],
    field_names: Iterable[str],
) -> str | None:
    """Return the effective timestamp only when the first parseable value clears the cutoff."""
    value = first_parseable_timestamp(payload, field_names)
    if value and is_on_or_after_vald_cutoff(value):
        return value
    return None


def max_timestamp(values: Iterable[Any]) -> str | None:
    """Return the latest parseable timestamp from *values* as the original string."""
    latest_value: str | None = None
    latest_dt: datetime | None = None
    for value in values:
        parsed = parse_timestamp(str(value)) if value is not None else None
        if parsed is None:
            continue
        if latest_dt is None or parsed > latest_dt:
            latest_dt = parsed
            latest_value = str(value)
    return latest_value


def _require_cutoff_datetime() -> datetime:
    if VALD_CUTOFF_DT is None:  # pragma: no cover - constant parse failure
        raise ValueError("Invalid VALD cutoff timestamp constant.")
    return VALD_CUTOFF_DT
