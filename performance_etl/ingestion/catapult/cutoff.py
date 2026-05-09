"""
Shared Catapult cutoff helpers.

Catapult activity data before 2024-07-01 Europe/Lisbon is out of scope and
must not be extracted, replayed, or published downstream.
"""

from __future__ import annotations

from datetime import datetime, timezone

from ingestion.common.utils import parse_timestamp

CATAPULT_CUTOFF_LOCAL_DATE = "2024-07-01"
CATAPULT_CUTOFF_UTC = "2024-06-30T23:00:00Z"
CATAPULT_CUTOFF_DT = parse_timestamp(CATAPULT_CUTOFF_UTC)


def require_catapult_cutoff_datetime() -> datetime:
    if CATAPULT_CUTOFF_DT is None:  # pragma: no cover - constant parse failure
        raise ValueError("Invalid Catapult cutoff timestamp constant.")
    return CATAPULT_CUTOFF_DT


def clamp_catapult_start_time(value: datetime) -> datetime:
    """Return *value* clamped so it never starts before the Catapult cutoff."""
    cutoff = require_catapult_cutoff_datetime()
    value_aware = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    return value_aware if value_aware >= cutoff else cutoff


def is_on_or_after_catapult_cutoff(value: datetime | str | None) -> bool:
    """Return True when *value* is on or after the Catapult cutoff."""
    if value is None:
        return False
    parsed = value if isinstance(value, datetime) else parse_timestamp(str(value))
    if parsed is None:
        return False
    parsed_aware = parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)
    return parsed_aware >= require_catapult_cutoff_datetime()
