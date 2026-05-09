"""
Helpers for resolving Europe/Lisbon calendar-day windows in UTC.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

_LISBON_TZ = ZoneInfo("Europe/Lisbon")
_UTC = timezone.utc


@dataclass(frozen=True)
class UtcDayWindow:
    """Inclusive/exclusive UTC window for one Europe/Lisbon calendar day."""

    day_start_utc: datetime
    day_end_utc: datetime

    def as_summary(self) -> dict[str, str]:
        """Return ISO-8601 UTC timestamps suitable for logs and summaries."""
        return {
            "day_start_utc": format_utc_timestamp(self.day_start_utc),
            "day_end_utc": format_utc_timestamp(self.day_end_utc),
        }


def resolve_lisbon_day_window_from_date(day: date) -> UtcDayWindow:
    """Return the UTC bounds for an explicit Europe/Lisbon calendar date."""
    day_start_local = datetime(day.year, day.month, day.day, tzinfo=_LISBON_TZ)
    day_end_local = day_start_local + timedelta(days=1)
    return UtcDayWindow(
        day_start_utc=day_start_local.astimezone(_UTC),
        day_end_utc=day_end_local.astimezone(_UTC),
    )


def resolve_lisbon_day_window_utc(
    reference_time: datetime | None = None,
) -> UtcDayWindow:
    """Return the UTC bounds for the Europe/Lisbon calendar day of ``reference_time``."""
    if reference_time is None:
        current_local = datetime.now(_LISBON_TZ)
    else:
        if reference_time.tzinfo is None:
            raise ValueError("reference_time must be timezone-aware.")
        current_local = reference_time.astimezone(_LISBON_TZ)

    day_start_local = current_local.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end_local = day_start_local + timedelta(days=1)
    return UtcDayWindow(
        day_start_utc=day_start_local.astimezone(_UTC),
        day_end_utc=day_end_local.astimezone(_UTC),
    )


def format_utc_timestamp(value: datetime) -> str:
    """Return an aware timestamp formatted as an ISO-8601 UTC string."""
    if value.tzinfo is None:
        raise ValueError("UTC timestamp formatting requires a timezone-aware datetime.")
    return value.astimezone(_UTC).isoformat().replace("+00:00", "Z")
