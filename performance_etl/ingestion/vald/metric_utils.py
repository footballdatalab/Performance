"""
Helpers for deterministic VALD long-fact metric rows.
"""

from __future__ import annotations

import hashlib
from decimal import Decimal, InvalidOperation
from typing import Any


def normalise_metric_value_for_key(value: Any) -> str:
    """Return a stable string representation for metric-row hashing."""
    try:
        numeric = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return str(value).strip()

    if not numeric.is_finite():
        return str(value).strip()

    normalised = format(numeric.normalize(), "f")
    if "." in normalised:
        normalised = normalised.rstrip("0").rstrip(".")
    if normalised in {"", "-0"}:
        return "0"
    return normalised


def build_metric_row_key(
    *,
    provider_profile_id: str,
    team_group_id: str,
    test_id: str,
    assessment_family: str,
    source_module: str,
    metric_name: str,
    side: str | None,
    rep_number: int | None,
    metric_value: Any,
    source_row_hint: str | None = None,
) -> str:
    """Build the stable hash key for a single silver metric row."""
    parts = (
        str(provider_profile_id).strip(),
        str(team_group_id).strip(),
        str(test_id).strip(),
        str(assessment_family).strip(),
        str(source_module).strip(),
        str(metric_name).strip(),
        str(side or "").strip(),
        "" if rep_number is None else str(rep_number),
        normalise_metric_value_for_key(metric_value),
        str(source_row_hint or "").strip(),
    )
    return hashlib.md5("|".join(parts).encode("utf-8"), usedforsecurity=False).hexdigest()
