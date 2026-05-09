"""
Shared utility functions for data parsing, validation and hashing.

Every helper returns ``None`` on invalid input rather than raising, making
them safe to use inline when transforming untrusted API payloads.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

# Pre-compiled patterns
_ISO_FALLBACK_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}"           # date
    r"[T ]\d{2}:\d{2}:\d{2}"        # time
    r"(\.\d+)?"                      # fractional seconds (optional)
    r"(Z|[+-]\d{2}:\d{2})?$"        # timezone (optional)
)

_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)


def parse_timestamp(value: str | None) -> datetime | None:
    """Parse an ISO-8601 timestamp string into an aware ``datetime``.

    Handles common API variants including trailing ``Z``, 7-digit fractional
    seconds (as used by .NET APIs), and ``+00:00`` offsets.

    Args:
        value: Timestamp string to parse.

    Returns:
        An aware ``datetime`` in UTC, or ``None`` if parsing fails.
    """
    if not value or not isinstance(value, str):
        return None
    value = value.strip()
    if not value:
        return None

    # Normalise trailing "Z" to "+00:00" for fromisoformat
    normalised = value.replace("Z", "+00:00")

    # Python's fromisoformat (3.11+) handles most variants.
    try:
        dt = datetime.fromisoformat(normalised)
        # If naive, assume UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, TypeError):
        pass

    # Fallback: truncate fractional seconds to 6 digits if longer
    if _ISO_FALLBACK_RE.match(value):
        truncated = re.sub(
            r"(\.\d{6})\d+",
            r"\1",
            value,
        ).replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(truncated)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except (ValueError, TypeError):
            pass

    return None


def safe_uuid(value: Any) -> str | None:
    """Validate and return a UUID string, or ``None`` if invalid.

    Args:
        value: Candidate UUID value (string or UUID object).

    Returns:
        Lowercase canonical UUID string, or ``None``.
    """
    if value is None:
        return None
    s = str(value).strip().lower()
    if _UUID_RE.match(s):
        return s
    # Attempt construction to catch non-hyphenated forms
    try:
        return str(uuid.UUID(s))
    except (ValueError, AttributeError):
        return None


def safe_numeric(value: Any) -> Decimal | None:
    """Convert a value to :class:`Decimal`, or ``None`` on failure.

    Args:
        value: Numeric-like value (int, float, string).

    Returns:
        ``Decimal`` instance, or ``None``.
    """
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def safe_int(value: Any) -> int | None:
    """Convert a value to ``int``, or ``None`` on failure.

    Handles float strings like ``"3.0"`` by truncating.

    Args:
        value: Integer-like value.

    Returns:
        ``int`` instance, or ``None``.
    """
    if value is None:
        return None
    try:
        return int(float(str(value)))
    except (ValueError, TypeError):
        return None


def safe_str(value: Any, max_len: int | None = None) -> str | None:
    """Convert a value to a stripped string, or ``None`` if blank.

    Args:
        value: Any value.
        max_len: Optional maximum length; the string will be truncated if
            exceeded.

    Returns:
        Non-empty string, or ``None``.
    """
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    if max_len is not None and len(s) > max_len:
        s = s[:max_len]
    return s


def hash_payload(payload: dict[str, Any]) -> str:
    """Return the SHA-256 hex digest of a deterministically serialised dict.

    Keys are sorted so that logically identical payloads produce the same
    hash regardless of insertion order.

    Args:
        payload: Dictionary to hash.

    Returns:
        64-character lowercase hex string.
    """
    canonical = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
