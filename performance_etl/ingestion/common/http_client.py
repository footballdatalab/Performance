"""
Base HTTP client with retry logic, rate limiting and timeout support.

Built on top of :mod:`requests` and :mod:`tenacity`.  Intended to be
sub-classed or composed by each provider's API client.
"""

from __future__ import annotations

import time
from typing import Any

import requests
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential_jitter,
)

from ingestion.common.logging import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Retry predicate
# ---------------------------------------------------------------------------


def _is_retryable(exc: BaseException) -> bool:
    """Return ``True`` for HTTP 429 or 5xx status codes."""
    if isinstance(exc, requests.exceptions.HTTPError):
        status = exc.response.status_code if exc.response is not None else 0
        return status == 429 or 500 <= status < 600
    # Retry on connection / timeout errors as well.
    if isinstance(exc, (requests.exceptions.ConnectionError, requests.exceptions.Timeout)):
        return True
    return False


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class BaseHttpClient:
    """Thin HTTP wrapper with retries, rate-limiting and default headers.

    Args:
        base_url: API root URL (no trailing slash).
        headers: Extra headers merged on every request.  A ``Bearer`` token
            can be supplied via ``{"Authorization": "Bearer <tok>"}``.
        rate_limit_ms: Minimum milliseconds to wait between successive
            requests.  Defaults to ``200``.
        timeout: Per-request timeout in seconds.  Defaults to ``60``.
        max_retries: Maximum number of retry attempts (including the initial
            call).  Defaults to ``3``.
    """

    def __init__(
        self,
        base_url: str,
        headers: dict[str, str] | None = None,
        rate_limit_ms: int = 200,
        timeout: int = 60,
        max_retries: int = 3,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self._rate_limit_s = rate_limit_ms / 1000.0
        self._last_request_ts: float = 0.0

        self._session = requests.Session()
        if headers:
            self._session.headers.update(headers)

    # ------------------------------------------------------------------
    # Rate limiting
    # ------------------------------------------------------------------

    def _wait_for_rate_limit(self) -> None:
        """Block until the rate-limit window has elapsed."""
        elapsed = time.monotonic() - self._last_request_ts
        remaining = self._rate_limit_s - elapsed
        if remaining > 0:
            time.sleep(remaining)
        self._last_request_ts = time.monotonic()

    # ------------------------------------------------------------------
    # Core request (with retry)
    # ------------------------------------------------------------------

    def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
    ) -> requests.Response:
        """Execute a single HTTP request with retry and rate limiting.

        Args:
            method: HTTP method (``GET``, ``POST``, etc.).
            path: URL path appended to *base_url* (leading slash optional).
            params: Query-string parameters.
            json: JSON body payload.

        Returns:
            :class:`requests.Response` on success.

        Raises:
            requests.exceptions.HTTPError: After all retries exhausted.
        """
        url = f"{self.base_url}/{path.lstrip('/')}"

        # Build a tenacity retry wrapper dynamically so max_retries is per-instance.
        @retry(
            retry=retry_if_exception(_is_retryable),
            stop=stop_after_attempt(self.max_retries),
            wait=wait_exponential_jitter(initial=1, max=60, jitter=2),
            reraise=True,
        )
        def _do_request() -> requests.Response:
            self._wait_for_rate_limit()
            logger.debug("HTTP %s %s params=%s", method, url, params)
            resp = self._session.request(
                method,
                url,
                params=params,
                json=json,
                timeout=self.timeout,
            )
            resp.raise_for_status()
            return resp

        return _do_request()

    # ------------------------------------------------------------------
    # Public convenience methods
    # ------------------------------------------------------------------

    def get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> requests.Response:
        """Send a GET request.

        Args:
            path: URL path relative to *base_url*.
            params: Optional query-string parameters.

        Returns:
            :class:`requests.Response`.
        """
        return self._request("GET", path, params=params)

    def post(
        self,
        path: str,
        json: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> requests.Response:
        """Send a POST request.

        Args:
            path: URL path relative to *base_url*.
            json: Optional JSON body.
            params: Optional query-string parameters.

        Returns:
            :class:`requests.Response`.
        """
        return self._request("POST", path, params=params, json=json)

    def put(
        self,
        path: str,
        json: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> requests.Response:
        """Send a PUT request.

        Args:
            path: URL path relative to *base_url*.
            json: Optional JSON body.
            params: Optional query-string parameters.

        Returns:
            :class:`requests.Response`.
        """
        return self._request("PUT", path, params=params, json=json)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Close the underlying ``requests.Session``."""
        self._session.close()

    def __enter__(self) -> "BaseHttpClient":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
