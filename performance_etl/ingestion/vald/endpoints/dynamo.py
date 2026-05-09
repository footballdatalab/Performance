"""
Endpoint wrapper for the VALD DynaMo API.

Provides methods to retrieve dynamometer tests, test details, and
force-trace data with paginated, watermark-based synchronisation.
"""

from __future__ import annotations

from typing import Any

import requests

from ingestion.common.http_client import BaseHttpClient
from ingestion.common.logging import get_logger

logger = get_logger(__name__)


class DynaMoEndpoint:
    """Methods for the VALD External DynaMo API.

    Args:
        client: A :class:`BaseHttpClient` configured for the ``dynamo``
            product (see :meth:`ValdClient.dynamo_client`).
    """

    def __init__(self, client: BaseHttpClient) -> None:
        self.client = client

    def get_tests(
        self,
        tenant_id: str,
        modified_from_utc: str,
        test_from_utc: str,
        test_to_utc: str,
        page: int = 1,
        include_rep_summaries: bool = False,
        include_reps: bool = False,
    ) -> dict:
        """Retrieve DynaMo tests for a tenant, paginated.

        Pagination: the response contains ``currentPage`` and ``totalPages``.
        Terminate when ``currentPage >= totalPages``.

        Args:
            tenant_id: UUID of the tenant.
            modified_from_utc: ISO-8601 UTC timestamp watermark.
            test_from_utc: Start of the test-date range (ISO-8601 UTC).
            test_to_utc: End of the test-date range (ISO-8601 UTC).
            page: 1-based page number (default ``1``).
            include_rep_summaries: When ``True``, include repetition
                summaries in the response.
            include_reps: When ``True``, include full repetition data
                in the response.

        Returns:
            Response dict with keys ``items``, ``currentPage``,
            ``totalItems``, and ``totalPages``.

        API:
            ``GET /v2022q2/teams/{tenant_id}/tests?modifiedFromUtc=...
            &testFromUtc=...&testToUtc=...&page=...``
        """
        params: dict[str, Any] = {
            "modifiedFromUtc": modified_from_utc,
            "testFromUtc": test_from_utc,
            "testToUtc": test_to_utc,
            "page": page,
        }
        if include_rep_summaries:
            params["includeRepSummaries"] = True
        if include_reps:
            params["includeReps"] = True

        logger.debug(
            "Fetching DynaMo tests — tenant=%s page=%d params=%s",
            tenant_id,
            page,
            params,
        )
        try:
            response = self.client.get(
                f"/v2022q2/teams/{tenant_id}/tests", params=params
            )
        except requests.exceptions.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                logger.info(
                    "DynaMo tests returned 404; treating as empty page "
                    "(tenant=%s page=%d modified_from=%s)",
                    tenant_id,
                    page,
                    modified_from_utc,
                )
                return {
                    "items": [],
                    "currentPage": page,
                    "totalItems": 0,
                    "totalPages": page,
                }
            raise
        return response.json()

    def get_test_detail(self, tenant_id: str, test_id: str) -> dict:
        """Retrieve the full detail for a specific DynaMo test.

        Args:
            tenant_id: UUID of the tenant.
            test_id: UUID of the test.

        Returns:
            Test-detail object.

        API:
            ``GET /v2022q2/teams/{tenant_id}/tests/{test_id}``
        """
        logger.debug(
            "Fetching DynaMo test detail — tenant=%s test=%s",
            tenant_id,
            test_id,
        )
        response = self.client.get(
            f"/v2022q2/teams/{tenant_id}/tests/{test_id}"
        )
        return response.json()

    def get_test_trace(self, tenant_id: str, test_id: str) -> dict:
        """Retrieve the force-trace data for a specific DynaMo test.

        Args:
            tenant_id: UUID of the tenant.
            test_id: UUID of the test.

        Returns:
            Trace-data object.

        API:
            ``GET /v2022q2/teams/{tenant_id}/tests/{test_id}/trace``
        """
        logger.debug(
            "Fetching DynaMo test trace — tenant=%s test=%s",
            tenant_id,
            test_id,
        )
        response = self.client.get(
            f"/v2022q2/teams/{tenant_id}/tests/{test_id}/trace"
        )
        return response.json()
