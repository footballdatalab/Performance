"""
VALD DynaMo extractor.

Incrementally captures DynaMo raw payloads using the page-watermark
synchronisation pattern. The watermark is saved as the request
timestamp captured before the first API call.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from ingestion.common.batch import BatchManager
from ingestion.common.logging import get_logger
from ingestion.common.watermark import WatermarkManager
from ingestion.vald.client import ValdClient
from ingestion.vald.cutoff import (
    VALD_CUTOFF_UTC,
    clamp_vald_watermark,
    effective_timestamp_at_or_after_cutoff,
    resolve_vald_modified_from_utc,
)
from ingestion.vald.endpoints.dynamo import DynaMoEndpoint
from ingestion.vald.loaders.raw_loader import ValdRawLoader

logger = get_logger(__name__)

_PROVIDER = "vald"
_SOURCE_ACCOUNT = "vald_default"
_API_NAME = "dynamo_tests"

_TEST_FROM_UTC = VALD_CUTOFF_UTC
_TEST_TO_UTC = "2099-12-31T23:59:59Z"
_TEST_TIMESTAMP_FIELDS = (
    "startTimeUtc",
    "startTimeUTC",
    "analysedDateUtc",
    "analysedDateUTC",
    "modifiedDateUtc",
    "modifiedDateUTC",
    "modifiedUtc",
)


class DynaMoExtractor:
    """Incremental DynaMo raw extractor."""

    def __init__(
        self,
        vald_client: ValdClient,
        raw_loader: ValdRawLoader,
        bronze_loader: object | None,
        watermark_mgr: WatermarkManager,
        batch_manager: BatchManager,
        intraday_current_day_only: bool = False,
    ) -> None:
        self.vald_client = vald_client
        self.raw_loader = raw_loader
        self.watermark_mgr = watermark_mgr
        self.batch_manager = batch_manager
        self._intraday_current_day_only = intraday_current_day_only
        self._endpoint = DynaMoEndpoint(vald_client.dynamo_client)

    def extract(self, tenant_id: str) -> dict[str, Any]:
        """Run incremental raw extraction for a single tenant."""
        watermark = self.watermark_mgr.get_watermark(
            provider=_PROVIDER,
            source_account=_SOURCE_ACCOUNT,
            api_name=_API_NAME,
            tenant_id=tenant_id,
        )
        watermark = clamp_vald_watermark(watermark)
        request_watermark = resolve_vald_modified_from_utc(
            watermark,
            intraday_current_day_only=self._intraday_current_day_only,
        )

        total_extracted = 0
        total_loaded = 0
        page = 1
        touched_test_ids: list[str] = []
        request_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        logger.info(
            "DynaMo: starting raw extraction for tenant=%s watermark=%s request_watermark=%s request_timestamp=%s",
            tenant_id,
            watermark,
            request_watermark,
            request_timestamp,
        )

        while True:
            response_data = self._endpoint.get_tests(
                tenant_id=tenant_id,
                modified_from_utc=request_watermark,
                test_from_utc=_TEST_FROM_UTC,
                test_to_utc=_TEST_TO_UTC,
                page=page,
                include_rep_summaries=True,
            )

            items = response_data.get("items", [])
            current_page = response_data.get("currentPage", page)
            total_pages = response_data.get("totalPages", 1)

            if items:
                filtered_items = [
                    item
                    for item in items
                    if effective_timestamp_at_or_after_cutoff(item, _TEST_TIMESTAMP_FIELDS)
                ]
                total_extracted += len(filtered_items)
                total_loaded += len(filtered_items)
                touched_test_ids.extend(
                    str(item.get("id") or item.get("testId"))
                    for item in filtered_items
                    if item.get("id") or item.get("testId")
                )
                if filtered_items:
                    filtered_response = dict(response_data)
                    filtered_response["items"] = filtered_items
                    self.raw_loader.load_raw(
                        table_name="vald_dynamo_tests",
                        api_endpoint=f"/v2022q2/teams/{tenant_id}/tests?modifiedFromUtc={request_watermark}&page={page}",
                        response_payload=filtered_response,
                        request_params={
                            "tenantId": tenant_id,
                            "modifiedFromUtc": request_watermark,
                            "testFromUtc": _TEST_FROM_UTC,
                            "testToUtc": _TEST_TO_UTC,
                            "page": page,
                        },
                        response_status=200,
                        page_number=page,
                        api_version="v2022q2",
                    )

                logger.info(
                    "DynaMo: fetched %d in-range tests for tenant=%s page=%d/%d (running total=%d)",
                    len(filtered_items),
                    tenant_id,
                    current_page,
                    total_pages,
                    total_extracted,
                )

            if current_page >= total_pages:
                logger.info(
                    "DynaMo: all pages consumed for tenant=%s (currentPage=%d, totalPages=%d)",
                    tenant_id,
                    current_page,
                    total_pages,
                )
                break

            page += 1

        if total_extracted > 0:
            self.watermark_mgr.update_watermark(
                provider=_PROVIDER,
                source_account=_SOURCE_ACCOUNT,
                api_name=_API_NAME,
                watermark_value=request_timestamp,
                records_synced=total_loaded,
                tenant_id=tenant_id,
            )

        try:
            reps_loaded = self._capture_details_raw(tenant_id, touched_test_ids)
        except Exception as exc:
            logger.warning("DynaMo: detail capture failed: %s", exc)
            reps_loaded = 0

        try:
            traces_loaded = self._capture_traces_raw(tenant_id, touched_test_ids)
        except Exception as exc:
            logger.warning("DynaMo: trace capture failed: %s", exc)
            traces_loaded = 0

        summary = {
            "records_extracted": total_extracted,
            "records_loaded": total_loaded,
            "reps_loaded": reps_loaded,
            "traces_loaded": traces_loaded,
            "request_modified_from_utc": request_watermark,
            "new_watermark": request_timestamp if total_extracted > 0 else watermark,
        }
        logger.info("DynaMo: extraction complete for tenant=%s: %s", tenant_id, summary)
        return summary

    def _capture_details_raw(self, tenant_id: str, test_ids: list[str]) -> int:
        """Fetch and store raw detail payloads for the touched tests."""
        ordered_test_ids = list(dict.fromkeys(test_ids))
        if not ordered_test_ids:
            logger.info("DynaMo: no touched tests for detail capture")
            return 0

        total = 0
        for index, test_id in enumerate(ordered_test_ids, start=1):
            try:
                detail = self._endpoint.get_test_detail(tenant_id, test_id)
                if detail:
                    self.raw_loader.load_raw(
                        table_name="vald_dynamo_test_details",
                        api_endpoint=f"/v2022q2/teams/{tenant_id}/tests/{test_id}",
                        response_payload=detail,
                        request_params={"tenantId": tenant_id, "testId": test_id},
                        api_version="v2022q2",
                    )
                    total += len(detail.get("repetitions", []))
            except Exception as exc:
                logger.warning("DynaMo: detail failed for test %s: %s", test_id, exc)

            if index % 500 == 0:
                logger.info("DynaMo: details progress %d/%d", index, len(ordered_test_ids))

        return total

    def _capture_traces_raw(self, tenant_id: str, test_ids: list[str]) -> int:
        """Fetch and store raw trace payloads for the touched tests."""
        ordered_test_ids = list(dict.fromkeys(test_ids))
        if not ordered_test_ids:
            logger.info("DynaMo: no touched tests for trace capture")
            return 0

        total = 0
        for index, test_id in enumerate(ordered_test_ids, start=1):
            try:
                trace = self._endpoint.get_test_trace(tenant_id, test_id)
                if trace:
                    self.raw_loader.load_raw(
                        table_name="vald_dynamo_traces",
                        api_endpoint=f"/v2022q2/teams/{tenant_id}/tests/{test_id}/trace",
                        response_payload=trace,
                        request_params={"tenantId": tenant_id, "testId": test_id},
                        api_version="v2022q2",
                    )
                    total += 1
            except Exception as exc:
                logger.warning("DynaMo: trace failed for test %s: %s", test_id, exc)

            if index % 500 == 0:
                logger.info("DynaMo: traces progress %d/%d", index, len(ordered_test_ids))

        return total
