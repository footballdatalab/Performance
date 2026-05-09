"""
VALD SmartSpeed extractor.

Incrementally captures SmartSpeed test summary raw payloads using the
page-watermark synchronisation pattern. The watermark is saved as the
request timestamp captured before the first API call.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from ingestion.common.batch import BatchManager
from ingestion.common.logging import get_logger
from ingestion.common.watermark import WatermarkManager
from ingestion.vald.client import ValdClient
from ingestion.vald.cutoff import (
    clamp_vald_watermark,
    effective_timestamp_at_or_after_cutoff,
    resolve_vald_modified_from_utc,
)
from ingestion.vald.endpoints.smartspeed import SmartSpeedEndpoint
from ingestion.vald.loaders.raw_loader import ValdRawLoader

logger = get_logger(__name__)

_PROVIDER = "vald"
_SOURCE_ACCOUNT = "vald_default"
_API_NAME = "smartspeed_tests"
_TEST_TIMESTAMP_FIELDS = (
    "testDateUtc",
    "testDateUTC",
    "modifiedDateUtc",
    "modifiedDateUTC",
)


class SmartSpeedExtractor:
    """Incremental SmartSpeed raw extractor."""

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
        self._endpoint = SmartSpeedEndpoint(vald_client.smartspeed_client)

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
            "SmartSpeed: starting raw extraction for tenant=%s watermark=%s request_watermark=%s request_timestamp=%s",
            tenant_id,
            watermark,
            request_watermark,
            request_timestamp,
        )

        while True:
            summaries = self._endpoint.get_test_summaries(
                team_id=tenant_id,
                page=page,
                modified_from_utc=request_watermark,
            )

            if not summaries:
                logger.info(
                    "SmartSpeed: no more data for tenant=%s at page=%d",
                    tenant_id,
                    page,
                )
                break

            filtered_summaries = [
                summary
                for summary in summaries
                if effective_timestamp_at_or_after_cutoff(summary, _TEST_TIMESTAMP_FIELDS)
            ]
            total_extracted += len(filtered_summaries)
            total_loaded += len(filtered_summaries)
            touched_test_ids.extend(
                str(summary.get("id") or summary.get("testId"))
                for summary in filtered_summaries
                if summary.get("id") or summary.get("testId")
            )

            if filtered_summaries:
                self.raw_loader.load_raw(
                    table_name="vald_smartspeed_test_summaries",
                    api_endpoint=f"/v1/team/{tenant_id}/tests?page={page}&modifiedFromUtc={request_watermark}",
                    response_payload=filtered_summaries,
                    request_params={
                        "teamId": tenant_id,
                        "page": page,
                        "modifiedFromUtc": request_watermark,
                    },
                    response_status=200,
                    page_number=page,
                    api_version="v1",
                )

            logger.info(
                "SmartSpeed: fetched %d in-range summaries for tenant=%s page=%d (running total=%d)",
                len(filtered_summaries),
                tenant_id,
                page,
                total_extracted,
            )
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
            details_loaded = self._capture_details_raw(tenant_id, touched_test_ids)
        except Exception as exc:
            logger.warning("SmartSpeed: detail capture failed: %s", exc)
            details_loaded = 0

        summary = {
            "records_extracted": total_extracted,
            "records_loaded": total_loaded,
            "details_loaded": details_loaded,
            "request_modified_from_utc": request_watermark,
            "new_watermark": request_timestamp if total_extracted > 0 else watermark,
        }
        logger.info("SmartSpeed: extraction complete for tenant=%s: %s", tenant_id, summary)
        return summary

    def _capture_details_raw(self, tenant_id: str, test_ids: list[str]) -> int:
        """Fetch and store raw detail payloads for the touched tests."""
        ordered_test_ids = list(dict.fromkeys(test_ids))
        if not ordered_test_ids:
            logger.info("SmartSpeed: no touched tests for detail capture")
            return 0

        total = 0
        for index, test_id in enumerate(ordered_test_ids, start=1):
            try:
                detail = self._endpoint.get_test_detail(tenant_id, test_id)
                if detail:
                    self.raw_loader.load_raw(
                        table_name="vald_smartspeed_test_details",
                        api_endpoint=f"/teams/{tenant_id}/tests/{test_id}",
                        response_payload=detail,
                        request_params={"teamId": tenant_id, "testId": test_id},
                        api_version="v1",
                    )
                    total += 1
            except Exception as exc:
                logger.warning("SmartSpeed: detail failed for test %s: %s", test_id, exc)

            if index % 200 == 0:
                logger.info("SmartSpeed: details progress %d/%d", index, len(ordered_test_ids))

        return total
