"""
VALD NordBord extractor.

Incrementally captures NordBord raw payloads using watermark-based
synchronisation (v2 API). A 204 No Content (empty list) signals
sync completion.
"""

from __future__ import annotations

from typing import Any

from ingestion.common.batch import BatchManager
from ingestion.common.logging import get_logger
from ingestion.common.watermark import WatermarkManager
from ingestion.vald.client import ValdClient
from ingestion.vald.cutoff import (
    clamp_vald_watermark,
    effective_timestamp_at_or_after_cutoff,
    max_timestamp,
    resolve_vald_modified_from_utc,
)
from ingestion.vald.endpoints.nordbord import NordBordEndpoint
from ingestion.vald.loaders.raw_loader import ValdRawLoader

logger = get_logger(__name__)

_PROVIDER = "vald"
_SOURCE_ACCOUNT = "vald_default"
_API_NAME = "nordbord_tests"
_TEST_TIMESTAMP_FIELDS = (
    "testDateUtc",
    "testDateUTC",
    "modifiedDateUtc",
    "modifiedDateUTC",
)
_ECC_EXERCISE_TIMESTAMP_FIELDS = (
    "exerciseDateUtc",
    "exerciseDateUTC",
    "modifiedDateUtc",
    "modifiedDateUTC",
    "modifiedUtc",
)
_ECC_REPETITION_TIMESTAMP_FIELDS = (
    "repetitionDateUtc",
    "repetitionDateUTC",
    "modifiedDateUtc",
    "modifiedDateUTC",
    "modifiedUtc",
)
_MODIFIED_TIMESTAMP_FIELDS = ("modifiedDateUtc", "modifiedDateUTC")


class NordBordExtractor:
    """Incremental NordBord raw extractor."""

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
        self._endpoint = NordBordEndpoint(vald_client.nordbord_client)

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
        max_modified: str | None = None
        captured_test_ids: list[str] = []

        logger.info(
            "NordBord: starting raw extraction for tenant=%s watermark=%s request_watermark=%s",
            tenant_id,
            watermark,
            request_watermark,
        )

        current_watermark = request_watermark
        prev_watermark: str | None = None
        while True:
            tests = self._endpoint.get_tests_v2(
                tenant_id=tenant_id,
                modified_from_utc=current_watermark,
            )

            if not tests:
                logger.info(
                    "NordBord: no more data for tenant=%s (204/empty)",
                    tenant_id,
                )
                break

            filtered_tests = [
                test
                for test in tests
                if effective_timestamp_at_or_after_cutoff(test, _TEST_TIMESTAMP_FIELDS)
            ]
            total_extracted += len(filtered_tests)
            total_loaded += len(filtered_tests)
            captured_test_ids.extend(
                str(test.get("id") or test.get("testId"))
                for test in filtered_tests
                if test.get("id") or test.get("testId")
            )
            if filtered_tests:
                self.raw_loader.load_raw(
                    table_name="vald_nordbord_tests",
                    api_endpoint=f"/tests/v2?tenantId={tenant_id}&modifiedFromUtc={current_watermark}",
                    response_payload=filtered_tests,
                    request_params={
                        "tenantId": tenant_id,
                        "modifiedFromUtc": current_watermark,
                    },
                    api_version="v2",
                )

            latest_modified = max_timestamp(
                test.get(field_name)
                for test in tests
                for field_name in _MODIFIED_TIMESTAMP_FIELDS
            )
            if latest_modified and (max_modified is None or latest_modified > max_modified):
                max_modified = latest_modified

            if max_modified:
                if max_modified == prev_watermark:
                    logger.info(
                        "NordBord: watermark stalled at %s; breaking to avoid infinite loop",
                        max_modified,
                    )
                    break
                prev_watermark = current_watermark
                current_watermark = max_modified

            logger.info(
                "NordBord: fetched %d tests for tenant=%s (running total=%d)",
                len(tests),
                tenant_id,
                total_extracted,
            )

        if max_modified:
            self.watermark_mgr.update_watermark(
                provider=_PROVIDER,
                source_account=_SOURCE_ACCOUNT,
                api_name=_API_NAME,
                watermark_value=max_modified,
                records_synced=total_loaded,
                tenant_id=tenant_id,
            )

        try:
            ecc_loaded = self._capture_ecc_raw(tenant_id, request_watermark)
        except Exception as exc:
            logger.warning("NordBord: ecc capture failed: %s", exc)
            ecc_loaded = 0

        try:
            metrics_loaded = self._capture_metrics_raw(tenant_id, captured_test_ids)
        except Exception as exc:
            logger.warning("NordBord: metric capture failed: %s", exc)
            metrics_loaded = 0

        summary = {
            "records_extracted": total_extracted,
            "records_loaded": total_loaded,
            "ecc_loaded": ecc_loaded,
            "metrics_loaded": metrics_loaded,
            "request_modified_from_utc": request_watermark,
            "new_watermark": max_modified or watermark,
        }
        logger.info("NordBord: extraction complete for tenant=%s: %s", tenant_id, summary)
        return summary

    def _capture_ecc_raw(self, tenant_id: str, watermark: str) -> int:
        """Fetch and store eccentric training raw payloads."""
        total = 0
        exercises = self._endpoint.get_ecc_exercises(tenant_id, modified_from_utc=watermark)
        filtered_exercises = [
            exercise
            for exercise in exercises or []
            if effective_timestamp_at_or_after_cutoff(
                exercise,
                _ECC_EXERCISE_TIMESTAMP_FIELDS,
            )
        ]
        if filtered_exercises:
            self.raw_loader.load_raw(
                table_name="vald_nordbord_ecc_exercises",
                api_endpoint="/training/exercises",
                response_payload=filtered_exercises,
                request_params={"tenantId": tenant_id, "modifiedFromUtc": watermark},
                api_version="v2",
            )
            total += len(filtered_exercises)

        reps = self._endpoint.get_ecc_repetitions(tenant_id, modified_from_utc=watermark)
        filtered_reps = [
            repetition
            for repetition in reps or []
            if effective_timestamp_at_or_after_cutoff(
                repetition,
                _ECC_REPETITION_TIMESTAMP_FIELDS,
            )
        ]
        if filtered_reps:
            self.raw_loader.load_raw(
                table_name="vald_nordbord_ecc_repetitions",
                api_endpoint="/training/repetitions",
                response_payload=filtered_reps,
                request_params={"tenantId": tenant_id, "modifiedFromUtc": watermark},
                api_version="v2",
            )
            total += len(filtered_reps)

        logger.info("NordBord: captured %d ecc records", total)
        return total

    def _capture_metrics_raw(self, tenant_id: str, test_ids: list[str]) -> int:
        """Fetch and store raw metric payloads for the touched tests."""
        ordered_test_ids = list(dict.fromkeys(test_ids))
        if not ordered_test_ids:
            logger.info("NordBord: no touched tests for metric capture")
            return 0

        total = 0
        for index, test_id in enumerate(ordered_test_ids, start=1):
            try:
                metrics = self._endpoint.get_test_metrics(tenant_id, test_id)
                if metrics:
                    self.raw_loader.load_raw(
                        table_name="vald_nordbord_test_metrics",
                        api_endpoint=f"/tests/{test_id}/metrics",
                        response_payload=metrics,
                        request_params={"tenantId": tenant_id, "testId": test_id},
                        api_version="v2",
                    )
                    total += 1
            except Exception as exc:
                logger.warning("NordBord: metrics failed for test %s: %s", test_id, exc)

            if index % 500 == 0:
                logger.info("NordBord: metrics progress %d/%d", index, len(ordered_test_ids))

        return total
