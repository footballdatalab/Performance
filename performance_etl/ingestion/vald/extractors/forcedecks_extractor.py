"""
VALD ForceDecks extractor.

Incrementally captures ForceDecks raw payloads using watermark-based
synchronisation. Each API call returns tests modified since the
watermark. A 204 No Content (empty list) signals sync completion.
"""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
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
from ingestion.vald.endpoints.forcedecks import ForceDecksEndpoint
from ingestion.vald.loaders.raw_loader import ValdRawLoader

logger = get_logger(__name__)

_PROVIDER = "vald"
_SOURCE_ACCOUNT = "vald_default"
_API_NAME = "forcedecks_tests"
_TEST_TIMESTAMP_FIELDS = (
    "recordedDateUtc",
    "recordedDateUTC",
    "testDateUtc",
    "testDateUTC",
    "analysedDateUtc",
    "analysedDateUTC",
    "modifiedDateUtc",
    "modifiedDateUTC",
)
_MODIFIED_TIMESTAMP_FIELDS = ("modifiedDateUtc", "modifiedDateUTC")
_DEFAULT_TRIALS_WORKERS = 16


def _resolve_trials_workers() -> int:
    raw_value = os.environ.get("VALD_FD_TRIALS_WORKERS")
    if raw_value in (None, ""):
        return _DEFAULT_TRIALS_WORKERS
    try:
        parsed = int(raw_value)
    except (TypeError, ValueError):
        return _DEFAULT_TRIALS_WORKERS
    return max(1, parsed)


class ForceDecksExtractor:
    """Incremental ForceDecks raw extractor."""

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
        self._endpoint = ForceDecksEndpoint(vald_client.forcedecks_client)

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
            "ForceDecks: starting raw extraction for tenant=%s watermark=%s request_watermark=%s",
            tenant_id,
            watermark,
            request_watermark,
        )

        current_watermark = request_watermark
        while True:
            tests = self._endpoint.get_tests(
                tenant_id=tenant_id,
                modified_from_utc=current_watermark,
            )

            if not tests:
                logger.info(
                    "ForceDecks: no more data for tenant=%s (204/empty)",
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
                    table_name="vald_forcedecks_tests",
                    api_endpoint=f"/tests?tenantId={tenant_id}&modifiedFromUtc={current_watermark}",
                    response_payload=filtered_tests,
                    request_params={
                        "tenantId": tenant_id,
                        "modifiedFromUtc": current_watermark,
                    },
                    api_version="v2020q1",
                )

            latest_modified = max_timestamp(
                test.get(field_name)
                for test in tests
                for field_name in _MODIFIED_TIMESTAMP_FIELDS
            )
            if latest_modified and (max_modified is None or latest_modified > max_modified):
                max_modified = latest_modified

            if max_modified:
                current_watermark = max_modified

            logger.info(
                "ForceDecks: fetched %d tests for tenant=%s (running total=%d)",
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
            self._extract_result_definitions()
        except Exception as exc:
            logger.warning("ForceDecks: result_definitions failed: %s", exc)

        try:
            trials_loaded = self._capture_trials_raw(tenant_id, captured_test_ids)
            logger.info("ForceDecks: captured %d trials", trials_loaded)
        except Exception as exc:
            logger.warning("ForceDecks: trial capture failed: %s", exc)
            trials_loaded = 0

        summary = {
            "records_extracted": total_extracted,
            "records_loaded": total_loaded,
            "trials_loaded": trials_loaded,
            "request_modified_from_utc": request_watermark,
            "new_watermark": max_modified or watermark,
        }
        logger.info("ForceDecks: extraction complete for tenant=%s: %s", tenant_id, summary)
        return summary

    def _extract_result_definitions(self) -> int:
        """Fetch and store ForceDecks result definitions."""
        definitions = self._endpoint.get_result_definitions()
        if not definitions:
            return 0

        if isinstance(definitions, dict):
            definitions = definitions.get("resultDefinitions", definitions.get("items", [definitions]))
        if not isinstance(definitions, list):
            definitions = [definitions]

        self.raw_loader.load_raw(
            table_name="vald_forcedecks_result_definitions",
            api_endpoint="/result-definitions",
            response_payload=definitions,
            api_version="v2020q1",
        )
        logger.info("ForceDecks: stored %d result definitions", len(definitions))
        return len(definitions)

    def _capture_trials_raw(self, tenant_id: str, test_ids: list[str]) -> int:
        """Fetch and store raw trial payloads for the touched tests.

        Trials are fetched and persisted concurrently because each call is an
        independent, IO-bound HTTP round-trip. The pooled DB connection used by
        ``ValdRawLoader.load_raw`` is borrowed per-insert, so worker threads do
        not share connection state.
        """
        ordered_test_ids = list(dict.fromkeys(test_ids))
        if not ordered_test_ids:
            logger.info("ForceDecks: no touched tests for trial capture")
            return 0

        max_workers = min(_resolve_trials_workers(), len(ordered_test_ids))
        logger.info(
            "ForceDecks: fetching trials for %d tests with %d workers (tenant=%s)",
            len(ordered_test_ids),
            max_workers,
            tenant_id,
        )

        def _fetch_and_store(test_id: str) -> int:
            try:
                trials = self._endpoint.get_trials(tenant_id, test_id)
            except Exception as exc:
                logger.warning(
                    "ForceDecks: failed to get trials for test %s: %s",
                    test_id,
                    exc,
                )
                return 0
            if not trials:
                return 0
            try:
                self.raw_loader.load_raw(
                    table_name="vald_forcedecks_trials",
                    api_endpoint=f"/tests/{test_id}/trials",
                    response_payload=trials,
                    request_params={"teamId": tenant_id, "testId": test_id},
                    api_version="v2020q1",
                )
            except Exception as exc:
                logger.warning(
                    "ForceDecks: failed to persist trials for test %s: %s",
                    test_id,
                    exc,
                )
                return 0
            return len(trials)

        total_loaded = 0
        completed = 0
        with ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="vald-fd-trials",
        ) as pool:
            futures = {
                pool.submit(_fetch_and_store, test_id): test_id
                for test_id in ordered_test_ids
            }
            for future in as_completed(futures):
                total_loaded += future.result()
                completed += 1
                if completed % 500 == 0:
                    logger.info(
                        "ForceDecks: trials progress %d/%d tests",
                        completed,
                        len(ordered_test_ids),
                    )

        return total_loaded
