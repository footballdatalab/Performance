"""
VALD HumanTrak extractor.

Incrementally extracts HumanTrak tests using watermark-based
synchronisation.  A 204 No Content (empty list) signals sync completion.
"""

from __future__ import annotations

from typing import Any

from ingestion.common.batch import BatchManager
from ingestion.common.logging import get_logger
from ingestion.common.watermark import WatermarkManager
from ingestion.vald.client import ValdClient
from ingestion.vald.endpoints.humantrak import HumanTrakEndpoint
from ingestion.vald.loaders.bronze_loader import ValdBronzeLoader
from ingestion.vald.loaders.raw_loader import ValdRawLoader

logger = get_logger(__name__)

_PROVIDER = "vald"
_SOURCE_ACCOUNT = "vald_default"
_API_NAME = "humantrak_tests"


class HumanTrakExtractor:
    """Incremental HumanTrak test extractor.

    Reads the watermark, fetches modified tests in a loop until the API
    returns an empty list (204), loads each page into raw and bronze,
    then advances the watermark.

    Args:
        vald_client: Initialised :class:`ValdClient`.
        raw_loader: Initialised :class:`ValdRawLoader`.
        bronze_loader: Initialised :class:`ValdBronzeLoader`.
        watermark_mgr: Initialised :class:`WatermarkManager`.
        batch_manager: Initialised :class:`BatchManager`.
    """

    def __init__(
        self,
        vald_client: ValdClient,
        raw_loader: ValdRawLoader,
        bronze_loader: ValdBronzeLoader,
        watermark_mgr: WatermarkManager,
        batch_manager: BatchManager,
    ) -> None:
        self.vald_client = vald_client
        self.raw_loader = raw_loader
        self.bronze_loader = bronze_loader
        self.watermark_mgr = watermark_mgr
        self.batch_manager = batch_manager

        self._endpoint = HumanTrakEndpoint(vald_client.humantrak_client)

    def extract(self, tenant_id: str) -> dict[str, Any]:
        """Run incremental extraction for a single tenant.

        Args:
            tenant_id: UUID of the tenant to sync.

        Returns:
            Summary dict with ``records_extracted``, ``records_loaded``,
            and ``new_watermark``.
        """
        watermark = self.watermark_mgr.get_watermark(
            provider=_PROVIDER,
            source_account=_SOURCE_ACCOUNT,
            api_name=_API_NAME,
            tenant_id=tenant_id,
        )

        total_extracted = 0
        total_loaded = 0
        max_modified: str | None = None

        logger.info(
            "HumanTrak: starting extraction for tenant=%s watermark=%s",
            tenant_id,
            watermark,
        )

        current_watermark = watermark
        while True:
            tests = self._endpoint.get_tests(
                tenant_id=tenant_id,
                modified_from_utc=current_watermark,
            )

            if not tests:
                logger.info(
                    "HumanTrak: no more data for tenant=%s (204/empty)",
                    tenant_id,
                )
                break

            total_extracted += len(tests)

            # Load into raw
            raw_id = self.raw_loader.load_raw(
                table_name="vald_humantrak_tests",
                api_endpoint=(
                    f"/v2/tests-by-modified-date?"
                    f"TenantId={tenant_id}&ModifiedFromUtc={current_watermark}"
                ),
                response_payload=tests,
                request_params={
                    "TenantId": tenant_id,
                    "ModifiedFromUtc": current_watermark,
                },
                api_version="v2",
            )

            # Load into bronze
            loaded = self.bronze_loader.load_humantrak_tests(tests, raw_id)
            total_loaded += loaded

            # Track maximum modifiedDateUtc
            for t in tests:
                mod = t.get("modifiedDateUtc") or t.get("modifiedDateUTC")
                if mod and (max_modified is None or mod > max_modified):
                    max_modified = mod

            if max_modified:
                current_watermark = max_modified

            logger.info(
                "HumanTrak: fetched %d tests for tenant=%s (running total=%d)",
                len(tests),
                tenant_id,
                total_extracted,
            )

        # Update watermark only after entire batch succeeds
        if max_modified:
            self.watermark_mgr.update_watermark(
                provider=_PROVIDER,
                source_account=_SOURCE_ACCOUNT,
                api_name=_API_NAME,
                watermark_value=max_modified,
                records_synced=total_loaded,
                tenant_id=tenant_id,
            )

        summary = {
            "records_extracted": total_extracted,
            "records_loaded": total_loaded,
            "new_watermark": max_modified or watermark,
        }
        logger.info("HumanTrak: extraction complete for tenant=%s: %s", tenant_id, summary)
        return summary
