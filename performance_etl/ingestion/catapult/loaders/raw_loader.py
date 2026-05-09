"""
Catapult raw-layer loader.

Stores Catapult API responses in the ``raw.catapult_*`` tables with enough
request metadata to support replay and audit.
"""

from __future__ import annotations

import json
from typing import Any

from ingestion.common.db import DatabaseManager
from ingestion.common.logging import get_logger

logger = get_logger(__name__)


class CatapultRawLoader:
    """Insert raw Catapult API responses into ``raw.catapult_*`` tables."""

    def __init__(
        self,
        db: DatabaseManager,
        batch_id: str,
        source_account: str,
    ) -> None:
        self.db = db
        self.batch_id = batch_id
        self.source_account = source_account

    def load_raw(
        self,
        table_name: str,
        api_endpoint: str,
        response_payload: dict | list,
        request_params: dict[str, Any] | None = None,
        response_status: int = 200,
        page_number: int | None = None,
        api_version: str | None = None,
    ) -> int:
        """Insert a single raw JSONB row into the requested Catapult table."""
        data = {
            "source_account": self.source_account,
            "api_endpoint": api_endpoint,
            "request_params": json.dumps(request_params) if request_params is not None else None,
            "response_payload": json.dumps(response_payload),
            "response_status": response_status,
            "page_number": page_number,
            "batch_id": self.batch_id,
            "api_version": api_version,
        }

        qualified_table = f"raw.{table_name}"
        raw_id = self.db.insert_raw(qualified_table, data)
        logger.debug(
            "Inserted Catapult raw record: table=%s endpoint=%s raw_id=%s batch=%s",
            qualified_table,
            api_endpoint,
            raw_id,
            self.batch_id,
        )
        return raw_id

    def load_raw_if_changed(
        self,
        table_name: str,
        api_endpoint: str,
        response_payload: dict | list,
        request_params: dict[str, Any] | None = None,
        response_status: int = 200,
        page_number: int | None = None,
        api_version: str | None = None,
    ) -> int:
        """Insert a raw row unless the latest snapshot is identical."""
        raw_id, _ = self.load_raw_if_changed_with_status(
            table_name=table_name,
            api_endpoint=api_endpoint,
            response_payload=response_payload,
            request_params=request_params,
            response_status=response_status,
            page_number=page_number,
            api_version=api_version,
        )
        return raw_id

    def load_raw_if_changed_with_status(
        self,
        table_name: str,
        api_endpoint: str,
        response_payload: dict | list,
        request_params: dict[str, Any] | None = None,
        response_status: int = 200,
        page_number: int | None = None,
        api_version: str | None = None,
    ) -> tuple[int, bool]:
        """Insert a raw row unless unchanged, returning ``(raw_id, inserted)``."""
        request_params_json = json.dumps(request_params) if request_params is not None else None
        response_payload_json = json.dumps(response_payload)
        qualified_table = f"raw.{table_name}"

        latest_raw = self.db.fetch_one(
            f"""
            WITH latest AS (
                SELECT raw_id, response_payload
                FROM {qualified_table}
                WHERE source_account = %s
                  AND api_endpoint = %s
                  AND COALESCE(request_params, '{{}}'::jsonb)
                      = COALESCE(%s::jsonb, '{{}}'::jsonb)
                ORDER BY raw_id DESC
                LIMIT 1
            )
            SELECT raw_id
            FROM latest
            WHERE response_payload = %s::jsonb
            """,
            (
                self.source_account,
                api_endpoint,
                request_params_json,
                response_payload_json,
            ),
        )
        if latest_raw:
            raw_id = int(latest_raw[0])
            logger.debug(
                "Reusing unchanged Catapult raw snapshot: table=%s endpoint=%s raw_id=%s",
                qualified_table,
                api_endpoint,
                raw_id,
            )
            return raw_id, False

        return (
            self.load_raw(
                table_name=table_name,
                api_endpoint=api_endpoint,
                response_payload=response_payload,
                request_params=request_params,
                response_status=response_status,
                page_number=page_number,
                api_version=api_version,
            ),
            True,
        )
