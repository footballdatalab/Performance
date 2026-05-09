"""
Helpers for deriving scoped Catapult raw->bronze replay inputs.
"""

from __future__ import annotations

from typing import Any

from ingestion.catapult.catalog import RAW_TO_BRONZE_TABLE_MAP

_REFERENCE_RAW_TABLES = {
    "raw.catapult_teams",
    "raw.catapult_athletes",
    "raw.catapult_positions",
    "raw.catapult_parameters",
    "raw.catapult_venues",
    "raw.catapult_tag_types",
    "raw.catapult_tags",
    "raw.catapult_entity_tags",
}

_RAW_TABLE_STAGE_KEYS = {
    "raw.catapult_teams": "reference",
    "raw.catapult_athletes": "reference",
    "raw.catapult_positions": "reference",
    "raw.catapult_parameters": "reference",
    "raw.catapult_venues": "reference",
    "raw.catapult_tag_types": "reference",
    "raw.catapult_tags": "reference",
    "raw.catapult_entity_tags": "reference",
    "raw.catapult_activities": "activities",
    "raw.catapult_periods": "periods",
    "raw.catapult_annotations": "annotations",
    "raw.catapult_stats": "stats",
    "raw.catapult_efforts": "efforts",
    "raw.catapult_events": "events",
    "raw.catapult_sensor_data": "sensor_data",
}


def build_batch_ids_by_source_table(raw_summary: dict[str, Any]) -> dict[str, list[str]]:
    """Return raw source-table batch IDs represented in an extraction summary."""
    batch_ids_by_source_table: dict[str, set[str]] = {
        source_table: set() for source_table in RAW_TO_BRONZE_TABLE_MAP
    }
    for account_raw_summary in dict(raw_summary.get("accounts", {})).values():
        for raw_table in _RAW_TABLE_STAGE_KEYS:
            stage_summary = dict(account_raw_summary.get(_RAW_TABLE_STAGE_KEYS[raw_table], {}))
            batch_id = stage_summary.get("batch_id")
            if batch_id and raw_rows_written_for_table(raw_table, account_raw_summary) > 0:
                batch_ids_by_source_table[raw_table].add(str(batch_id))
    return {
        source_table: sorted(batch_ids)
        for source_table, batch_ids in batch_ids_by_source_table.items()
        if batch_ids
    }


def merge_batch_ids_by_source_table(
    *batch_id_maps: dict[str, list[str]] | None,
) -> dict[str, list[str]]:
    """Merge source-table batch ID maps, preserving deterministic ordering."""
    merged: dict[str, set[str]] = {}
    for batch_id_map in batch_id_maps:
        if not batch_id_map:
            continue
        for source_table, batch_ids in batch_id_map.items():
            merged.setdefault(source_table, set()).update(str(batch_id) for batch_id in batch_ids)
    return {source_table: sorted(batch_ids) for source_table, batch_ids in sorted(merged.items())}


def batch_ids_for_account(raw_table: str, account_raw_summary: dict[str, Any]) -> list[str]:
    """Return the extraction batch ID for one account/table when rows were written."""
    stage_key = _RAW_TABLE_STAGE_KEYS[raw_table]
    stage_summary = dict(account_raw_summary.get(stage_key, {}))
    batch_id = stage_summary.get("batch_id")
    if not batch_id or raw_rows_written_for_table(raw_table, account_raw_summary) <= 0:
        return []
    return [str(batch_id)]


def raw_rows_written_for_table(raw_table: str, account_raw_summary: dict[str, Any]) -> int:
    """Return raw row writes for a source table within one account summary."""
    stage_key = _RAW_TABLE_STAGE_KEYS[raw_table]
    stage_summary = dict(account_raw_summary.get(stage_key, {}))
    if raw_table in _REFERENCE_RAW_TABLES:
        endpoint_name = raw_table.removeprefix("raw.catapult_")
        if endpoint_name == "entity_tags":
            return int(dict(stage_summary.get("entity_tags", {})).get("raw_rows_written", 0) or 0)
        endpoints = dict(stage_summary.get("endpoints", {}))
        return int(dict(endpoints.get(endpoint_name, {})).get("raw_rows_written", 0) or 0)
    return int(stage_summary.get("raw_rows_written", 0) or 0)
