"""
Catapult table catalog for the platform foundation.
"""

from __future__ import annotations

RAW_TABLES = [
    "raw.catapult_teams",
    "raw.catapult_positions",
    "raw.catapult_athletes",
    "raw.catapult_parameters",
    "raw.catapult_venues",
    "raw.catapult_tag_types",
    "raw.catapult_tags",
    "raw.catapult_entity_tags",
    "raw.catapult_activities",
    "raw.catapult_periods",
    "raw.catapult_annotations",
    "raw.catapult_stats",
    "raw.catapult_efforts",
    "raw.catapult_events",
    "raw.catapult_sensor_data",
]

REFERENCE_RAW_TABLES = [
    "raw.catapult_teams",
    "raw.catapult_positions",
    "raw.catapult_athletes",
    "raw.catapult_parameters",
    "raw.catapult_venues",
    "raw.catapult_tag_types",
    "raw.catapult_tags",
    "raw.catapult_entity_tags",
]

SESSION_RAW_TABLES = [
    "raw.catapult_activities",
    "raw.catapult_periods",
    "raw.catapult_annotations",
]

PERFORMANCE_RAW_TABLES = [
    "raw.catapult_stats",
    "raw.catapult_efforts",
    "raw.catapult_events",
    "raw.catapult_sensor_data",
]

BRONZE_TABLES = [
    "bronze.catapult_teams",
    "bronze.catapult_positions",
    "bronze.catapult_parameters",
    "bronze.catapult_venues",
    "bronze.catapult_tag_types",
    "bronze.catapult_tags",
    "bronze.catapult_athletes",
    "bronze.catapult_activities",
    "bronze.catapult_periods",
    "bronze.catapult_annotations",
    "bronze.catapult_entity_tags",
    "bronze.catapult_stats",
    "bronze.catapult_efforts",
    "bronze.catapult_events",
    "bronze.catapult_sensor_data",
]

REFERENCE_BRONZE_TABLES = [
    "bronze.catapult_teams",
    "bronze.catapult_positions",
    "bronze.catapult_parameters",
    "bronze.catapult_venues",
    "bronze.catapult_tag_types",
    "bronze.catapult_tags",
    "bronze.catapult_athletes",
    "bronze.catapult_entity_tags",
]

SESSION_BRONZE_TABLES = [
    "bronze.catapult_activities",
    "bronze.catapult_periods",
    "bronze.catapult_annotations",
]

PERFORMANCE_BRONZE_TABLES = [
    "bronze.catapult_stats",
    "bronze.catapult_efforts",
    "bronze.catapult_events",
    "bronze.catapult_sensor_data",
]

PARTITIONED_BRONZE_TABLES = [
    "bronze.catapult_stats",
    "bronze.catapult_sensor_data",
]

RAW_TO_BRONZE_REPLAY_ORDER = [
    *REFERENCE_RAW_TABLES,
    *SESSION_RAW_TABLES,
    *PERFORMANCE_RAW_TABLES,
]

RAW_TO_BRONZE_TABLE_MAP = {
    "raw.catapult_teams": "bronze.catapult_teams",
    "raw.catapult_athletes": "bronze.catapult_athletes",
    "raw.catapult_positions": "bronze.catapult_positions",
    "raw.catapult_parameters": "bronze.catapult_parameters",
    "raw.catapult_venues": "bronze.catapult_venues",
    "raw.catapult_tag_types": "bronze.catapult_tag_types",
    "raw.catapult_tags": "bronze.catapult_tags",
    "raw.catapult_entity_tags": "bronze.catapult_entity_tags",
    "raw.catapult_activities": "bronze.catapult_activities",
    "raw.catapult_periods": "bronze.catapult_periods",
    "raw.catapult_annotations": "bronze.catapult_annotations",
    "raw.catapult_stats": "bronze.catapult_stats",
    "raw.catapult_efforts": "bronze.catapult_efforts",
    "raw.catapult_events": "bronze.catapult_events",
    "raw.catapult_sensor_data": "bronze.catapult_sensor_data",
}

UNSUPPORTED_CATAPULT_TABLES = [
    "silver.catapult_athlete_profile",
    "silver.stg_catapult_activities",
    "silver.stg_catapult_athlete_sessions",
    "gold.mart_daily_load",
    "gold.mart_session_detail",
    "gold.mart_microcycle_summary",
    "gold.mart_squad_overview",
]
