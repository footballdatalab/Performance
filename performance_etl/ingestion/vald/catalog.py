"""
VALD table and module catalog.
"""

from __future__ import annotations

ALL_MODULES = [
    "forcedecks",
    "forceframe",
    "nordbord",
    "smartspeed",
    "dynamo",
]

REFERENCE_RAW_TABLES = [
    "raw.vald_profiles",
]

REFERENCE_BRONZE_TABLES = [
    "bronze.vald_profiles",
    "bronze.vald_profile_categories",
]

MODULE_RAW_TABLES = {
    "forcedecks": [
        "raw.vald_forcedecks_tests",
        "raw.vald_forcedecks_result_definitions",
        "raw.vald_forcedecks_trials",
    ],
    "forceframe": [
        "raw.vald_forceframe_tests",
        "raw.vald_forceframe_test_metrics",
        "raw.vald_forceframe_force_traces",
    ],
    "nordbord": [
        "raw.vald_nordbord_tests",
        "raw.vald_nordbord_ecc_exercises",
        "raw.vald_nordbord_ecc_repetitions",
        "raw.vald_nordbord_test_metrics",
    ],
    "smartspeed": [
        "raw.vald_smartspeed_test_summaries",
        "raw.vald_smartspeed_test_details",
    ],
    "dynamo": [
        "raw.vald_dynamo_tests",
        "raw.vald_dynamo_test_details",
        "raw.vald_dynamo_traces",
    ],
}

INTRADAY_DEFERRED_RAW_TABLES = [
    "raw.vald_forceframe_force_traces",
]

MODULE_BRONZE_TABLES = {
    "forcedecks": [
        "bronze.vald_forcedecks_result_definitions",
        "bronze.vald_forcedecks_tests",
        "bronze.vald_forcedecks_trials",
        "bronze.vald_forcedecks_trial_results",
    ],
    "forceframe": [
        "bronze.vald_forceframe_tests",
        "bronze.vald_forceframe_test_metrics",
        "bronze.vald_forceframe_force_traces",
    ],
    "nordbord": [
        "bronze.vald_nordbord_tests",
        "bronze.vald_nordbord_test_metrics",
        "bronze.vald_nordbord_ecc_exercises",
        "bronze.vald_nordbord_ecc_repetitions",
    ],
    "smartspeed": [
        "bronze.vald_smartspeed_test_summaries",
        "bronze.vald_smartspeed_test_details",
        "bronze.vald_smartspeed_rep_results",
    ],
    "dynamo": [
        "bronze.vald_dynamo_tests",
        "bronze.vald_dynamo_rep_summaries",
        "bronze.vald_dynamo_repetitions",
        "bronze.vald_dynamo_traces",
    ],
}

ACTIVE_BRONZE_TABLES = [
    *REFERENCE_BRONZE_TABLES,
    *MODULE_BRONZE_TABLES["forcedecks"],
    *MODULE_BRONZE_TABLES["forceframe"],
    *MODULE_BRONZE_TABLES["nordbord"],
    *MODULE_BRONZE_TABLES["smartspeed"],
    *MODULE_BRONZE_TABLES["dynamo"],
]

ACTIVE_SILVER_TABLES = [
    "silver.vald_target_group_membership",
    "silver.vald_athlete_profile",
    "silver.vald_assessment_metric",
    "silver.vald_reference_metric_coverage",
    "silver.data_quality_flag",
]

ACTIVE_GOLD_TABLES = [
    "gold.vald_nordics",
    "gold.vald_forceframe",
    "gold.vald_forcedecks",
    "gold.vald_dynamo",
    "gold.vald_speed",
]

REMOVED_RAW_TABLES = [
    "raw.pipeline_stage_cursor",
    "raw.vald_tenants",
    "raw.vald_categories",
    "raw.vald_groups",
]

REMOVED_BRONZE_TABLES = [
    "bronze.vald_tenants",
    "bronze.vald_categories",
    "bronze.vald_groups",
]

OBSOLETE_VALD_TABLES = [
    *REMOVED_RAW_TABLES,
    *REMOVED_BRONZE_TABLES,
    "raw.vald_forceframe_training_exercises",
    "raw.vald_forceframe_training_repetitions",
    "raw.vald_nordbord_force_traces",
    "raw.vald_nordbord_iso_sessions",
    "raw.vald_nordbord_iso_exercises",
    "raw.vald_nordbord_iso_repetitions",
    "raw.vald_humantrak_tests",
    "raw.vald_humantrak_repetitions",
    "bronze.vald_forceframe_training_exercises",
    "bronze.vald_forceframe_training_repetitions",
    "bronze.vald_nordbord_force_traces",
    "bronze.vald_nordbord_iso_sessions",
    "bronze.vald_nordbord_iso_exercises",
    "bronze.vald_nordbord_iso_repetitions",
    "bronze.vald_humantrak_tests",
    "bronze.vald_humantrak_repetitions",
    "bronze.vald_humantrak_metric_groups",
    "bronze.vald_humantrak_metric_summaries",
    "bronze.vald_humantrak_metric_asymmetries",
    "silver.vald_metric_quality_baseline",
    "gold.vald_jumps",
    "gold.vald_forcedecks_other",
]
