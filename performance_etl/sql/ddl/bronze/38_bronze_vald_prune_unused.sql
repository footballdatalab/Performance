-- =============================================================================
-- File: 38_bronze_vald_prune_unused.sql
-- Description: Drop bronze VALD tables for feeds that are no longer ingested.
-- =============================================================================

DROP TABLE IF EXISTS bronze.vald_forceframe_training_exercises;
DROP TABLE IF EXISTS bronze.vald_forceframe_training_repetitions;
DROP TABLE IF EXISTS bronze.vald_nordbord_force_traces;
DROP TABLE IF EXISTS bronze.vald_nordbord_iso_sessions;
DROP TABLE IF EXISTS bronze.vald_nordbord_iso_exercises;
DROP TABLE IF EXISTS bronze.vald_nordbord_iso_repetitions;
DROP TABLE IF EXISTS bronze.vald_humantrak_repetitions;
DROP TABLE IF EXISTS bronze.vald_humantrak_metric_asymmetries;
DROP TABLE IF EXISTS bronze.vald_humantrak_metric_summaries;
DROP TABLE IF EXISTS bronze.vald_humantrak_metric_groups;
DROP TABLE IF EXISTS bronze.vald_humantrak_tests;
DROP TABLE IF EXISTS bronze.vald_tenants;
DROP TABLE IF EXISTS bronze.vald_categories;
DROP TABLE IF EXISTS bronze.vald_groups;
