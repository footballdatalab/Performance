-- =============================================================================
-- 15_raw_vald_prune_unused.sql
-- Drop raw VALD tables for feeds that are no longer ingested.
-- =============================================================================

DROP TABLE IF EXISTS raw.vald_forceframe_training_exercises;
DROP TABLE IF EXISTS raw.vald_forceframe_training_repetitions;
DROP TABLE IF EXISTS raw.vald_nordbord_force_traces;
DROP TABLE IF EXISTS raw.vald_nordbord_iso_sessions;
DROP TABLE IF EXISTS raw.vald_nordbord_iso_exercises;
DROP TABLE IF EXISTS raw.vald_nordbord_iso_repetitions;
DROP TABLE IF EXISTS raw.vald_humantrak_repetitions;
DROP TABLE IF EXISTS raw.vald_humantrak_tests;
DROP TABLE IF EXISTS raw.pipeline_stage_cursor;
DROP TABLE IF EXISTS raw.vald_tenants;
DROP TABLE IF EXISTS raw.vald_categories;
DROP TABLE IF EXISTS raw.vald_groups;
