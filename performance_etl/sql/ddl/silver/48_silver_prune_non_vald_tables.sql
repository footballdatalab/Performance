-- ============================================================================
-- 48_silver_prune_non_vald_tables.sql
-- Silver schema: drop non-VALD legacy tables from the old shared warehouse model
-- ============================================================================

DROP TABLE IF EXISTS silver.athlete_mapping_audit CASCADE;
DROP TABLE IF EXISTS silver.athlete_match_candidate CASCADE;
DROP TABLE IF EXISTS silver.athlete_match_rejection CASCADE;
DROP TABLE IF EXISTS silver.athlete_provider_link CASCADE;
DROP TABLE IF EXISTS silver.athlete_team_membership CASCADE;
DROP TABLE IF EXISTS silver.master_athlete CASCADE;
DROP TABLE IF EXISTS silver.dim_position CASCADE;
DROP TABLE IF EXISTS silver.dim_season CASCADE;
DROP TABLE IF EXISTS silver.dim_team CASCADE;
DROP TABLE IF EXISTS silver.master_tag CASCADE;
DROP TABLE IF EXISTS silver.microcycle CASCADE;
DROP TABLE IF EXISTS silver.tag_account_mapping CASCADE;
DROP TABLE IF EXISTS silver.tag_approval_request CASCADE;
DROP TABLE IF EXISTS silver.tag_mismatch_log CASCADE;
