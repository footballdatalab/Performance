-- ============================================================================
-- 56_gold_prune_non_vald_tables.sql
-- Gold schema: drop non-VALD legacy marts from the old shared warehouse model
-- ============================================================================

DROP TABLE IF EXISTS gold.athlete_profile CASCADE;
DROP TABLE IF EXISTS gold.daily_monitoring CASCADE;
DROP TABLE IF EXISTS gold.velocity_benchmark CASCADE;
DROP TABLE IF EXISTS gold.team_history CASCADE;
DROP TABLE IF EXISTS gold.rtp_support CASCADE;
