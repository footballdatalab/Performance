-- ============================================================================
-- 50_silver_vald_athlete_profile_soft_delete.sql
-- Phase 8.7.C site #13 (2026-05-09): add soft-delete columns to
-- silver.vald_athlete_profile so the silver ETL can mark profiles as
-- deactivated instead of DELETEing them. Locked decision #7 satisfied.
--
-- The previous code path (silver_etl._delete_excluded_profiles) was the
-- last DELETE on this table and the only place it could happen — every
-- other write goes through scoped UPSERT. After this migration + the
-- accompanying _deactivate_excluded_profiles refactor, the table will
-- never see a DELETE from the ETL pipeline.
--
-- Rationale (per user direction 2026-05-08, Option 1):
--   * Athletes who leave the squad must remain queryable for historical
--     reports (their old assessment metrics in silver.vald_assessment_metric
--     should still resolve to their name + DOB via JOIN).
--   * Reactivation is a clean ``is_active = TRUE``: when a returning
--     athlete shows up in the membership again, _deactivate_excluded_profiles
--     is paired with the upsert that sets is_active = TRUE in the same run.
--   * Audit trail: deactivated_at preserves WHEN the squad change happened.
-- ============================================================================

ALTER TABLE silver.vald_athlete_profile
    ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS deactivated_at TIMESTAMPTZ;

-- Partial index for the common "active profiles only" read path.
-- Existing readers must add ``WHERE is_active = TRUE`` (audit done in
-- Phase 8.7.C reader audit). After the migration the readers default
-- behaviour does NOT silently include deactivated profiles, but the
-- partial index ensures the active subset stays fast.
CREATE INDEX IF NOT EXISTS idx_vald_athlete_profile_active
    ON silver.vald_athlete_profile (provider_profile_id, tenant_id)
    WHERE is_active = TRUE;
