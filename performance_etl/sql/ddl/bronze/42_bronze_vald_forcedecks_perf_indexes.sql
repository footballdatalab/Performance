-- =============================================================================
-- File: 42_bronze_vald_forcedecks_perf_indexes.sql
-- Description: Covering indexes for the ForceDecks Silver hot path.
--              The Silver `_insert_forcedecks_family` query joins
--              trial_results -> trials -> tests and groups by
--              recorded date. The pre-existing indexes (test_id /
--              trial_id / tenant_profile_modified) cover the join
--              keys but force heap fetches for the projected
--              columns. The covering indexes below let the planner
--              run index-only scans for the Silver insert and the
--              parallel trial-results backfill (idempotent: any
--              re-run is a no-op thanks to IF NOT EXISTS).
-- =============================================================================

CREATE INDEX IF NOT EXISTS ix_fd_trial_results_test_cover
    ON bronze.vald_forcedecks_trial_results (test_id, profile_id)
    INCLUDE (trial_id, result_id, value, limb, repeat);

CREATE INDEX IF NOT EXISTS ix_fd_trials_trial_cover
    ON bronze.vald_forcedecks_trials (trial_id)
    INCLUDE (recorded_utc, limb);

CREATE INDEX IF NOT EXISTS ix_fd_tests_profile_recorded
    ON bronze.vald_forcedecks_tests (profile_id, recorded_date_utc);

CREATE INDEX IF NOT EXISTS ix_fd_trials_profile_bucket
    ON bronze.vald_forcedecks_trials ((abs(hashtext(profile_id::text)) % 8));
