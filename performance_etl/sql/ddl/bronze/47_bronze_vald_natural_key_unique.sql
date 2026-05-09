-- ============================================================================
-- 47_bronze_vald_natural_key_unique.sql
-- Phase 8.7.B.1 (2026-05-09): add natural-key UNIQUE constraints to the three
-- bronze VALD tables that used the DELETE-then-INSERT idempotency pattern.
--
-- Once these constraints exist, Phase 8.7.B can replace each
-- `DELETE FROM t WHERE …; INSERT INTO t …`  pair with
-- `INSERT INTO t … ON CONFLICT (natural_key) DO UPDATE …` in a single
-- atomic statement, satisfying locked decision #7.
--
-- All three tables were verified clean of duplicates on 2026-05-09 against
-- the production warehouse (zero dup groups for each natural key). No dedup
-- step is required.
--
-- NULLS NOT DISTINCT (PG 15+) treats NULL as equal so logically-duplicate
-- rows with NULL columns trigger the constraint instead of slipping through.
-- We confirmed PostgreSQL 17 in the audit.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. bronze.vald_forcedecks_trial_results
--    Natural key: (trial_id, result_id, limb, repeat)
--    `limb` and `repeat` may be NULL; NULLS NOT DISTINCT keeps logical dups out.
-- ----------------------------------------------------------------------------
CREATE UNIQUE INDEX IF NOT EXISTS uq_vald_forcedecks_trial_results_nk
    ON bronze.vald_forcedecks_trial_results
    (trial_id, result_id, limb, repeat)
    NULLS NOT DISTINCT;

-- ----------------------------------------------------------------------------
-- 2. bronze.vald_forceframe_force_traces
--    Natural key: (test_id, tick) — both NOT NULL.
--    Volume note: 34.5M rows; index build will take a few minutes the first
--    time the migration runs. Subsequent runs are fast (IF NOT EXISTS).
-- ----------------------------------------------------------------------------
CREATE UNIQUE INDEX IF NOT EXISTS uq_vald_forceframe_force_traces_nk
    ON bronze.vald_forceframe_force_traces
    (test_id, tick);

-- ----------------------------------------------------------------------------
-- 3. bronze.vald_dynamo_traces
--    Natural key: (test_id) — the loader writes one row per test (force_trace
--    + imu_trace are JSONB blobs).
-- ----------------------------------------------------------------------------
CREATE UNIQUE INDEX IF NOT EXISTS uq_vald_dynamo_traces_nk
    ON bronze.vald_dynamo_traces
    (test_id);
