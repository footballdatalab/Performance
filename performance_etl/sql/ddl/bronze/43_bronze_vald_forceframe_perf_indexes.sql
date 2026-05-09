-- =============================================================================
-- File: 43_bronze_vald_forceframe_perf_indexes.sql
-- Description: Support the ForceFrame deferred bronze replay hot path.
--              ForceFrame trace replays replace one test at a time via
--              DELETE ... WHERE test_id = %s before bulk re-inserting the
--              current trace samples. Indexing test_id keeps that delete
--              path stable as bronze.vald_forceframe_force_traces grows.
-- =============================================================================

CREATE INDEX IF NOT EXISTS ix_ff_force_traces_test_id
    ON bronze.vald_forceframe_force_traces (test_id);
