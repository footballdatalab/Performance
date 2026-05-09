-- =============================================================================
-- File: 44_bronze_vald_dynamo_perf_indexes.sql
-- Description: Support the DynaMo trace replay hot path.
--              DynaMo trace replays replace one test at a time via
--              DELETE ... WHERE test_id = %s before inserting the current
--              trace JSON payload. Indexing test_id keeps that delete path
--              stable as bronze.vald_dynamo_traces grows.
-- =============================================================================

CREATE INDEX IF NOT EXISTS ix_dyn_traces_test_id
    ON bronze.vald_dynamo_traces (test_id);
