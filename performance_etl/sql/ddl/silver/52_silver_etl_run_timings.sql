-- ============================================================================
-- 52_silver_etl_run_timings.sql
-- Phase 8.8.A (2026-05-09): per-stage ETL timing instrumentation.
--
-- Every long-running ETL stage records a row here on completion (success
-- AND failure). Used to build a flame-graph-equivalent breakdown of
-- where wall-clock time goes, and as the before/after measurement
-- substrate for sub-phases 8.8.B–8.8.F.
--
-- Design notes:
--   * `run_id` groups every stage of a single ETL invocation. Generated
--     once at the top of the pipeline entry point and threaded through.
--   * `stage` is a stable identifier (e.g. "bronze.replay",
--     "silver.assessment_metric"). `sub_stage` is a free-text sub-key
--     for per-family / per-table / per-shard granularity.
--   * `rows_read` / `rows_written` / `bytes_read` / `peak_memory_mb` are
--     optional — wrapped block fills them via the mutable metrics dict
--     yielded by `track_stage`.
--   * `extra` is JSONB for any per-stage details (worker counts, batch
--     sizes, the SQL fingerprint, etc).
--   * Idempotent: `CREATE TABLE IF NOT EXISTS` + named indexes so
--     re-running bootstrap is a no-op.
-- ============================================================================

CREATE TABLE IF NOT EXISTS silver.etl_run_timings (
    timing_id      BIGSERIAL    PRIMARY KEY,
    run_id         UUID         NOT NULL,
    pipeline       VARCHAR(50)  NOT NULL,
    stage          VARCHAR(100) NOT NULL,
    sub_stage      VARCHAR(100),
    started_at     TIMESTAMPTZ  NOT NULL,
    finished_at    TIMESTAMPTZ  NOT NULL,
    elapsed_ms     BIGINT       NOT NULL,
    status         VARCHAR(20)  NOT NULL,
    rows_read      BIGINT,
    rows_written   BIGINT,
    bytes_read     BIGINT,
    peak_memory_mb INTEGER,
    error_message  TEXT,
    extra          JSONB,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),

    CONSTRAINT chk_etl_timings_status
        CHECK (status IN ('success', 'failed', 'skipped')),
    CONSTRAINT chk_etl_timings_pipeline
        CHECK (pipeline IN ('vald', 'catapult', 'zerozero', 'common'))
);

CREATE INDEX IF NOT EXISTS idx_etl_timings_run
    ON silver.etl_run_timings (run_id);
CREATE INDEX IF NOT EXISTS idx_etl_timings_pipeline_stage
    ON silver.etl_run_timings (pipeline, stage, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_etl_timings_started
    ON silver.etl_run_timings (started_at DESC);
CREATE INDEX IF NOT EXISTS idx_etl_timings_status
    ON silver.etl_run_timings (status, started_at DESC)
    WHERE status = 'failed';
