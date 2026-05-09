-- ============================================================================
-- 13_raw_vald_reconciliation.sql
-- Raw schema: reconcile obsolete VALD raw tables with the current extractors
-- ============================================================================

-- HumanTrak test-type metrics are not written by the current extractor.
DROP TABLE IF EXISTS raw.vald_humantrak_test_type_metrics;

-- Older DynaMo detail loads targeted a non-existent raw table name.
DROP TABLE IF EXISTS raw.vald_dynamo_repetitions;

CREATE TABLE IF NOT EXISTS raw.vald_dynamo_traces (
    raw_id              BIGSERIAL PRIMARY KEY,
    source_account      VARCHAR(50)     NOT NULL,
    api_endpoint        VARCHAR(255)    NOT NULL,
    request_params      JSONB,
    response_payload    JSONB           NOT NULL,
    response_status     INTEGER,
    page_number         INTEGER,
    batch_id            UUID            NOT NULL,
    ingested_at         TIMESTAMPTZ     NOT NULL DEFAULT now(),
    api_version         VARCHAR(20)
);

CREATE INDEX IF NOT EXISTS idx_raw_vald_dyn_trace_ingested
    ON raw.vald_dynamo_traces (ingested_at);
CREATE INDEX IF NOT EXISTS idx_raw_vald_dyn_trace_batch
    ON raw.vald_dynamo_traces (batch_id);

CREATE TABLE IF NOT EXISTS raw.vald_replay_cursor (
    source_table         VARCHAR(255)    PRIMARY KEY,
    last_raw_id          BIGINT          NOT NULL DEFAULT 0,
    updated_at           TIMESTAMPTZ     NOT NULL DEFAULT now()
);
