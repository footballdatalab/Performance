-- ============================================================================
-- 11_raw_vald_tables.sql
-- Raw schema: VALD API response storage (JSONB, append-only)
-- One table per active VALD endpoint, covering ForceDecks, ForceFrame,
-- NordBord, SmartSpeed, and Dynamo products.
-- source_account is always 'VALD' for these tables.
-- ============================================================================

-- Profiles
CREATE TABLE IF NOT EXISTS raw.vald_profiles (
    raw_id              BIGSERIAL PRIMARY KEY,
    source_account      VARCHAR(50)     NOT NULL,
    api_endpoint        VARCHAR(255)    NOT NULL,
    request_params      JSONB,
    response_payload    JSONB           NOT NULL,
    response_status     INTEGER,
    batch_id            UUID            NOT NULL,
    ingested_at         TIMESTAMPTZ     NOT NULL DEFAULT now(),
    api_version         VARCHAR(20)
);

-- ---------------------------------------------------------------------------
-- ForceDecks
-- ---------------------------------------------------------------------------

-- ForceDecks Tests
CREATE TABLE IF NOT EXISTS raw.vald_forcedecks_tests (
    raw_id              BIGSERIAL PRIMARY KEY,
    source_account      VARCHAR(50)     NOT NULL,
    api_endpoint        VARCHAR(255)    NOT NULL,
    request_params      JSONB,
    response_payload    JSONB           NOT NULL,
    response_status     INTEGER,
    batch_id            UUID            NOT NULL,
    ingested_at         TIMESTAMPTZ     NOT NULL DEFAULT now(),
    api_version         VARCHAR(20)
);

-- ForceDecks Trials
CREATE TABLE IF NOT EXISTS raw.vald_forcedecks_trials (
    raw_id              BIGSERIAL PRIMARY KEY,
    source_account      VARCHAR(50)     NOT NULL,
    api_endpoint        VARCHAR(255)    NOT NULL,
    request_params      JSONB,
    response_payload    JSONB           NOT NULL,
    response_status     INTEGER,
    batch_id            UUID            NOT NULL,
    ingested_at         TIMESTAMPTZ     NOT NULL DEFAULT now(),
    api_version         VARCHAR(20)
);

-- ForceDecks Result Definitions
CREATE TABLE IF NOT EXISTS raw.vald_forcedecks_result_definitions (
    raw_id              BIGSERIAL PRIMARY KEY,
    source_account      VARCHAR(50)     NOT NULL,
    api_endpoint        VARCHAR(255)    NOT NULL,
    request_params      JSONB,
    response_payload    JSONB           NOT NULL,
    response_status     INTEGER,
    batch_id            UUID            NOT NULL,
    ingested_at         TIMESTAMPTZ     NOT NULL DEFAULT now(),
    api_version         VARCHAR(20)
);

-- ---------------------------------------------------------------------------
-- ForceFrame
-- ---------------------------------------------------------------------------

-- ForceFrame Tests
CREATE TABLE IF NOT EXISTS raw.vald_forceframe_tests (
    raw_id              BIGSERIAL PRIMARY KEY,
    source_account      VARCHAR(50)     NOT NULL,
    api_endpoint        VARCHAR(255)    NOT NULL,
    request_params      JSONB,
    response_payload    JSONB           NOT NULL,
    response_status     INTEGER,
    batch_id            UUID            NOT NULL,
    ingested_at         TIMESTAMPTZ     NOT NULL DEFAULT now(),
    api_version         VARCHAR(20)
);

-- ForceFrame Test Metrics
CREATE TABLE IF NOT EXISTS raw.vald_forceframe_test_metrics (
    raw_id              BIGSERIAL PRIMARY KEY,
    source_account      VARCHAR(50)     NOT NULL,
    api_endpoint        VARCHAR(255)    NOT NULL,
    request_params      JSONB,
    response_payload    JSONB           NOT NULL,
    response_status     INTEGER,
    batch_id            UUID            NOT NULL,
    ingested_at         TIMESTAMPTZ     NOT NULL DEFAULT now(),
    api_version         VARCHAR(20)
);

-- ForceFrame Force Traces
CREATE TABLE IF NOT EXISTS raw.vald_forceframe_force_traces (
    raw_id              BIGSERIAL PRIMARY KEY,
    source_account      VARCHAR(50)     NOT NULL,
    api_endpoint        VARCHAR(255)    NOT NULL,
    request_params      JSONB,
    response_payload    JSONB           NOT NULL,
    response_status     INTEGER,
    batch_id            UUID            NOT NULL,
    ingested_at         TIMESTAMPTZ     NOT NULL DEFAULT now(),
    api_version         VARCHAR(20)
);

-- ---------------------------------------------------------------------------
-- NordBord
-- ---------------------------------------------------------------------------

-- NordBord Tests
CREATE TABLE IF NOT EXISTS raw.vald_nordbord_tests (
    raw_id              BIGSERIAL PRIMARY KEY,
    source_account      VARCHAR(50)     NOT NULL,
    api_endpoint        VARCHAR(255)    NOT NULL,
    request_params      JSONB,
    response_payload    JSONB           NOT NULL,
    response_status     INTEGER,
    batch_id            UUID            NOT NULL,
    ingested_at         TIMESTAMPTZ     NOT NULL DEFAULT now(),
    api_version         VARCHAR(20)
);

-- NordBord Test Metrics
CREATE TABLE IF NOT EXISTS raw.vald_nordbord_test_metrics (
    raw_id              BIGSERIAL PRIMARY KEY,
    source_account      VARCHAR(50)     NOT NULL,
    api_endpoint        VARCHAR(255)    NOT NULL,
    request_params      JSONB,
    response_payload    JSONB           NOT NULL,
    response_status     INTEGER,
    batch_id            UUID            NOT NULL,
    ingested_at         TIMESTAMPTZ     NOT NULL DEFAULT now(),
    api_version         VARCHAR(20)
);

-- NordBord Eccentric Exercises
CREATE TABLE IF NOT EXISTS raw.vald_nordbord_ecc_exercises (
    raw_id              BIGSERIAL PRIMARY KEY,
    source_account      VARCHAR(50)     NOT NULL,
    api_endpoint        VARCHAR(255)    NOT NULL,
    request_params      JSONB,
    response_payload    JSONB           NOT NULL,
    response_status     INTEGER,
    batch_id            UUID            NOT NULL,
    ingested_at         TIMESTAMPTZ     NOT NULL DEFAULT now(),
    api_version         VARCHAR(20)
);

-- NordBord Eccentric Repetitions
CREATE TABLE IF NOT EXISTS raw.vald_nordbord_ecc_repetitions (
    raw_id              BIGSERIAL PRIMARY KEY,
    source_account      VARCHAR(50)     NOT NULL,
    api_endpoint        VARCHAR(255)    NOT NULL,
    request_params      JSONB,
    response_payload    JSONB           NOT NULL,
    response_status     INTEGER,
    batch_id            UUID            NOT NULL,
    ingested_at         TIMESTAMPTZ     NOT NULL DEFAULT now(),
    api_version         VARCHAR(20)
);

-- ---------------------------------------------------------------------------
-- SmartSpeed
-- ---------------------------------------------------------------------------

-- SmartSpeed Test Summaries
CREATE TABLE IF NOT EXISTS raw.vald_smartspeed_test_summaries (
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

-- SmartSpeed Test Details
CREATE TABLE IF NOT EXISTS raw.vald_smartspeed_test_details (
    raw_id              BIGSERIAL PRIMARY KEY,
    source_account      VARCHAR(50)     NOT NULL,
    api_endpoint        VARCHAR(255)    NOT NULL,
    request_params      JSONB,
    response_payload    JSONB           NOT NULL,
    response_status     INTEGER,
    batch_id            UUID            NOT NULL,
    ingested_at         TIMESTAMPTZ     NOT NULL DEFAULT now(),
    api_version         VARCHAR(20)
);

-- ---------------------------------------------------------------------------
-- Dynamo
-- ---------------------------------------------------------------------------

-- Dynamo Tests
CREATE TABLE IF NOT EXISTS raw.vald_dynamo_tests (
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

-- Dynamo Test Details
CREATE TABLE IF NOT EXISTS raw.vald_dynamo_test_details (
    raw_id              BIGSERIAL PRIMARY KEY,
    source_account      VARCHAR(50)     NOT NULL,
    api_endpoint        VARCHAR(255)    NOT NULL,
    request_params      JSONB,
    response_payload    JSONB           NOT NULL,
    response_status     INTEGER,
    batch_id            UUID            NOT NULL,
    ingested_at         TIMESTAMPTZ     NOT NULL DEFAULT now(),
    api_version         VARCHAR(20)
);

-- Dynamo Traces
CREATE TABLE IF NOT EXISTS raw.vald_dynamo_traces (
    raw_id              BIGSERIAL PRIMARY KEY,
    source_account      VARCHAR(50)     NOT NULL,
    api_endpoint        VARCHAR(255)    NOT NULL,
    request_params      JSONB,
    response_payload    JSONB           NOT NULL,
    response_status     INTEGER,
    batch_id            UUID            NOT NULL,
    ingested_at         TIMESTAMPTZ     NOT NULL DEFAULT now(),
    api_version         VARCHAR(20)
);

-- ---------------------------------------------------------------------------
-- Indexes on main data / test tables for ingested_at and batch_id
-- ---------------------------------------------------------------------------

-- ForceDecks Tests
CREATE INDEX IF NOT EXISTS idx_raw_vald_fd_tests_ingested   ON raw.vald_forcedecks_tests (ingested_at);
CREATE INDEX IF NOT EXISTS idx_raw_vald_fd_tests_batch      ON raw.vald_forcedecks_tests (batch_id);

-- ForceDecks Trials
CREATE INDEX IF NOT EXISTS idx_raw_vald_fd_trials_ingested  ON raw.vald_forcedecks_trials (ingested_at);
CREATE INDEX IF NOT EXISTS idx_raw_vald_fd_trials_batch     ON raw.vald_forcedecks_trials (batch_id);

-- ForceFrame Tests
CREATE INDEX IF NOT EXISTS idx_raw_vald_ff_tests_ingested   ON raw.vald_forceframe_tests (ingested_at);
CREATE INDEX IF NOT EXISTS idx_raw_vald_ff_tests_batch      ON raw.vald_forceframe_tests (batch_id);

-- NordBord Tests
CREATE INDEX IF NOT EXISTS idx_raw_vald_nb_tests_ingested   ON raw.vald_nordbord_tests (ingested_at);
CREATE INDEX IF NOT EXISTS idx_raw_vald_nb_tests_batch      ON raw.vald_nordbord_tests (batch_id);

-- SmartSpeed Test Summaries
CREATE INDEX IF NOT EXISTS idx_raw_vald_ss_summ_ingested    ON raw.vald_smartspeed_test_summaries (ingested_at);
CREATE INDEX IF NOT EXISTS idx_raw_vald_ss_summ_batch       ON raw.vald_smartspeed_test_summaries (batch_id);

-- SmartSpeed Test Details
CREATE INDEX IF NOT EXISTS idx_raw_vald_ss_det_ingested     ON raw.vald_smartspeed_test_details (ingested_at);
CREATE INDEX IF NOT EXISTS idx_raw_vald_ss_det_batch        ON raw.vald_smartspeed_test_details (batch_id);

-- Dynamo Tests
CREATE INDEX IF NOT EXISTS idx_raw_vald_dyn_tests_ingested  ON raw.vald_dynamo_tests (ingested_at);
CREATE INDEX IF NOT EXISTS idx_raw_vald_dyn_tests_batch     ON raw.vald_dynamo_tests (batch_id);

-- Dynamo Test Details
CREATE INDEX IF NOT EXISTS idx_raw_vald_dyn_det_ingested    ON raw.vald_dynamo_test_details (ingested_at);
CREATE INDEX IF NOT EXISTS idx_raw_vald_dyn_det_batch       ON raw.vald_dynamo_test_details (batch_id);

-- Dynamo Traces
CREATE INDEX IF NOT EXISTS idx_raw_vald_dyn_trace_ingested  ON raw.vald_dynamo_traces (ingested_at);
CREATE INDEX IF NOT EXISTS idx_raw_vald_dyn_trace_batch     ON raw.vald_dynamo_traces (batch_id);
