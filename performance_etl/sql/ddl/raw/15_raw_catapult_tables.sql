-- ============================================================================
-- 15_raw_catapult_tables.sql
-- Raw schema: Catapult API response storage (JSONB, append-only)
-- One table per active Catapult endpoint in the phase-1 landing footprint.
-- ============================================================================

CREATE TABLE IF NOT EXISTS raw.catapult_teams (
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

CREATE TABLE IF NOT EXISTS raw.catapult_athletes (
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

CREATE TABLE IF NOT EXISTS raw.catapult_positions (
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

CREATE TABLE IF NOT EXISTS raw.catapult_parameters (
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

CREATE TABLE IF NOT EXISTS raw.catapult_venues (
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

CREATE TABLE IF NOT EXISTS raw.catapult_tag_types (
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

CREATE TABLE IF NOT EXISTS raw.catapult_tags (
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

CREATE TABLE IF NOT EXISTS raw.catapult_entity_tags (
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

CREATE TABLE IF NOT EXISTS raw.catapult_activities (
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

CREATE TABLE IF NOT EXISTS raw.catapult_periods (
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

CREATE TABLE IF NOT EXISTS raw.catapult_annotations (
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

CREATE TABLE IF NOT EXISTS raw.catapult_stats (
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

CREATE TABLE IF NOT EXISTS raw.catapult_efforts (
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

CREATE TABLE IF NOT EXISTS raw.catapult_events (
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

CREATE TABLE IF NOT EXISTS raw.catapult_sensor_data (
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
