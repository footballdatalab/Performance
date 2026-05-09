-- =============================================================================
-- File: 35_bronze_vald_smartspeed.sql
-- Description: Bronze schema — VALD SmartSpeed tables.
--              Parsed and flattened from raw JSONB into typed columns.
--              Covers test summaries, test details, and rep results.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- bronze.vald_smartspeed_test_summaries
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.vald_smartspeed_test_summaries (
    test_id                 UUID            NOT NULL,
    test_result_id          UUID,
    tenant_id               UUID,
    profile_id              UUID,
    group_under_test_id     UUID,
    test_name               VARCHAR(255),
    test_type_name          VARCHAR(100),
    rep_count               INTEGER,
    device_count            INTEGER,
    test_date_utc           TIMESTAMPTZ,
    is_valid                BOOLEAN,
    all_groups              JSONB,

    -- lineage & audit
    raw_id                  BIGINT,
    batch_id                UUID,
    ingested_at             TIMESTAMPTZ     DEFAULT now(),
    created_at              TIMESTAMPTZ     DEFAULT now(),
    updated_at              TIMESTAMPTZ     DEFAULT now(),

    CONSTRAINT pk_vald_smartspeed_test_summaries PRIMARY KEY (test_id)
);

CREATE INDEX IF NOT EXISTS ix_vald_smartspeed_test_summaries_tenant_profile_date
    ON bronze.vald_smartspeed_test_summaries (tenant_id, profile_id, test_date_utc);

-- -----------------------------------------------------------------------------
-- bronze.vald_smartspeed_test_details
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.vald_smartspeed_test_details (
    test_id                     UUID            NOT NULL,
    tenant_id                   UUID            NOT NULL,
    profile_id                  UUID            NOT NULL,
    session_id                  UUID,
    group_under_test_id         UUID,
    test_date_utc               TIMESTAMPTZ,
    trial_index                 INTEGER,
    tag                         VARCHAR(50),
    additional_test_result      JSONB,
    rep_results                 JSONB,

    -- lineage & audit
    raw_id                      BIGINT,
    batch_id                    UUID,
    ingested_at                 TIMESTAMPTZ     DEFAULT now(),
    created_at                  TIMESTAMPTZ     DEFAULT now(),
    updated_at                  TIMESTAMPTZ     DEFAULT now(),

    CONSTRAINT pk_vald_smartspeed_test_details PRIMARY KEY (test_id)
);

-- -----------------------------------------------------------------------------
-- bronze.vald_smartspeed_rep_results
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.vald_smartspeed_rep_results (
    rep_result_id           BIGSERIAL       NOT NULL,
    test_id                 UUID            NOT NULL,
    rep_number              INTEGER         NOT NULL,
    rep_data                JSONB,

    -- lineage & audit
    raw_id                  BIGINT,
    batch_id                UUID,
    ingested_at             TIMESTAMPTZ     DEFAULT now(),
    created_at              TIMESTAMPTZ     DEFAULT now(),
    updated_at              TIMESTAMPTZ     DEFAULT now(),

    CONSTRAINT pk_vald_smartspeed_rep_results PRIMARY KEY (rep_result_id),
    CONSTRAINT uq_vald_smartspeed_rep_results UNIQUE (test_id, rep_number)
);
