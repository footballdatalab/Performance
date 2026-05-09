-- =============================================================================
-- File: 31_bronze_vald_forcedecks.sql
-- Description: Bronze schema — VALD ForceDecks tables.
--              Parsed and flattened from raw JSONB into typed columns.
--              Covers tests, trials, trial results, and result definitions.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- bronze.vald_forcedecks_tests
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.vald_forcedecks_tests (
    test_id                 UUID            NOT NULL,
    tenant_id               UUID            NOT NULL,
    profile_id              UUID            NOT NULL,
    recording_id            UUID,
    modified_date_utc       TIMESTAMPTZ,
    recorded_date_utc       TIMESTAMPTZ,
    analysed_date_utc       TIMESTAMPTZ,
    test_type               VARCHAR(100),
    notes                   TEXT,
    weight                  NUMERIC,
    parameter               JSONB,
    extended_parameters     JSONB,
    attributes              JSONB,

    -- lineage & audit
    raw_id                  BIGINT,
    batch_id                UUID,
    ingested_at             TIMESTAMPTZ     DEFAULT now(),
    created_at              TIMESTAMPTZ     DEFAULT now(),
    updated_at              TIMESTAMPTZ     DEFAULT now(),

    CONSTRAINT pk_vald_forcedecks_tests PRIMARY KEY (test_id)
);

CREATE INDEX IF NOT EXISTS ix_vald_forcedecks_tests_tenant_profile_modified
    ON bronze.vald_forcedecks_tests (tenant_id, profile_id, modified_date_utc);

-- -----------------------------------------------------------------------------
-- bronze.vald_forcedecks_trials
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.vald_forcedecks_trials (
    trial_id                UUID            NOT NULL,
    test_id                 UUID            NOT NULL,
    profile_id              UUID            NOT NULL,
    recorded_utc            TIMESTAMPTZ,
    start_time              NUMERIC,              -- seconds offset from test start
    end_time                NUMERIC,              -- seconds offset from test start
    limb                    VARCHAR(20),
    last_modified_utc       TIMESTAMPTZ,
    results                 JSONB,

    -- lineage & audit
    raw_id                  BIGINT,
    batch_id                UUID,
    ingested_at             TIMESTAMPTZ     DEFAULT now(),
    created_at              TIMESTAMPTZ     DEFAULT now(),
    updated_at              TIMESTAMPTZ     DEFAULT now(),

    CONSTRAINT pk_vald_forcedecks_trials PRIMARY KEY (trial_id)
);

CREATE INDEX IF NOT EXISTS ix_vald_forcedecks_trials_test
    ON bronze.vald_forcedecks_trials (test_id);

-- -----------------------------------------------------------------------------
-- bronze.vald_forcedecks_trial_results
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.vald_forcedecks_trial_results (
    trial_result_id         BIGSERIAL       NOT NULL,
    trial_id                UUID            NOT NULL,
    test_id                 UUID            NOT NULL,
    profile_id              UUID            NOT NULL,
    result_id               INTEGER         NOT NULL,
    value                   NUMERIC,
    time                    NUMERIC,
    limb                    VARCHAR(20),
    repeat                  INTEGER,

    -- lineage & audit
    raw_id                  BIGINT,
    batch_id                UUID,
    ingested_at             TIMESTAMPTZ     DEFAULT now(),
    created_at              TIMESTAMPTZ     DEFAULT now(),
    updated_at              TIMESTAMPTZ     DEFAULT now(),

    CONSTRAINT pk_vald_forcedecks_trial_results PRIMARY KEY (trial_result_id)
);

CREATE INDEX IF NOT EXISTS ix_vald_forcedecks_trial_results_test
    ON bronze.vald_forcedecks_trial_results (test_id, profile_id);
CREATE INDEX IF NOT EXISTS ix_vald_forcedecks_trial_results_trial
    ON bronze.vald_forcedecks_trial_results (trial_id, result_id);

-- -----------------------------------------------------------------------------
-- bronze.vald_forcedecks_result_definitions
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.vald_forcedecks_result_definitions (
    result_id                   INTEGER         NOT NULL,
    result_id_string            VARCHAR(50),
    result_name                 VARCHAR(255),
    result_description          TEXT,
    result_group                VARCHAR(255),
    supports_asymmetry          BOOLEAN,
    is_repeat_result            BOOLEAN,
    result_unit                 VARCHAR(50),
    result_unit_name            VARCHAR(100),
    result_unit_scale_factor    NUMERIC,
    number_of_decimal_places    INTEGER,
    trend_direction             VARCHAR(20),

    -- lineage & audit
    raw_id                      BIGINT,
    batch_id                    UUID,
    ingested_at                 TIMESTAMPTZ     DEFAULT now(),
    created_at                  TIMESTAMPTZ     DEFAULT now(),
    updated_at                  TIMESTAMPTZ     DEFAULT now(),

    CONSTRAINT pk_vald_forcedecks_result_definitions PRIMARY KEY (result_id)
);
