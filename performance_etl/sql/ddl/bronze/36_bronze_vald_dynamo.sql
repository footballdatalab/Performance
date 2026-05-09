-- =============================================================================
-- File: 36_bronze_vald_dynamo.sql
-- Description: Bronze schema — VALD DynaMo tables.
--              Parsed and flattened from raw JSONB into typed columns.
--              Covers tests, rep summaries, repetitions, and traces.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- bronze.vald_dynamo_tests
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.vald_dynamo_tests (
    test_id                 UUID            NOT NULL,
    tenant_id               UUID            NOT NULL,
    profile_id              UUID            NOT NULL,
    test_category           VARCHAR(50),
    body_region             VARCHAR(100),
    movement                VARCHAR(100),
    position                VARCHAR(100),
    laterality              VARCHAR(50),
    attachments             JSONB,
    start_time_utc          TIMESTAMPTZ,
    duration_seconds        NUMERIC,
    hardware_info           JSONB,
    software_info           JSONB,
    analysis_info           JSONB,
    analysed_date_utc       TIMESTAMPTZ,
    asymmetries             JSONB,
    ratios                  JSONB,

    -- lineage & audit
    raw_id                  BIGINT,
    batch_id                UUID,
    ingested_at             TIMESTAMPTZ     DEFAULT now(),
    created_at              TIMESTAMPTZ     DEFAULT now(),
    updated_at              TIMESTAMPTZ     DEFAULT now(),

    CONSTRAINT pk_vald_dynamo_tests PRIMARY KEY (test_id)
);

CREATE INDEX IF NOT EXISTS ix_vald_dynamo_tests_tenant_profile_test_date
    ON bronze.vald_dynamo_tests (
        tenant_id,
        profile_id,
        (COALESCE(start_time_utc, analysed_date_utc))
    );

-- -----------------------------------------------------------------------------
-- bronze.vald_dynamo_rep_summaries
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.vald_dynamo_rep_summaries (
    rep_summary_id          BIGSERIAL       NOT NULL,
    test_id                 UUID            NOT NULL,
    movement_type           VARCHAR(100),
    side                    VARCHAR(50),
    max_force_newtons       NUMERIC,
    avg_force_newtons       NUMERIC,
    max_impulse_ns          NUMERIC,
    avg_impulse_ns          NUMERIC,
    max_rfd_nps             NUMERIC,
    avg_rfd_nps             NUMERIC,
    avg_time_to_peak_s      NUMERIC,
    min_time_to_peak_s      NUMERIC,
    max_rom_degrees         NUMERIC,
    avg_rom_degrees         NUMERIC,
    summary_payload         JSONB,

    -- lineage & audit
    raw_id                  BIGINT,
    batch_id                UUID,
    ingested_at             TIMESTAMPTZ     DEFAULT now(),
    created_at              TIMESTAMPTZ     DEFAULT now(),
    updated_at              TIMESTAMPTZ     DEFAULT now(),

    CONSTRAINT pk_vald_dynamo_rep_summaries PRIMARY KEY (rep_summary_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_vald_dynamo_rep_summaries_nk
    ON bronze.vald_dynamo_rep_summaries (test_id, movement_type, side);

-- -----------------------------------------------------------------------------
-- bronze.vald_dynamo_repetitions
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.vald_dynamo_repetitions (
    repetition_id           BIGSERIAL       NOT NULL,
    test_id                 UUID            NOT NULL,
    repetition_number       INTEGER         NOT NULL,
    side                    VARCHAR(50),
    impulse_ns              NUMERIC,
    rfd_nps                 NUMERIC,
    time_to_peak_s          NUMERIC,
    rom_degrees             NUMERIC,
    rep_payload             JSONB,

    -- lineage & audit
    raw_id                  BIGINT,
    batch_id                UUID,
    ingested_at             TIMESTAMPTZ     DEFAULT now(),
    created_at              TIMESTAMPTZ     DEFAULT now(),
    updated_at              TIMESTAMPTZ     DEFAULT now(),

    CONSTRAINT pk_vald_dynamo_repetitions PRIMARY KEY (repetition_id),
    CONSTRAINT uq_vald_dynamo_repetitions UNIQUE (test_id, repetition_number, side)
);

-- -----------------------------------------------------------------------------
-- bronze.vald_dynamo_traces  (append-only — no updated_at)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.vald_dynamo_traces (
    trace_id                BIGSERIAL       NOT NULL,
    test_id                 UUID            NOT NULL,
    profile_id              UUID            NOT NULL,
    tenant_id               UUID            NOT NULL,
    start_time_utc          TIMESTAMPTZ,
    trace_type              VARCHAR(20),
    force_trace             JSONB,
    imu_trace               JSONB,

    -- lineage & audit
    raw_id                  BIGINT,
    batch_id                UUID,
    ingested_at             TIMESTAMPTZ     DEFAULT now(),
    created_at              TIMESTAMPTZ     DEFAULT now(),

    CONSTRAINT pk_vald_dynamo_traces PRIMARY KEY (trace_id)
);
