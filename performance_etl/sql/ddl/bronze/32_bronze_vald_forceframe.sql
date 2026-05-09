-- =============================================================================
-- File: 32_bronze_vald_forceframe.sql
-- Description: Bronze schema - VALD ForceFrame tables.
--              Parsed and flattened from raw JSONB into typed columns.
--              Covers tests, test metrics, and force traces.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- bronze.vald_forceframe_tests
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.vald_forceframe_tests (
    test_id                     UUID            NOT NULL,
    tenant_id                   UUID            NOT NULL,
    profile_id                  UUID            NOT NULL,
    test_date_utc               TIMESTAMPTZ,
    test_type_id                UUID,
    test_type_name              VARCHAR(100),
    test_position_id            UUID,
    test_position_name          VARCHAR(100),
    notes                       TEXT,
    device                      VARCHAR(100),
    modified_date_utc           TIMESTAMPTZ,
    inner_left_avg_force        NUMERIC,
    inner_left_impulse          NUMERIC,
    inner_left_max_force        NUMERIC,
    inner_left_repetitions      INTEGER,
    inner_right_avg_force       NUMERIC,
    inner_right_impulse         NUMERIC,
    inner_right_max_force       NUMERIC,
    inner_right_repetitions     INTEGER,
    outer_left_avg_force        NUMERIC,
    outer_left_impulse          NUMERIC,
    outer_left_max_force        NUMERIC,
    outer_left_repetitions      INTEGER,
    outer_right_avg_force       NUMERIC,
    outer_right_impulse         NUMERIC,
    outer_right_max_force       NUMERIC,
    outer_right_repetitions     INTEGER,

    -- lineage & audit
    raw_id                      BIGINT,
    batch_id                    UUID,
    ingested_at                 TIMESTAMPTZ     DEFAULT now(),
    created_at                  TIMESTAMPTZ     DEFAULT now(),
    updated_at                  TIMESTAMPTZ     DEFAULT now(),

    CONSTRAINT pk_vald_forceframe_tests PRIMARY KEY (test_id)
);

-- -----------------------------------------------------------------------------
-- bronze.vald_forceframe_test_metrics
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.vald_forceframe_test_metrics (
    test_id                     UUID            NOT NULL,
    tenant_id                   UUID            NOT NULL,
    metrics_payload             JSONB,

    -- lineage & audit
    raw_id                      BIGINT,
    batch_id                    UUID,
    ingested_at                 TIMESTAMPTZ     DEFAULT now(),
    created_at                  TIMESTAMPTZ     DEFAULT now(),
    updated_at                  TIMESTAMPTZ     DEFAULT now(),

    CONSTRAINT pk_vald_forceframe_test_metrics PRIMARY KEY (test_id)
);

-- -----------------------------------------------------------------------------
-- bronze.vald_forceframe_force_traces (append-only; no updated_at)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.vald_forceframe_force_traces (
    trace_id                    BIGSERIAL       NOT NULL,
    test_id                     UUID            NOT NULL,
    profile_id                  UUID            NOT NULL,
    tick                        INTEGER         NOT NULL,
    inner_left_force            NUMERIC,
    inner_right_force           NUMERIC,
    outer_left_force            NUMERIC,
    outer_right_force           NUMERIC,

    -- lineage & audit
    raw_id                      BIGINT,
    batch_id                    UUID,
    ingested_at                 TIMESTAMPTZ     DEFAULT now(),
    created_at                  TIMESTAMPTZ     DEFAULT now(),

    CONSTRAINT pk_vald_forceframe_force_traces PRIMARY KEY (trace_id)
);
