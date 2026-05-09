-- =============================================================================
-- File: 33_bronze_vald_nordbord.sql
-- Description: Bronze schema - VALD NordBord tables.
--              Parsed and flattened from raw JSONB into typed columns.
--              Covers tests, test metrics, eccentric exercises, and
--              eccentric repetitions.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- bronze.vald_nordbord_tests
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.vald_nordbord_tests (
    test_id                     UUID            NOT NULL,
    tenant_id                   UUID            NOT NULL,
    profile_id                  UUID            NOT NULL,
    test_date_utc               TIMESTAMPTZ,
    test_type_id                UUID,
    test_type_name              VARCHAR(100),
    notes                       TEXT,
    device                      VARCHAR(100),
    modified_date_utc           TIMESTAMPTZ,
    left_avg_force              NUMERIC,
    left_impulse                NUMERIC,
    left_max_force              NUMERIC,
    left_torque                 NUMERIC,
    left_calibration            NUMERIC,
    left_repetitions            INTEGER,
    right_avg_force             NUMERIC,
    right_impulse               NUMERIC,
    right_max_force             NUMERIC,
    right_torque                NUMERIC,
    right_calibration           NUMERIC,
    right_repetitions           INTEGER,

    -- lineage & audit
    raw_id                      BIGINT,
    batch_id                    UUID,
    ingested_at                 TIMESTAMPTZ     DEFAULT now(),
    created_at                  TIMESTAMPTZ     DEFAULT now(),
    updated_at                  TIMESTAMPTZ     DEFAULT now(),

    CONSTRAINT pk_vald_nordbord_tests PRIMARY KEY (test_id)
);

-- -----------------------------------------------------------------------------
-- bronze.vald_nordbord_test_metrics
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.vald_nordbord_test_metrics (
    test_id                     UUID            NOT NULL,
    tenant_id                   UUID            NOT NULL,
    metrics_payload             JSONB,

    -- lineage & audit
    raw_id                      BIGINT,
    batch_id                    UUID,
    ingested_at                 TIMESTAMPTZ     DEFAULT now(),
    created_at                  TIMESTAMPTZ     DEFAULT now(),
    updated_at                  TIMESTAMPTZ     DEFAULT now(),

    CONSTRAINT pk_vald_nordbord_test_metrics PRIMARY KEY (test_id)
);

-- -----------------------------------------------------------------------------
-- bronze.vald_nordbord_ecc_exercises
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.vald_nordbord_ecc_exercises (
    exercise_id                 UUID            NOT NULL,
    session_id                  UUID            NOT NULL,
    program_exercise_id         UUID,
    profile_id                  UUID            NOT NULL,
    tenant_id                   UUID            NOT NULL,
    exercise_date_utc           TIMESTAMPTZ,
    modified_date_utc           TIMESTAMPTZ,
    force_left                  NUMERIC,
    force_right                 NUMERIC,
    impulse_left                NUMERIC,
    impulse_right               NUMERIC,

    -- lineage & audit
    raw_id                      BIGINT,
    batch_id                    UUID,
    ingested_at                 TIMESTAMPTZ     DEFAULT now(),
    created_at                  TIMESTAMPTZ     DEFAULT now(),
    updated_at                  TIMESTAMPTZ     DEFAULT now(),

    CONSTRAINT pk_vald_nordbord_ecc_exercises PRIMARY KEY (exercise_id)
);

-- -----------------------------------------------------------------------------
-- bronze.vald_nordbord_ecc_repetitions
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.vald_nordbord_ecc_repetitions (
    repetition_id               UUID            NOT NULL,
    profile_id                  UUID            NOT NULL,
    tenant_id                   UUID            NOT NULL,
    session_id                  UUID,
    session_exercise_id         UUID            NOT NULL,
    program_exercise_id         UUID,
    repetition_number           INTEGER,
    repetition_date_utc         TIMESTAMPTZ,
    modified_date_utc           TIMESTAMPTZ,
    force_left                  NUMERIC,
    force_right                 NUMERIC,
    impulse_left                NUMERIC,
    impulse_right               NUMERIC,

    -- lineage & audit
    raw_id                      BIGINT,
    batch_id                    UUID,
    ingested_at                 TIMESTAMPTZ     DEFAULT now(),
    created_at                  TIMESTAMPTZ     DEFAULT now(),
    updated_at                  TIMESTAMPTZ     DEFAULT now(),

    CONSTRAINT pk_vald_nordbord_ecc_repetitions PRIMARY KEY (repetition_id)
);
