-- ============================================================================
-- 42_silver_vald_entities.sql
-- Silver schema: VALD-only scoped entities and long-form assessment facts
-- ============================================================================

CREATE TABLE IF NOT EXISTS silver.vald_athlete_profile (
    vald_profile_pk           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    provider_profile_id       UUID            NOT NULL,
    tenant_id                 UUID            NOT NULL,
    provider_full_name        VARCHAR(255),
    provider_given_name       VARCHAR(255),
    provider_family_name      VARCHAR(255),
    provider_status           VARCHAR(50),
    first_seen_at             TIMESTAMPTZ,
    last_seen_at              TIMESTAMPTZ,
    target_group_id           UUID,
    target_group_name         VARCHAR(255),
    target_category_id        UUID,
    target_category_name      VARCHAR(255),
    created_at                TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at                TIMESTAMPTZ     NOT NULL DEFAULT now(),

    CONSTRAINT uq_vald_profile UNIQUE (provider_profile_id, tenant_id)
);

ALTER TABLE silver.vald_athlete_profile
    ADD COLUMN IF NOT EXISTS target_group_id UUID,
    ADD COLUMN IF NOT EXISTS target_group_name VARCHAR(255),
    ADD COLUMN IF NOT EXISTS target_category_id UUID,
    ADD COLUMN IF NOT EXISTS target_category_name VARCHAR(255);

CREATE INDEX IF NOT EXISTS idx_vald_profile_name
    ON silver.vald_athlete_profile USING gin (provider_full_name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_vald_profile_target_group
    ON silver.vald_athlete_profile (target_group_id);

-- One row per VALD profile x target Active group.
CREATE TABLE IF NOT EXISTS silver.vald_target_group_membership (
    membership_id            BIGSERIAL PRIMARY KEY,
    provider_profile_id      UUID            NOT NULL,
    tenant_id                UUID            NOT NULL,
    target_group_id          UUID            NOT NULL,
    target_group_name        VARCHAR(255)    NOT NULL,
    target_category_id       UUID            NOT NULL,
    target_category_name     VARCHAR(255)    NOT NULL,
    is_ambiguous             BOOLEAN         NOT NULL DEFAULT FALSE,
    include_in_gold          BOOLEAN         NOT NULL DEFAULT FALSE,
    raw_id                   BIGINT,
    created_at               TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ     NOT NULL DEFAULT now(),

    CONSTRAINT uq_vald_target_group_membership
        UNIQUE (provider_profile_id, target_group_id)
);

CREATE INDEX IF NOT EXISTS idx_vald_target_group_membership_profile
    ON silver.vald_target_group_membership (provider_profile_id);
CREATE INDEX IF NOT EXISTS idx_vald_target_group_membership_group
    ON silver.vald_target_group_membership (target_group_id, include_in_gold);

-- Canonical long fact table for VALD assessments scoped to the target groups.
CREATE TABLE IF NOT EXISTS silver.vald_assessment_metric (
    assessment_metric_id      BIGSERIAL PRIMARY KEY,
    provider_profile_id       UUID            NOT NULL,
    athlete_name              VARCHAR(255),
    team_name                 VARCHAR(255)    NOT NULL,
    team_group_name           VARCHAR(255)    NOT NULL,
    team_group_id             UUID            NOT NULL,
    category_id               UUID            NOT NULL,
    test_date                 TIMESTAMPTZ     NOT NULL,
    source_module             VARCHAR(50)     NOT NULL,
    assessment_family         VARCHAR(50)     NOT NULL,
    test_id                   UUID            NOT NULL,
    test_name                 VARCHAR(255),
    test_type                 VARCHAR(100),
    metric_name               VARCHAR(255)    NOT NULL,
    metric_value              NUMERIC,
    metric_unit               VARCHAR(50),
    side                      VARCHAR(50),
    rep_number                INTEGER,
    metric_row_key            VARCHAR(64),
    created_at                TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at                TIMESTAMPTZ     NOT NULL DEFAULT now(),

    CONSTRAINT chk_vald_source_module
        CHECK (source_module IN (
            'forcedecks',
            'forceframe',
            'nordbord',
            'smartspeed',
            'dynamo'
        ))
);

ALTER TABLE silver.vald_assessment_metric
    ADD COLUMN IF NOT EXISTS metric_row_key VARCHAR(64);

ALTER TABLE silver.vald_assessment_metric
    DROP CONSTRAINT IF EXISTS chk_vald_assessment_family;

ALTER TABLE silver.vald_assessment_metric
    ADD CONSTRAINT chk_vald_assessment_family
        CHECK (assessment_family IN (
            'nordics',
            'forceframe',
            'forcedecks',
            'dynamo',
            'speed'
        ));

CREATE INDEX IF NOT EXISTS idx_vald_assessment_metric_profile_date
    ON silver.vald_assessment_metric (provider_profile_id, test_date DESC);
CREATE INDEX IF NOT EXISTS idx_vald_assessment_metric_family
    ON silver.vald_assessment_metric (assessment_family, test_date DESC);
CREATE INDEX IF NOT EXISTS idx_vald_assessment_metric_team
    ON silver.vald_assessment_metric (team_group_id, test_date DESC);
CREATE INDEX IF NOT EXISTS idx_vald_assessment_metric_test
    ON silver.vald_assessment_metric (test_id, metric_name);
CREATE UNIQUE INDEX IF NOT EXISTS uq_vald_assessment_metric_row_key
    ON silver.vald_assessment_metric (metric_row_key)
    WHERE metric_row_key IS NOT NULL;
