-- ============================================================================
-- 55_gold_vald_assessment_marts.sql
-- Gold schema: Group-scoped VALD assessment marts by family
-- ============================================================================

CREATE TABLE IF NOT EXISTS gold.vald_nordics (
    metric_id                 BIGSERIAL PRIMARY KEY,
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
    created_at                TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at                TIMESTAMPTZ     NOT NULL DEFAULT now()
);

DROP TABLE IF EXISTS gold.vald_jumps CASCADE;
DROP TABLE IF EXISTS gold.vald_forcedecks_other CASCADE;

CREATE TABLE IF NOT EXISTS gold.vald_forceframe (
    metric_id                 BIGSERIAL PRIMARY KEY,
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
    created_at                TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at                TIMESTAMPTZ     NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS gold.vald_forcedecks (
    metric_id                 BIGSERIAL PRIMARY KEY,
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
    created_at                TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at                TIMESTAMPTZ     NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS gold.vald_dynamo (
    metric_id                 BIGSERIAL PRIMARY KEY,
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
    created_at                TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at                TIMESTAMPTZ     NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS gold.vald_speed (
    metric_id                 BIGSERIAL PRIMARY KEY,
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
    rep_number                INTEGER,
    created_at                TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at                TIMESTAMPTZ     NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_gold_vald_nordics_profile_date
    ON gold.vald_nordics (provider_profile_id, test_date DESC);
CREATE INDEX IF NOT EXISTS idx_gold_vald_forceframe_profile_date
    ON gold.vald_forceframe (provider_profile_id, test_date DESC);
CREATE INDEX IF NOT EXISTS idx_gold_vald_forcedecks_profile_date
    ON gold.vald_forcedecks (provider_profile_id, test_date DESC);
CREATE INDEX IF NOT EXISTS idx_gold_vald_dynamo_profile_date
    ON gold.vald_dynamo (provider_profile_id, test_date DESC);
CREATE INDEX IF NOT EXISTS idx_gold_vald_speed_profile_date
    ON gold.vald_speed (provider_profile_id, test_date DESC);
