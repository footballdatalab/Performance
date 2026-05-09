-- =============================================================================
-- File: 30_bronze_vald_reference.sql
-- Description: Bronze schema - active VALD reference tables.
--              Parsed and flattened from raw JSONB into typed columns.
--              Keeps only profiles and the derived profile-category bridge.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- bronze.vald_profiles
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.vald_profiles (
    vald_profile_id     UUID            NOT NULL,
    tenant_id           UUID            NOT NULL,
    given_name          VARCHAR(255),
    family_name         VARCHAR(255),
    external_id         VARCHAR(255),

    -- lineage & audit
    raw_id              BIGINT,
    batch_id            UUID,
    ingested_at         TIMESTAMPTZ     DEFAULT now(),
    created_at          TIMESTAMPTZ     DEFAULT now(),
    updated_at          TIMESTAMPTZ     DEFAULT now(),

    CONSTRAINT pk_vald_profiles PRIMARY KEY (vald_profile_id)
);

CREATE INDEX IF NOT EXISTS ix_vald_profiles_tenant
    ON bronze.vald_profiles (tenant_id);

-- -----------------------------------------------------------------------------
-- bronze.vald_profile_categories
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.vald_profile_categories (
    vald_profile_id     UUID            NOT NULL,
    tenant_id           UUID            NOT NULL,
    category_id         UUID,
    category_name       VARCHAR(255)    NOT NULL,
    group_id            UUID,
    group_name          VARCHAR(255),

    -- lineage & audit
    raw_id              BIGINT,
    ingested_at         TIMESTAMPTZ     DEFAULT now(),
    created_at          TIMESTAMPTZ     DEFAULT now(),
    updated_at          TIMESTAMPTZ     DEFAULT now(),

    CONSTRAINT uq_vald_profile_categories UNIQUE (vald_profile_id, category_id)
);

CREATE INDEX IF NOT EXISTS ix_vald_profile_categories_profile
    ON bronze.vald_profile_categories (vald_profile_id);

CREATE INDEX IF NOT EXISTS ix_vald_profile_categories_group
    ON bronze.vald_profile_categories (group_id);
