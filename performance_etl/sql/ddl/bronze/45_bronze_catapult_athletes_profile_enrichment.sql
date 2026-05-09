-- ============================================================================
-- 45_bronze_catapult_athletes_profile_enrichment.sql
-- Retain non-duplicated athlete profile fields from the Catapult athletes API.
-- ============================================================================

ALTER TABLE IF EXISTS bronze.catapult_athletes
    ADD COLUMN IF NOT EXISTS current_team_id TEXT,
    ADD COLUMN IF NOT EXISTS gender VARCHAR(50),
    ADD COLUMN IF NOT EXISTS nickname VARCHAR(255),
    ADD COLUMN IF NOT EXISTS height INTEGER,
    ADD COLUMN IF NOT EXISTS weight INTEGER,
    ADD COLUMN IF NOT EXISTS velocity_max NUMERIC,
    ADD COLUMN IF NOT EXISTS acceleration_max NUMERIC,
    ADD COLUMN IF NOT EXISTS heart_rate_max NUMERIC,
    ADD COLUMN IF NOT EXISTS player_load_max NUMERIC,
    ADD COLUMN IF NOT EXISTS max_player_load_per_minute NUMERIC,
    ADD COLUMN IF NOT EXISTS image VARCHAR(512),
    ADD COLUMN IF NOT EXISTS icon VARCHAR(255),
    ADD COLUMN IF NOT EXISTS stroke_colour VARCHAR(50),
    ADD COLUMN IF NOT EXISTS fill_colour VARCHAR(50),
    ADD COLUMN IF NOT EXISTS trail_colour_start VARCHAR(50),
    ADD COLUMN IF NOT EXISTS trail_colour_end VARCHAR(50),
    ADD COLUMN IF NOT EXISTS is_synced BOOLEAN,
    ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN,
    ADD COLUMN IF NOT EXISTS is_demo BOOLEAN,
    ADD COLUMN IF NOT EXISTS provider_created_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS provider_modified_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS ix_catapult_athletes_current_team
    ON bronze.catapult_athletes (source_account, current_team_id);

DO $$
BEGIN
    IF to_regclass('bronze.catapult_athletes') IS NOT NULL
       AND to_regclass('bronze.catapult_teams') IS NOT NULL
       AND NOT EXISTS (
           SELECT 1
           FROM pg_constraint
           WHERE conname = 'fk_catapult_athletes_current_team'
             AND conrelid = 'bronze.catapult_athletes'::regclass
       ) THEN
        ALTER TABLE bronze.catapult_athletes
            ADD CONSTRAINT fk_catapult_athletes_current_team
            FOREIGN KEY (source_account, current_team_id)
            REFERENCES bronze.catapult_teams (source_account, team_id);
    END IF;
END $$;
