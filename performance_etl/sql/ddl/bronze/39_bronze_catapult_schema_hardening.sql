-- ============================================================================
-- 39_bronze_catapult_schema_hardening.sql
-- Reconcile Catapult bronze identifier columns to text-safe storage.
-- ============================================================================

ALTER TABLE IF EXISTS bronze.catapult_tags
    DROP CONSTRAINT IF EXISTS fk_catapult_tags_tag_type;

ALTER TABLE IF EXISTS bronze.catapult_athletes
    DROP CONSTRAINT IF EXISTS fk_catapult_athletes_current_team,
    DROP CONSTRAINT IF EXISTS fk_catapult_athletes_team,
    DROP CONSTRAINT IF EXISTS fk_catapult_athletes_position;

ALTER TABLE IF EXISTS bronze.catapult_activities
    DROP CONSTRAINT IF EXISTS fk_catapult_activities_team,
    DROP CONSTRAINT IF EXISTS fk_catapult_activities_venue;

ALTER TABLE IF EXISTS bronze.catapult_periods
    DROP CONSTRAINT IF EXISTS fk_catapult_periods_activity;

ALTER TABLE IF EXISTS bronze.catapult_annotations
    DROP CONSTRAINT IF EXISTS fk_catapult_annotations_activity,
    DROP CONSTRAINT IF EXISTS fk_catapult_annotations_period,
    DROP CONSTRAINT IF EXISTS fk_catapult_annotations_athlete;

ALTER TABLE IF EXISTS bronze.catapult_entity_tags
    DROP CONSTRAINT IF EXISTS fk_catapult_entity_tags_tag;

ALTER TABLE IF EXISTS bronze.catapult_stats
    DROP CONSTRAINT IF EXISTS fk_catapult_stats_activity,
    DROP CONSTRAINT IF EXISTS fk_catapult_stats_athlete,
    DROP CONSTRAINT IF EXISTS fk_catapult_stats_period,
    DROP CONSTRAINT IF EXISTS chk_catapult_stats_period_key;

ALTER TABLE IF EXISTS bronze.catapult_efforts
    DROP CONSTRAINT IF EXISTS fk_catapult_efforts_activity,
    DROP CONSTRAINT IF EXISTS fk_catapult_efforts_athlete,
    DROP CONSTRAINT IF EXISTS fk_catapult_efforts_period;

ALTER TABLE IF EXISTS bronze.catapult_events
    DROP CONSTRAINT IF EXISTS fk_catapult_events_activity,
    DROP CONSTRAINT IF EXISTS fk_catapult_events_athlete,
    DROP CONSTRAINT IF EXISTS fk_catapult_events_period;

ALTER TABLE IF EXISTS bronze.catapult_sensor_data
    DROP CONSTRAINT IF EXISTS fk_catapult_sensor_data_activity,
    DROP CONSTRAINT IF EXISTS fk_catapult_sensor_data_athlete,
    DROP CONSTRAINT IF EXISTS fk_catapult_sensor_data_period;

DROP INDEX IF EXISTS bronze.uq_catapult_stats_row;

ALTER TABLE IF EXISTS bronze.catapult_teams
    ALTER COLUMN team_id TYPE TEXT USING team_id::text;

ALTER TABLE IF EXISTS bronze.catapult_positions
    ALTER COLUMN position_id TYPE TEXT USING position_id::text;

ALTER TABLE IF EXISTS bronze.catapult_parameters
    ALTER COLUMN parameter_id TYPE TEXT USING parameter_id::text;

ALTER TABLE IF EXISTS bronze.catapult_venues
    ALTER COLUMN venue_id TYPE TEXT USING venue_id::text;

ALTER TABLE IF EXISTS bronze.catapult_tag_types
    ALTER COLUMN tag_type_id TYPE TEXT USING tag_type_id::text;

ALTER TABLE IF EXISTS bronze.catapult_tags
    ALTER COLUMN tag_id TYPE TEXT USING tag_id::text,
    ALTER COLUMN tag_type_id TYPE TEXT USING tag_type_id::text;

ALTER TABLE IF EXISTS bronze.catapult_athletes
    ADD COLUMN IF NOT EXISTS current_team_id TEXT;

ALTER TABLE IF EXISTS bronze.catapult_athletes
    ALTER COLUMN current_team_id TYPE TEXT USING current_team_id::text,
    ALTER COLUMN athlete_id TYPE TEXT USING athlete_id::text,
    ALTER COLUMN position_id TYPE TEXT USING position_id::text;

ALTER TABLE IF EXISTS bronze.catapult_activities
    ALTER COLUMN activity_id TYPE TEXT USING activity_id::text;

ALTER TABLE IF EXISTS bronze.catapult_periods
    ALTER COLUMN period_id TYPE TEXT USING period_id::text,
    ALTER COLUMN activity_id TYPE TEXT USING activity_id::text;

ALTER TABLE IF EXISTS bronze.catapult_annotations
    ALTER COLUMN annotation_id TYPE TEXT USING annotation_id::text,
    ALTER COLUMN activity_id TYPE TEXT USING activity_id::text,
    ALTER COLUMN period_id TYPE TEXT USING period_id::text,
    ALTER COLUMN athlete_id TYPE TEXT USING athlete_id::text;

ALTER TABLE IF EXISTS bronze.catapult_entity_tags
    ALTER COLUMN tag_id TYPE TEXT USING tag_id::text;

ALTER TABLE IF EXISTS bronze.catapult_stats
    ALTER COLUMN activity_id TYPE TEXT USING activity_id::text,
    ALTER COLUMN athlete_id TYPE TEXT USING athlete_id::text,
    ALTER COLUMN period_id TYPE TEXT USING period_id::text,
    ALTER COLUMN period_key DROP DEFAULT,
    ALTER COLUMN period_key TYPE TEXT USING COALESCE(period_id::text, period_key::text, ''),
    ALTER COLUMN period_key SET DEFAULT '';

ALTER TABLE IF EXISTS bronze.catapult_efforts
    ALTER COLUMN activity_id TYPE TEXT USING activity_id::text,
    ALTER COLUMN athlete_id TYPE TEXT USING athlete_id::text;

ALTER TABLE IF EXISTS bronze.catapult_events
    ALTER COLUMN activity_id TYPE TEXT USING activity_id::text,
    ALTER COLUMN athlete_id TYPE TEXT USING athlete_id::text;

ALTER TABLE IF EXISTS bronze.catapult_sensor_data
    ALTER COLUMN activity_id TYPE TEXT USING activity_id::text,
    ALTER COLUMN athlete_id TYPE TEXT USING athlete_id::text;

ALTER TABLE IF EXISTS bronze.catapult_tags
    ADD CONSTRAINT fk_catapult_tags_tag_type
    FOREIGN KEY (source_account, tag_type_id)
    REFERENCES bronze.catapult_tag_types (source_account, tag_type_id);

ALTER TABLE IF EXISTS bronze.catapult_athletes
    ADD CONSTRAINT fk_catapult_athletes_current_team
    FOREIGN KEY (source_account, current_team_id)
    REFERENCES bronze.catapult_teams (source_account, team_id),
    ADD CONSTRAINT fk_catapult_athletes_position
    FOREIGN KEY (source_account, position_id)
    REFERENCES bronze.catapult_positions (source_account, position_id);

ALTER TABLE IF EXISTS bronze.catapult_periods
    ADD CONSTRAINT fk_catapult_periods_activity
    FOREIGN KEY (source_account, activity_id)
    REFERENCES bronze.catapult_activities (source_account, activity_id);

ALTER TABLE IF EXISTS bronze.catapult_annotations
    ADD CONSTRAINT fk_catapult_annotations_activity
    FOREIGN KEY (source_account, activity_id)
    REFERENCES bronze.catapult_activities (source_account, activity_id),
    ADD CONSTRAINT fk_catapult_annotations_period
    FOREIGN KEY (source_account, period_id)
    REFERENCES bronze.catapult_periods (source_account, period_id),
    ADD CONSTRAINT fk_catapult_annotations_athlete
    FOREIGN KEY (source_account, athlete_id)
    REFERENCES bronze.catapult_athletes (source_account, athlete_id);

ALTER TABLE IF EXISTS bronze.catapult_entity_tags
    ADD CONSTRAINT fk_catapult_entity_tags_tag
    FOREIGN KEY (source_account, tag_id)
    REFERENCES bronze.catapult_tags (source_account, tag_id);

ALTER TABLE IF EXISTS bronze.catapult_stats
    ADD CONSTRAINT fk_catapult_stats_activity
    FOREIGN KEY (source_account, activity_id)
    REFERENCES bronze.catapult_activities (source_account, activity_id),
    ADD CONSTRAINT fk_catapult_stats_athlete
    FOREIGN KEY (source_account, athlete_id)
    REFERENCES bronze.catapult_athletes (source_account, athlete_id),
    ADD CONSTRAINT fk_catapult_stats_period
    FOREIGN KEY (source_account, period_id)
    REFERENCES bronze.catapult_periods (source_account, period_id),
    ADD CONSTRAINT chk_catapult_stats_period_key
    CHECK (period_id IS NULL OR period_key = period_id);

CREATE UNIQUE INDEX IF NOT EXISTS uq_catapult_stats_row
    ON bronze.catapult_stats (
        source_account,
        activity_id,
        athlete_id,
        start_time,
        period_key
    );

ALTER TABLE IF EXISTS bronze.catapult_efforts
    ADD CONSTRAINT fk_catapult_efforts_activity
    FOREIGN KEY (source_account, activity_id)
    REFERENCES bronze.catapult_activities (source_account, activity_id),
    ADD CONSTRAINT fk_catapult_efforts_athlete
    FOREIGN KEY (source_account, athlete_id)
    REFERENCES bronze.catapult_athletes (source_account, athlete_id);

ALTER TABLE IF EXISTS bronze.catapult_events
    ADD CONSTRAINT fk_catapult_events_activity
    FOREIGN KEY (source_account, activity_id)
    REFERENCES bronze.catapult_activities (source_account, activity_id),
    ADD CONSTRAINT fk_catapult_events_athlete
    FOREIGN KEY (source_account, athlete_id)
    REFERENCES bronze.catapult_athletes (source_account, athlete_id);

ALTER TABLE IF EXISTS bronze.catapult_sensor_data
    ADD CONSTRAINT fk_catapult_sensor_data_activity
    FOREIGN KEY (source_account, activity_id)
    REFERENCES bronze.catapult_activities (source_account, activity_id),
    ADD CONSTRAINT fk_catapult_sensor_data_athlete
    FOREIGN KEY (source_account, athlete_id)
    REFERENCES bronze.catapult_athletes (source_account, athlete_id);
