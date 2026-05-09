-- ============================================================================
-- 40_bronze_catapult_trim_sparse_columns.sql
-- Drop Catapult bronze columns that were consistently provider-empty in review.
-- ============================================================================

DROP INDEX IF EXISTS bronze.ix_catapult_athletes_team;
DROP INDEX IF EXISTS bronze.ix_catapult_activities_team_time;

ALTER TABLE IF EXISTS bronze.catapult_athletes
    DROP CONSTRAINT IF EXISTS fk_catapult_athletes_team;

ALTER TABLE IF EXISTS bronze.catapult_activities
    DROP CONSTRAINT IF EXISTS fk_catapult_activities_team,
    DROP CONSTRAINT IF EXISTS fk_catapult_activities_venue;

ALTER TABLE IF EXISTS bronze.catapult_efforts
    DROP CONSTRAINT IF EXISTS fk_catapult_efforts_period;

ALTER TABLE IF EXISTS bronze.catapult_events
    DROP CONSTRAINT IF EXISTS fk_catapult_events_period;

ALTER TABLE IF EXISTS bronze.catapult_sensor_data
    DROP CONSTRAINT IF EXISTS fk_catapult_sensor_data_period;

ALTER TABLE IF EXISTS bronze.catapult_teams
    DROP COLUMN IF EXISTS timezone;

ALTER TABLE IF EXISTS bronze.catapult_athletes
    DROP COLUMN IF EXISTS team_id,
    DROP COLUMN IF EXISTS athlete_status;

ALTER TABLE IF EXISTS bronze.catapult_activities
    DROP COLUMN IF EXISTS team_id,
    DROP COLUMN IF EXISTS venue_id,
    DROP COLUMN IF EXISTS activity_type,
    DROP COLUMN IF EXISTS timezone;

ALTER TABLE IF EXISTS bronze.catapult_periods
    DROP COLUMN IF EXISTS period_order;

ALTER TABLE IF EXISTS bronze.catapult_stats
    DROP COLUMN IF EXISTS team_id;

ALTER TABLE IF EXISTS bronze.catapult_efforts
    DROP COLUMN IF EXISTS period_id;

ALTER TABLE IF EXISTS bronze.catapult_events
    DROP COLUMN IF EXISTS period_id;

ALTER TABLE IF EXISTS bronze.catapult_sensor_data
    DROP COLUMN IF EXISTS period_id;

CREATE INDEX IF NOT EXISTS ix_catapult_activities_time
    ON bronze.catapult_activities (source_account, start_time DESC);
