-- ============================================================================
-- 38_bronze_catapult_tables.sql
-- Bronze schema: Catapult typed landing tables with provider-internal links.
-- ============================================================================

CREATE TABLE IF NOT EXISTS bronze.catapult_teams (
    source_account      VARCHAR(50)     NOT NULL,
    team_id             TEXT            NOT NULL,
    team_name           VARCHAR(255),
    team_code           VARCHAR(100),
    raw_id              BIGINT,
    batch_id            UUID,
    ingested_at         TIMESTAMPTZ     NOT NULL DEFAULT now(),
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),

    CONSTRAINT pk_catapult_teams PRIMARY KEY (source_account, team_id)
);

CREATE INDEX IF NOT EXISTS ix_catapult_teams_name
    ON bronze.catapult_teams (source_account, team_name);

CREATE TABLE IF NOT EXISTS bronze.catapult_positions (
    source_account      VARCHAR(50)     NOT NULL,
    position_id         TEXT            NOT NULL,
    position_name       VARCHAR(255),
    position_slug       VARCHAR(255),
    sport_name          VARCHAR(255),
    raw_id              BIGINT,
    batch_id            UUID,
    ingested_at         TIMESTAMPTZ     NOT NULL DEFAULT now(),
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),

    CONSTRAINT pk_catapult_positions PRIMARY KEY (source_account, position_id)
);

CREATE TABLE IF NOT EXISTS bronze.catapult_parameters (
    source_account      VARCHAR(50)     NOT NULL,
    parameter_id        TEXT            NOT NULL,
    parameter_name      VARCHAR(255),
    parameter_slug      VARCHAR(255),
    parameter_unit      VARCHAR(100),
    raw_id              BIGINT,
    batch_id            UUID,
    ingested_at         TIMESTAMPTZ     NOT NULL DEFAULT now(),
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),

    CONSTRAINT pk_catapult_parameters PRIMARY KEY (source_account, parameter_id)
);

CREATE INDEX IF NOT EXISTS ix_catapult_parameters_name
    ON bronze.catapult_parameters (source_account, parameter_name);

CREATE TABLE IF NOT EXISTS bronze.catapult_venues (
    source_account      VARCHAR(50)     NOT NULL,
    venue_id            TEXT            NOT NULL,
    venue_name          VARCHAR(255),
    venue_city          VARCHAR(255),
    venue_country       VARCHAR(255),
    latitude            NUMERIC,
    longitude           NUMERIC,
    raw_id              BIGINT,
    batch_id            UUID,
    ingested_at         TIMESTAMPTZ     NOT NULL DEFAULT now(),
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),

    CONSTRAINT pk_catapult_venues PRIMARY KEY (source_account, venue_id)
);

CREATE TABLE IF NOT EXISTS bronze.catapult_tag_types (
    source_account      VARCHAR(50)     NOT NULL,
    tag_type_id         TEXT            NOT NULL,
    tag_type_name       VARCHAR(255),
    raw_id              BIGINT,
    batch_id            UUID,
    ingested_at         TIMESTAMPTZ     NOT NULL DEFAULT now(),
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),

    CONSTRAINT pk_catapult_tag_types PRIMARY KEY (source_account, tag_type_id)
);

CREATE TABLE IF NOT EXISTS bronze.catapult_tags (
    source_account      VARCHAR(50)     NOT NULL,
    tag_id              TEXT            NOT NULL,
    tag_type_id         TEXT,
    tag_name            VARCHAR(255),
    raw_id              BIGINT,
    batch_id            UUID,
    ingested_at         TIMESTAMPTZ     NOT NULL DEFAULT now(),
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),

    CONSTRAINT pk_catapult_tags PRIMARY KEY (source_account, tag_id),
    CONSTRAINT fk_catapult_tags_tag_type
        FOREIGN KEY (source_account, tag_type_id)
        REFERENCES bronze.catapult_tag_types (source_account, tag_type_id)
);

CREATE INDEX IF NOT EXISTS ix_catapult_tags_name
    ON bronze.catapult_tags (source_account, tag_name);

CREATE TABLE IF NOT EXISTS bronze.catapult_athletes (
    source_account      VARCHAR(50)     NOT NULL,
    athlete_id          TEXT            NOT NULL,
    current_team_id     TEXT,
    position_id         TEXT,
    first_name          VARCHAR(255),
    last_name           VARCHAR(255),
    full_name           VARCHAR(255),
    gender              VARCHAR(50),
    nickname            VARCHAR(255),
    height              INTEGER,
    weight              INTEGER,
    date_of_birth       DATE,
    jersey_number       VARCHAR(50),
    velocity_max        NUMERIC,
    acceleration_max    NUMERIC,
    heart_rate_max      NUMERIC,
    player_load_max     NUMERIC,
    max_player_load_per_minute NUMERIC,
    image               VARCHAR(512),
    icon                VARCHAR(255),
    stroke_colour       VARCHAR(50),
    fill_colour         VARCHAR(50),
    trail_colour_start  VARCHAR(50),
    trail_colour_end    VARCHAR(50),
    is_synced           BOOLEAN,
    is_deleted          BOOLEAN,
    is_demo             BOOLEAN,
    provider_created_at TIMESTAMPTZ,
    provider_modified_at TIMESTAMPTZ,
    raw_id              BIGINT,
    batch_id            UUID,
    ingested_at         TIMESTAMPTZ     NOT NULL DEFAULT now(),
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),

    CONSTRAINT pk_catapult_athletes PRIMARY KEY (source_account, athlete_id),
    CONSTRAINT fk_catapult_athletes_current_team
        FOREIGN KEY (source_account, current_team_id)
        REFERENCES bronze.catapult_teams (source_account, team_id),
    CONSTRAINT fk_catapult_athletes_position
        FOREIGN KEY (source_account, position_id)
        REFERENCES bronze.catapult_positions (source_account, position_id)
);

CREATE INDEX IF NOT EXISTS ix_catapult_athletes_name
    ON bronze.catapult_athletes (source_account, full_name);

CREATE INDEX IF NOT EXISTS ix_catapult_athletes_current_team
    ON bronze.catapult_athletes (source_account, current_team_id);

CREATE TABLE IF NOT EXISTS bronze.catapult_activities (
    source_account      VARCHAR(50)     NOT NULL,
    activity_id         TEXT            NOT NULL,
    activity_name       VARCHAR(255),
    start_time          TIMESTAMPTZ     NOT NULL,
    end_time            TIMESTAMPTZ,
    duration_seconds    NUMERIC,
    raw_id              BIGINT,
    batch_id            UUID,
    ingested_at         TIMESTAMPTZ     NOT NULL DEFAULT now(),
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),

    CONSTRAINT pk_catapult_activities PRIMARY KEY (source_account, activity_id)
);

CREATE INDEX IF NOT EXISTS ix_catapult_activities_time
    ON bronze.catapult_activities (source_account, start_time DESC);
CREATE INDEX IF NOT EXISTS ix_catapult_activities_name
    ON bronze.catapult_activities (source_account, activity_name);

CREATE TABLE IF NOT EXISTS bronze.catapult_periods (
    source_account      VARCHAR(50)     NOT NULL,
    period_id           TEXT            NOT NULL,
    activity_id         TEXT            NOT NULL,
    period_name         VARCHAR(255),
    start_time          TIMESTAMPTZ,
    end_time            TIMESTAMPTZ,
    duration_seconds    NUMERIC,
    raw_id              BIGINT,
    batch_id            UUID,
    ingested_at         TIMESTAMPTZ     NOT NULL DEFAULT now(),
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),

    CONSTRAINT pk_catapult_periods PRIMARY KEY (source_account, period_id),
    CONSTRAINT fk_catapult_periods_activity
        FOREIGN KEY (source_account, activity_id)
        REFERENCES bronze.catapult_activities (source_account, activity_id)
);

CREATE INDEX IF NOT EXISTS ix_catapult_periods_activity
    ON bronze.catapult_periods (source_account, activity_id, start_time DESC);

CREATE TABLE IF NOT EXISTS bronze.catapult_annotations (
    source_account      VARCHAR(50)     NOT NULL,
    annotation_id       TEXT            NOT NULL,
    annotation_scope    VARCHAR(20)     NOT NULL,
    activity_id         TEXT,
    period_id           TEXT,
    athlete_id          TEXT,
    annotation_text     TEXT,
    created_by          VARCHAR(255),
    recorded_at         TIMESTAMPTZ,
    raw_id              BIGINT,
    batch_id            UUID,
    ingested_at         TIMESTAMPTZ     NOT NULL DEFAULT now(),
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),

    CONSTRAINT pk_catapult_annotations PRIMARY KEY (source_account, annotation_id),
    CONSTRAINT chk_catapult_annotations_scope
        CHECK (annotation_scope IN ('activity', 'period', 'athlete')),
    CONSTRAINT chk_catapult_annotations_target
        CHECK (
            (annotation_scope = 'activity' AND activity_id IS NOT NULL AND period_id IS NULL AND athlete_id IS NULL)
            OR
            (annotation_scope = 'period' AND activity_id IS NULL AND period_id IS NOT NULL AND athlete_id IS NULL)
            OR
            (annotation_scope = 'athlete' AND activity_id IS NULL AND period_id IS NULL AND athlete_id IS NOT NULL)
        ),
    CONSTRAINT fk_catapult_annotations_activity
        FOREIGN KEY (source_account, activity_id)
        REFERENCES bronze.catapult_activities (source_account, activity_id),
    CONSTRAINT fk_catapult_annotations_period
        FOREIGN KEY (source_account, period_id)
        REFERENCES bronze.catapult_periods (source_account, period_id),
    CONSTRAINT fk_catapult_annotations_athlete
        FOREIGN KEY (source_account, athlete_id)
        REFERENCES bronze.catapult_athletes (source_account, athlete_id)
);

CREATE INDEX IF NOT EXISTS ix_catapult_annotations_recorded_at
    ON bronze.catapult_annotations (source_account, recorded_at DESC);

CREATE TABLE IF NOT EXISTS bronze.catapult_entity_tags (
    source_account      VARCHAR(50)     NOT NULL,
    record_hash         VARCHAR(64)     NOT NULL,
    entity_type         VARCHAR(50)     NOT NULL,
    entity_id           VARCHAR(100)    NOT NULL,
    tag_id              TEXT            NOT NULL,
    tagged_at           TIMESTAMPTZ,
    raw_id              BIGINT,
    batch_id            UUID,
    ingested_at         TIMESTAMPTZ     NOT NULL DEFAULT now(),
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),

    CONSTRAINT pk_catapult_entity_tags PRIMARY KEY (record_hash),
    CONSTRAINT uq_catapult_entity_tags UNIQUE (source_account, entity_type, entity_id, tag_id),
    CONSTRAINT fk_catapult_entity_tags_tag
        FOREIGN KEY (source_account, tag_id)
        REFERENCES bronze.catapult_tags (source_account, tag_id)
);

CREATE INDEX IF NOT EXISTS ix_catapult_entity_tags_lookup
    ON bronze.catapult_entity_tags (source_account, entity_type, entity_id);

CREATE TABLE IF NOT EXISTS bronze.catapult_stats (
    source_account                      VARCHAR(50)     NOT NULL,
    activity_id                         TEXT            NOT NULL,
    athlete_id                          TEXT            NOT NULL,
    period_id                           TEXT,
    period_key                          TEXT            NOT NULL DEFAULT '',
    start_time                          TIMESTAMPTZ     NOT NULL,
    end_time                            TIMESTAMPTZ,
    total_distance                      NUMERIC,
    player_load                         NUMERIC,
    max_velocity                        NUMERIC,
    high_speed_running_distance         NUMERIC,
    sprint_distance                     NUMERIC,
    velocity_band_1_distance            NUMERIC,
    velocity_band_2_distance            NUMERIC,
    velocity_band_3_distance            NUMERIC,
    velocity_band_4_distance            NUMERIC,
    velocity_band_5_distance            NUMERIC,
    velocity_band_6_distance            NUMERIC,
    velocity_band_7_distance            NUMERIC,
    velocity_band_8_distance            NUMERIC,
    player_load_per_minute              NUMERIC,
    acceleration_efforts                NUMERIC,
    deceleration_efforts                NUMERIC,
    high_intensity_accelerations        NUMERIC,
    high_intensity_decelerations        NUMERIC,
    heart_rate_average                  NUMERIC,
    heart_rate_max                      NUMERIC,
    metabolic_power_average             NUMERIC,
    high_metabolic_load_distance        NUMERIC,
    all_parameters                      JSONB           NOT NULL DEFAULT '{}'::jsonb,
    raw_id                              BIGINT,
    batch_id                            UUID,
    ingested_at                         TIMESTAMPTZ     NOT NULL DEFAULT now(),
    created_at                          TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at                          TIMESTAMPTZ     NOT NULL DEFAULT now(),

    CONSTRAINT fk_catapult_stats_activity
        FOREIGN KEY (source_account, activity_id)
        REFERENCES bronze.catapult_activities (source_account, activity_id),
    CONSTRAINT fk_catapult_stats_athlete
        FOREIGN KEY (source_account, athlete_id)
        REFERENCES bronze.catapult_athletes (source_account, athlete_id),
    CONSTRAINT fk_catapult_stats_period
        FOREIGN KEY (source_account, period_id)
        REFERENCES bronze.catapult_periods (source_account, period_id),
    CONSTRAINT chk_catapult_stats_period_key
        CHECK (period_id IS NULL OR period_key = period_id)
) PARTITION BY RANGE (start_time);

CREATE UNIQUE INDEX IF NOT EXISTS uq_catapult_stats_row
    ON bronze.catapult_stats (
        source_account,
        activity_id,
        athlete_id,
        start_time,
        period_key
    );

CREATE TABLE IF NOT EXISTS bronze.catapult_efforts (
    source_account      VARCHAR(50)     NOT NULL,
    record_hash         VARCHAR(64)     NOT NULL,
    activity_id         TEXT            NOT NULL,
    athlete_id          TEXT            NOT NULL,
    effort_type         VARCHAR(100),
    magnitude           NUMERIC,
    start_time          TIMESTAMPTZ,
    end_time            TIMESTAMPTZ,
    raw_id              BIGINT,
    batch_id            UUID,
    ingested_at         TIMESTAMPTZ     NOT NULL DEFAULT now(),
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),

    CONSTRAINT pk_catapult_efforts PRIMARY KEY (record_hash),
    CONSTRAINT fk_catapult_efforts_activity
        FOREIGN KEY (source_account, activity_id)
        REFERENCES bronze.catapult_activities (source_account, activity_id),
    CONSTRAINT fk_catapult_efforts_athlete
        FOREIGN KEY (source_account, athlete_id)
        REFERENCES bronze.catapult_athletes (source_account, athlete_id)
);

CREATE INDEX IF NOT EXISTS ix_catapult_efforts_activity_time
    ON bronze.catapult_efforts (source_account, activity_id, start_time DESC);
CREATE INDEX IF NOT EXISTS ix_catapult_efforts_athlete_time
    ON bronze.catapult_efforts (source_account, athlete_id, start_time DESC);

CREATE TABLE IF NOT EXISTS bronze.catapult_events (
    source_account      VARCHAR(50)     NOT NULL,
    record_hash         VARCHAR(64)     NOT NULL,
    activity_id         TEXT            NOT NULL,
    athlete_id          TEXT            NOT NULL,
    event_type          VARCHAR(100),
    event_value         NUMERIC,
    occurred_at         TIMESTAMPTZ,
    event_payload       JSONB,
    raw_id              BIGINT,
    batch_id            UUID,
    ingested_at         TIMESTAMPTZ     NOT NULL DEFAULT now(),
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),

    CONSTRAINT pk_catapult_events PRIMARY KEY (record_hash),
    CONSTRAINT uq_catapult_events_row UNIQUE (source_account, activity_id, athlete_id, record_hash),
    CONSTRAINT fk_catapult_events_activity
        FOREIGN KEY (source_account, activity_id)
        REFERENCES bronze.catapult_activities (source_account, activity_id),
    CONSTRAINT fk_catapult_events_athlete
        FOREIGN KEY (source_account, athlete_id)
        REFERENCES bronze.catapult_athletes (source_account, athlete_id)
);

CREATE INDEX IF NOT EXISTS ix_catapult_events_activity_time
    ON bronze.catapult_events (source_account, activity_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS ix_catapult_events_athlete_time
    ON bronze.catapult_events (source_account, athlete_id, occurred_at DESC);

CREATE TABLE IF NOT EXISTS bronze.catapult_sensor_data (
    source_account      VARCHAR(50)     NOT NULL,
    record_hash         VARCHAR(64)     NOT NULL,
    activity_id         TEXT            NOT NULL,
    athlete_id          TEXT            NOT NULL,
    recorded_at         TIMESTAMPTZ     NOT NULL,
    latitude            NUMERIC,
    longitude           NUMERIC,
    velocity            NUMERIC,
    heart_rate          NUMERIC,
    accel_x             NUMERIC,
    accel_y             NUMERIC,
    accel_z             NUMERIC,
    raw_id              BIGINT,
    batch_id            UUID,
    ingested_at         TIMESTAMPTZ     NOT NULL DEFAULT now(),
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),

    CONSTRAINT fk_catapult_sensor_data_activity
        FOREIGN KEY (source_account, activity_id)
        REFERENCES bronze.catapult_activities (source_account, activity_id),
    CONSTRAINT fk_catapult_sensor_data_athlete
        FOREIGN KEY (source_account, athlete_id)
        REFERENCES bronze.catapult_athletes (source_account, athlete_id)
) PARTITION BY RANGE (recorded_at);

CREATE UNIQUE INDEX IF NOT EXISTS uq_catapult_sensor_data_row
    ON bronze.catapult_sensor_data (source_account, recorded_at, record_hash);
