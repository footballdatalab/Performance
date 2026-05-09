-- ============================================================================
-- 50_gold_focus_tables.sql
-- Gold schema: Focus Physical Tactical Profile tables.
-- Stores uploaded CSV/Excel data parsed into structured rows with session
-- isolation per user.
-- ============================================================================

CREATE TABLE IF NOT EXISTS gold.focus_upload_sessions (
    session_id          UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id             VARCHAR(100)    NOT NULL,
    team                VARCHAR(50)     NOT NULL,
    original_filenames  TEXT[]          NOT NULL,
    row_count           INTEGER         NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_focus_sessions_user
    ON gold.focus_upload_sessions (user_id, created_at DESC);

CREATE TABLE IF NOT EXISTS gold.focus_uploaded_data (
    row_id              BIGSERIAL       PRIMARY KEY,
    session_id          UUID            NOT NULL
                        REFERENCES gold.focus_upload_sessions(session_id) ON DELETE CASCADE,
    player              VARCHAR(255),
    action_type         VARCHAR(255),
    session_name        VARCHAR(255),
    start_zone          VARCHAR(100),
    end_zone            VARCHAR(100),
    velocity_band       VARCHAR(100),
    jersey_number       VARCHAR(50),
    all_columns         JSONB           NOT NULL,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_focus_data_session
    ON gold.focus_uploaded_data (session_id);
CREATE INDEX IF NOT EXISTS ix_focus_data_player
    ON gold.focus_uploaded_data (session_id, player);
