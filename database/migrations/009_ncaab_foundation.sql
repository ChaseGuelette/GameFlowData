-- Migration 009: NCAAB Foundation Tables
-- Creates ncaab_teams, ncaab_game_schedule, ncaab_team_box_scores,
-- and ncaab_raw_game_lines for game-level prediction pipeline.

-- ============================================================================
-- TEAMS — 363 D1 programs
-- ============================================================================

CREATE TABLE IF NOT EXISTS ncaab_teams (
    team_id         SERIAL PRIMARY KEY,
    espn_team_id    TEXT UNIQUE,
    team_name       TEXT NOT NULL,
    short_name      TEXT,
    abbreviation    TEXT,
    conference      TEXT,
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ncaab_teams_espn
    ON ncaab_teams(espn_team_id);
CREATE INDEX IF NOT EXISTS idx_ncaab_teams_abbrev
    ON ncaab_teams(abbreviation);

ALTER TABLE ncaab_teams ENABLE ROW LEVEL SECURITY;


-- ============================================================================
-- GAME SCHEDULE — one row per game
-- ============================================================================

CREATE TABLE IF NOT EXISTS ncaab_game_schedule (
    game_id         BIGINT PRIMARY KEY,
    game_date       DATE NOT NULL,
    game_datetime   TIMESTAMPTZ,
    season          INTEGER NOT NULL,
    season_type     TEXT,
    home_team_id    INTEGER REFERENCES ncaab_teams(team_id),
    away_team_id    INTEGER REFERENCES ncaab_teams(team_id),
    home_score      INTEGER,
    away_score      INTEGER,
    is_final        BOOLEAN DEFAULT FALSE,
    venue           TEXT,
    neutral_site    BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ncaab_schedule_date
    ON ncaab_game_schedule(game_date DESC);
CREATE INDEX IF NOT EXISTS idx_ncaab_schedule_season
    ON ncaab_game_schedule(season);

ALTER TABLE ncaab_game_schedule ENABLE ROW LEVEL SECURITY;


-- ============================================================================
-- TEAM BOX SCORES — one row per team per game (from CBBpy/ESPN)
-- ============================================================================

CREATE TABLE IF NOT EXISTS ncaab_team_box_scores (
    box_id          BIGSERIAL PRIMARY KEY,
    game_id         BIGINT NOT NULL REFERENCES ncaab_game_schedule(game_id),
    team_id         INTEGER NOT NULL REFERENCES ncaab_teams(team_id),
    game_date       DATE NOT NULL,
    season          INTEGER NOT NULL,
    is_home         BOOLEAN NOT NULL,
    -- Raw box score counting stats
    pts             INTEGER,
    fgm             INTEGER,
    fga             INTEGER,
    fg3m            INTEGER,
    fg3a            INTEGER,
    ftm             INTEGER,
    fta             INTEGER,
    oreb            INTEGER,
    dreb            INTEGER,
    reb             INTEGER,
    ast             INTEGER,
    stl             INTEGER,
    blk             INTEGER,
    tov             INTEGER,
    pf              INTEGER,
    -- Derived stats
    poss_est        NUMERIC(8,2),
    efg_pct         NUMERIC(5,4),
    tov_pct         NUMERIC(5,4),
    orb_pct         NUMERIC(5,4),
    ft_rate         NUMERIC(5,4),
    -- Opponent stats (for defensive Four Factors)
    opp_pts         INTEGER,
    opp_poss_est    NUMERIC(8,2),
    opp_efg_pct     NUMERIC(5,4),
    opp_tov_pct     NUMERIC(5,4),
    opp_drb_pct     NUMERIC(5,4),
    opp_ft_rate     NUMERIC(5,4),
    created_at      TIMESTAMPTZ DEFAULT now(),
    UNIQUE(game_id, team_id)
);

CREATE INDEX IF NOT EXISTS idx_ncaab_box_team_date
    ON ncaab_team_box_scores(team_id, game_date DESC);

ALTER TABLE ncaab_team_box_scores ENABLE ROW LEVEL SECURITY;


-- ============================================================================
-- RAW GAME LINES — Odds API ingest (mirrors mlb_raw_game_lines)
-- ============================================================================

CREATE TABLE IF NOT EXISTS ncaab_raw_game_lines (
    staging_id              BIGSERIAL PRIMARY KEY,
    api_game_id             TEXT NOT NULL,
    bookmaker               TEXT NOT NULL,
    bookmaker_name          TEXT,
    market_key              TEXT NOT NULL,
    outcome_label           TEXT,
    line                    NUMERIC(8,3),
    odds_american           INTEGER,
    commence_time           TEXT,
    home_team               TEXT,
    away_team               TEXT,
    snapshot_time           TEXT,
    market_last_update      TEXT,
    bookmaker_last_update   TEXT,
    -- Linked post-ingest (NULL until linker runs)
    game_id                 BIGINT,
    home_team_id            INTEGER,
    away_team_id            INTEGER,
    created_at              TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ncaab_lines_game_id
    ON ncaab_raw_game_lines(game_id);
CREATE INDEX IF NOT EXISTS idx_ncaab_lines_api_game
    ON ncaab_raw_game_lines(api_game_id);
CREATE INDEX IF NOT EXISTS idx_ncaab_lines_unlinked
    ON ncaab_raw_game_lines(game_id) WHERE game_id IS NULL;

ALTER TABLE ncaab_raw_game_lines ENABLE ROW LEVEL SECURITY;
