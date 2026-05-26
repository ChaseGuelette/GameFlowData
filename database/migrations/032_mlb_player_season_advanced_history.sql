-- Point-in-time FanGraphs season-to-date snapshots, indexed by as_of_date.
-- Replaces the leaky `mlb_player_season_advanced` join pattern that joined on
-- season only and returned end-of-season values into past-date backtests.
-- Feature-store joins MUST use `as_of_date < game_date` (strict).
--
-- Production incident context: MLB inference hard-requires this table via
-- src/models/mlb/mlb_batter_feature_store.py and src/models/mlb/mlb_feature_store.py.
-- Keep this migration under database/migrations so normal GameFlow DB migration
-- discovery sees it; the original copy lived at migrations/create_mlb_player_season_advanced_history.sql.

CREATE TABLE IF NOT EXISTS mlb_player_season_advanced_history (
    player_id    INT NOT NULL,
    season       INT NOT NULL,
    player_type  TEXT NOT NULL CHECK (player_type IN ('pitcher','batter')),
    as_of_date   DATE NOT NULL,

    -- Pitcher columns
    war       FLOAT,
    babip     FLOAT,
    fip       FLOAT,
    xfip      FLOAT,
    xera      FLOAT,
    siera     FLOAT,
    era       FLOAT,
    lob_pct   FLOAT,
    gb_pct    FLOAT,
    k_per_9   FLOAT,
    bb_per_9  FLOAT,
    hr_per_9  FLOAT,
    ip        FLOAT,

    -- Batter/shared percentage columns
    wrc_plus  FLOAT,
    woba      FLOAT,
    iso       FLOAT,
    bb_pct    FLOAT,
    k_pct     FLOAT,
    hard_pct  FLOAT,
    avg       FLOAT,
    obp       FLOAT,
    slg       FLOAT,
    ops       FLOAT,
    pa        INT,

    fangraphs_id INT,
    scraped_at   TIMESTAMPTZ DEFAULT now(),

    PRIMARY KEY (player_id, season, player_type, as_of_date)
);

CREATE INDEX IF NOT EXISTS idx_fg_history_lookup
    ON mlb_player_season_advanced_history (player_id, player_type, as_of_date DESC);
