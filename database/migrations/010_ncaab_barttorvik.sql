-- Migration 010: NCAAB Barttorvik Ratings Table
-- Stores point-in-time adjusted efficiency snapshots from barttorvik.com.

CREATE TABLE IF NOT EXISTS ncaab_barttorvik_ratings (
    rating_id       BIGSERIAL PRIMARY KEY,
    team_name       TEXT NOT NULL,
    team_id         INTEGER REFERENCES ncaab_teams(team_id),
    season          INTEGER NOT NULL,
    snapshot_date   DATE NOT NULL,
    -- Core adjusted efficiency metrics
    adj_oe          NUMERIC(6,2),
    adj_de          NUMERIC(6,2),
    adj_em          NUMERIC(6,2),
    adj_tempo       NUMERIC(6,2),
    barthag         NUMERIC(8,6),
    -- Four Factors (offensive)
    efg_pct         NUMERIC(5,4),
    tov_pct         NUMERIC(5,4),
    orb_pct         NUMERIC(5,4),
    ft_rate         NUMERIC(5,4),
    -- Four Factors (defensive / opponent)
    opp_efg_pct     NUMERIC(5,4),
    opp_tov_pct     NUMERIC(5,4),
    opp_orb_pct     NUMERIC(5,4),
    opp_ft_rate     NUMERIC(5,4),
    -- Ranks
    adj_oe_rank     SMALLINT,
    adj_de_rank     SMALLINT,
    adj_em_rank     SMALLINT,
    games_played    SMALLINT,
    created_at      TIMESTAMPTZ DEFAULT now(),
    UNIQUE(team_name, season, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_ncaab_bart_team_date
    ON ncaab_barttorvik_ratings(team_id, snapshot_date DESC);
CREATE INDEX IF NOT EXISTS idx_ncaab_bart_season
    ON ncaab_barttorvik_ratings(season, snapshot_date DESC);

ALTER TABLE ncaab_barttorvik_ratings ENABLE ROW LEVEL SECURITY;
