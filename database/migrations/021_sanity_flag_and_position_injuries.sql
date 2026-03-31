-- Migration 021: Add sanity_flag column + position-matched injury feature columns
-- Session 88: Improve under-bet sanity checks and position-aware injury features

-- Sanity flag: stores warning reason(s) when a prediction looks suspect
-- e.g., "q50_divergence:62%|l5_above_line:18.0>14.5"
ALTER TABLE daily_predictions
ADD COLUMN IF NOT EXISTS sanity_flag TEXT;

-- Position-matched injury features: captures role-specific injury impact
-- Same-position group (G/W/B) teammates who are Out
ALTER TABLE daily_predictions
ADD COLUMN IF NOT EXISTS feat_team_out_same_pos_count SMALLINT;

ALTER TABLE daily_predictions
ADD COLUMN IF NOT EXISTS feat_team_out_same_pos_min_sum DOUBLE PRECISION;

ALTER TABLE daily_predictions
ADD COLUMN IF NOT EXISTS feat_team_out_same_pos_usg_sum DOUBLE PRECISION;

-- Sum of starter_prob for out same-position players (high value = starter-level competition is out)
ALTER TABLE daily_predictions
ADD COLUMN IF NOT EXISTS feat_team_out_same_pos_starter_sum DOUBLE PRECISION;
