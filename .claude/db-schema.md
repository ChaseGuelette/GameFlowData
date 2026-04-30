# Database Schema — public schema
> Auto-generated from information_schema. Regenerate after migrations.
> Format: column (data_type, N=nullable / NN=not null)

## admin_users
- user_id (uuid, NN)
- created_at (timestamptz, N)

## arb_opportunities
- id (uuid, NN), scan_time (timestamptz, N), sport (text, N), arb_type (text, N), market_type (text, N)
- player_name (text, N), stat_type (text, N), line (numeric, N)
- kalshi_ticker (text, N), kalshi_side (text, N), kalshi_price (numeric, N), kalshi_volume (int4, N), kalshi_fee (numeric, N)
- poly_condition_id (text, N), poly_side (text, N), poly_price (numeric, N), poly_liquidity (numeric, N), poly_fee (numeric, N)
- sportsbook_implied (numeric, N), combined_cost (numeric, N), gross_margin (numeric, N), net_margin (numeric, N)
- price_discrepancy (numeric, N), min_fillable (int4, N), estimated_profit (numeric, N)
- status (text, N), team1 (text, N), team2 (text, N), description (text, N)

## arb_paper_bets
- id (uuid, NN), placed_at (timestamptz, N), resolved_at (timestamptz, N), game_date (date, N), sport (text, N)
- arb_type (text, N), market_type (text, N), team1 (text, N), team2 (text, N), kalshi_yes_team (text, N), description (text, N)
- kalshi_ticker (text, N), kalshi_side (text, N), kalshi_price (numeric, N), kalshi_fee (numeric, N), kalshi_contracts (int4, N), kalshi_outcome (text, N)
- poly_condition_id (text, N), poly_side (text, N), poly_price (numeric, N), poly_fee (numeric, N), poly_contracts (int4, N), poly_outcome (text, N)
- combined_cost (numeric, N), net_margin (numeric, N), status (text, N), pnl (numeric, N), arb_opportunity_id (uuid, N)

## arb_paper_trading_daily_log
- id (uuid, NN), log_date (date, N), sport (text, N)
- arbs_placed (int4, N), arbs_resolved (int4, N), arbs_profit (int4, N), arbs_loss (int4, N), arbs_cancelled (int4, N)
- total_pnl (numeric, N), cumulative_pnl (numeric, N), created_at (timestamptz, N)

## chat_conversations
- id (uuid, NN), user_id (uuid, N), player_id (int8, N), player_name (text, N)
- stat (text, N), game_date (date, N), created_at (timestamptz, N), updated_at (timestamptz, N)

## chat_messages
- id (uuid, NN), conversation_id (uuid, N) → chat_conversations.id
- role (text, N), content (text, N), created_at (timestamptz, N)

## daily_prediction_samples
- id (uuid, NN), prediction_date (date, N), player_id (int8, N), game_id (int8, N)
- stat (text, N), n_samples (int4, N), samples_gz (bytea, N), created_at (timestamptz, N)

## daily_predictions
- prediction_date (date, N), player_id (int8, N), player_name (text, N), game_id (int8, N)
- team_id (int8, N), opponent_id (int8, N), stat (text, N)
- pred_mean (float8, N), pred_std (float8, N), pred_median (float8, N)
- pred_q10/q20/q30/q40/q50/q60/q70/q80/q90 (float8, N)
- line (float8, N), over_odds (int4, N), under_odds (int4, N)
- over_prob (float8, N), under_prob (float8, N), implied_over (float8, N), implied_under (float8, N)
- over_edge (float8, N), under_edge (float8, N)
- game_time (timestamptz, N), is_recommended (bool, N), bookmaker (text, N), prop_line (float8, N), sanity_flag (text, N)
- feat_* (float8, N) — many feature columns
- bl_over_prob/bl_under_prob/bl_over_edge/bl_under_edge/bl_confidence/bl_weight (float8, N)

## defense_by_position_latest
- team_id (int8, N), position_group (text, N), season_id (int4, N)
- games_l5/l15/szn (int4, N)
- pts_allowed_per100/reb_allowed_per100/ast_allowed_per100/stl_allowed_per100/blk_allowed_per100 etc. (float8, N)

## dfs_paper_daily_log
- id (uuid, NN), entry_date (date, N), entries_placed/won/lost/partial (int4, N)
- total_staked (numeric, N), total_pnl (numeric, N), roi_pct (numeric, N), cumulative_pnl (numeric, N)
- bankroll_after (numeric, N), created_at (timestamptz, N)

## dfs_paper_entries
- id (uuid, NN), entry_date (date, N), slip_type (text, N), platform (text, N), num_legs (int4, N)
- stake (numeric, N), status (text, N), legs_won/lost/push/cancelled (int4, N)
- payout_multiplier (numeric, N), pnl (numeric, N), avg_edge (float8, N), min_edge (float8, N)
- created_at (timestamptz, N), resolved_at (timestamptz, N)

## dfs_paper_legs
- id (uuid, NN), entry_id (uuid, N) → dfs_paper_entries.id
- player_id (int8, N), player_name (text, N), game_id (int8, N)
- stat_type (text, N), line (float8, N), direction (text, N), dfs_bookmaker (text, N)
- market_prob (float8, N), market_books (int4, N), edge (float8, N), status (text, N), actual_value (float8, N)

## game_id_map
- external_game_id (text, NN), game_id (int8, N), game_date_utc (date, N)
- home_team_id (int8, N) → teams.team_id, away_team_id (int8, N) → teams.team_id
- mapped_source (text, N), mapped_at (timestamptz, N)

## game_id_map_staging
- external_game_id (text, NN), game_id (int8, N), game_date_utc (date, N)
- mapped_source (text, N), mapped_at (timestamptz, N)

## job_executions
- id (uuid, NN), job_name (text, N), started_at (timestamptz, N), ended_at (timestamptz, N)
- status (text, N), duration_seconds (float8, N), error_message (text, N), metrics (jsonb, N), created_at (timestamptz, N)

## kalshi_cancel_queue
- id (uuid, NN), kalshi_order_id (text, N), game_date (date, N), ticker (text, N), sport (text, N)
- player_id (int8, N), player_name (text, N), stat_type (text, N), line (float8, N), side (text, N)
- contracts (int4, N), expected_cost (numeric, N), game_start_time (timestamptz, N)
- detected_at (timestamptz, N), status (text, N), approved_at (timestamptz, N)
- executed_at (timestamptz, N), cancel_error (text, N)

## kalshi_live_orders
- id (uuid, NN), game_date (date, N), ticker (text, N), sport (text, N)
- player_id (int8, N), player_name (text, N), stat_type (text, N), line (float8, N), side (text, N)
- order_type (text, N), contracts (int4, N), kalshi_order_id (text, N)
- fill_price (int4, N), fill_count (int4, N), total_cost (numeric, N), fee_paid (numeric, N)
- model_prob (float8, N), kalshi_implied (float8, N), edge (float8, N), fee_adjusted_edge (float8, N)
- status (text, N), actual_value (float8, N), pnl (numeric, N)
- placed_at (timestamptz, N), filled_at (timestamptz, N), resolved_at (timestamptz, N), game_start_time (timestamptz, N)

## kalshi_live_trading_config
- id (int4, NN), starting_bankroll (numeric, N), is_halted (bool, N), halt_reason (text, N), halted_at (timestamptz, N)
- daily_loss_reset_date (date, N), streak_count (int4, N), last_updated (timestamptz, N), hwm_dollars (numeric, N)

## kalshi_live_trading_daily_log
- game_date (date, NN), total_trades (int4, N), trades_won/lost/cancelled/pending (int4, N)
- total_cost (numeric, N), total_pnl (numeric, N), roi_pct (numeric, N), cumulative_pnl (numeric, N)
- balance_after (numeric, N), created_at (timestamptz, N), updated_at (timestamptz, N)

## kalshi_markets
- ticker (text, NN), event_ticker (text, N), series_ticker (text, N), sport (text, N)
- player_name (text, N), stat_type (text, N), line (float8, N), market_title (text, N)
- player_id (int8, N), game_id (int8, N)
- yes_price (int4, N), no_price (int4, N), yes_bid (int4, N), yes_ask (int4, N), bid_ask_spread (int4, N)
- volume (int4, N), open_interest (int4, N)
- model_prob (float8, N), kalshi_implied (float8, N), raw_edge (float8, N)
- maker_fee_adjusted_edge (float8, N), taker_fee_adjusted_edge (float8, N)
- sportsbook_consensus_line (float8, N), line_vs_sportsbook (float8, N)
- close_time (timestamptz, N), market_status (text, N), snapshot_time (timestamptz, N), created_at (timestamptz, N)
- bl_model_prob (float8, N), bl_edge (float8, N), bl_confidence (float8, N)
- market_type (text, N), team1 (text, N), team2 (text, N)

## kalshi_orderbook_snapshots
- id (uuid, NN), ticker (text, N), snapshot_time (timestamptz, N)
- yes_bid (int4, N), yes_ask (int4, N), yes_bid_size (int4, N), yes_ask_size (int4, N)
- depth (jsonb, N), mid_price (float8, N), spread (int4, N)
- total_bid_depth (int4, N), total_ask_depth (int4, N)

## kalshi_paper_bets
- id (uuid, NN), game_date (date, N), ticker (text, N), sport (text, N)
- player_id (int8, N), player_name (text, N), stat_type (text, N), line (float8, N), side (text, N)
- price (int4, N), contracts (int4, N), is_maker (bool, N), expected_fee (numeric, N)
- model_prob (float8, N), kalshi_implied (float8, N), edge (float8, N), fee_adjusted_edge (float8, N)
- status (text, N), fill_price (int4, N), actual_value (float8, N), pnl (numeric, N)
- placed_at (timestamptz, N), resolved_at (timestamptz, N), created_at (timestamptz, N)
- bet_reasoning (jsonb, N), close_time (timestamptz, N)

## kalshi_paper_trading_daily_log
- id (uuid, NN), game_date (date, N), sport (text, N)
- total_bets (int4, N), bets_won/lost/cancelled/pending (int4, N)
- total_cost (numeric, N), total_pnl (numeric, N), roi_pct (numeric, N), cumulative_pnl (numeric, N)
- bankroll_after (numeric, N), created_at (timestamptz, N), updated_at (timestamptz, N)

## kalshi_trade_queue
- id (uuid, NN), game_date (date, N), ticker (text, N), sport (text, N)
- player_id (int8, N), player_name (text, N), stat_type (text, N), line (float8, N), side (text, N)
- yes_price (int4, N), contracts (int4, N), expected_cost (numeric, N), expected_fee (numeric, N)
- model_prob (float8, N), kalshi_implied (float8, N), edge (float8, N), fee_adjusted_edge (float8, N)
- status (text, N), proposed_at (timestamptz, N), approved_at (timestamptz, N), executed_at (timestamptz, N), expires_at (timestamptz, N)
- sportsbook_consensus_line (float8, N)

## league_position_averages
- season_id (int4, N), position_group (text, N)
- league_off_rtg/reb_per100/ast_per100/stl_per100/blk_per100/threes_per100/tov_per100/fta_per100/oreb_per100/pf_per100 (float8, N)
- total_possessions (int4, N), total_games (int4, N), updated_at (timestamptz, N)

## league_priors_history
- season_id (int4, N), position_group (text, N), snapshot_date (date, N)
- (same stat columns as league_position_averages), created_at (timestamptz, N)

## mlb_active_roster
- player_id (int4, NN), team_id (int4, N), player_name (text, N), position (text, N)
- jersey_number (text, N), status (text, N), updated_at (timestamptz, N)

## mlb_batters_latest
- player_id (int4, NN), player_name (text, N), team_id (int4, N), season (int4, N)
- games_played (int4, N), pa (int4, N), ab (int4, N), hits (int4, N), doubles (int4, N), triples (int4, N), hr (int4, N)
- rbi (int4, N), runs (int4, N), bb (int4, N), so (int4, N), sb (int4, N)
- avg (float8, N), obp (float8, N), slg (float8, N), ops (float8, N)
- batting_order_avg (float8, N), updated_at (timestamptz, N)

## mlb_daily_prediction_samples
- id (uuid, NN), prediction_date (date, N), player_id (int4, N), game_id (int4, N)
- stat_type (text, N), n_samples (int4, N), samples_gz (bytea, N), created_at (timestamptz, N)

## mlb_daily_predictions
- id (uuid, NN), prediction_date (date, N), player_id (int4, N), player_name (text, N)
- game_id (int4, N), team_id (int4, N), opponent_id (int4, N), stat_type (text, N)
- pred_mean/std/median (float8, N), pred_q10-q90 (float8, N)
- line (float8, N), over_prob/under_prob (float8, N), implied_over/under (float8, N)
- over_edge/under_edge (float8, N), over_odds/under_odds (int4, N)
- bookmaker (text, N), is_recommended (bool, N), created_at (timestamptz, N)
- bl_over_prob/bl_under_prob/bl_over_edge/bl_under_edge/bl_confidence (float8, N)
- sportsbook_consensus_line (float8, N)

## mlb_game_lineups
- id (uuid, NN), game_id (int4, N), player_id (int4, N), team_id (int4, N)
- batting_order (int4, N), position (text, N), is_starting (bool, N)
- game_date (date, N), scraped_at (timestamptz, N)

## mlb_game_schedule
- game_id (int4, NN), game_date (date, N), game_time_utc (timestamptz, N)
- home_team_id (int4, N) → mlb_teams.team_id, away_team_id (int4, N) → mlb_teams.team_id
- home_team_name (text, N), away_team_name (text, N)
- status (text, N), venue (text, N), created_at (timestamptz, N)

## mlb_game_weather
- id (uuid, NN), game_id (int4, N), game_date (date, N), venue (text, N)
- temperature_f (float8, N), wind_speed_mph (float8, N), wind_direction (text, N)
- humidity_pct (float8, N), precipitation_in (float8, N), conditions (text, N)
- is_dome (bool, N), fetched_at (timestamptz, N)

## mlb_paper_bets
- id (uuid, NN), game_date (date, N), player_id (int4, N), player_name (text, N)
- game_id (int4, N), stat_type (text, N), line (float8, N), bet_direction (text, N)
- odds (int4, N), implied_prob (float8, N), model_prob (float8, N), edge (float8, N)
- stake (numeric, N), kelly_fraction (float8, N)
- status (text, N), actual_value (float8, N), pnl (numeric, N)
- placed_at (timestamptz, N), resolved_at (timestamptz, N), prediction_id (uuid, N)

## mlb_park_factors
- park_id (text, NN), venue_name (text, N), team_id (int4, N)
- pf_overall (float8, N), pf_h (float8, N), pf_hr (float8, N), pf_r (float8, N)
- pf_2b (float8, N), pf_3b (float8, N), pf_bb (float8, N), pf_so (float8, N)
- is_dome (bool, N), updated_at (timestamptz, N)

## mlb_pitchers_latest
- player_id (int4, NN), player_name (text, N), team_id (int4, N), season (int4, N)
- games_played (int4, N), gs (int4, N), ip (float8, N), w (int4, N), l (int4, N), sv (int4, N)
- era (float8, N), whip (float8, N), k9 (float8, N), bb9 (float8, N), h9 (float8, N), hr9 (float8, N)
- so (int4, N), bb (int4, N), hits_allowed (int4, N), er (int4, N)
- updated_at (timestamptz, N)

## mlb_player_average_batting
- player_id (int4, NN), game_id (int4, N), team_id (int4, N), game_date (date, N), season (int4, N)
- games_l7/l14/l30/szn (int4, N)
- avg_hits/doubles/triples/hr/rbi/runs/bb/so/sb (float8, N)
- avg_avg/obp/slg/ops (float8, N)
- rolling_batting_order (float8, N), updated_at (timestamptz, N)

## mlb_player_average_pitching
- player_id (int4, NN), game_id (int4, N), team_id (int4, N), game_date (date, N), season (int4, N)
- games_l7/l14/l30/szn (int4, N)
- avg_ip/so/bb/hits/er/hr (float8, N)
- avg_era/whip/k9/bb9 (float8, N), updated_at (timestamptz, N)

## mlb_player_average_statcast_batting
- player_id (int4, NN), game_id (int4, N), game_date (date, N), season (int4, N)
- games_l14/l30/szn (int4, N)
- avg_exit_velocity/launch_angle/barrel_rate/hard_hit_rate/xba/xslg/xwoba (float8, N)
- updated_at (timestamptz, N)

## mlb_player_average_statcast_pitching
- player_id (int4, NN), game_id (int4, N), game_date (date, N), season (int4, N)
- games_l14/l30/szn (int4, N)
- avg_spin_rate/velocity/whiff_rate/chase_rate/xera/xfip (float8, N)
- updated_at (timestamptz, N)

## mlb_player_game_statcast_batting
- id (uuid, NN), player_id (int4, N) → mlb_players.player_id, game_id (int4, N), game_date (date, N)
- exit_velocity (float8, N), launch_angle (float8, N), barrel_rate (float8, N)
- hard_hit_rate (float8, N), xba (float8, N), xslg (float8, N), xwoba (float8, N)
- created_at (timestamptz, N)

## mlb_player_game_statcast_pitching
- id (uuid, NN), player_id (int4, N) → mlb_players.player_id, game_id (int4, N), game_date (date, N)
- spin_rate (float8, N), velocity (float8, N), whiff_rate (float8, N), chase_rate (float8, N)
- xera (float8, N), xfip (float8, N), created_at (timestamptz, N)

## mlb_player_game_stats_batting
- id (uuid, NN), player_id (int4, N) → mlb_players.player_id
- game_id (int4, N) → mlb_game_schedule.game_id, team_id (int4, N) → mlb_teams.team_id
- game_date (date, N), season (int4, N)
- ab (int4, N), hits (int4, N), doubles (int4, N), triples (int4, N), hr (int4, N)
- rbi (int4, N), runs (int4, N), bb (int4, N), so (int4, N), sb (int4, N)
- avg (float8, N), obp (float8, N), slg (float8, N), ops (float8, N)
- batting_order (int4, N), created_at (timestamptz, N)

## mlb_player_game_stats_pitching
- id (uuid, NN), player_id (int4, N) → mlb_players.player_id
- game_id (int4, N) → mlb_game_schedule.game_id, team_id (int4, N) → mlb_teams.team_id
- game_date (date, N), season (int4, N)
- ip (float8, N), so (int4, N), bb (int4, N), hits_allowed (int4, N), er (int4, N), hr_allowed (int4, N)
- era (float8, N), whip (float8, N), win (bool, N), loss (bool, N), save (bool, N)
- created_at (timestamptz, N)

## mlb_player_season_advanced
- id (uuid, NN), player_id (int4, N) → mlb_players.player_id, season (int4, N)
- wrc_plus (float8, N), war (float8, N), babip (float8, N), iso (float8, N)
- k_pct (float8, N), bb_pct (float8, N), gb_pct (float8, N), fb_pct (float8, N)
- hard_pct (float8, N), soft_pct (float8, N), updated_at (timestamptz, N)

## mlb_players
- player_id (int4, NN), player_name (text, N), position (text, N), bats (text, N), throws (text, N)
- team_id (int4, N), mlb_id (int4, N), created_at (timestamptz, N)

## mlb_raw_game_lines
- id (uuid, NN), game_id (int4, N), bookmaker (text, N), market_key (text, N)
- home_team (text, N), away_team (text, N), home_odds (int4, N), away_odds (int4, N)
- spread (float8, N), total (float8, N), snapshot_time (timestamptz, N)

## mlb_raw_player_props
- id (uuid, NN), game_id (int4, N), player_id (int4, N), player_name (text, N)
- bookmaker (text, N), market_key (text, N), stat_type (text, N)
- line (float8, N), over_odds (int4, N), under_odds (int4, N)
- snapshot_time (timestamptz, N), game_date (date, N)

## mlb_teams
- team_id (int4, NN), team_name (text, N), city (text, N), abbreviation (text, N)
- league (text, N), division (text, N), venue (text, N)

## paper_bets (NBA)
- id (uuid, NN), game_date (date, N), player_id (int8, N), player_name (text, N)
- stat_type (text, N), line (float8, N), bet_direction (text, N)
- odds_at_bet (int4, N), implied_prob (float8, N), model_prob (float8, N)
- edge (float8, N), stake (numeric, N), kelly_fraction (float8, N)
- status (text, N), actual_value (float8, N), pnl (numeric, N)
- placed_at (timestamptz, N), resolved_at (timestamptz, N), prediction_id (uuid, N)

## paper_trading_daily_log (NBA)
- id (uuid, NN), game_date (date, N)
- total_bets (int4, N), bets_won/lost/push/pending (int4, N)
- total_staked (numeric, N), total_pnl (numeric, N), roi_pct (numeric, N), cumulative_pnl (numeric, N)
- bankroll_after (numeric, N), created_at (timestamptz, N), updated_at (timestamptz, N)

## player_average_advanced_stats
- player_id (int8, NN) → players.player_id, game_id (int8, N), season_id (int4, N), game_date (date, N)
- team_id (int8, N) → teams.team_id, game_number (int4, N)
- games_l5/l15/szn (int4, N)
- avg_off_rtg/def_rtg/net_rtg/ts_pct/efg_pct/usg_pct/ast_ratio/ast_pct/ast_tov/tov_ratio (float8, N)
- avg_reb_pct/oreb_pct/dreb_pct/pace/poss/pie (float8, N)

## player_average_game_stats
- player_id (int8, NN) → players.player_id, game_id (int8, N), season_id (int4, N), game_date (date, N)
- team_id (int8, N) → teams.team_id, game_number (int4, N)
- games_l5/l15/szn (int4, N)
- avg_min/pts/reb/ast/stl/blk/tov/fgm/fga/fg_pct/fg3m/fg3a/fg3_pct/ftm/fta/ft_pct/oreb/dreb/pf/plus_minus (float8, N)

## player_game_advanced_stats
- player_id (int8, NN) → players.player_id, game_id (int8, NN)
- team_id (int8, N) → teams.team_id, season_id (int4, N), game_date (date, N)
- off_rtg/def_rtg/net_rtg/ts_pct/efg_pct/usg_pct/ast_ratio/ast_pct/ast_tov/tov_ratio (float8, N)
- reb_pct/oreb_pct/dreb_pct/pace/poss/pie (float8, N)

## player_game_stats
- player_id (int8, NN) → players.player_id, game_id (int8, NN) → team_game_stats
- season_id (int4, N), matchup (text, N), wl (text, N), min (float8, N)
- fgm/fga (int4, N), fg_pct (float8, N), fg3m/fg3a (int4, N), fg3_pct (float8, N)
- ftm/fta (int4, N), ft_pct (float8, N)
- oreb/dreb/reb/ast/stl/blk/tov/pf/pts (int4, N), plus_minus (int4, N)
- team_id (int8, N) → teams.team_id, game_date (date, N), started (bool, N), did_not_play (bool, N)

## player_position_history
- player_id (int8, NN) → players.player_id, team_id (int8, N) → teams.team_id
- snapshot_date (date, NN), season_id (int4, N)
- primary_position (text, N), position_group (text, N), position_confidence (float8, N)
- total_games_in_window (int4, N), created_at (timestamptz, N)

## player_stats_latest
- player_id (int8, NN), game_id (int8, N), season_id (int4, N), game_date (date, N)
- team_id (int8, N), game_number (int4, N)
- games_l5/l15/szn (int4, N)
- (rolling averages for all NBA stats — same columns as player_average_game_stats + advanced stats)
- updated_at (timestamptz, N)

## players
- player_id (int8, NN), player_name (text, N)
- primary_position (text, N), position_group (text, N)

## polymarket_markets
- id (uuid, NN), condition_id (text, N), token_id_yes/no (text, N)
- event_slug (text, N), sport (text, N), market_type (text, N)
- player_name (text, N), stat_type (text, N), line (float8, N), question (text, N)
- player_id (int8, N), team1 (text, N), team2 (text, N)
- yes_price/no_price (float8, N), yes_bid/yes_ask (float8, N)
- volume (float8, N), liquidity (float8, N), market_status (text, N)
- end_date (timestamptz, N), snapshot_time (timestamptz, N), category (text, N)

## rapidapi_injuries
- id (uuid, NN), report_date (date, N), team (text, N), player (text, N)
- status (text, N), reason (text, N), report_time (text, N)
- injury_category (text, N), injury_detail (text, N), scraped_at (timestamptz, N)
- player_id (int8, N), nba_team_id (int8, N)

## raw_game_lines_live / raw_game_lines_staging
- staging_id (uuid, NN), api_game_id (text, N), bookmaker (text, N), market_key (text, N)
- outcome_label (text, N), line (float8, N), odds_american (int4, N)
- commence_time (timestamptz, N), home_team (text, N), away_team (text, N)
- inserted_at (timestamptz, N), market_last_update (timestamptz, N), bookmaker_last_update (timestamptz, N)
- bookmaker_name (text, N), snapshot_time (timestamptz, N)
- nba_game_id (int8, N), nba_home_team_id (int8, N), nba_away_team_id (int8, N)

## raw_player_props_archive / raw_player_props_combined / raw_player_props_live
- api_game_id (text, N), api_player_name (text, N), bookmaker (text, N), market_key (text, N)
- outcome_label (text, N), line (float8, N), odds_american (int4, N)
- commence_time (timestamptz, N), home_team (text, N), away_team (text, N)
- inserted_at (timestamptz, N), market_last_update (timestamptz, N), bookmaker_last_update (timestamptz, N)
- bookmaker_name (text, N), snapshot_time (timestamptz, N)
- game_id (int8, N), player_id (int8, N), team_id (int8, N)
- archived_at (timestamptz, N)  ← archive only

## team_allowed_by_position
- team_id (int8, NN) → teams.team_id, position_group (text, N), season_id (int4, N)
- (defensive stats allowed per position — mirrors defense_by_position_latest structure)

## team_average_game_stats
- team_id (int8, NN) → teams.team_id, game_id (int8, N), season_id (int4, N), game_date (date, N)
- games_l5/l15/szn (int4, N)
- avg_pts/reb/ast/stl/blk/tov/fgm/fga/fg_pct/fg3m/fg3a/fg3_pct/ftm/fta/ft_pct/oreb/dreb/pf/plus_minus (float8, N)

## team_game_stats
- game_id (int8, NN), team_id (int8, NN) → teams.team_id, opponent_id (int8, N) → teams.team_id
- season_id (int4, N), matchup (text, N), wl (text, N)
- pts/reb/ast/stl/blk/tov/fgm/fga/fg3m/fg3a/ftm/fta/oreb/dreb/pf/plus_minus (int4/float8, N)

## team_play_types
- team_id (int8, N), season_id (int4, N), play_type (text, N)
- poss_pct (float8, N), pts_per_poss (float8, N), freq_pct (float8, N), updated_at (timestamptz, N)

## team_stats_latest
- team_id (int8, NN) → teams.team_id, game_id (int8, N), season_id (int4, N), game_date (date, N)
- games_l5/l15/szn (int4, N)
- (rolling averages for all team stats — same columns as team_average_game_stats)
- updated_at (timestamptz, N)

## teams
- team_id (int8, NN), team_name (text, N), city (text, N), abbreviation (text, N)

## user_bets
- id (uuid, NN), user_id (uuid, N), player_id (int8, N), player_name (text, N)
- stat_type (text, N), line (float8, N), bet_direction (text, N)
- odds (int4, N), stake (numeric, N), status (text, N)
- placed_at (timestamptz, N), resolved_at (timestamptz, N)

## user_bets_daily_log
- id (uuid, NN), user_id (uuid, N), game_date (date, N)
- total_bets (int4, N), bets_won/lost (int4, N), total_staked (numeric, N), total_pnl (numeric, N)
- created_at (timestamptz, N)

## user_profiles
- user_id (uuid, NN), display_name (text, N), avatar_url (text, N)
- created_at (timestamptz, N), updated_at (timestamptz, N)

## user_sportsbooks
- id (uuid, NN), user_id (uuid, N), sportsbook_id (text, N), api_key (text, N)
- created_at (timestamptz, N)

## user_subscriptions
- id (uuid, NN), user_id (uuid, N), stripe_customer_id (text, N), stripe_subscription_id (text, N)
- status (text, N), period_end (timestamptz, N), created_at (timestamptz, N), updated_at (timestamptz, N)

## verified_market_links
- id (uuid, NN), kalshi_ticker (text, N), poly_condition_id (text, N)
- player_name (text, N), stat_type (text, N), line (float8, N)
- verified_at (timestamptz, N), created_by (text, N)
