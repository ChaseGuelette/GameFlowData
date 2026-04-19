# Graph Report - src  (2026-04-19)

## Corpus Check
- Large corpus: 211 files · ~220,869 words. Semantic extraction will be expensive (many Claude tokens). Consider running on a subfolder, or use --no-semantic to run AST-only.

## Summary
- 2960 nodes · 6528 edges · 82 communities detected
- Extraction: 60% EXTRACTED · 40% INFERRED · 0% AMBIGUOUS · INFERRED: 2635 edges (avg confidence: 0.64)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- [[_COMMUNITY_Backtesting Engine|Backtesting Engine]]
- [[_COMMUNITY_Kalshi Trading Alerts|Kalshi Trading Alerts]]
- [[_COMMUNITY_Quantile ML Models|Quantile ML Models]]
- [[_COMMUNITY_Calibration Drift Analysis|Calibration Drift Analysis]]
- [[_COMMUNITY_Job Scripts & Utilities|Job Scripts & Utilities]]
- [[_COMMUNITY_Paper Trading & Bet Audit|Paper Trading & Bet Audit]]
- [[_COMMUNITY_Discord Alert Embeds|Discord Alert Embeds]]
- [[_COMMUNITY_Arbitrage Paper Trader|Arbitrage Paper Trader]]
- [[_COMMUNITY_Opponent Stats Backfill|Opponent Stats Backfill]]
- [[_COMMUNITY_MLB Player Averages|MLB Player Averages]]
- [[_COMMUNITY_NBA CDN Scraper|NBA CDN Scraper]]
- [[_COMMUNITY_MLB Stats Scraper|MLB Stats Scraper]]
- [[_COMMUNITY_Job Scheduler|Job Scheduler]]
- [[_COMMUNITY_Discord Card Renderer|Discord Card Renderer]]
- [[_COMMUNITY_Discord Bot|Discord Bot]]
- [[_COMMUNITY_Bet Simulator & ESPN Injuries|Bet Simulator & ESPN Injuries]]
- [[_COMMUNITY_Injury Data Linker|Injury Data Linker]]
- [[_COMMUNITY_Backtesting Visualizer|Backtesting Visualizer]]
- [[_COMMUNITY_MLB Statcast Backfill|MLB Statcast Backfill]]
- [[_COMMUNITY_Database Health Checker|Database Health Checker]]
- [[_COMMUNITY_Per-Stat Calibration Metrics|Per-Stat Calibration Metrics]]
- [[_COMMUNITY_Game Lines Scraper|Game Lines Scraper]]
- [[_COMMUNITY_MLB Daily Stats Job|MLB Daily Stats Job]]
- [[_COMMUNITY_MLB Game Linker|MLB Game Linker]]
- [[_COMMUNITY_Calibration Run Artifacts|Calibration Run Artifacts]]
- [[_COMMUNITY_MLB FanGraphs Scraper|MLB FanGraphs Scraper]]
- [[_COMMUNITY_Calibration Concepts|Calibration Concepts]]
- [[_COMMUNITY_Calibration Evaluator|Calibration Evaluator]]
- [[_COMMUNITY_Calibration Monitor|Calibration Monitor]]
- [[_COMMUNITY_Player Prop Scraper|Player Prop Scraper]]
- [[_COMMUNITY_Truncated NegBin Model|Truncated NegBin Model]]
- [[_COMMUNITY_Player Name Mapper|Player Name Mapper]]
- [[_COMMUNITY_MLB Game Lines Scraper|MLB Game Lines Scraper]]
- [[_COMMUNITY_MLB Player Props Scraper|MLB Player Props Scraper]]
- [[_COMMUNITY_NBA Player Props Scraper|NBA Player Props Scraper]]
- [[_COMMUNITY_Kalshi Analysis Metrics|Kalshi Analysis Metrics]]
- [[_COMMUNITY_NCAAB BartTorvik Scraper|NCAAB BartTorvik Scraper]]
- [[_COMMUNITY_Starting Lineup Backfill|Starting Lineup Backfill]]
- [[_COMMUNITY_NCAAB Team Averages|NCAAB Team Averages]]
- [[_COMMUNITY_Live Odds Scraper|Live Odds Scraper]]
- [[_COMMUNITY_MLB Matchup Features|MLB Matchup Features]]
- [[_COMMUNITY_Play Type Scraper|Play Type Scraper]]
- [[_COMMUNITY_League Priors Backfill|League Priors Backfill]]
- [[_COMMUNITY_NCAAB Team Linker|NCAAB Team Linker]]
- [[_COMMUNITY_Combo Config|Combo Config]]
- [[_COMMUNITY_Debug IDs Tool|Debug IDs Tool]]
- [[_COMMUNITY_MLB Config|MLB Config]]
- [[_COMMUNITY_NCAAB Config|NCAAB Config]]
- [[_COMMUNITY_src __init__|src __init__]]
- [[_COMMUNITY_arbitrage __init__|arbitrage __init__]]
- [[_COMMUNITY_backtesting MLB __init__|backtesting MLB __init__]]
- [[_COMMUNITY_stat_config rationale|stat_config rationale]]
- [[_COMMUNITY_db __init__|db __init__]]
- [[_COMMUNITY_diagnostics __init__|diagnostics __init__]]
- [[_COMMUNITY_binomial model rationale A|binomial model rationale A]]
- [[_COMMUNITY_binomial model rationale B|binomial model rationale B]]
- [[_COMMUNITY_black litterman rationale|black litterman rationale]]
- [[_COMMUNITY_negbin model rationale A|negbin model rationale A]]
- [[_COMMUNITY_negbin model rationale B|negbin model rationale B]]
- [[_COMMUNITY_negbin model rationale C|negbin model rationale C]]
- [[_COMMUNITY_quantile trainer rationale A|quantile trainer rationale A]]
- [[_COMMUNITY_quantile trainer rationale B|quantile trainer rationale B]]
- [[_COMMUNITY_quantile trainer rationale C|quantile trainer rationale C]]
- [[_COMMUNITY_truncated negbin rationale A|truncated negbin rationale A]]
- [[_COMMUNITY_truncated negbin rationale B|truncated negbin rationale B]]
- [[_COMMUNITY_models __init__|models __init__]]
- [[_COMMUNITY_mlb binary model rationale|mlb binary model rationale]]
- [[_COMMUNITY_models MLB __init__|models MLB __init__]]
- [[_COMMUNITY_orchestration __init__|orchestration __init__]]
- [[_COMMUNITY_calibration monitor rationale A|calibration monitor rationale A]]
- [[_COMMUNITY_calibration monitor rationale B|calibration monitor rationale B]]
- [[_COMMUNITY_kalshi analysis rationale A|kalshi analysis rationale A]]
- [[_COMMUNITY_kalshi analysis rationale B|kalshi analysis rationale B]]
- [[_COMMUNITY_processing __init__|processing __init__]]
- [[_COMMUNITY_processing MLB __init__|processing MLB __init__]]
- [[_COMMUNITY_processing NCAAB __init__|processing NCAAB __init__]]
- [[_COMMUNITY_scrapers __init__|scrapers __init__]]
- [[_COMMUNITY_scrapers kalshi __init__|scrapers kalshi __init__]]
- [[_COMMUNITY_scrapers MLB __init__|scrapers MLB __init__]]
- [[_COMMUNITY_scrapers NCAAB __init__|scrapers NCAAB __init__]]
- [[_COMMUNITY_scrapers polymarket __init__|scrapers polymarket __init__]]
- [[_COMMUNITY_tools __init__|tools __init__]]

## God Nodes (most connected - your core abstractions)
1. `BlackLittermanBlender` - 136 edges
2. `FeatureStore` - 118 edges
3. `StatConfigSet` - 108 edges
4. `BLConfig` - 102 edges
5. `BetSimulator` - 89 edges
6. `get_engine()` - 89 edges
7. `MetricsCalculator` - 81 edges
8. `MonteCarloPredictor` - 81 edges
9. `PerformanceMetrics` - 72 edges
10. `PlayerPropsModelPipeline` - 70 edges

## Surprising Connections (you probably didn't know these)
- `Bet simulation and P&L tracking for backtesting.` --uses--> `StatConfigSet`  [INFERRED]
  src\backtesting\bet_simulator.py → src\config\stat_config.py
- `Paper Trading Module for NBA Player Props.  Converts daily predictions into pa` --uses--> `StatConfigSet`  [INFERRED]
  src\paper_trading\paper_trader.py → src\config\stat_config.py
- `Get float from environment variable.` --uses--> `StatConfigSet`  [INFERRED]
  src\paper_trading\paper_trader.py → src\config\stat_config.py
- `Get float or None from environment variable.` --uses--> `StatConfigSet`  [INFERRED]
  src\paper_trading\paper_trader.py → src\config\stat_config.py
- `Converts daily predictions into paper bets and tracks P&L.      Supports stand` --uses--> `StatConfigSet`  [INFERRED]
  src\paper_trading\paper_trader.py → src\config\stat_config.py

## Hyperedges (group relationships)
- **January 23 2026 Calibration Failed Run Cluster** — run_20260123_140949_calfailed, run_20260123_141529_calfailed, run_20260123_142234_calfailed, run_20260123_154145_calfailed [INFERRED 0.90]
- **January 23 2026 Recent Production Calibration Failed Cluster** — run_20260123_191910_calfailed_recprod, run_20260123_192705_calfailed_recprod [INFERRED 0.90]
- **January 25-26 2026 Recent Production Calibration Cluster** — run_20260125_173042_calfailed_recprod, run_20260125_184844_calwarn_recprod, run_20260126_143317_calwarn_recprod [INFERRED 0.85]
- **NBA April 2026 Calibration Warning Cluster** — nba_run_20260415_152254_calwarn, nba_run_20260415_152608_calwarn [INFERRED 0.90]
- **Calibration Gate System (Warning + Failed + Threshold)** — calibration_warning_status, calibration_failed_status, hard_fail_threshold_10pct, deployment_gate_concept, calibration_gap_concept [INFERRED 0.85]
- **Jan 26 Calibration Warning Runs (5.2-5.9% gap range)** — run_20260126_144530_calib_warn, run_20260126_145847_calib_warn, run_20260126_150518_calib_warn, run_20260126_151958_calib_warn [INFERRED 0.90]
- **Feb 5 Calibration Failed Runs (25.4-25.6% gap range)** — run_20260205_124823_calib_fail, run_20260205_131610_calib_fail, run_20260205_165808_calib_fail [INFERRED 0.90]
- **Feb 9-10 Calibration Failed Runs (25.8-27.4% gap range)** — run_20260209_084752_calib_fail, run_20260209_175106_calib_fail, run_20260210_052402_calib_fail, run_20260210_095220_calib_fail [INFERRED 0.90]
- **All Calibration Check Artifacts (Warnings and Failures)** — run_20260126_144530_calib_warn, run_20260126_145847_calib_warn, run_20260126_150518_calib_warn, run_20260126_151958_calib_warn, run_20260128_181252_calib_warn, run_20260131_112534_calib_fail, run_20260205_124823_calib_fail, run_20260205_131610_calib_fail, run_20260205_165808_calib_fail, run_20260209_084752_calib_fail, run_20260209_175106_calib_fail, run_20260210_052402_calib_fail, run_20260210_095220_calib_fail [INFERRED 0.85]

## Communities

### Community 0 - "Backtesting Engine"
Cohesion: 0.02
Nodes (229): BacktestHarness, BacktestResult, Backtesting harness for evaluating prediction models on historical data., Get BL blender for a specific stat, or None if BL is disabled for that stat., Run backtest over date range.          Args:             start_date: Start da, Get all dates with games in the range., Run predictions for a single date using efficient batch processing., Container for backtest results. (+221 more)

### Community 1 - "Kalshi Trading Alerts"
Cohesion: 0.01
Nodes (195): _build_kalshi_circuit_breaker_embed(), _build_kalshi_trade_placed_embed(), _build_kalshi_trade_resolved_embed(), Build Discord embed for a resolved Kalshi trade., Build Discord embed for a circuit breaker trigger., Send a Kalshi trading alert synchronously.      Args:         alert_type: "pl, Build Discord embed for a Kalshi trade placement., send_kalshi_trade_alert_sync() (+187 more)

### Community 2 - "Quantile ML Models"
Cohesion: 0.02
Nodes (158): _binomial_nll_eval(), _binomial_obj(), BinomialConfig, BinomialModel, exists(), load(), Binomial Model for integer count prediction (hits in at-bats).  Predicts the h, Predicts hit probability p of a Binomial(n, p) distribution.      At-bats n is (+150 more)

### Community 3 - "Calibration Drift Analysis"
Cohesion: 0.02
Nodes (153): analyze_combined_calibration(), analyze_minutes_rate_correlation(), main(), Analyze calibration drift between individual models and combined predictions., Evaluate Monte Carlo (minutes × rate) calibration against actual totals., Analyze correlation between actual minutes and per-minute rates., analyze_bimodality(), analyze_segment() (+145 more)

### Community 4 - "Job Scripts & Utilities"
Cohesion: 0.01
Nodes (182): Synchronous wrapper for send_predictions_alert.      Use this in synchronous c, send_predictions_alert_sync(), archive_batch(), main(), Move one batch of old rows to archive. Returns number of rows moved., main(), Incremental Backfill team_id in raw_player_props_combined.  Only processes rec, main() (+174 more)

### Community 5 - "Paper Trading & Bet Audit"
Cohesion: 0.02
Nodes (128): audit_bets(), find_missed_bet_dates(), find_unresolved_with_stats(), main(), Show bet counts by date and status., Find dates that have pending bets AND game stats available., Find dates where predictions exist but fewer bets were placed than recommended., backfill_date() (+120 more)

### Community 6 - "Discord Alert Embeds"
Cohesion: 0.02
Nodes (129): _build_alert_embed(), _build_arb_alert_embed(), _build_arb_paper_placement_embed(), _build_arb_paper_summary_embed(), _build_calibration_embed(), _build_job_alert_embed(), _build_kalshi_alert_embed(), _build_kalshi_analysis_embed() (+121 more)

### Community 7 - "Arbitrage Paper Trader"
Cohesion: 0.03
Nodes (101): ArbPaperTrader, Arb Paper Trader ================ Paper trades arbitrage opportunities detecte, Insert arb paper bets into arb_paper_bets.          Uses ON CONFLICT DO NOTHIN, Resolve placed arbs for a specific game date.          For pure arbs (moneylin, Paper trader for Kalshi↔Polymarket arbitrage opportunities.      Attributes:, Resolve all placed arbs where games may now be Final.          Finds all uniqu, Load Final games from the schedule for a given date., Check if a game between team1 and team2 is in the Final games list. (+93 more)

### Community 8 - "Opponent Stats Backfill"
Cohesion: 0.04
Nodes (73): backfill_team_allowed_by_position(), batch_insert_to_db(), compute_rolling_metrics(), fetch_raw_allowed_stats(), backfill_opponent_allowed_incremental(), batch_insert_to_db(), compute_rolling_metrics(), fetch_raw_allowed_stats_incremental() (+65 more)

### Community 9 - "MLB Player Averages"
Cohesion: 0.04
Nodes (72): calculate_batting_averages(), _calculate_batting_rate_stats(), calculate_pitching_averages(), _calculate_pitching_context(), _calculate_pitching_rate_stats(), _count_games_in_window(), fetch_batting_stats(), fetch_pitching_stats() (+64 more)

### Community 10 - "NBA CDN Scraper"
Cohesion: 0.04
Nodes (69): build_matchup(), determine_wl(), ensure_players_exist(), fetch_boxscore(), fetch_schedule(), get_existing_game_ids(), get_games_by_date(), insert_player_stats() (+61 more)

### Community 11 - "MLB Stats Scraper"
Cohesion: 0.05
Nodes (37): main(), MLB Historical Backfill ======================= Orchestrates full season backf, Run full backfill for specified seasons., run_backfill(), _int(), main(), MLBStatsScraper, _parse_ip() (+29 more)

### Community 12 - "Job Scheduler"
Cohesion: 0.05
Nodes (60): check_dependency(), main(), _parse_metrics_from_output(), Record a job execution to the job_executions table in Supabase.      Non-fatal, Return True if upstream job succeeded within max_age_hours.      Checks in-mem, Extract metrics from job output for display in alerts.      Args:         scr, Send Discord alert for job completion.      Non-fatal: failures are logged but, Run a job script as a subprocess and send alert on completion.      Args: (+52 more)

### Community 13 - "Discord Card Renderer"
Cohesion: 0.06
Nodes (47): HeadshotCache, PickCardRenderer, Image renderers for social media pick cards.  Three renderer classes:   - Pic, Render an individual pick card.          Args:             pick: dict from da, Renders a multi-pick daily slate card., Render a slate card with multiple picks., Renders a results recap card for the previous day's bets., Render a results card.          Args:             bets: list from data_provid (+39 more)

### Community 14 - "Discord Bot"
Cohesion: 0.05
Nodes (43): create_bot(), GameFlowBot, GameFlowData Discord Bot - Main bot class with slash commands., Send daily prediction alert to the alerts channel., Factory function to create bot instance., Discord bot for GameFlowData predictions and paper trading., Called when bot is ready to set up slash commands., Called when bot is fully connected. (+35 more)

### Community 15 - "Bet Simulator & ESPN Injuries"
Cohesion: 0.06
Nodes (32): BetOutcome, BetSide, Bet simulation and P&L tracking for backtesting., Enum, ESPNInjuryScraper, InjuryChangeDetector, InjuryRecord, InjuryStatus (+24 more)

### Community 16 - "Injury Data Linker"
Cohesion: 0.07
Nodes (42): backfill_team_from_games(), build_injury_mappings(), bulk_update_injuries(), ensure_columns(), fuzzy_match_player(), load_manual_mappings(), load_reference_data(), main() (+34 more)

### Community 17 - "Backtesting Visualizer"
Cohesion: 0.08
Nodes (35): enrich_bets(), _enrich_from_csv(), _enrich_from_db(), _fmt_val(), generate_chart(), generate_dashboard(), _get_db_engine(), get_other_bookmaker_lines() (+27 more)

### Community 18 - "MLB Statcast Backfill"
Cohesion: 0.1
Nodes (25): get_date_range(), load_progress(), main(), MLB Statcast Backfill ===================== Orchestrates bulk Statcast data ba, Run Statcast backfill for the specified date range., Load set of already-processed dates from progress file., Save processed dates to progress file., Build list of dates to process. (+17 more)

### Community 19 - "Database Health Checker"
Cohesion: 0.1
Nodes (18): CheckResult, DatabaseHealthChecker, main(), Check 2: Game data completeness in recent days., Check 3: Prop linking health (using staging_id ranges to avoid timeouts)., Check 4: Aggregation tables are in sync with source data., Check 5: Injury data linking health., Result of a single health check. (+10 more)

### Community 20 - "Per-Stat Calibration Metrics"
Cohesion: 0.11
Nodes (28): build_json_report(), compute_bias(), compute_brier_score(), compute_ece(), compute_quantile_coverage(), compute_sharpness(), _detect_date_col(), _ece_and_curve() (+20 more)

### Community 21 - "Game Lines Scraper"
Cohesion: 0.09
Nodes (16): Scrape odds for a specific date using the historical endpoint.     Even for 'to, scrape_odds_for_date(), GameLineScraper, Fetches ALL game lines (Moneyline, Spread, Total) for a specific timestamp., Parses the bulk response and inserts into raw_game_lines_staging., NCAABGameLineScraper, NCAAB Daily Game Lines Scraper ================================ Scrapes NCAAB, Parse game lines response and insert into ncaab_raw_game_lines. (+8 more)

### Community 22 - "MLB Daily Stats Job"
Cohesion: 0.11
Nodes (16): main(), Run a shell command and return success status., Send MLB daily P&L summary to Discord performance channel.      Non-fatal: fai, Resolve all pending MLB paper bets using newly available game stats., resolve_pending_mlb_bets(), run_command(), _send_mlb_pnl_summary(), _get_env_float() (+8 more)

### Community 23 - "MLB Game Linker"
Cohesion: 0.12
Nodes (26): apply_updates(), build_game_lookup(), build_player_lookup(), build_team_id_lookup(), find_closest_game_date(), link_backfill(), link_incremental(), main() (+18 more)

### Community 24 - "Calibration Run Artifacts"
Cohesion: 0.18
Nodes (27): Calibration Failed Status, Calibration Gap 11.5%, Calibration Gap 12.5%, Calibration Gap 27.4%, Calibration Gap 5.3%, Calibration Gap 6.0%, Calibration Gap 6.5%, Calibration Gap 9.2% (+19 more)

### Community 25 - "MLB FanGraphs Scraper"
Cohesion: 0.16
Nodes (13): MLBFanGraphsScraper, _pct_to_float(), MLB FanGraphs Advanced Stats Scraper ===================================== Scr, Fetch and upsert FanGraphs pitching stats for a season.          Returns:, Map a FanGraphs player to our mlb_players.player_id.          Strategy:, Insert player stub if not in mlb_players., Convert to float, returning None for NaN/None., Convert to int, returning None for NaN/None. (+5 more)

### Community 26 - "Calibration Concepts"
Cohesion: 0.27
Nodes (20): Calibration Failed, Calibration Gap, Calibration Warning, Deployment Gate on Calibration, Hard Fail Threshold (10%), Recent Production Calibration Check, Rationale: Calibration Gate Prevents Degraded Model Deployment, Run 20260126_144530 Calibration Warning (5.9% gap) (+12 more)

### Community 27 - "Calibration Evaluator"
Cohesion: 0.13
Nodes (10): CalibrationEvaluator, CalibrationReport, Calibration evaluation results., Estimate P(over) from quantiles using linear interpolation., Convert American odds to implied probability., Calculate ROI for a set of bets., Calculate combined ROI across all bets., Evaluate quantile calibration and betting performance. (+2 more)

### Community 28 - "Calibration Monitor"
Cohesion: 0.14
Nodes (15): CalibrationMetrics, _compute_bias_by_stat(), compute_calibration_drift(), _compute_edge_accuracy(), _compute_prob_calibration(), _compute_quantile_coverage(), _load_resolved_bets(), Calibration drift monitor for paper trading.  Computes calibration metrics fro (+7 more)

### Community 29 - "Player Prop Scraper"
Cohesion: 0.12
Nodes (11): generate_snapshot_timestamps(), Step 2: Get Player Props for a single game.         Endpoint: /v4/historical/sp, Parses the API response and inserts it into the staging table.         Captures, High-speed bulk insert using psycopg2.extras.execute_values, Generates the list of timestamps to scrape based on NBA seasons.      Args:, Resolve the final market list from CLI args., Load progress from JSON file. Only resumes if markets match., Save processed IDs to local JSON file for resume capability. (+3 more)

### Community 30 - "Truncated NegBin Model"
Cohesion: 0.13
Nodes (11): load(), Truncated Negative Binomial Model for THREES Count Prediction.  This module im, Fit the truncated NegBin model.          Args:             X: Features (posit, # IMPORTANT: Apply truncation factor FIRST, then add small epsilon for log stabi, Configuration for the Truncated Negative Binomial model., Inverse CDF for zero-truncated negative binomial.          Maps uniform sample, Sample for a single player given feature dict.          Args:             fea, Predicts parameters of a zero-truncated negative binomial distribution.      T (+3 more)

### Community 31 - "Player Name Mapper"
Cohesion: 0.17
Nodes (16): cmd_add(), cmd_analyze(), cmd_apply(), cmd_export(), cmd_import(), find_best_matches(), main(), Player Name Mapping Utility =========================== Helps identify and man (+8 more)

### Community 32 - "MLB Game Lines Scraper"
Cohesion: 0.17
Nodes (11): MLBGameLineScraper, MLB Daily Game Lines Scraper ============================= Scrapes MLB game li, Parse game lines response and insert into mlb_raw_game_lines., Scrape live MLB game lines., Scrape historical MLB game lines for a specific date., Backfill MLB game lines over a date range., Fetch live game lines for all upcoming MLB games.          Returns:, Fetch historical game lines at a specific timestamp.          Returns: (+3 more)

### Community 33 - "MLB Player Props Scraper"
Cohesion: 0.17
Nodes (10): MLBDailyPropsScraper, MLB Daily Player Props Scraper =============================== Scrapes live ML, Fetch props for a single MLB game.          If is_live=True, uses live endpoin, Parse props and insert into specified table., Scrape live MLB props., Scrape historical MLB props for a specific date., Get list of current/upcoming MLB games., Get events for a specific past date. (+2 more)

### Community 34 - "NBA Player Props Scraper"
Cohesion: 0.18
Nodes (9): DailyPlayerPropsScraper, Parse props and insert into specified table., Run scrape for current time -> target_table., Run scrape for 12pm/6pm snapshots on date -> raw_player_props_combined, Get list of current/upcoming NBA games., Get events for a specific past date., Fetch props for a game.         If is_live=True, use live endpoint (date_str ig, run_historical_scrape() (+1 more)

### Community 35 - "Kalshi Analysis Metrics"
Cohesion: 0.18
Nodes (9): _break_even_win_rate(), compute_kalshi_analysis(), KalshiAnalysisMetrics, Kalshi Paper Bet Analysis Module ================================= Importable, Compute Kalshi paper trading analysis over the last N days.      Queries resol, Taker fee in dollars for one contract at given price (cents)., Minimum win rate to be profitable at taker pricing., _taker_fee_per_contract() (+1 more)

### Community 36 - "NCAAB BartTorvik Scraper"
Cohesion: 0.19
Nodes (7): BarttorviKScraper, NCAAB Barttorvik Ratings Scraper =================================== Downloads, Map Barttorvik CSV columns to our schema names.          Handles year-to-year, Batch-insert into ncaab_barttorvik_ratings.          Uses ON CONFLICT (team_na, Full pipeline: fetch CSV -> normalize -> store.          Returns:, Download and store one snapshot per historical season., Download bulk CSV from barttorvik.com.          Args:             season: Sea

### Community 37 - "Starting Lineup Backfill"
Cohesion: 0.22
Nodes (12): backfill_game(), extract_starter_data(), fallback_backfill(), fetch_boxscore(), get_games_needing_backfill(), main(), Backfill starter data from CDN boxscores.  Fetches the CDN boxscore JSON for h, For games still missing starter data, use minutes proxy. (+4 more)

### Community 38 - "NCAAB Team Averages"
Cohesion: 0.27
Nodes (9): compute_rolling_averages(), fetch_team_box_scores(), NCAAB Team Rolling Averages ============================= Computes shift(1) ro, Store rolling averages into ncaab_team_rolling_averages., Recompute rolling averages only for teams with new box scores., Fetch team box scores for rolling average computation., Compute shift(1) rolling averages for each team within each season.      All s, run_incremental() (+1 more)

### Community 39 - "Live Odds Scraper"
Cohesion: 0.24
Nodes (4): LiveOddsScraper, Ensure the staging table exists., Fetch LIVE odds from the API.         Endpoint: /v4/sports/basketball_nba/odds, Parse response and insert into raw_game_lines_live.

### Community 40 - "MLB Matchup Features"
Cohesion: 0.25
Nodes (7): compute_matchup_features_bulk(), get_opposing_team_batting_stats(), get_pitcher_handedness(), MLB matchup features: opposing team batting tendencies and pitcher context.  C, Get pitcher's throwing hand from mlb_players.throws., Bulk computation of opposing team batting stats for training.      For each (g, Get opposing team's batting tendencies from their last 10 games.      Aggregat

### Community 41 - "Play Type Scraper"
Cohesion: 0.36
Nodes (7): fetch_play_type(), main(), Fetch all 22 combinations (11 play types x 2 groupings)., Full-refresh save: delete existing season rows, insert fresh data., Fetch a single play type + grouping combo with retry logic., save_to_db(), scrape_all_play_types()

### Community 42 - "League Priors Backfill"
Cohesion: 0.38
Nodes (6): backfill_league_priors(), calculate_and_insert_snapshot(), get_season_start_date(), Returns the start date (Oct 1st) of the season for a given snapshot., Generates monthly snapshots.     Auto-detects season_id from the database., Calculates stats from season_start up to snapshot_date.     Selects season_id d

### Community 43 - "NCAAB Team Linker"
Cohesion: 0.5
Nodes (3): link_barttorvik_teams(), NCAAB Barttorvik Team Linker ================================ Populates team_i, Link Barttorvik team names to ncaab_teams.team_id.      Strategy:     1. Manu

### Community 44 - "Combo Config"
Cohesion: 1.0
Nodes (1): Centralized combo market definitions and stat-to-market mappings.  Combo marke

### Community 45 - "Debug IDs Tool"
Cohesion: 1.0
Nodes (0): 

### Community 46 - "MLB Config"
Cohesion: 1.0
Nodes (1): MLB Processing Configuration ============================= Shared constants fo

### Community 47 - "NCAAB Config"
Cohesion: 1.0
Nodes (1): NCAAB Processing Configuration ================================ Shared constan

### Community 48 - "src __init__"
Cohesion: 1.0
Nodes (0): 

### Community 49 - "arbitrage __init__"
Cohesion: 1.0
Nodes (0): 

### Community 50 - "backtesting MLB __init__"
Cohesion: 1.0
Nodes (0): 

### Community 51 - "stat_config rationale"
Cohesion: 1.0
Nodes (1): Parse CLI arguments into StatConfigSet.          Args:             edge_value

### Community 52 - "db __init__"
Cohesion: 1.0
Nodes (0): 

### Community 53 - "diagnostics __init__"
Cohesion: 1.0
Nodes (0): 

### Community 54 - "binomial model rationale A"
Cohesion: 1.0
Nodes (1): Load model from *directory*.

### Community 55 - "binomial model rationale B"
Cohesion: 1.0
Nodes (1): Check whether a saved BinomialModel exists in *directory*.

### Community 56 - "black litterman rationale"
Cohesion: 1.0
Nodes (1): Convert American odds to decimal odds.          Args:             american_od

### Community 57 - "negbin model rationale A"
Cohesion: 1.0
Nodes (1): Inverse CDF for standard NegBin.          Maps uniform samples u ∈ (0,1) to in

### Community 58 - "negbin model rationale B"
Cohesion: 1.0
Nodes (1): Load model from *directory*.          Supports both v2 (native booster) and v1

### Community 59 - "negbin model rationale C"
Cohesion: 1.0
Nodes (1): Check whether a saved NegBinModel exists in *directory*.

### Community 60 - "quantile trainer rationale A"
Cohesion: 1.0
Nodes (1): Create config from dictionary.

### Community 61 - "quantile trainer rationale B"
Cohesion: 1.0
Nodes (1): Union of all per-quantile feature names (sorted for determinism).

### Community 62 - "quantile trainer rationale C"
Cohesion: 1.0
Nodes (1): Load models from disk.

### Community 63 - "truncated negbin rationale A"
Cohesion: 1.0
Nodes (1): Load model from disk.

### Community 64 - "truncated negbin rationale B"
Cohesion: 1.0
Nodes (1): Check if a count model exists in the directory.

### Community 65 - "models __init__"
Cohesion: 1.0
Nodes (0): 

### Community 66 - "mlb binary model rationale"
Cohesion: 1.0
Nodes (1): Load a saved binary model.

### Community 67 - "models MLB __init__"
Cohesion: 1.0
Nodes (0): 

### Community 68 - "orchestration __init__"
Cohesion: 1.0
Nodes (0): 

### Community 69 - "calibration monitor rationale A"
Cohesion: 1.0
Nodes (1): Alerts that drive severity. Bias excluded (systematic under-prediction is expect

### Community 70 - "calibration monitor rationale B"
Cohesion: 1.0
Nodes (1): healthy', 'warning', or 'critical' based on alert count.

### Community 71 - "kalshi analysis rationale A"
Cohesion: 1.0
Nodes (1): Returns embed color hint: healthy / warning / critical.

### Community 72 - "kalshi analysis rationale B"
Cohesion: 1.0
Nodes (1): 95% Wilson-ish CI (normal approximation).

### Community 73 - "processing __init__"
Cohesion: 1.0
Nodes (0): 

### Community 74 - "processing MLB __init__"
Cohesion: 1.0
Nodes (0): 

### Community 75 - "processing NCAAB __init__"
Cohesion: 1.0
Nodes (0): 

### Community 76 - "scrapers __init__"
Cohesion: 1.0
Nodes (0): 

### Community 77 - "scrapers kalshi __init__"
Cohesion: 1.0
Nodes (0): 

### Community 78 - "scrapers MLB __init__"
Cohesion: 1.0
Nodes (0): 

### Community 79 - "scrapers NCAAB __init__"
Cohesion: 1.0
Nodes (0): 

### Community 80 - "scrapers polymarket __init__"
Cohesion: 1.0
Nodes (0): 

### Community 81 - "tools __init__"
Cohesion: 1.0
Nodes (0): 

## Knowledge Gaps
- **917 isolated node(s):** `Market Matcher ============== Cross-platform market matching logic for Kalshi`, `Extract game date from a Kalshi game ticker.      Example: KXMLBGAME-26APR1913`, `Extract game start time (hour, minute) ET from a Kalshi game ticker.      Exam`, `Extract game date from a Polymarket event slug.      Example: mlb-bal-cle-2026`, `A matched Kalshi + Polymarket pair for any market type.` (+912 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **Thin community `Combo Config`** (2 nodes): `Centralized combo market definitions and stat-to-market mappings.  Combo marke`, `combo_config.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Debug IDs Tool`** (2 nodes): `check_ids()`, `debug_ids.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `MLB Config`** (2 nodes): `MLB Processing Configuration ============================= Shared constants fo`, `mlb_config.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `NCAAB Config`** (2 nodes): `NCAAB Processing Configuration ================================ Shared constan`, `ncaab_config.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `src __init__`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `arbitrage __init__`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `backtesting MLB __init__`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `stat_config rationale`** (1 nodes): `Parse CLI arguments into StatConfigSet.          Args:             edge_value`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `db __init__`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `diagnostics __init__`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `binomial model rationale A`** (1 nodes): `Load model from *directory*.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `binomial model rationale B`** (1 nodes): `Check whether a saved BinomialModel exists in *directory*.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `black litterman rationale`** (1 nodes): `Convert American odds to decimal odds.          Args:             american_od`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `negbin model rationale A`** (1 nodes): `Inverse CDF for standard NegBin.          Maps uniform samples u ∈ (0,1) to in`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `negbin model rationale B`** (1 nodes): `Load model from *directory*.          Supports both v2 (native booster) and v1`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `negbin model rationale C`** (1 nodes): `Check whether a saved NegBinModel exists in *directory*.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `quantile trainer rationale A`** (1 nodes): `Create config from dictionary.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `quantile trainer rationale B`** (1 nodes): `Union of all per-quantile feature names (sorted for determinism).`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `quantile trainer rationale C`** (1 nodes): `Load models from disk.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `truncated negbin rationale A`** (1 nodes): `Load model from disk.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `truncated negbin rationale B`** (1 nodes): `Check if a count model exists in the directory.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `models __init__`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `mlb binary model rationale`** (1 nodes): `Load a saved binary model.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `models MLB __init__`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `orchestration __init__`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `calibration monitor rationale A`** (1 nodes): `Alerts that drive severity. Bias excluded (systematic under-prediction is expect`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `calibration monitor rationale B`** (1 nodes): `healthy', 'warning', or 'critical' based on alert count.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `kalshi analysis rationale A`** (1 nodes): `Returns embed color hint: healthy / warning / critical.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `kalshi analysis rationale B`** (1 nodes): `95% Wilson-ish CI (normal approximation).`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `processing __init__`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `processing MLB __init__`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `processing NCAAB __init__`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `scrapers __init__`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `scrapers kalshi __init__`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `scrapers MLB __init__`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `scrapers NCAAB __init__`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `scrapers polymarket __init__`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `tools __init__`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `get_engine()` connect `Job Scripts & Utilities` to `Backtesting Engine`, `Kalshi Trading Alerts`, `Quantile ML Models`, `Calibration Drift Analysis`, `Paper Trading & Bet Audit`, `Arbitrage Paper Trader`, `Opponent Stats Backfill`, `MLB Player Averages`, `NBA CDN Scraper`, `MLB Stats Scraper`, `Job Scheduler`, `Discord Card Renderer`, `Bet Simulator & ESPN Injuries`, `Injury Data Linker`, `Backtesting Visualizer`, `MLB Statcast Backfill`, `Database Health Checker`, `Per-Stat Calibration Metrics`, `MLB Daily Stats Job`, `MLB Game Linker`, `Calibration Monitor`, `Player Name Mapper`, `Kalshi Analysis Metrics`, `Starting Lineup Backfill`?**
  _High betweenness centrality (0.179) - this node is a cross-community bridge._
- **Why does `parse_args()` connect `Job Scripts & Utilities` to `Backtesting Engine`, `Kalshi Trading Alerts`, `Calibration Drift Analysis`, `Paper Trading & Bet Audit`, `Discord Alert Embeds`, `Opponent Stats Backfill`, `MLB Player Averages`, `NBA CDN Scraper`, `MLB Stats Scraper`, `Job Scheduler`, `Discord Card Renderer`, `Backtesting Visualizer`, `MLB Statcast Backfill`, `Database Health Checker`, `Per-Stat Calibration Metrics`, `MLB Daily Stats Job`, `MLB Game Linker`, `Player Name Mapper`, `Starting Lineup Backfill`, `Play Type Scraper`?**
  _High betweenness centrality (0.091) - this node is a cross-community bridge._
- **Why does `StatConfigSet` connect `Backtesting Engine` to `Kalshi Trading Alerts`, `Paper Trading & Bet Audit`, `Bet Simulator & ESPN Injuries`?**
  _High betweenness centrality (0.046) - this node is a cross-community bridge._
- **Are the 129 inferred relationships involving `BlackLittermanBlender` (e.g. with `BacktestResult` and `BacktestHarness`) actually correct?**
  _`BlackLittermanBlender` has 129 INFERRED edges - model-reasoned connections that need verification._
- **Are the 99 inferred relationships involving `FeatureStore` (e.g. with `Run a backtest over a date range using trained model artifacts.  Usage:     p` and `Parse date string in YYYY-MM-DD format for argparse.`) actually correct?**
  _`FeatureStore` has 99 INFERRED edges - model-reasoned connections that need verification._
- **Are the 100 inferred relationships involving `StatConfigSet` (e.g. with `BacktestResult` and `BacktestHarness`) actually correct?**
  _`StatConfigSet` has 100 INFERRED edges - model-reasoned connections that need verification._
- **Are the 99 inferred relationships involving `BLConfig` (e.g. with `BacktestResult` and `BacktestHarness`) actually correct?**
  _`BLConfig` has 99 INFERRED edges - model-reasoned connections that need verification._