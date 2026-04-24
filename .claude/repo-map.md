# Repo Map (auto-generated — regenerate when structure changes significantly)

src/
  models/                 # NBA models (feature_store, daily_runner, train_pipeline, etc.)
    mlb/                  # MLB models (mlb_daily_runner, mlb_batter_train_pipeline, etc.)
    artifacts/            # Timestamped model run directories
  scrapers/               # NBA scrapers (nba_unified_scraper, daily_player_props_scraper, etc.)
    mlb/                  # MLB scrapers (mlb_lineup_scraper, mlb_stats_scraper, mlb_weather_scraper, etc.)
    kalshi/               # Kalshi (kalshi_client, kalshi_market_scraper, kalshi_utils, etc.)
    polymarket/           # Polymarket (polymarket_client, polymarket_market_scraper, etc.)
    ncaab/                # NCAAB scrapers
  processing/             # Feature engineering (feature_selection, backfill_*, nba_linker_local, etc.)
  orchestration/          # Scheduler + job files (scheduler.py, inference_job, edge_refresh_job, etc.)
  backtesting/            # Backtest harness, sweep, bet_simulator, performance_metrics
  db/                     # Database client (client.py)
  diagnostics/            # Calibration checks (calibration_per_stat, db_health_check)
  tools/                  # Utilities (compare_models, query_player, backfill_prediction_features)
  arbitrage/              # Arb scanner (arb_scanner, market_matcher, team_normalizer)
scripts/                  # CLI scripts (sync_local_db, analyze_*_paper_bets, promote_model, etc.)
dashboard/
  src/app/
    (public)/             # Landing, picks, pricing, terms, privacy
    (auth)/               # Login, signup
    (protected)/          # Dashboard, stats, dfs, bot-tracker, arb-scanner, history, performance, account, subscribe
    api/                  # API routes: stripe/, kalshi/, arb/, games, scoreboard, ask, slate
  src/components/         # React components
  src/lib/                # Supabase client, utils
  src/types/              # TypeScript types
brain/                    # BrainTree knowledge base
  Models/, Pipeline/, Product/, Infrastructure/, Business/, Operations/, Decisions/, Templates/, Assets/
  Handoffs/               # Session handoffs (handoff-000 through handoff-047)
  Execution-Plan.md       # Phased roadmap (at brain root, NOT in a subdirectory)
  BRAIN-INDEX.md          # Brain index with folder links and session log
database/                 # schema.sql
supabase/migrations/      # SQL migration files
.claude/                  # Agent definitions, hooks, settings
