# Component Documentation

> Part of [[Pipeline]]

Reference documentation for individual pipeline components. These docs live in the project's `docs/` folder and provide detailed module-level documentation.

## Model & ML Docs
- [[feature_store_documentation]] - Training and inference feature assembly for NBA models
- [[black_litterman_documentation]] - Probability blending with market prior (Black-Litterman)
- [[truncated_negbin_documentation]] - Count model for THREES stat (C4 architecture)
- [[model_pipeline_runbook]] - Training, backtesting, and daily prediction guide
- [[monte_carlo_tuning]] - Monte Carlo simulation parameter tuning notes
- [[calibration_per_stat_documentation]] - Per-stat calibration diagnostic with quantile coverage, bias, ECE, Brier score
- [[nba_feature_catalog]] - Complete reference for all NBA prediction model features
- [[mlb_feature_catalog]] - Complete reference for MLB pitcher strikeout model features
- [[under_prediction_research]] - Analysis of systematic under-prediction as beneficial edge
- [[minutes_bimodality_analysis]] - Starter minutes bimodality investigation for blowout games
- [[stat_config_documentation]] - Per-stat configuration for edge thresholds and BL tau values

## Pipeline & Scraper Docs
- [[game_lines_scraper_documentation]] - Historical odds scraper for moneyline, spreads, totals
- [[player_prop_scraper_documentation]] - Historical player props snapshot scraper
- [[nba_player_position_documentation]] - Player position updates via NBA API
- [[update_league_position_averages_documentation]] - League averages by position group
- [[update_player_position_history_documentation]] - Snapshot-based player role history
- [[injury_database_documentation]] - Injury data Supabase storage and queries
- [[injury_scraper_job_documentation]] - Scheduled injury scraping orchestration
- [[play_type_scraper_documentation]] - Team-level offensive/defensive play type data from NBA API
- [[nba_linker_local_documentation]] - Local NBA ID matching and uploads
- [[populate_average_stats_documentation]] - Rolling averages for player/team stats
- [[mlb_statcast_scraper_documentation]] - Daily Statcast pitch-level aggregation (exit velo, barrel%, xBA, pitch mix)
- [[mlb_fangraphs_scraper_documentation]] - Season-level FanGraphs advanced stats (wRC+, FIP, WAR)
- [[mlb_processing_pipeline_documentation]] - MLB Phase 2 raw data to model-ready feature transformation
- [[ncaab_pipeline_documentation]] - NCAAB game-level prediction pipeline (spreads, ML, totals)
- [[daily_pipeline_automation]] - Frequency-separated job scripts for cron scheduling

## Infrastructure & Operations Docs
- [[railway_deployment]] - Cloud deployment guide for Railway platform
- [[scalability]] - Architecture capacity analysis and scaling path
- [[db_health_check_documentation]] - Comprehensive database health validation
- [[db_client_documentation]] - Database engine access and connectivity checks
- [[dashboard_documentation]] - Next.js web application for predictions, stats, and paper trading
- [[social_image_generator_documentation]] - CLI tool for branded pick images (Instagram/TikTok/Discord)
- [[query_player_documentation]] - CLI tool for querying stored predictions
- [[solokit_test_commands]] - Solokit test command configuration notes
- [[index]] - Master documentation index for the docs folder

## Planning & Research Docs
- [[mlb_expansion_plan]] - Full MLB data pipeline plan and architecture
- [[paid_subscription_plan]] - Stripe subscription system implementation plan ($19.99/mo)
- [[discord_bot_development]] - Discord bot development plan and implementation status

## Database & Tables Docs
- [[league_priors_history]] - League baseline snapshots by position
- [[player_position_history]] - Role snapshots for players
- [[team_allowed_by_position]] - Opponent defense by position group
