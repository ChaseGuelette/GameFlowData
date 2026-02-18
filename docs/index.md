# Documentation Index

## Configuration
- [Stat Config](stat_config_documentation.md) - Per-stat configuration for edge thresholds and BL tau values.

## Scrapers
- [Game Lines Scraper](game_lines_scraper_documentation.md) - Historical odds for moneyline, spreads, totals.
- [Player Prop Scraper](player_prop_scraper_documentation.md) - Historical player props snapshots.
- [NBA Player Position](nba_player_position_documentation.md) - Updates player positions via NBA API.
- [Update League Position Averages](update_league_position_averages_documentation.md) - League averages by position group.
- [Update Player Position History](update_player_position_history_documentation.md) - Snapshot-based role history.
- [Injury Database](injury_database_documentation.md) - Supabase storage and queries.
- [Injury Scraper Job](injury_scraper_job_documentation.md) - Scheduled scraping orchestration.

## Processing
- [NBA Linker Local](nba_linker_local_documentation.md) - Local ID matching and uploads.
- [Populate Average Stats](populate_average_stats_documentation.md) - Rolling averages for player/team stats.

## Models
- [Feature Store](feature_store_documentation.md) - Training and inference feature assembly.
- [Black-Litterman Blending](black_litterman_documentation.md) - Probability blending with market prior.
- [Truncated Negative Binomial](truncated_negbin_documentation.md) - Count model for THREES (C4 architecture).
- [Model Pipeline Runbook](model_pipeline_runbook.md) - Training, backtesting, and daily prediction guide.
- [Monte Carlo Tuning](monte_carlo_tuning.md) - MC simulation parameter tuning notes.

## Orchestration
- [Daily Pipeline Automation](daily_pipeline_automation.md) - Frequency-separated job scripts for cron scheduling.

## Deployment
- [Railway Deployment](railway_deployment.md) - Cloud deployment guide for Railway platform.
- [Scalability](scalability.md) - Architecture capacity analysis and scaling path.

## Diagnostics
- [Database Health Check](db_health_check_documentation.md) - Comprehensive database health validation.
- [Per-Stat Calibration](calibration_per_stat_documentation.md) - Per-stat calibration diagnostic with quantile coverage, bias, ECE, Brier score.

## Tools
- [Query Player](query_player_documentation.md) - CLI tool for querying stored predictions.

## Dashboard
- [Dashboard](dashboard_documentation.md) - Next.js web application for viewing predictions.

## Database and Tables
- [DB Client](db_client_documentation.md) - Database engine access and connectivity checks.
- [League Priors History](league_priors_history.md) - League baseline snapshots by position.
- [Player Position History](player_position_history.md) - Role snapshots for players.
- [Team Allowed By Position](team_allowed_by_position.md) - Opponent defense by position group.
