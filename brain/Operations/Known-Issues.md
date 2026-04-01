# Known Issues

> Part of [[Operations]]

## Active Bugs
- `test_finds_latest_run_directory` failing — expects `run_*` prefix but code now expects `nba_run_*`
- **MLB paper bets disabled** (`--skip-bets`) — `batter_total_bases` and `batter_runs_scored` trained with at_bats leakage. Re-enable after retraining.

## Recently Fixed (Session 15)
- **Railway MLB daily stats failing**: Supavisor strips `-c` startup params → role-level 8s timeout killed batting/pitching rolling average queries. Fix: explicit `SET statement_timeout = '120000'` in `fetch_batter_season_games()` and `fetch_pitcher_season_games()`.
- **MLB bets not resolving**: Zero 2026 game stats in DB — schedule existed but all games stuck at "Scheduled". Backfilled locally, 946 bets resolved.
- **pitcher_outs column mismatch**: Mapped to `"outs"` but actual column is `"outs_recorded"` in `mlb_paper_trader.py`.
- **MLB Discord P&L missing**: `mlb_daily_stats_job.py` never sent post-resolution P&L summary. Added `_send_mlb_pnl_summary()`.

## Technical Debt
- `raw_player_props_combined` at **67M+ rows** — queries take 9-14s. Needs archiving or partitioning.
- In-memory rate limiting on `/api/ask` — won't work multi-instance (needs Redis)
- No pagination on history/performance pages
- DFS/heatmap tables use horizontal scroll on mobile (should be card layouts)
- AI chat not persisted across modal close
- No CI/CD — deploys are manual git push

## Deferred Issues (from ISSUES.md)
4 of 43 total issues remain deferred:
1. **ISS-017** — Ratio column names say "l15" but compute L3/L5 (deferred to next retrain)
2. **ISS-018** — Pre-game inference requires game row to exist (needs new metadata source)
3. **ISS-020** — `validate_features=False` disables XGBoost safety (blocked on pandas 3.0 compat)
4. **ISS-023** — Stage 2 dedup keeps one stat per player per game (needs correlation-aware Kelly)

## Performance Bottlenecks
- DFS `get_dfs_lines` RPC: 9-14s on 67M+ row table
- `authenticated` role 8s timeout (workaround: SECURITY DEFINER with 30s override)

See full issue tracker: `ISSUES.md` at project root.

#issues #operations #bugs
