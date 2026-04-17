> Part of [[Handoffs]]

**Date**: April 16, 2026 at ~3:00 PM

## Summary

Full Kalshi go-live preparation: implemented all three pre-launch code fixes from the analysis plan, set 9 Railway env vars for live trading config, completely rewrote the analysis script with 14 sections (up from 8), created the `/check-kalshi` Claude skill, fixed a failing Python test, fixed two ruff lint errors, fixed a Discord balance display bug (all bets showed same balance), and expanded the overflow Discord embed with win rate + ROI.

## What Was Done

### Go-Live Code Fixes (from analysis plan)
- **`src/paper_trading/kalshi_paper_trader.py`**: Removed `pitcher_outs` from MLB SUPPORTED_STATS whitelist; updated MLB resolution query to use list-based `col_expr` join (fixes `batter_hrr` h+r+rbi compound stat); updated `KALSHI_DAILY_EXPOSURE_PCT` default 0.60 → 0.90
- **`src/paper_trading/kalshi_live_trader.py`**: Added `batter_hrr` to MLB SUPPORTED_STATS (now matches paper trader); updated MLB resolution query to list-based `col_expr` join (same fix as paper trader)
- **`src/paper_trading/mlb_paper_trader.py`**: Converted `MLB_STAT_RESOLUTION` from `dict[str, tuple[str, str]]` to `dict[str, tuple[str, list[str]]]`; all values now lists (`"so"` → `["so"]`, `"h + r + rbi"` → `["h", "r", "rbi"]`); resolution query uses `col_expr = " + ".join(f"s.{c}" for c in columns)`

### Railway Env Vars Set (9 vars, deploy skipped)
`KALSHI_LIVE_STARTING_BANKROLL=300`, `KALSHI_LIVE_KELLY_FRACTION=0.10`, `KALSHI_LIVE_MIN_EDGE=0.15`, `KALSHI_LIVE_MAX_CONTRACTS=50`, `KALSHI_LIVE_MAX_DAILY_EXPOSURE=500`, `KALSHI_DAILY_EXPOSURE_PCT=0.90`, `KALSHI_LIVE_DRAWDOWN_LIMIT=0.30`, `KALSHI_LIVE_DAILY_LOSS_LIMIT=20`, `KALSHI_LIVE_CONSEC_LOSS_LIMIT=5`, `KALSHI_ALLOW_YES_BETS=false`
**NOT set yet**: `KALSHI_LIVE_TRADING_ENABLED=true` — Chase flips this after funding the account.

### Analysis Script Rewrite
- **`scripts/analyze_kalshi_paper_bets.py`**: Complete rewrite — 14 sections vs old 8. New additions: before/after NO-only split (`--split-date`, default 2026-04-11), separate real-only and combined tables per stat type, cross-sectional consistency check (✓/✗ per stat + ALL-PROFITABLE badge), edge buckets corrected to 15-20%/20-25%/25-30%/30%+ (was 5%/10%/15%/20%), monotonicity check, weekly comparison table with WoW delta, bankroll trajectory from daily_log, dual Z-scores (real and combined), go-live 6-check scorecard with scale-up milestones. New flags: `--split-date`, `--no-split`, `--no-bankroll`.

### Claude Skill
- **`.claude/commands/check-kalshi.md`**: New `/check-kalshi` skill. Runs the script (30-day + 7-day windows), queries DB for bankroll/circuit-breaker status, interprets cross-sectional consistency, weekly trend, overflow analysis, and gives a scale-up recommendation.

### Bug Fixes
- **`tests/test_run_sweep.py`**: `test_to_dict` assertion updated to include `"max_weight": 0.50` — `SweepConfig.to_dict()` had gained this field but the test was never updated.
- **Ruff errors**: Removed unused `of_no` variable in `section_by_stat()` and unused `no_only_flag` in `run_analysis()`.
- **`src/paper_trading/kalshi_paper_trader.py`** — Discord balance bug: resolution loop was calling `self.get_bankroll()` once and stamping every bet with the same balance (yesterday's closing). Fixed by accumulating `running_balance += update["pnl"]` per bet — each alert now shows the correct incremental balance.
- **`src/orchestration/kalshi_daily_summary_job.py`**: `_get_overflow_stats()` SQL expanded to fetch `overflow_won`, `overflow_lost`, and `overflow_cost` in addition to total count and P&L.
- **`src/discord_bot/alerts.py`**: Overflow embed field expanded to show W-L record, win rate, and ROI (was: count + hypothetical P&L only).

## Decisions Made

- **`pitcher_outs` removed from paper trader whitelist** — no production model exists for it and it was a latent mismatch with the live trader. `batter_hrr` kept (model trained, scraper working, will auto-activate on model promotion).
- **`KALSHI_DAILY_EXPOSURE_PCT` default raised to 0.90** — overflow analysis showed 87% of edge was lost to the old 0.60/$80 cap. Higher exposure captures the same-quality bets, not riskier ones.
- **Edge buckets aligned to 15%+ min_edge threshold** — the old script used 5%/10% buckets that are irrelevant since min_edge is 15%. New buckets match the go-live analysis exactly.
- **`KALSHI_LIVE_TRADING_ENABLED` intentionally NOT set** — Chase needs to fund the Kalshi account first, then flip it manually.

## Blockers and Open Questions

- **Live trading not yet enabled** — waiting on Chase to fund the Kalshi account. All code and env vars are ready.
- **batter_hrr model not yet promoted** — sweep still pending (backfill → linker → sweep → promote). No bets will be placed for HRR until promoted.
- **NBA playoff model sweep** — `src/models/artifacts/production_playoffs/` exists in git status, suggesting a playoff-specific model may be in progress. Check if it needs promoting.

## Recommended Next Steps

1. **Fund Kalshi account** → set `KALSHI_LIVE_TRADING_ENABLED=true` on Railway → deploy → monitor Discord for green live trade alerts
2. **Run `/check-kalshi`** after first live day to confirm alerts look correct
3. **batter_hrr sweep**: Run backfill → linker → BL sweep → check ROI > 0% + Z > 1.5 → promote
4. **NBA playoffs**: Check if `production_playoffs/` model needs to be promoted before playoff games start
5. **Phase 3 Stripe**: Next major product milestone — subscribe page, webhook, customer portal

## Files to Read on Resume

- [[Handoffs]] — this handoff-008 + prior handoff-007 for context on Polymarket/batter_rbis
- [[Execution-Plan]] — check Phase 1.9 (batter_hrr) and Phase 7 (Kalshi live) status
- `scripts/analyze_kalshi_paper_bets.py` — understand the new script structure before running `/check-kalshi`
- `.claude/commands/check-kalshi.md` — the new skill definition
- `src/paper_trading/kalshi_paper_trader.py` — all three go-live fixes are here
