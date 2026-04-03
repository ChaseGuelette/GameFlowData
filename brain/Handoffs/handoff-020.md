> Part of [[Handoffs]]

**Date**: April 01, 2026 at 5:31 PM

## Summary
Evaluated MLB model training outputs for 4 batter models. Decided to drop `batter_home_runs` entirely (no edge against market). Identified best backtest config for `batter_hits` (+36.3% ROI). `batter_runs_scored` and `batter_total_bases` retrained clean — ready for backtest sweeps.

## What Was Done

### MLB Model Evaluation
- **`batter_runs_scored` (NegBin)**: Training output analyzed. Good convergence, minimal bias, excellent zero calibration. Concern: constant alpha=0.135 (no player-specific variance learned). Assessment: usable, needs backtest sweep.
- **`batter_total_bases` (NegBin)**: Training output analyzed. Alpha varies 0.87-1.30 (good — learns player-specific overdispersion). Higher mu range than runs. Assessment: promising, needs backtest sweep.
- **`batter_home_runs` (Binary)**: Backtest sweep showed disaster — max 8 bets/month in any config, -12.3% best ROI. Event too rare (~7% HR rate) for model to find exploitable edge. **Decision: drop entirely from pipeline AND dashboard predictions.**
- **`batter_hits` (Binomial)**: Backtest sweep showed excellent results. Best config: tau=0.75, z_max=1.0, mw=0.65, edge=0.08 — +36.3% ROI, 63% hit rate, 314 bets, 3.52 Sharpe, 22.3% max drawdown.

### Pitcher K Config Reference
- Looked up pitcher_strikeouts best config: tau=0.9, z_max=0.75, mw=0.65, edge=0.02 — +10.1% ROI, 57% win, 453 bets, 0.93 Sharpe, 29.5% MaxDD.

### Brain Updates
- Updated `brain/Models/MLB-Model.md` — status changes for all 6 models, added batter_hits and HR backtest result tables, updated "What's NOT Built" section
- Updated `brain/Execution-Plan.md` — Steps 1.3 and 1.6 progress notes

## Decisions Made
- **Drop `batter_home_runs` entirely** — not just from paper trading but from dashboard predictions too. User explicitly said "No reason to have it on the dashboard predictions." The event is too rare and binary for the model to beat market pricing.
- **Best batter_hits config**: tau=0.75, z_max=1.0, mw=0.65, edge=0.08 — good balance of volume (314 bets), ROI (+36.3%), and risk (22.3% MaxDD, 3.52 Sharpe).

## Blockers and Open Questions
- **HR removal not yet implemented in code** — need to remove from daily runner, paper trader, and dashboard prediction display
- **Batter hits config not yet promoted** — need to write the config to production
- **TB and runs backtest sweeps not yet run** — models are retrained and ready
- **Changes from Session 18 (Kalshi overflow/alignment) still not deployed to Railway**

## Recommended Next Steps
1. **Remove `batter_home_runs` from pipeline** — daily runner, paper trader, dashboard predictions. Code changes needed in `mlb_daily_runner.py`, `mlb_paper_trader.py`, and dashboard stat config.
2. **Promote batter_hits config** — write tau=0.75, z_max=1.0, mw=0.65, edge=0.08 to production config
3. **Run `batter_total_bases` backtest sweep** — model is retrained and ready: `python src/backtesting/mlb/run_mlb_sweep.py --stat total_bases ...`
4. **Run `batter_runs_scored` backtest sweep** — same pattern with retrained model
5. **Deploy to Railway** — Kalshi overflow tracking + paper/live alignment from Session 18 still local

## Files to Read on Resume
- [[MLB-Model]] — Updated model status with backtest results and HR drop decision
- [[Execution-Plan]] — Steps 1.3 and 1.6 progress
- `src/models/mlb/mlb_daily_runner.py` — Where to remove HR from inference
- `src/paper_trading/mlb_paper_trader.py` — Where to remove HR from paper trading
- `src/paper_trading/kalshi_paper_trader.py` — Overflow bet tracking (Session 18, not yet deployed)
