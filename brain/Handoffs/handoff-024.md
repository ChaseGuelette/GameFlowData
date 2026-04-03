# Handoff 024 — MLB Model Promotion: 3 Stats Live, 3 Dropped, Dashboard Updated

> Part of [[Handoffs]]

**Date**: April 03, 2026 at 5:11 PM

## Summary

Completed the MLB model evaluation and promotion cycle. Ran backtest sweeps for all 6 batter stats, confirmed `batter_total_bases` (0/540 profitable), `batter_runs_scored` (trivial edge), and `batter_home_runs` (no edge) should be dropped. Promoted 3 models with per-stat optimal Black-Litterman configs. Built per-stat BL architecture into daily runner, backtest sweep, and paper trader. Updated dashboard to remove dropped stats and support per-stat Model Picks. Combined backtest: 1,064 bets, +21.25% ROI, 1.19 Sharpe over Jul 1-Sep 28.

## What Was Done

### Models Promoted (with per-stat BL configs)
| Model | BL Config | Edge | Backtest ROI |
|-------|-----------|------|-------------|
| `pitcher_strikeouts` | tau=0.9, z_max=0.25, mw=0.8 | 5% | +8.0% (645 bets) |
| `batter_hits` | tau=0.75, z_max=1.0, mw=0.8 | 8% | +33.2% (282 bets) |
| `batter_rbis` | tau=0.9, z_max=0.25, mw=0.8 | 12% | +44.2% (137 bets) |

### Models Dropped
| Model | Reason |
|-------|--------|
| `batter_home_runs` (Binary) | No edge — max 8 bets/month, -12.3% ROI (Session 19) |
| `batter_total_bases` (NegBin) | 0/540 configs profitable. Best ROI: 0.00% |
| `batter_runs_scored` (NegBin) | 3/540 profitable, all trivial ($211 profit) |

### Backend Changes
- `src/models/mlb/mlb_stat_config.py` — Created per-stat `STAT_BL_CONFIGS` dict and per-stat edge thresholds in `MLB_STATS`. Removed TB, runs from stat config.
- `src/models/mlb/mlb_daily_runner.py` — Replaced single global BL config with per-stat blenders from `STAT_BL_CONFIGS`. Removed TB/runs from `stat_to_market` dicts. Per-stat `is_recommended` flag.
- `src/paper_trading/mlb_paper_trader.py` — Removed TB/runs/HR from `MLB_STAT_RESOLUTION`.
- `src/backtesting/mlb/run_mlb_sweep.py` — Added `--combined` flag for multi-stat backtesting with per-stat configs. Added `run_combined_config()` function. Updated `compute_edges_for_config()` to accept per-stat blender dict.
- `src/models/mlb/mlb_model_suite.py` — TB/runs kept in model loading (for backtesting), removed from production stat config.
- `src/models/negbin_model.py` — Added exposure/offset support (`log(projected_ab)` base margin) for NegBin models.

### Frontend Changes
- `dashboard/src/lib/sport-config.ts` — Removed TB/HR/runs from `statTypes` and `filterTabs`. Added RBIs tab.
- `dashboard/src/app/(protected)/dashboard/page.tsx` — Added `.in('stat', config.statTypes)` to Supabase query (prevents old predictions showing). MLB Model Picks uses edge=5% and no client-side BL tau (backend handles per-stat). Display text shows "Per-Stat BL Edge" with stat-specific thresholds.

### Verification
- Dashboard build: clean (no errors)
- Python tests: 694 passed, 0 failed
- Combined backtest validated: 1,064 bets, +21.25% ROI, 1.19 Sharpe

## Decisions Made

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Drop TB, runs, HR | All 3 dropped | Backtest sweeps confirmed no viable configs across 540+ parameter combinations each |
| Per-stat BL configs | Each stat gets own tau/z_max/mw/edge | Different stats have different optimal market-blending parameters |
| Frontend stat filtering | Query-level `.in('stat', config.statTypes)` | Prevents old predictions from dropped stats appearing on historical dates |
| Model Picks for MLB | Trust backend `is_recommended`, no frontend re-filtering | Backend already applies per-stat edge thresholds, frontend's global 9% would wrongly filter pitcher K (5%) |
| Keep TB/runs in backtesting paths | Model suite still loads them | Allows future re-evaluation without code changes |

## Blockers and Open Questions

1. **NegBin exposure/offset not yet tested via retrain**: The exposure implementation (Session 23 plan) added `log(projected_ab)` base margin support to NegBinModel, but TB and runs were dropped before retraining with it. If these stats are ever revisited, the exposure code is ready.
2. **`pitcher_outs` not promoted**: Still in `MLB_STATS` with 8% edge threshold but no sweep-optimized BL config. Low priority — pitcher K is the primary pitching stat.
3. **MLB season hasn't started yet**: All backtests are on 2025 data. The 2026 season will be the real validation.

## Recommended Next Steps

1. **Deploy to Railway/Vercel** — Push changes to production. The MLB daily runner will use the new per-stat configs immediately.
2. **Remove `--skip-bets` flag from MLB inference scheduler** — Paper betting was disabled while leaky models were being retrained. Now that configs are promoted, re-enable it in `src/orchestration/scheduler.py`.
3. **Phase 3: Stripe monetization** — No technical blockers. Ready to start.
4. **Monitor first week of MLB 2026 predictions** — Verify per-stat BL configs produce reasonable bets in production.

## Files to Read on Resume

- [[MLB-Model]] — Full model status, configs, backtest results
- [[Execution-Plan]] — Phase 1 now complete, Phase 3 ready
- `src/models/mlb/mlb_stat_config.py` — Per-stat BL configs (the source of truth)
- `src/orchestration/scheduler.py` — Check if `--skip-bets` flag needs removal
- `dashboard/src/lib/sport-config.ts` — Frontend stat configuration

#mlb #model #promotion #backtest #dashboard
