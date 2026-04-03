# Handoff 023 — Black-Litterman Kalshi Blending + Pipeline Debugging

> Part of [[Handoffs]]

**Date**: April 03, 2026 at 5:01 PM

## Summary

Wired Black-Litterman probability blending into the Kalshi edge calculator and paper trader, matching the proven NBA Model Picks methodology (tau=0.5, z_max=1.0). Discovered and fixed the root cause of the Kalshi pipeline producing no data since April 2: an incorrectly encoded `KALSHI_PRIVATE_KEY_B64` env var on Railway. Successfully ran edge computation locally, verified 607 markets matched with BL probabilities, and confirmed paper trades are flowing to Discord.

## What Was Done

- **DB migration**: Added `bl_model_prob`, `bl_edge`, `bl_confidence` columns to `kalshi_markets` table
- **`src/models/kalshi_edge.py`**: Added `_find_sportsbook_odds()` method, BL blending in `compute_edges()` loop (sportsbook devigged prior with Kalshi fallback), BL fee-adjusted edge computation, BL columns in UPDATE statement
- **`src/paper_trading/kalshi_paper_trader.py`**: Updated SELECT query to fetch BL columns, bet selection now uses BL-blended probability when available (falls back to raw model_prob)
- **Railway env var fix**: `KALSHI_PRIVATE_KEY_B64` was incorrectly encoded (raw key data without PEM headers). Re-encoded from local `KalshiBot.txt` with correct full-PEM base64
- **Local edge computation**: Ran `python -m src.models.kalshi_edge --date 2026-04-03 --sport nba` — 964 markets loaded, 607 matched, 607 updated with edges + BL probabilities
- **Code committed and pushed**: `bc9129d` — Railway will auto-deploy with BL changes

## Decisions Made

| Decision | Choice | Rationale |
|----------|--------|-----------|
| BL config | tau=0.5, z_max=1.0 | Same as proven NBA sportsbook sweep |
| Market prior | Sportsbook devigged → Kalshi fallback | Sportsbook is more efficient market |
| Use BL building blocks, not `blend_prediction()` | Individual `devig()`, `compute_confidence()`, `blend()` calls | Avoids `>` vs `>=` issue for Kalshi integer lines |
| Edge threshold | Keep 15% taker-fee-adjusted | Compensates for ~7% Kalshi taker fees |
| Store BL results in DB | `bl_model_prob`, `bl_edge`, `bl_confidence` columns | Dashboard visibility + paper trader reads from DB |

## Blockers and Open Questions

1. **Kalshi refresh job completion logs missing**: The 20:50 UTC run started but never logged completion. May have been killed during the Railway re-deployment. Future runs should work now that the code is pushed.
2. **BL edge values for extreme probabilities**: Some markets with `model_prob=0.0` show `bl_prob=0.08-0.09` despite `bl_confidence=1.0`. The log-odds clipping at 1e-6 may be producing unexpected results at the extremes. Worth investigating if paper bets on these markets lose money.
3. **357 unmatched markets**: `3pm` (150), `stl` (78), `blk` (48) have no MC samples. We don't model these stats — expected behavior but limits Kalshi coverage.

## Recommended Next Steps

1. **Monitor Kalshi paper trading results** (highest priority) — Check next day's Discord for BL-blended paper trade notifications. Verify the automated Railway pipeline produces edges without manual intervention.
2. **Investigate BL extreme probability behavior** — The `bl_confidence=1.0` with `bl_prob` far from `model_prob=0.0` cases need review. Check if `BlackLittermanBlender.blend()` handles near-zero probabilities correctly in log-odds space.
3. **Consider adding 3PM model** — 150 Kalshi markets for 3-pointers have no model coverage. Could be a profitable expansion.
4. **Phase 3 (Stripe monetization)** — No blockers, ready to start whenever.

## Files to Read on Resume

- `src/models/kalshi_edge.py` — BL blending implementation (the core change this session)
- `src/paper_trading/kalshi_paper_trader.py` — Paper trader using BL probability
- `src/models/black_litterman.py` — BL blender API (devig, compute_confidence, blend)
- `src/orchestration/kalshi_refresh_job.py` — Full pipeline orchestration
- [[NBA-Model]] — Model status and calibration

#kalshi #black-litterman #pipeline #infrastructure
