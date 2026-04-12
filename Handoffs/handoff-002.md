> Part of [[Handoffs]]

**Date**: April 12, 2026 at 3:00 PM

## Summary

This session investigated the MLB Kalshi bet volume drop (327 → 18 bets), diagnosed the root causes, fixed a missing `pitcher_outs` stat in the Kalshi whitelist, and implemented full groundwork for the `batter_hrr` (H+R+RBI) combined market model across 6 files. The H+R+RBI model targets Kalshi's combined hit+run+RBI player prop markets, which have 1k–5k volume on the NO side — filling the gap left by Kalshi having no standalone RBI markets.

## What Was Done

### MLB Volume Investigation
- Confirmed root causes of 327 → 18 bet drop:
  1. Opening-week market inefficiency normalized as season aged (most of the 327 were essentially illusory — at $100 bankroll + 327 candidates, proportional scaling = 0.02, floors all bets to 0 contracts)
  2. `pitcher_outs` was missing from `SUPPORTED_STATS["mlb"]` whitelist — fixed
  3. With $390 bankroll + 18 candidates now, scale = 0.87 → all 18 get real bets placed
- Identified that Kalshi has no standalone `KXMLBRBI` series → ~50% of regular model picks (RBI bets) are structurally unreachable on Kalshi

### batter_hrr Model Groundwork (6 files)

- **`src/models/mlb/mlb_stat_config.py`** — Added `batter_hrr` NegBin at 10% edge threshold
- **`src/models/mlb/mlb_batter_feature_store.py`** — Added `hrr` market key, SQL target `"h + bgs.r + bgs.rbi"` (SQL injection trick via `.replace("{target_col}", target_col)`), `BATTER_HRR_FEATURES` list, and `BATTER_FEATURE_MAP["hrr"]` entry
- **`src/paper_trading/mlb_paper_trader.py`** — Added `batter_hrr` to `MLB_STAT_RESOLUTION` with compound expression `"h + r + rbi"`
- **`src/paper_trading/kalshi_paper_trader.py`** — Added `batter_hrr` to `SUPPORTED_STATS["mlb"]` (alongside the earlier `pitcher_outs` fix)
- **`src/scrapers/kalshi/kalshi_utils.py`** — Added `"HRR": "batter_hrr"` to `KALSHI_STAT_MAP` and `"KXMLBHRR": "batter_hrr"` placeholder to `KALSHI_PROP_SERIES["mlb"]` with TODO comment
- **`src/backtesting/mlb/mlb_backtest_harness.py`** — Added `batter_hrr` to `STAT_ACTUALS` with `"h + r + rbi"` compound expression

### Plan Document
- **`plans/batter_hrr_model.md`** — Full implementation plan: why HRR works (hits dominant, correlated with R+RBI, avg 1.8–2.8/game vs 0.8 for standalone runs), risk assessment, training instructions, go/no-go gates, what won't be solved

## Decisions Made

- **HRR over standalone RBIs**: Runs alone averaged ~0.8/game (too noisy, failed as a model). H+R+RBI averages ~1.8–2.8/game. Hits are ~50% of the value and predictable. The combination creates a more stable "offensive contribution" stat that the NegBin handles well.
- **No sportsbook lines for HRR training**: `prop_line_batter_hrr` will be 0 for all training rows. This is by design — the model learns to rely on rolling average features and will effectively ignore the prop_line feature.
- **`KXMLBHRR` ticker is a placeholder**: Must be confirmed against a live Kalshi HRR market before deploying. Find a live HRR market, read the ticker prefix, update `kalshi_utils.py`.
- **NegBin at 10% edge threshold**: Same architecture as `batter_rbis` (both are overdispersed count data). 10% threshold is between `batter_rbis`'s 12% and `pitcher_strikeouts`'s 8% — reasonable starting point for a new market with uncertain liquidity.

## Blockers and Open Questions

- **KXMLBHRR ticker unconfirmed**: The Kalshi series prefix for H+R+RBI markets is a TODO placeholder. The scraper won't pick up HRR markets until this is verified and updated.
- **batter_hrr model not yet trained**: All config/plumbing is wired, but the model hasn't been trained. Need to run the training pipeline and backtest before paper trading.
- **Backtest gate required**: Require ROI > 0%, Z-score > 1.5, n ≥ 50 bets before deploying. Do NOT deploy the model just because it trains cleanly.
- **Seasonal pitcher K config transition**: Reminder to swap `z_max=0.25 → 0.5` in `STAT_BL_CONFIGS` around late May / early June (from Session 29).
- **NBA calibration check due Apr 13**: Model is 18+ days old, 3-week trigger hits tomorrow.
- **Apply Supabase migration 023**: MLB Stats Vault views + RLS (from Session 27).

## Recommended Next Steps

1. **Confirm KXMLBHRR ticker** — Find a live Kalshi H+R+RBI market, read the ticker (e.g., `KXMLBHRR-26APR12-TROUTML-O2.5`), confirm the series prefix is `KXMLBHRR`, update `kalshi_utils.py`
2. **Train `batter_hrr` model** — `python src/models/mlb/train_pipeline.py --stats batter_hrr --model-type negbin` on 2022–2025 season data
3. **Backtest `batter_hrr`** — Run `mlb_backtest_harness.py` on 2024–2025 seasons. Apply go/no-go gates (ROI > 0%, Z > 1.5, n ≥ 50) before enabling paper trading
4. **NBA calibration check** — Due tomorrow (Apr 13); model is 18+ days old, triggers 3-week recalibration review
5. **Apply Supabase migration 023** — MLB Stats Vault views + RLS (if not yet done)
6. **Monitor pitcher K UNDER-only** — Confirm UNDER-only filter is working in paper trader Discord alerts (~2-day check)

## Files to Read on Resume

- `src/scrapers/kalshi/kalshi_utils.py` — `KXMLBHRR` placeholder + TODO for ticker confirmation
- `src/models/mlb/mlb_batter_feature_store.py` — `batter_hrr` additions (target SQL, features)
- `src/models/mlb/mlb_stat_config.py` — `batter_hrr` NegBin config + all stat direction/edge configs
- `plans/batter_hrr_model.md` — Full implementation plan including go/no-go gates
- `memory/MEMORY.md` — Scraping/Railway rules (CRITICAL), model calibration triggers
