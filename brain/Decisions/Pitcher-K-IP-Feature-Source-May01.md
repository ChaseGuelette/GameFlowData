# Pitcher K: IP Model as Feature Source Architecture

**Date**: May 1, 2026
**Status**: Planned — not yet implemented
**Precedes**: [[MLB-Model-Architecture-Overhaul-Apr28]]

## Why This Change

Copula backtest (Apr 13–26, 2026) showed the copula model is worse than the single model.
The no-BL baseline for the copula was entirely negative across all edge thresholds, while the
single model's no-BL baseline showed positive ROI at moderate edges.

Root cause: ρ(IP, K/IP) = **-0.0179** (essentially zero). IP and K/IP have genuinely different
causal pathways — IP residuals are driven by pitch count accumulation, manager pull decisions,
and bullpen state; K/IP residuals are driven by stuff quality, umpire zone, and sequencing luck.
These processes are approximately independent after conditioning on pitcher features.

This is NOT ratio suppression. The copula adds zero value over independent sampling when ρ ≈ 0.

**Why Option 4 (IP as feature source) still works despite ρ ≈ 0:**
ρ(IP, K/IP) ≈ 0 does not mean ρ(IP, K_total) ≈ 0. A pitcher going 4 IP vs 8 IP dominates
total K variance — the volume channel. Telling the K model "this pitcher is at short-outing risk
tonight" changes expected K total even though it doesn't change K-rate. IP predictions carry
information through the volume pathway, not the rate pathway.

## Architecture After Implementation

```
IP sub-model  →  predicted_ip_q25, predicted_ip_spread
                         ↓ (appended as features)
Direct K model  →  K prediction
```

Training:
1. Train IP sub-model on enriched pitcher features
2. Generate batch IP predictions on training set (no lookahead within copula pipeline)
3. Append `predicted_ip_q25` and `predicted_ip_spread` to training DataFrame
4. Train direct K model on enriched features

Inference (two-stage):
1. IP model predicts IP quantiles for the game
2. q25 and spread appended to features dict
3. K model runs on enriched features

## Missing Features That Are Blocking IP Model Quality

Two gaps identified from the current `PITCHER_K_FEATURES`:

### 1. `pitcher_min_ip_l5` (not yet in table)
- Only `pitcher_avg_ip_l5` exists — the mean doesn't capture blowup risk
- `pitcher_min_ip_l5` = MIN(IP) over last 5 starts = Q10-Q20 proxy for short-outing risk
- Source: `mlb_player_average_pitching` table (pre-computed rolling stats)
- Implementation: add `rolling_with_groupby(ip_shifted, group_key, window=5, agg="min")` to
  the populate averages script; DB migration to add column

### 2. `team_bullpen_ip_last_3d` (exists for batters, not pitchers)
- Bullpen workload of pitcher's OWN team = manager's leash signal
  (tired pen → let starter go longer; rested pen → shorter leash)
- Data exists in `mlb_bullpen_daily_status` — already scraped, wired into `BATTER_BASE_FEATURES`
  as `opp_bullpen_ip_last_3d`
- Not in `PITCHER_K_FEATURES` at all
- Implementation: add LEFT JOIN in pitcher training query against `mlb_bullpen_daily_status`
  on `bull.team_id = pgs.team_id AND bull.game_date = pgs.game_date`

## Validation Gate (Between IP Retrain and K Retrain)

Compute `corr(predicted_ip_q25, pitcher_avg_ip_l5)` on 2025-07-01 → 2025-09-28 holdout:

| Range | Interpretation | Action |
|-------|---------------|--------|
| > 0.85 | IP model doesn't add beyond static L5 mean | Use delta feature: `predicted_ip_q25 - pitcher_avg_ip_l5` |
| 0.5–0.7 | Sweet spot — game-specific short-outing risk captured | Add raw q25 and spread |
| < 0.5 | IP model is noisy | Investigate before wiring in |

## Implementation Steps

### Phase 1: Static Feature Additions

**1. `mlb_populate_averages.py` (or equivalent):**
```python
min_ip_l5 = rolling_with_groupby(ip_shifted, group_key, window=5, agg="min")
df["min_ip_l5"] = min_ip_l5
```

**2. DB migration (local only):**
```sql
ALTER TABLE mlb_player_average_pitching ADD COLUMN IF NOT EXISTS min_ip_l5 FLOAT;
```
Then re-run populate script to fill values.

**3. `mlb_feature_store.py` — LATERAL JOIN for `mlb_player_average_pitching`:**
```sql
-- Add to SELECT:
pa.min_ip_l5 AS pitcher_min_ip_l5
```
Add `"pitcher_min_ip_l5"` to `PITCHER_K_FEATURES`. Update `get_player_game_features()`.

**4. `mlb_feature_store.py` — bullpen LEFT JOIN:**
```sql
LEFT JOIN mlb_bullpen_daily_status bull_own
    ON bull_own.team_id = pgs.team_id
   AND bull_own.game_date = pgs.game_date
```
```sql
COALESCE(bull_own.bullpen_ip_last_3d, 0) AS team_bullpen_ip_last_3d,
COALESCE(bull_own.bullpen_pitches_last_3d, 0) AS team_bullpen_pitches_last_3d
```
Add both to `PITCHER_K_FEATURES`. Update `get_player_game_features()`.

### Phase 2: Retrain IP Sub-Model

```bash
python src/models/mlb/mlb_train_pipeline.py --local --copula \
  --tune --tuning-trials 100 \
  --train-seasons 2024 2025 \
  --cal-season 2025 --cal-end-date 2025-07-01
```
Dynamic feature selection will pick up `pitcher_min_ip_l5` and `team_bullpen_ip_last_3d`.

### Phase 3: Validation Gate

Check `corr(predicted_ip_q25, pitcher_avg_ip_l5)` and decide feature strategy (see table above).

### Phase 4: Wire IP Predictions into K Model Training

**`mlb_train_pipeline.py` (non-copula path):**
```python
# After ip_pipeline is trained — generate batch IP predictions on training data
X_ip = train_df[ip_pipeline.feature_names]
ip_preds = ip_pipeline.predict_batch(X_ip)
train_df["predicted_ip_q25"] = ip_preds["q0.25"]
train_df["predicted_ip_spread"] = ip_preds["q0.75"] - ip_preds["q0.25"]
# K model's dynamic feature selector now has access to these columns
```

**`mlb_monte_carlo.py` — inference two-stage:**
```python
def predict(self, features: dict) -> PropPrediction:
    if self.ip_pipeline is not None:
        ip_pred = self.ip_pipeline.predict_single(features)
        features["predicted_ip_q25"] = ip_pred.q25
        features["predicted_ip_spread"] = ip_pred.q75 - ip_pred.q25
    # Run K model as normal
    ...
```

**`mlb_model_suite.py`:** Load IP pipeline from `ip_model/` subdir (if present) and pass to K predictor.

### Phase 5: Retrain K Model and Backtest

```bash
python src/models/mlb/mlb_train_pipeline.py --local \
  --tune --tuning-trials 100 \
  --train-seasons 2024 2025 \
  --cal-season 2025 --cal-end-date 2025-07-01
```

Backtest comparison:
```bash
python src/backtesting/mlb/run_mlb_sweep.py --local \
  --stats pitcher_strikeouts \
  --model-dir src/models/mlb/artifacts/<new_run_dir> \
  --start 2026-03-27 --end 2026-04-29 \
  --tau none 0.25 0.5 0.75 0.9 \
  --edge 0.08 0.10 0.12 0.15 \
  --z-max 0.25 1.0 --max-weight 0.5 0.8 \
  --n-samples 5000
```

**Success criterion:** no-BL baseline ROI > 0 at edge=0.08 (was -2.87% for copula, +1.45% for
old single model). Target: match or exceed old single model's best config (+24% ROI, Sharpe 2.78
on 2025 holdout — though 2026 early-season data will show lower absolute ROI due to sample size).

## Key Files

| File | Change |
|------|--------|
| `src/models/mlb/mlb_feature_store.py` | Add `pitcher_min_ip_l5` + `team_bullpen_ip_last_3d` to PITCHER_K_FEATURES, SQL, and inference |
| Populate averages script (find in `src/processing/mlb/`) | Add `min_ip_l5` rolling MIN computation |
| `src/models/mlb/mlb_train_pipeline.py` | Generate batch IP predictions on training data for K model |
| `src/models/mlb/mlb_monte_carlo.py` | Two-stage inference: IP model → append features → K model |
| `src/models/mlb/mlb_model_suite.py` | Load IP pipeline alongside K model |

## What NOT to Do

- Do NOT remove the copula architecture — it stays as an alternative predictor
- Do NOT add `opp_team_pitches_per_pa_l10` yet — data not in any table, too much work for unclear gain
- Do NOT use in-sample IP predictions without care — the batch predictions on training data
  will have seen those rows; verify holdout backtest is the real validation signal
- Do NOT deploy to Railway until holdout backtest confirms improvement

## Related Docs

- [[MLB-Model-Architecture-Overhaul-Apr28]] — Full copula architecture implementation (Apr 28)
- [[Execution-Plan]] — Phase 1 MLB model steps
