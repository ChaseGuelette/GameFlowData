# Black-Litterman Probability Blending

## Overview

`src/models/black_litterman.py` implements a Black-Litterman-inspired probability blending layer for sports betting. It anchors the model's overconfident probability estimates to the market's well-calibrated prior, only allowing the posterior to deviate when the model shows high-confidence disagreement.

## Problem Solved

The Monte Carlo predictor produces raw P(over) via `(samples > line).mean()`. This amplifies small mean shifts into extreme probability estimates (e.g., model says 84% over when actual is 49.1%). The market, by contrast, is well-calibrated (Brier score 0.2495 vs model's 0.2705). BL blending uses the market as an anchor and limits model influence to cases where the model's distribution strongly disagrees with the line.

## Components

### `BLConfig` (dataclass)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `tau` | 0.05 | Global scaling of model influence (0.01=conservative, 0.30=aggressive) |
| `max_weight` | 0.50 | Hard cap on blending weight — never trust model more than market |
| `min_prob` | 0.01 | Floor for probabilities to avoid log(0) |
| `max_prob` | 0.99 | Ceiling for probabilities to avoid log(0) |

### `BlackLittermanBlender` (class)

#### `american_to_decimal(american_odds) -> float` (static)

Converts American odds to decimal odds.
- `-110` → `1.909`
- `+150` → `2.50`
- `0` → `2.0` (fallback to even money)

#### `devig(over_odds, under_odds) -> tuple[float, float]`

Removes bookmaker vig using multiplicative normalization. For 2-outcome markets (over/under), this is mathematically equivalent to Shin's method and the additive method.

1. Convert American odds to decimal
2. Compute raw implied probabilities: `1.0 / decimal_odds`
3. Normalize: `raw_prob / booksum` so they sum to 1.0

#### `compute_confidence(samples, line) -> float`

Computes per-prediction confidence from the MC distribution's z-score:

```
z = |mean(samples) - line| / std(samples)
confidence = 1 - exp(-0.5 * z²)
```

| z-score | confidence | Interpretation |
|---------|-----------|----------------|
| 0.0 | 0.00 | Line at distribution center → posterior = market |
| 0.5 | 0.12 | Mild disagreement |
| 1.0 | 0.39 | Moderate disagreement |
| 2.0 | 0.86 | Strong disagreement |
| 3.0 | 0.99 | Extreme disagreement |

Edge cases: returns 0.0 for empty samples or zero standard deviation.

#### `blend(model_prob, market_prob, confidence) -> float`

Blends model and market probabilities in log-odds space:

```
model_p, market_p = clip(prob, min_prob, max_prob)
market_logit = log(market_p / (1 - market_p))
model_logit = log(model_p / (1 - model_p))
w = min(tau * confidence, max_weight)
posterior_logit = market_logit + w * (model_logit - market_logit)
posterior = sigmoid(posterior_logit)
```

Log-odds space (rather than linear probability) correctly handles boundary effects near 0 and 1.

#### `blend_prediction(samples, line, over_odds, under_odds) -> dict`

Full pipeline for a single prediction. Returns:

| Key | Description |
|-----|-------------|
| `market_over` | Devigged market P(over) |
| `market_under` | Devigged market P(under) |
| `model_over` | Raw MC empirical P(over) |
| `model_under` | Raw MC empirical P(under) |
| `confidence` | BL per-prediction confidence [0, 1] |
| `posterior_over` | Blended posterior P(over) |
| `posterior_under` | Blended posterior P(under) |

## Integration

### Backtesting (`backtest_harness.py`)

The `BacktestHarness` accepts an optional `bl_blender: BlackLittermanBlender` field. When set, `_calculate_edges()` takes the BL path:

1. Calls `blend_prediction()` for each row with MC samples and sportsbook odds
2. Uses `posterior_over`/`posterior_under` as the probability for edge calculation
3. Uses `market_over`/`market_under` (devigged) as the implied prob baseline
4. Edge = `posterior_prob - devigged_market_prob`
5. Stores all diagnostic columns in the predictions DataFrame

When `bl_blender` is None, the original path is used unchanged.

### CLI (`run_backtest.py`)

```bash
# BL disabled (default) — original behavior
python src/backtesting/run_backtest.py --start 2024-10-22 --end 2025-01-15

# BL enabled with conservative tau
python src/backtesting/run_backtest.py --start 2024-10-22 --end 2025-01-15 --bl-tau 0.05

# BL enabled with aggressive tau
python src/backtesting/run_backtest.py --start 2024-10-22 --end 2025-01-15 --bl-tau 0.20
```

### Bet Simulator (`bet_simulator.py`)

The `Bet` dataclass has an optional `posterior_prob` field that stores the BL-blended probability for diagnostic purposes. This is populated automatically when BL is enabled.

## Testing

39 unit tests in `tests/test_black_litterman.py` covering:

- **American-to-decimal conversion**: Negative odds, positive odds, even money, edge cases
- **Devigging**: Standard lines, favorite/longshot, symmetry, sum-to-one invariant
- **Confidence**: Z-score mapping, boundary safety (empty samples, zero std), symmetry
- **Blending**: tau=0 returns market, weight cap, boundary clamping, posterior-between-market-and-model
- **Full pipeline**: Output keys, probability ranges, realistic NBA scenarios
- **Config**: Defaults, custom values, None handling

## Known Issue: Confidence Function Structural Flaw (2026-01-31)

A comprehensive parameter sweep (40 configs across tau × edge threshold × Kelly fraction) revealed that **all BL-enabled configurations produce 0-12 bets**, while no-BL configs produce 600-873 bets at +3% ROI.

### Root Cause

The confidence formula `confidence = 1 - exp(-0.5 * z²)` is structurally too conservative for realistic betting edges:

| P(over) | Raw Edge | z-score | Confidence | w (tau=0.10) | Edge After BL |
|---------|----------|---------|------------|--------------|---------------|
| 0.55 | 3.0% | ~0.13 | 0.008 | 0.0008 | 0.006% |
| 0.60 | 8.0% | ~0.26 | 0.033 | 0.003 | 0.07% |
| 0.70 | 18.0% | ~0.53 | 0.131 | 0.013 | 0.6% |
| 0.80 | 28.0% | ~1.05 | 0.423 | 0.042 | 3.4% |

The formula requires z > 1.0 for meaningful weight (confidence > 0.39), but profitable edges in betting markets typically have z < 0.5. This means the BL layer crushes all realistic edges below any practical threshold.

### Mathematical Proof

For a profitable bet at -110 odds, the model needs P(over) ≈ 0.55 (3% edge). The MC distribution has mean ≈ line and std ≈ several points, giving z = |mean - line| / std ≈ 0.13. At this z-score:

```
confidence = 1 - exp(-0.5 × 0.13²) = 1 - exp(-0.0085) ≈ 0.008
w = min(0.10 × 0.008, 0.50) = 0.0008
edge_after_BL = 0.0008 × 0.03 = 0.000024 (0.002%)
```

This is the fundamental issue — the BL layer was designed assuming model disagreement would produce z > 1.0, but the MC distribution is well-calibrated (mean near line), so genuine edges manifest as small z-scores.

### Proposed Fixes

1. **Fixed-weight tau:** Use `w = tau` directly (skip confidence scaling). Simpler, gives the model a constant voice.
2. **Linear confidence ramp:** `confidence = min(z / z_max, 1.0)` where `z_max ≈ 1.0`. Proportional response to disagreement.
3. **Sizing-only BL:** Use BL posterior only for Kelly sizing, not edge filtering. Let the no-BL path determine bet eligibility.

### Current Recommendation

Use the **no-BL path** for betting decisions until the confidence function is redesigned. The model shows genuine edge without BL (+3% ROI, REB +7.9%).

## References

- Black & Litterman (1992), "Global Portfolio Optimization"
- Idzorek (2005), "A Step-by-Step Guide to the Black-Litterman Model"
- Hubacek et al. (2019), "Exploiting Sports-Betting Market Using Machine Learning"
