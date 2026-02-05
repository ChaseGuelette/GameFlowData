# Monte Carlo Prediction Tuning Guide

This guide explains how to tune the `MonteCarloPredictor` to improve calibration and betting performance.

## Overview

The Monte Carlo predictor generates probability distributions by:
1. Sampling from the **minutes model** distribution
2. Sampling from the **rate model** distribution (pts/min, reb/min, etc.)
3. Combining them: `total_stat = minutes × rate`

Two common calibration issues arise:
- **Bias**: Systematic over/under-prediction (e.g., always predicting too high)
- **Variance underestimation**: Distribution too narrow (overconfident)

We provide mechanisms to fix these:

| Issue | Fix | Parameter |
|-------|-----|-----------|
| Bias from minutes-rate correlation | **Gaussian Copula** (recommended) | `copula_params` |
| Bias from minutes-rate correlation | Legacy Correlated Sampling (deprecated) | `correlation_config` |
| Variance underestimation | Variance Inflation | `variance_inflation` |

---

## Variance Inflation

### What it does

Variance inflation widens the prediction distribution around the median. This fixes **overconfidence** where the model thinks players are more consistent than they really are.

**Before inflation:**
```
Q10: 12 pts    Q50: 18 pts    Q90: 24 pts   (narrow)
```

**After 1.15x inflation:**
```
Q10: 11 pts    Q50: 18 pts    Q90: 25 pts   (wider)
```

### When to use it

Use variance inflation when:
- Calibration shows Q10 gap is **positive** (actual coverage > target)
- Calibration shows Q90 gap is **negative** (actual coverage < target)
- The stat has **low correlation** with minutes (like rebounds)

### Default configuration

```python
DEFAULT_VARIANCE_INFLATION = {
    "pts": 1.0,    # No inflation (handled by correlation adjustment)
    "reb": 1.15,   # 15% wider - rebounds are high-variance
    "ast": 1.0,    # No inflation (handled by correlation adjustment)
    "threes": 1.0, # No inflation
}
```

### How to customize

```python
from src.models.monte_carlo import MonteCarloPredictor

# Option 1: Override specific stats
predictor = MonteCarloPredictor(
    pipeline,
    variance_inflation={
        "pts": 1.0,
        "reb": 1.20,   # Increase to 20% for more conservative predictions
        "ast": 1.10,   # Add 10% inflation to assists
        "threes": 1.0,
    }
)

# Option 2: Disable all variance inflation
predictor = MonteCarloPredictor(
    pipeline,
    variance_inflation={"pts": 1.0, "reb": 1.0, "ast": 1.0, "threes": 1.0}
)
```

### Tuning guide

| Calibration Gap at Q90 | Recommended Inflation |
|------------------------|----------------------|
| -2% to -4% | 1.05 - 1.10 |
| -4% to -6% | 1.10 - 1.15 |
| -6% to -8% | 1.15 - 1.20 |
| > -8% | 1.20 - 1.25 |

---

## Gaussian Copula Sampling (Recommended)

### What it does

Gaussian copula sampling is the **recommended** approach for handling minutes-rate correlation. It replaces the legacy correlated sampling with a principled statistical method that preserves both marginal distributions exactly while inducing the correct rank dependency between minutes and per-minute rates.

**How it works:**
1. Draw shared `z_minutes` from standard normal for all stats
2. For each stat, generate correlated `z_rate` via Cholesky decomposition:
   `z_rate = ρ·z_minutes + √(1-ρ²)·z_independent`
3. Convert to uniforms via Φ(z) → [0,1]
4. Map uniforms through marginal inverse CDFs (quantile functions) to get actual samples
5. Combine: `total_stat = minutes × rate`

**Key advantage:** The marginal distributions (minutes and rate) are preserved exactly — the copula only controls how they co-move. The legacy approach applied multiplicative rate factors that distorted the rate distribution.

### When it's used

Copula sampling is **automatically enabled** when `copula_params.json` exists in the model artifacts directory. The training pipeline computes and saves this file automatically.

- `run_backtest.py`: Auto-loads from model dir
- `run_daily.py`: Auto-loads from model dir
- Falls back to legacy correlation adjustment when file is missing (backward compatible)

### Copula parameters from training

The training pipeline computes Spearman rank correlations from the training data:

```
=== Computing Gaussian Copula Parameters ===
  PTS: Spearman ρ = 0.3140    (strong positive — high-minute games → higher pts/min)
  REB: Spearman ρ = -0.0460   (negligible — rebounds independent of minutes)
  AST: Spearman ρ = 0.1760    (moderate positive — playmakers get more minutes)
  THREES: Spearman ρ = 0.1200  (mild positive)
```

These are converted to Pearson ρ via: `ρ_pearson = 2·sin(π·ρ_spearman / 6)`

### How to customize

```python
from src.models.monte_carlo import MonteCarloPredictor

# Copula is auto-loaded from artifacts, but you can override:
predictor = MonteCarloPredictor(
    pipeline,
    copula_params={
        "pts": 0.314,   # Spearman ρ
        "reb": -0.046,
        "ast": 0.176,
        "threes": 0.12,
    }
)

# Disable copula (falls back to legacy correlation adjustment)
predictor = MonteCarloPredictor(pipeline, copula_params=None)
```

### Computing copula parameters from data

```python
from src.models.monte_carlo import compute_copula_params_from_data

# Load your training data
df = feature_store.get_training_dataset(["22023", "22024"])

# Compute Spearman rank correlations
params = compute_copula_params_from_data(df)
# Returns: {"pts": 0.314, "reb": -0.046, "ast": 0.176, "threes": 0.12}
```

---

## Hurdle Model Sampling (Zero-Inflated Distributions)

### What it does

For stats with significant zero mass (like THREES, where ~35% of samples are exactly 0), the Monte Carlo predictor uses **hurdle model sampling** instead of standard quantile inverse CDF sampling.

**Two-stage sampling:**
1. **Bernoulli draw:** Is this sample zero or positive? Uses calibrated P(zero) from the classifier.
2. **Positive sampling:** For non-zero samples, map through the positive-only distribution inverse CDF.

**Key insight:** The Bernoulli zero/positive decision is **independent of the copula**. The copula correlation only affects the positive rate magnitude — a player who plays more minutes isn't necessarily more or less likely to score zero 3s (that's driven by role/position), but *given* they score at least one, higher minutes may correlate with higher rate.

### When it's used

Hurdle model sampling is **automatically enabled** when:
1. The model artifacts contain `threes_is_hurdle.json` flag file
2. The stat being predicted has a `HurdleQuantileModel` in the pipeline

The training pipeline automatically creates hurdle models for THREES (and potentially other zero-inflated stats).

### Artifacts

Hurdle model artifacts:
- `threes_zero_classifier.joblib` — XGBoost binary classifier for P(zero)
- `threes_zero_calibrator.joblib` — Isotonic regression calibrator for P(zero)
- `threes_rate_model.joblib` — Quantile models trained on positive samples only
- `threes_is_hurdle.json` — Flag file indicating hurdle architecture

### How it differs from standard sampling

| Aspect | Standard Quantile | Hurdle Model |
|--------|------------------|--------------|
| Zero handling | Continuous CDF (may not predict exact 0) | Explicit P(zero) + positive distribution |
| Quantile Q ≤ P(zero) | Returns small positive number | Returns exactly 0 |
| Copula integration | Correlated uniform for rate | Bernoulli independent; copula affects positive samples only |
| Calibration target | Q0.10 coverage ≈ 10% | Q0.10 = 0 when P(zero) > 10% |

### Example

```python
# Automatic detection — no configuration needed
predictor = MonteCarloPredictor(pipeline, copula_params=copula_params)

# For a player with P(zero) = 0.35:
# - Q0.10 = 0 (10% < 35%)
# - Q0.25 = 0 (25% < 35%)
# - Q0.50 = positive_model.predict(adjusted_q=0.23)  # (50-35)/(1-35) = 0.23

# MC sampling:
# - ~35% of samples are exactly 0
# - ~65% of samples come from positive distribution
# - Positive samples use copula-correlated uniforms
```

---

## Legacy Correlated Sampling (Deprecated)

> **Note:** This mechanism is superseded by Gaussian copula sampling. It remains as a fallback when `copula_params.json` is not available in the model artifacts.

### What it does

Correlated sampling adjusts rate predictions based on predicted minutes using hardcoded bucket-based multiplicative factors. This fixes **bias** caused by the fact that players who play more minutes tend to have higher per-minute rates (for PTS and AST).

**Example:**
- Player predicted to play 35 minutes → rate scaled UP by ~18%
- Player predicted to play 15 minutes → rate scaled DOWN by ~16%

### When to use it

Only used when copula params are unavailable. For new training runs, the copula approach is preferred.

### Default configuration

```python
DEFAULT_CORRELATION_CONFIG = {
    "pts": {
        "minutes_points": [12.5, 17.5, 22.5, 27.5, 32.5, 37.5, 42.5],
        "rate_factors": [0.75, 0.84, 0.94, 1.06, 1.18, 1.30, 1.29],
        "enabled": True,
    },
    "reb": {
        "minutes_points": [12.5, 17.5, 22.5, 27.5, 32.5, 37.5, 42.5],
        "rate_factors": [1.02, 1.01, 1.00, 0.99, 0.99, 0.98, 0.97],
        "enabled": True,
    },
    "ast": {
        "minutes_points": [12.5, 17.5, 22.5, 27.5, 32.5, 37.5, 42.5],
        "rate_factors": [0.82, 0.88, 0.93, 1.00, 1.08, 1.14, 1.12],
        "enabled": True,
    },
    # ...
}
```

### How to customize

```python
# Disable correlated sampling entirely
predictor = MonteCarloPredictor(
    pipeline,
    use_correlated_sampling=False
)

# Custom correlation config
predictor = MonteCarloPredictor(
    pipeline,
    correlation_config={
        "pts": {
            "minutes_points": [15, 25, 35],
            "rate_factors": [0.85, 1.0, 1.15],
            "enabled": True,
        },
        # ... other stats
    }
)
```

### Computing correlation factors from data

```python
from src.models.monte_carlo import compute_correlation_config_from_data

# Load your training data
df = feature_store.get_training_dataset(["22023", "22024"])

# Compute factors
config = compute_correlation_config_from_data(df)
print(config)

# Use in predictor
predictor = MonteCarloPredictor(pipeline, correlation_config=config)
```

---

## Edge Threshold

The edge threshold controls the **minimum perceived edge** required to place a bet.

### Where to configure

The edge threshold is set in the **backtest harness**, not the Monte Carlo predictor:

```python
# In src/backtesting/backtest_harness.py
@dataclass
class BacktestHarness:
    edge_threshold: float = 0.05  # Default: 5% minimum edge
```

### How to adjust

**Option 1: When running backtest programmatically**

```python
from src.backtesting.backtest_harness import BacktestHarness

harness = BacktestHarness(
    engine=engine,
    feature_store=feature_store,
    model_pipeline=pipeline,
    predictor=predictor,
    edge_threshold=0.08,  # Raise to 8% for more selective betting
    starting_bankroll=2500,
    kelly_fraction=0.125,
)
```

**Option 2: Modify the default**

Edit `src/backtesting/backtest_harness.py`:

```python
@dataclass
class BacktestHarness:
    edge_threshold: float = 0.08  # Changed from 0.05
```

### Recommended values

| Scenario | Edge Threshold | Effect |
|----------|---------------|--------|
| Aggressive | 0.05 (5%) | More bets, higher variance |
| Balanced | 0.07 (7%) | Moderate selectivity |
| Conservative | 0.10 (10%) | Fewer bets, higher quality |
| Very Selective | 0.15 (15%) | Only large dislocations |

### Why raise edge threshold?

If your model is overconfident (which ours was), it perceives edges that don't exist:
- Model says: "I have a 12% edge!" → Kelly says: "Bet 2.5%!"
- Reality: "You have a 2% edge." → Kelly says: "Bet 0.4%!"

By raising the edge threshold:
- You filter out false-positive "edges"
- Remaining bets have higher actual edge
- Kelly sizing becomes more appropriate

---

## Running Analysis

To diagnose calibration issues, use the analysis script:

```bash
python -m src.models.analyze_calibration_drift \
    --model-path src/models/artifacts/run_XXXXXXXX \
    --seasons 22024 22025 \
    --sample-size 2000 \
    --output analysis.json
```

This will show:
1. **Minutes-rate correlation** by stat
2. **Combined calibration** (actual vs target coverage for each quantile)
3. **Bias** (mean predicted vs mean actual)

---

## Tail Distribution Adjustment

### What it does

Tail adjustment extends the extrapolated tails of the distribution more aggressively. This helps capture **fat-tailed** distributions where extreme outcomes (very low or very high) happen more often than a normal distribution would predict.

### When to use it

- **Q10 over-coverage** (actual > 10%): Increase `lower_tail_multiplier` to push the floor lower
- **Q90 under-coverage** (actual < 90%): Increase `upper_tail_multiplier` to push the ceiling higher

### Default configuration

```python
DEFAULT_TAIL_ADJUSTMENT = {
    "lower_tail_multiplier": 1.3,  # Extend lower tail 30% more
    "upper_tail_multiplier": 1.0,  # Keep upper tail as-is
}
```

### How to customize

```python
predictor = MonteCarloPredictor(
    pipeline,
    tail_adjustment={
        "lower_tail_multiplier": 1.5,  # More aggressive lower tail
        "upper_tail_multiplier": 1.2,  # Also extend upper tail
    }
)
```

### Tuning guide

| Q10 Gap | lower_tail_multiplier |
|---------|----------------------|
| +3% to +5% | 1.2 |
| +5% to +8% | 1.3 - 1.4 |
| > +8% | 1.5+ |

---

## Blowout/Foul Factor

### What it does

The blowout/foul factor models **unexpected minutes reductions** that happen in real games:
- **Blowouts**: Team winning/losing by 20+, starters sit in 4th quarter
- **Foul trouble**: Player fouls out or sits with 5 fouls
- **Minor injuries**: Player tweaks something and doesn't return

This creates a **mixture distribution** where:
- ~92% of samples: Normal minutes distribution
- ~8% of samples: Minutes reduced by 35%

### Default configuration

```python
DEFAULT_BLOWOUT_CONFIG = {
    "enabled": True,
    "probability": 0.08,       # 8% of games have unexpected reduction
    "minutes_reduction": 0.35, # Reduce minutes by 35% in those games
}
```

### How to customize

```python
# More aggressive blowout modeling
predictor = MonteCarloPredictor(
    pipeline,
    blowout_config={
        "enabled": True,
        "probability": 0.12,       # 12% of games
        "minutes_reduction": 0.40, # 40% reduction
    }
)

# Disable blowout factor entirely
predictor = MonteCarloPredictor(
    pipeline,
    blowout_config={"enabled": False}
)
```

### Effect on calibration

The blowout factor primarily affects:
- **Lower quantiles (Q10, Q25)**: More low outcomes possible
- **Overall variance**: Slightly wider distribution
- **Mean prediction**: Slightly lower (accounting for bad games)

---

## Quick Reference

```python
from src.models.monte_carlo import MonteCarloPredictor, load_copula_params

# Recommended: Load copula params from training artifacts
copula_params = load_copula_params("src/models/artifacts/run_XXXXXXXX")

# Full configuration example
predictor = MonteCarloPredictor(
    model_pipeline=pipeline,
    n_samples=10000,                    # Monte Carlo samples
    random_state=42,                    # For reproducibility
    copula_params=copula_params,        # Gaussian copula (recommended, auto-computed by training)
    use_correlated_sampling=True,       # Legacy correlation fallback (ignored when copula is set)
    correlation_config=None,            # Legacy config (ignored when copula is set)
    variance_inflation={                # Custom variance inflation
        "pts": 1.0,
        "reb": 1.15,
        "ast": 1.05,
        "threes": 1.0,
    },
    tail_adjustment={                   # Tail extension multipliers
        "lower_tail_multiplier": 1.3,
        "upper_tail_multiplier": 1.0,
    },
    blowout_config={                    # Unexpected minutes reduction
        "enabled": True,
        "probability": 0.08,
        "minutes_reduction": 0.35,
    },
)
```

---

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---------|--------------|-----|
| Q90 gap negative (e.g., -6%) | Variance too narrow | Increase `variance_inflation` or `upper_tail_multiplier` |
| Q10 gap positive (e.g., +8%) | Lower tail too short | Increase `lower_tail_multiplier` or enable `blowout_config` |
| Mean predicted < Mean actual | Missing minutes-rate correlation | Enable copula (`copula_params`) or legacy `use_correlated_sampling` |
| Mean predicted > Mean actual | Over-adjusted correlation | Check copula ρ values, or reduce legacy `rate_factors` |
| High hit rate but negative ROI | Overconfident edges | Raise `edge_threshold` |
| Low outcomes happening unexpectedly | Not modeling blowouts/fouls | Enable `blowout_config` with higher `probability` |
| REB specifically has issues | REB has no correlation to leverage | Use `variance_inflation` for REB (default: 1.15) |
