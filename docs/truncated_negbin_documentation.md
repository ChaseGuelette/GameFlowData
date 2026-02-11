# Truncated Negative Binomial Model Documentation

> **ARCHIVED (2026-02-10):** This model (C4 architecture) was archived along with all THREES-related code to `archive/threes_model/` due to poor market coverage (50% missing lines) and insufficient betting volume (2 bets out of 78 in backtesting). Scrapers still collect `player_threes` market data for future optionality. This documentation is preserved for reference if the model is restored.

## Overview

The `TruncatedNegBinModel` class in `src/models/truncated_negbin.py` implements a two-stage XGBoost-based model for predicting the parameters of a zero-truncated negative binomial distribution. This model is designed for discrete count outcomes like made three-pointers where:

1. Values are non-negative integers (0, 1, 2, 3, ...)
2. There is overdispersion (variance > mean)
3. There is a significant zero mass handled separately

## Why Truncated Negative Binomial?

### The Problem with Quantile Regression

The previous C3 hurdle model used quantile regression on positive samples, which failed because:

1. **Discrete vs Continuous**: Quantile regression produces continuous values (e.g., Q10=1.2), but made threes are integers
2. **Coarse Interpolation**: 5 quantile points over 6-7 meaningful values creates crude approximation
3. **Boundary Extrapolation**: With p_zero~0.47, Q10 maps to 5.7th percentile of positive distribution, requiring extrapolation

### Why Negative Binomial?

- **Overdispersion**: Made threes have variance ~2.8 vs mean ~2.1. Poisson assumes variance = mean.
- **Integer Outputs**: NegBin is a discrete distribution — samples are always integers.
- **Truncation**: We condition on positive outcomes (handled by zero classifier separately).

## Architecture

### Two-Stage Model

```
Stage 1: Zero Classifier (XGBoost + Isotonic)
  └── P(threes = 0 | features)

Stage 2: Count Model (TruncatedNegBinModel)
  └── For positive samples: predict μ (mean) and α (overdispersion)
  └── Sample integers from truncated NegBin(μ, α)
```

### Parameterization

The model uses **mu-alpha parameterization**:

- `μ` (mu): Mean of the **untruncated** distribution
- `α` (alpha): Overdispersion parameter

Relationship to scipy's (n, p) notation:
- `n = 1 / α` (number of successes)
- `p = 1 / (1 + α × μ)` (success probability)

Variance = μ + α × μ² (always > mean when α > 0)

### Truncation Adjustment (Critical Fix - 2026-02-09)

The mu model must be trained to predict the **untruncated** mean, not observed values. Observed values come from the truncated distribution (conditioned on X > 0):

```
E[X | X > 0] = μ / (1 - P(X=0))
```

Since observed values are inflated by the factor `1 / (1 - P(X=0))`, training targets must be scaled down:

```python
# Compute truncation factor from global MLE
p_zero_global = nbinom.pmf(0, 1/alpha, n/(n+mu))  # ~0.26 for THREES
truncation_factor = 1.0 - p_zero_global

# Correct training target
log_mu_target = np.log((y + 0.5) * truncation_factor)
```

Without this adjustment, the XGBoost model predicts μ≈2.5 instead of the correct μ≈1.66, causing severe over-coverage (~25% gap at Q10).

## Usage

### Training

```python
from src.models.truncated_negbin import TruncatedNegBinModel, TruncatedNegBinConfig

# Create model with optional config
config = TruncatedNegBinConfig(
    mu_n_estimators=1000,
    mu_max_depth=5,
    alpha_n_estimators=500,
)
model = TruncatedNegBinModel(config)

# Fit on positive samples only (y >= 1)
X = df_positive[feature_columns]
y = df_positive['fg3m']  # integers >= 1

result = model.fit(X, y)
# Returns: {'global_mu': ..., 'global_alpha': ..., 'n_samples': ...}
```

### Prediction

```python
# Predict parameters
mu, alpha = model.predict_params(X_new)

# Sample from the distribution
samples = model.sample(X_new, n_samples=10000)  # shape: (n_rows, n_samples)

# Convenience method for single row
features_dict = {'player_avg_fg3a_l5': 5.2, 'player_avg_min_l5': 32.0, ...}
samples = model.sample_single(features_dict, n_samples=1000)
```

### Persistence

```python
from pathlib import Path

# Save
model.save(Path("models/artifacts"))

# Load
loaded = TruncatedNegBinModel.load(Path("models/artifacts"))

# Check existence
if TruncatedNegBinModel.exists(Path("models/artifacts")):
    model = TruncatedNegBinModel.load(path)
```

## Configuration

```python
@dataclass
class TruncatedNegBinConfig:
    # Mu model hyperparameters
    mu_n_estimators: int = 1000
    mu_max_depth: int = 5
    mu_learning_rate: float = 0.03
    mu_early_stopping: int = 50

    # Alpha model hyperparameters
    alpha_n_estimators: int = 500
    alpha_max_depth: int = 4
    alpha_learning_rate: float = 0.05
    alpha_early_stopping: int = 30

    # Clamping bounds for log transforms
    log_mu_min: float = -2.0   # exp(-2) ≈ 0.14
    log_mu_max: float = 3.0    # exp(3) ≈ 20
    log_alpha_min: float = -2.0
    log_alpha_max: float = 2.0  # exp(2) ≈ 7.4
```

## Sampling Strategy

The model uses **inverse CDF sampling** (not rejection sampling):

```python
def _truncated_negbin_invcdf(self, u, mu, alpha):
    """Map uniform u to truncated NegBin."""
    n = 1.0 / alpha
    p = n / (n + mu)

    # P(X=0) under full NegBin
    p_zero = nbinom.pmf(0, n, p)

    # Adjust uniform to skip zero mass
    adjusted_u = u * (1 - p_zero) + p_zero

    # Inverse CDF
    samples = nbinom.ppf(adjusted_u, n, p)

    return np.maximum(1, samples.astype(int))
```

This is more efficient than rejection sampling and guarantees all samples are >= 1.

## Integration with Monte Carlo

The `MonteCarloPredictor` in `monte_carlo.py` uses the count model via:

```python
def _sample_threes_count(self, features: dict) -> np.ndarray:
    """Sample THREES using the count model (C4 architecture)."""
    # Step 1: Get calibrated P(zero)
    p_zero = self._get_calibrated_p_zero(features)

    # Step 2: Bernoulli draw - which samples are zero?
    is_zero = self.rng.random(self.n_samples) < p_zero

    # Step 3: Initialize result array
    samples = np.zeros(self.n_samples, dtype=int)

    # Step 4: Sample positive values from truncated NegBin
    if (~is_zero).sum() > 0:
        positive_samples = self.count_model.sample(
            X, n_samples=(~is_zero).sum()
        )
        samples[~is_zero] = positive_samples

    return samples  # integers: 0, 1, 2, 3, ...
```

Note: THREES is **not included in the copula** because the count model features already encode minutes context (player_avg_min_l5, etc.). Including it in the copula would double-count the minutes correlation.

## Artifacts

When saved, the model creates:

| File | Description |
|------|-------------|
| `truncated_negbin_meta.json` | Global mu/alpha, feature names |
| `truncated_negbin_mu_model.joblib` | XGBoost regressor for log(μ) |
| `truncated_negbin_alpha_model.joblib` | XGBoost regressor for log(α) |

The training pipeline also saves:

| File | Description |
|------|-------------|
| `threes_zero_classifier.joblib` | XGBoost binary classifier |
| `threes_zero_calibrator.joblib` | Isotonic regression for P(zero) |
| `threes_zero_feature_names.joblib` | Feature names for zero classifier |
| `threes_is_hurdle.json` | Flag with `model_type: "count"` |

## Validation

Phase 0 validation script (`scripts/validate_threes_negbin.py`) confirms the truncated NegBin fits the data:

```
VALIDATION SUMMARY
==================================================
All Positive Samples           | p=0.0234 | WMAPE=4.2%  | PASS
High Volume (4+ 3PA avg)       | p=0.0456 | WMAPE=3.8%  | PASS
Moderate Volume (2-4 3PA avg)  | p=0.0123 | WMAPE=4.5%  | PASS
Low Volume (0-2 3PA avg)       | p=0.0089 | WMAPE=4.9%  | PASS
==================================================
OVERALL: PASS - Truncated NegBin is a good fit across all segments
```

Chi-squared p-values may be low due to large sample sizes, but WMAPE < 5% indicates practical acceptability.

## Testing

17 unit tests in `tests/test_truncated_negbin.py`:

- Model fitting convergence
- Parameter prediction ranges (mu > 0, alpha > 0)
- Integer sampling (all samples >= 1)
- Batch sampling shapes
- Distribution statistics (reasonable mean/variance)
- Save/load roundtrip
- Edge cases (zero values rejected, unfitted model raises)

Run tests:
```bash
python -m pytest tests/test_truncated_negbin.py -v
```
