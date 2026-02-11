# C4: THREES Count Model Rebuild

## Executive Summary

The current hurdle model for three-pointers is fundamentally broken: quantile regression on a discrete, zero-inflated distribution produces continuous interpolated values that can't properly represent a variable with ~47% zeros and a range of {0, 1, 2, 3, 4, 5, 6, 7, 8}. This plan replaces the positive-distribution quantile regression (Step 2 of the hurdle) with a **conditional count model** that respects the discrete nature of made threes, while preserving the existing zero classifier, feature store, Monte Carlo sampling framework, and backtest infrastructure.

**Architecture change: Hurdle + Quantile Regression → Hurdle + Truncated Negative Binomial**

Everything else stays the same.

---

## Why the Current Model Fails (Root Cause)

The hurdle model concept is correct — splitting P(zero) from the positive distribution is the right structure for 47% zero-inflation. The failure is specifically in **how the positive distribution is modeled**.

### Problem 1: Quantile regression produces continuous values for a discrete outcome

The positive model predicts Q10=1.2, Q25=1.8, Q50=2.6, etc. Made threes can only be integers. When the MC sampler inverse-transforms through this continuous CDF, it generates fractional values (2.37 threes) that need rounding, introducing systematic bias.

### Problem 2: Interpolation between 5 quantile points is too coarse

With only 5 anchor points (Q10, Q25, Q50, Q75, Q90) over a distribution with 6-7 meaningful values {1, 2, 3, 4, 5, 6, 7}, the interpolated CDF is a crude approximation. The adjusted quantile mapping `(q - p_zero) / (1 - p_zero)` then interpolates between these already-approximate points, compounding error.

### Problem 3: The combining math breaks at the boundaries

When `p_zero ≈ 0.47` and you want Q10, the formula returns 0 — which is correct. But for Q50, it computes `adjusted_q = (0.50 - 0.47) / (1 - 0.47) = 0.057`, asking for the 5.7th percentile of the positive distribution. The positive model was trained on Q10 as its lowest quantile, so this requires extrapolation below the training range. Garbage in, garbage out.

---

## Proposed Architecture

```
                    THREES MODEL v2
                          │
           ┌──────────────┴──────────────┐
           ▼                             ▼
   STEP 1: Zero Classifier       STEP 2: Count Model (NEW)
   (KEEP — improve calibration)  (REPLACE quantile regression)
           │                             │
           ▼                             ▼
      p_zero = 0.35               μ = 2.4, α = 1.8
      (calibrated via              (Truncated NegBin parameters,
       isotonic regression)         conditional on threes > 0)
           │                             │
           └──────────────┬──────────────┘
                          ▼
                  MC SAMPLING (10,000 draws)
                          │
              ┌───────────┴───────────┐
              │  For each sample:     │
              │  1. Bernoulli(p_zero) │
              │     → 0 or positive?  │
              │  2. If positive:      │
              │     NegBin(μ, α) + 1  │
              │     (shifted, min=1)  │
              └───────────────────────┘
                          │
                          ▼
              Integer samples: [0, 0, 2, 0, 3, 1, 0, 4, ...]
              P(over 2.5) = (samples > 2.5).mean()
```

### Why Truncated Negative Binomial?

| Distribution | Pros | Cons |
|---|---|---|
| **Poisson** | Simple (1 param) | Variance = mean constraint. Threes are overdispersed. |
| **Negative Binomial** | Handles overdispersion (2 params) | Best fit for count data with extra variance |
| **Multinomial** | Exact discrete probs | Collapses context — different players with same P(k) have different mechanics |
| **Zero-Truncated Poisson** | Simple | Still can't handle overdispersion |
| **Zero-Truncated NegBin** | Overdispersion + truncation at 0 | Slightly more complex fitting |

The positive samples (threes > 0) have mean ≈ 2.1 and variance ≈ 2.8 (overdispersed relative to Poisson where var = mean). Negative Binomial handles this naturally with its dispersion parameter α.

---

## Implementation Plan

### Phase 0: Validate Assumptions (1 day)

**Goal:** Confirm the truncated negative binomial is a good fit before building anything.

**Tasks:**

1. Pull positive-only threes distribution from the database

```python
SELECT fg3m FROM player_game_stats
WHERE fg3m > 0 AND min >= 10
  AND season_id IN ('22022', '22023', '22024')
```

2. Fit truncated NegBin and compare to empirical distribution

```python
from scipy.stats import nbinom
from scipy.optimize import minimize

def truncated_negbin_nll(params, data):
    """Negative log-likelihood for zero-truncated negative binomial."""
    n, p = params  # n = num successes, p = success prob
    if n <= 0 or p <= 0 or p >= 1:
        return 1e10
    p_zero = nbinom.pmf(0, n, p)
    log_probs = nbinom.logpmf(data, n, p) - np.log(1 - p_zero)
    return -np.sum(log_probs)

result = minimize(truncated_negbin_nll, x0=[2.0, 0.5], args=(positive_threes,),
                  method='Nelder-Mead')
```

3. Chi-squared goodness-of-fit test — compare predicted vs observed frequencies for k=1,2,...,8+

4. Segment by player archetype — verify the distribution family holds for:
   - High-volume shooters (avg 4+ 3PA)
   - Moderate shooters (2-3 3PA)
   - Low-volume shooters (0-1 3PA)

**Exit criteria:** Truncated NegBin fits with p > 0.05 on chi-squared test across segments. If it fails, fall back to empirical discrete distribution.

---

### Phase 1: Build the Count Model (3-4 days)

**Goal:** Replace `threes_rate_model.joblib` (quantile regression) with a model that predicts NegBin parameters.

#### 1a. Model Class: `TruncatedNegBinModel`

**File:** `src/models/truncated_negbin.py`

```python
class TruncatedNegBinModel:
    """Predicts parameters of a zero-truncated negative binomial distribution.

    Architecture:
    - XGBoost regressor for mu (mean of the full NegBin, before truncation)
    - XGBoost regressor for log_alpha (dispersion parameter)
    - Both trained via custom loss that maximizes truncated NegBin likelihood
    """

    def __init__(self, xgb_params=None):
        self.mu_model = None       # XGBoost predicting log(mu)
        self.alpha_model = None    # XGBoost predicting log(alpha)

    def fit(self, X, y, sample_weight=None):
        """Two-stage fitting on positive-only samples (y >= 1)."""
        pass

    def predict_params(self, X):
        """Returns (mu, alpha) arrays for each sample."""
        pass

    def sample(self, X, n_samples=1):
        """Draw n_samples from the predicted truncated NegBin for each row."""
        pass

    def save(self, path): pass
    def load(self, path): pass
```

**Design decision:** Train two separate XGBoost models (mu and alpha) rather than a single model with custom multi-output loss. Simpler to debug and iterate.

#### 1b. Training Data Preparation

```python
# In train_pipeline.py:
threes_mask_positive = df_train['fg3m'] > 0
df_threes_positive = df_train[threes_mask_positive].copy()

# Count model target (fg3m directly, NOT fg3m_per_min)
y_threes_count = df_threes_positive['fg3m'].values  # integers >= 1
```

**Critical change:** The new count model predicts `fg3m` directly (a count), not `threes_per_min` (a rate). The Negative Binomial is a count distribution.

#### 1c. Features

Use existing `RATE_FEATURES_THREES` from `feature_store.py`, plus:

- `player_avg_min_l5` — minutes expectation affects count
- `has_prop_line_threes` — binary flag for when no line available (fixes COALESCE-to-0 issue)

```python
# In feature_store.py, add:
'has_prop_line_threes',  # NEW: 1 if prop_line_threes > 0, else 0
```

---

### Phase 2: Integrate with Monte Carlo Sampler (2 days)

**Goal:** Make `MonteCarloPredictor` use the new count model for threes.

#### 2a. Modify `MonteCarloPredictor.predict()`

```python
def predict(self, features, stats=['pts', 'reb', 'ast', 'threes']):
    samples = {}

    # --- Standard path for PTS, REB, AST (unchanged) ---
    minutes_samples = self._sample_minutes(features)
    for stat in ['pts', 'reb', 'ast']:
        if stat in stats:
            rate_samples = self._sample_rate_copula(features, stat, minutes_samples)
            samples[stat] = minutes_samples * rate_samples

    # --- New path for THREES ---
    if 'threes' in stats:
        samples['threes'] = self._sample_threes_count(features)

    return samples
```

#### 2b. New Sampling Method: `_sample_threes_count()`

```python
def _sample_threes_count(self, features):
    """Hurdle model sampling with count distribution."""
    n = self.n_samples

    # Step 1: Get P(zero) from classifier
    p_zero = self.pipeline.threes_zero_classifier.predict_proba(features)

    # Step 2: Bernoulli — which samples are zero?
    is_zero = self.rng.random(n) < p_zero

    # Step 3: For positive samples, draw from truncated NegBin
    mu, alpha = self.pipeline.threes_count_model.predict_params(features)
    nb_n = 1.0 / alpha
    nb_p = 1.0 / (1.0 + alpha * mu)
    positive_samples = self._sample_truncated_negbin(nb_n, nb_p, count=n)

    # Combine
    result = np.where(is_zero, 0, positive_samples)
    return result.astype(int)
```

#### 2c. Copula Interaction

**Decision:** Remove threes from the copula entirely. The count model already receives minutes-related features.

```json
// copula_params.json
{
    "pts": 0.314,
    "reb": -0.046,
    "ast": 0.176
    // threes removed
}
```

---

### Phase 3: Improve Zero Classifier Calibration (1 day)

**Goal:** Ensure p_zero predictions are accurate.

#### 3a. Diagnose Current Calibration

```python
from sklearn.calibration import calibration_curve

y_true = (df_holdout['fg3m'] == 0).astype(int)
y_calibrated = isotonic_calibrator.predict(y_pred)

fraction_of_positives, mean_predicted = calibration_curve(
    y_true, y_calibrated, n_bins=10, strategy='quantile'
)
# Plot: should lie on diagonal
```

#### 3b. Potential Fixes

| Issue | Fix |
|---|---|
| Isotonic calibration didn't have enough data | Use full calibration season with ≥5000 samples |
| Calibration on training data | Ensure strict temporal split |
| Feature leakage | Audit features for fg3m-derived leakage |

#### 3c. Alternative: Platt Scaling

If isotonic is noisy:

```python
from sklearn.calibration import CalibratedClassifierCV
calibrated_clf = CalibratedClassifierCV(zero_classifier, method='sigmoid', cv='prefit')
```

---

### Phase 4: Training Pipeline Integration (1-2 days)

**Goal:** Wire new model into `train_pipeline.py`.

#### 4a. New Artifacts

```
src/models/artifacts/run_YYYYMMDD_HHMMSS/
├── minutes_model.joblib          (unchanged)
├── pts_rate_model.joblib         (unchanged)
├── reb_rate_model.joblib         (unchanged)
├── ast_rate_model.joblib         (unchanged)
├── threes_count_model.joblib     (NEW — replaces threes_rate_model.joblib)
├── threes_zero_classifier.joblib (unchanged)
├── threes_zero_calibrator.joblib (unchanged)
├── threes_is_hurdle.json         (add model_type: "count")
├── copula_params.json            (threes removed)
└── calibration_report.json       (updated with count model metrics)
```

#### 4b. Backward Compatibility

```python
def load_all(self, model_dir):
    hurdle_config = json.load(open('threes_is_hurdle.json'))
    if hurdle_config.get('model_type') == 'count':
        self.threes_model = TruncatedNegBinModel.load('threes_count_model.joblib')
        self.threes_model_type = 'count'
    else:
        # Legacy hurdle + quantile
        self.threes_model_type = 'hurdle_quantile'
```

---

### Phase 5: Calibration Validation & Backtesting (2 days)

**Goal:** Verify new model meets calibration targets and produces positive ROI.

#### 5a. Calibration Targets

| Quantile | Target | Acceptable Gap | Fail |
|---|---|---|---|
| Q10 | 10% | ±5% | >10% |
| Q25 | 25% | ±5% | >10% |
| Q50 | 50% | ±3% | >5% |
| Q75 | 75% | ±5% | >10% |
| Q90 | 90% | ±5% | >10% |

#### 5b. Backtest

```bash
# With threes
python src/backtesting/run_backtest.py \
    --stats pts reb ast threes --edge-threshold 0.05

# Without threes (baseline)
python src/backtesting/run_backtest.py \
    --stats pts reb ast --edge-threshold 0.05
```

#### 5c. Success Criteria

| Metric | Minimum | Target |
|---|---|---|
| Threes calibration Q50 gap | < 5% | < 3% |
| Threes ROI (isolated) | > 0% | > 3% |
| Combined ROI | ≥ baseline | Higher |
| No regression on PTS/REB/AST | Within 1% | Equal or better |

---

### Phase 6: Production Deployment (1 day)

No changes needed to:
- `daily_predictions` table schema
- `daily_prediction_samples` table
- `query_player.py` CLI tool
- `bet_simulator.py`

Update documentation:
- `monte_carlo_tuning.md`
- `model_pipeline_runbook.md`

---

## Timeline Summary

| Phase | Duration | Dependencies |
|---|---|---|
| Phase 0: Validate assumptions | 1 day | None |
| Phase 1: Build count model | 3-4 days | Phase 0 passes |
| Phase 2: MC sampler integration | 2 days | Phase 1 |
| Phase 3: Zero classifier calibration | 1 day | Parallel with Phase 1-2 |
| Phase 4: Training pipeline integration | 1-2 days | Phases 1-3 |
| Phase 5: Calibration & backtesting | 2 days | Phase 4 |
| Phase 6: Production deployment | 1 day | Phase 5 passes |
| **Total** | **~10-12 days** | |

---

## Risk Assessment

### High Risk

**Truncated NegBin doesn't fit the data well.**
- Mitigation: Phase 0 validates before code investment
- Fallback: empirical discrete distribution (multinomial over {1,2,...,8})

**Zero classifier remains poorly calibrated.**
- Mitigation: Phase 3 addresses independently
- Fallback: player-cluster-specific priors

### Medium Risk

**Count model doesn't capture minutes interaction well enough.**
- Since predicting fg3m directly (not rate × minutes), model needs to learn minutes → threes through features
- If `player_avg_min_l5` and `line_spread` aren't sufficient, may need predicted minutes as feature

### Low Risk

**MC sampling performance.** Rejection sampling has >70% acceptance rate. 10,000 draws in <10ms.

**Backward compatibility.** Model type detection handles graceful fallback.

---

## What NOT to Do

1. **Don't predict fg3a × fg3_pct** — fg3_pct is too noisy on per-game basis
2. **Don't add more quantile points** — doesn't fix discrete data problem
3. **Don't train on full distribution with single model** — XGBoost quantile regression struggles with point mass at zero
4. **Don't block paper trading on PTS/REB/AST waiting for threes** — ship what works, iterate on what doesn't
