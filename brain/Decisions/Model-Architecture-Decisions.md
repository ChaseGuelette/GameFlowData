# Model Architecture Decisions

> Part of [[Decisions]]

## Decision Log

### 1. XGBoost Quantile Regression (over ordinal classifiers)
Gives full probability distribution (Q10-Q90) needed for MC simulation. Handles heteroskedastic NBA stats naturally.

### 2. Minutes x Rate Decomposition (not direct stat prediction)
Variance is driven by playing time. Separate modeling handles blowouts, OT, injury exits. Copula preserves correlation. **Caveat**: Can create fake edges for variable-minutes players — mitigated by Q50 vs L5 sanity check (Decision #14).

### 3. Gaussian Copula for Minutes-Rate Correlation
Independent sampling produced correlated errors. Copula (PTS rho=0.314, AST rho=0.176) preserves marginals while inducing correct rank dependency.

### 4. Empirical CDF (not Gaussian CDF)
MC distributions are non-Gaussian (skewed, zero-inflated). Gaussian CDF produces phantom edges at tails.

### 6. Per-100 Possessions for Opponent Defense
Per-36 ignores game pace. A 110 poss/game team has fundamentally different distributions than 95.

### 7. COVID Seasons Excluded
Bubble/shortened seasons don't represent normal NBA. Training on 22023+22024+22025 (3 full seasons).

### 8. Black-Litterman in Log-Odds Space
Additive blending in probability space can produce impossible values. Linear ramp confidence (not exponential — exponential crushed weights, producing 0-12 bets).

### 9. Sharpest-Book Line Selection (lowest vig)
Ensures we beat the sharpest available line, not just the worst. Over/under sides evaluated independently.

### 10. Combo Stats Derived, Not Modeled
PRA = pts_samples + reb_samples + ast_samples element-wise. Correlations preserved via shared copula minutes draws. No separate model or storage needed.

### 11. THREES Model Archived
50% missing lines, 2 bets out of 78 in backtest. Scrapers still collect data.

### 12. DFS Market Edge Without Model
Compares DFS lines against devigged sportsbook consensus — works for all 6 stats including those the model doesn't predict.

### 13. `player_starter_prob` Rejected for Production
A/B backtest: ROI -3.19pp, AST -6.24pp. Smooths calibration but reduces edge-finding.

### 14. Q50 vs L5 Sanity Check
Prevents minutes x rate decomposition from creating fake under edges on variable-minutes players. 30% divergence threshold. Reduced PTS PnL loss from -$15K to -$386.

### 15. 5-Minute Refresh Cadence
Fuzzy cache reduced linker from 15s to <1s, enabling ~156 scrape+refresh cycles/day.

### 16. NLL-Based Feature Selection for NegBin Models
Per-quantile pinball loss is the wrong metric for a distributional model that outputs (mu, alpha), not quantile values. NLL-based selection trains a Poisson proxy, scores by NB NLL contribution. Produces a single feature set (not 5 per-quantile sets unioned) and runs faster (~2 min vs ~4 min).

### 17. PMF-Based Calibration for NegBin (not MC sampling)
Quantile coverage metrics are misleading for discrete NegBin distributions (e.g., +33% Q10 gap is structural when 40% of outcomes are 0). Direct PMF/CDF gives meaningful metrics: NLL, bias, zero fraction, per-prop-line P(over) accuracy. Vectorized scipy runs in ~5 sec vs ~40 min of MC sampling.

### 18. Optuna NB NLL Tuner for NegBin Hyperparameters
The quantile tuner (optimizes calibration gap) doesn't apply to distributional models. The NegBin tuner trains the full 2-output XGBoostLSS per trial, optimizes validation NB NLL. Reuses pre-computed DMatrices and global MLE across trials for speed. Enabled via `--tune` flag.

#decisions #model #architecture
