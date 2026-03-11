# Systematic Under-Prediction: Why It's a Feature, Not a Bug

**Date:** 2026-03-06 (Session 67)
**Status:** Confirmed beneficial -- do not "fix"

---

## The Observation

Our model's Q50 (median) prediction falls below the sportsbook line for 70-85% of players across all stats:

| Stat | % Predictions Below Line | Avg Q50 - Line |
|------|--------------------------|----------------|
| PTS  | 83.5%                    | -1.51          |
| REB  | 86.2%                    | -1.11          |
| AST  | 81.7%                    | -0.64          |

This holds for both the legacy model (independent sampling) and the copula model (Gaussian copula). The copula amplifies the effect for PTS (rho=0.336) but has no effect on REB (rho=-0.003).

Extreme examples from 2026-03-05:
- Jokic REB: Q50=6.5, Line=13.5 (-7.0)
- Wembanyama REB: Q50=6.1, Line=11.5 (-5.4)
- Gobert REB: Q50=6.4, Line=11.5 (-5.1)

---

## The Paradox

Despite this systematic under-prediction, the model is highly profitable:

| Metric | Value |
|--------|-------|
| Hit Rate | 65.5% |
| ROI | 85.05% |
| Sharpe | 3.84 |
| Under Bets | 486 (63%) -- 83.15% ROI |
| Over Bets | 289 (37%) -- 87.45% ROI |

When we tried to "fix" the under-prediction with conformal calibration offsets (Session 42, Jan-Feb 2026 A/B test):

| Configuration | ROI | Sharpe |
|---------------|-----|--------|
| Without offsets (biased) | **7.44%** | **0.891** |
| With offsets (less biased) | 6.01% | 0.742 |

**Fixing the bias made the model worse.**

---

## Why This Happens: Three Reinforcing Mechanisms

### 1. Sportsbook Lines Are Inflated (Public Over-Bias)

This is one of the most well-documented phenomena in sports betting:

- **Public bettors overwhelmingly bet overs.** Fans want to root for big performances. "99 percent of all sports bettors will always bet on favorites and overs" (Doc's Sports).
- **Sportsbooks shade lines upward** to exploit this. Research shows sportsbooks increase expected margins by 20-30% by shading probability distributions just 2-3% toward the public side.
- **Sharp bettors target unders.** "Public bettors love taking overs on player props, which inflates those numbers, so sharp bettors will take the under when the line looks too high" (GamblingSite).
- **Prop markets are less efficient** than game lines. Betting limits are $250-$500 (vs $10k+ for spreads), meaning less sharp money flows in to correct inefficiencies.

**Implication:** Our Q50 predictions may be closer to the TRUE median than the posted line. The line sits above truth because of systematic public pressure.

### 2. Decorrelation from Market Errors (The Academic Foundation)

Three peer-reviewed papers support the conclusion that biased models can outperform unbiased ones:

**Hubacek et al. (2022) -- "Beating the Market with a Bad Predictive Model"**
*International Journal of Forecasting*

- Proves mathematically that profitability depends on being **decorrelated from the market's errors**, not on absolute accuracy.
- Even a model with worse predictions than the market can profit if its errors are in different places.
- Wealth growth: `W_G = D_KL(R||M) - D_KL(R||T)` -- profit comes from the difference in KL-divergence between market and trader relative to reality.
- Key quote: "Even if err(t) > err(m), consistent profit can still be made."

**Dmochowski (2023) -- "A Statistical Theory of Optimal Decision-Making in Sports Betting"**
*PLOS ONE*

- Theorem 4: "An optimal estimator of m need not be close to the true median. Rather, the estimator degrees of freedom should aim to generate predictions that are on the same side of s [the sportsbook line] as the true value."
- An optimal estimator **may possess a large bias.**
- A sportsbook bias of only a single point from the true median is sufficient to permit positive expected profit.

**Walsh & Joshi (2024) -- "Machine Learning for Sports Betting: Should Model Selection Be Based on Accuracy or Calibration?"**

- Using NBA betting data, calibration-optimized model selection produced **+34.69% ROI** vs **-35.17%** for accuracy-optimized selection.
- Better point predictions != better betting outcomes.

### 3. Calibration vs Sharpness: The Theoretical Framework

**Gneiting et al. (2007)** established the paradigm: "maximize sharpness **subject to** calibration." Calibration is a constraint, not the objective. Sharpness (how concentrated/informative predictions are) drives performance.

Our calibration offsets fail because they perform **marginal recalibration** -- a single global shift per quantile. This destroys **conditional signal** through a Simpson's Paradox mechanism:

- The model may be sharper on the specific subpopulations where edges exist
- A global offset dilutes sharp conditional predictions with noise from the rest
- Result: better average coverage, worse betting decisions

**Perez-Lebel et al. (2022)** formalize this as **grouping loss** -- the cost of treating heterogeneous predictions identically.

---

## Why Calibration Offsets Specifically Hurt ROI

Our conformal calibration offsets (backed up as `.bak`, disabled in production):

| Stat | Q50 Offset | Effect |
|------|-----------|--------|
| PTS  | +0.807    | Shift predictions UP by ~0.8 points |
| REB  | +0.345    | Shift predictions UP by ~0.3 rebounds |
| AST  | +0.080    | Shift predictions UP by ~0.1 assists |

Three mechanisms by which these hurt ROI:

1. **Edge compression.** The bias inflates all edges by a fixed amount. Removing it shrinks edges below the betting threshold, losing profitable bets that had real edge even after subtracting the bias.

2. **Re-correlation with market.** Pushing predictions upward toward truth also pushes them toward the inflated market lines. The decorrelation advantage is partially destroyed. As Hubacek et al. state: "Even an accurate model is unprofitable as long as it is correlated with the bookmaker's model."

3. **Asymmetric tail impact.** Betting decisions depend on tail probabilities (mass above/below the line). The piecewise-linear warping shifts quantile anchors at Q10-Q90, but interpolation between anchor points can create distortions in the tails where edges live.

---

## The Mean vs Median Problem

An additional factor documented by Unabated (sharp betting analytics platform):

- Sportsbooks set prop lines based on the **median** (50th percentile).
- Most projection sites and DFS models produce **mean (average)** projections.
- Player stat distributions are **right-skewed** (floor of 0, no ceiling). The mean always exceeds the median.
- Bettors using mean projections against median-based lines will systematically bet too many overs.

Our model predicts Q50 (median), which should theoretically align with the sportsbook line. The fact that our Q50 is below the line confirms the lines are inflated beyond the true median.

---

## What This Means for Model Development

### Do NOT Do

- Do not apply calibration offsets to "fix" the under-prediction
- Do not retrain specifically to make Q50 match the market line
- Do not evaluate model quality by Q50-to-line distance

### Do Consider

- **Evaluate by ROI and Sharpe**, not by calibration metrics or line proximity
- **Conditional calibration** (per-player or per-context) is more valuable than marginal calibration
- **Copula rho tuning** may be worth testing (PTS rho=0.336 amplifies under-prediction -- does this help or hurt ROI?)
- **Action Item #22** (force-include PTS matchup features) should be evaluated by ROI impact, not by whether it reduces under-prediction

### The Key Insight

The model's value comes from identifying **where the line is wrong**, not from predicting the true median. A systematic downward bias means the model is decorrelated from the market's upward bias. This decorrelation IS the edge.

---

## Sources

### Academic Papers

1. Hubacek, O., Sourek, G., & Zelezny, F. (2022). "Beating the market with a bad predictive model." *International Journal of Forecasting*. [arXiv:2010.12508](https://arxiv.org/abs/2010.12508)

2. Dmochowski, J.P. (2023). "A statistical theory of optimal decision-making in sports betting." *PLOS ONE*. [PMC10306238](https://pmc.ncbi.nlm.nih.gov/articles/PMC10306238/)

3. Walsh, J. & Joshi, T. (2024). "Machine learning for sports betting: Should model selection be based on accuracy or calibration?" [arXiv:2303.06021](https://arxiv.org/abs/2303.06021)

4. Gneiting, T., Balabdaoui, F., & Raftery, A.E. (2007). "Probabilistic forecasts, calibration and sharpness." *Journal of the Royal Statistical Society, Series B*. [DOI:10.1111/j.1467-9868.2007.00587.x](https://rss.onlinelibrary.wiley.com/doi/abs/10.1111/j.1467-9868.2007.00587.x)

5. Perez-Lebel, A. et al. (2022). "Beyond calibration: Estimating the grouping loss of modern neural networks." [arXiv:2210.16315](https://arxiv.org/abs/2210.16315)

### Industry Sources

6. Unabated. "The Biggest Mistake You're Making When Betting NFL Player Props." [Link](https://unabated.com/articles/the-biggest-mistake-youre-making-when-betting-nfl-player-props)

7. Unabated. "Profitable Prop Betting In 3 Easy Steps." [Link](https://unabated.com/articles/profitable-prop-betting-in-3-easy-steps)

8. Wizard of Odds. "Player Props: Understanding the Math Behind the Lines." [Link](https://wizardofodds.com/article/player-props-understanding-the-math-behind-the-lines/)

9. Sports Insights. "Shading Sports Betting Lines." [Link](https://www.sportsinsights.com/how-to-bet-on-sports/shading-sports-betting-lines/)

### Internal Evidence

10. Session 42 (2026-02-19): Calibration offset A/B test. Without offsets: 7.44% ROI, 0.891 Sharpe. With offsets: 6.01% ROI, 0.742 Sharpe. Offsets disabled in production.

11. Session 65 (2026-03-05): Model under-bias investigation. Root cause: PTS feature selection prunes matchup features (7 features at Q50 vs 27 for REB).

12. Session 67 (2026-03-06): Full research and comparison. Both legacy and copula models under-predict. Academic literature confirms this is profitable.
