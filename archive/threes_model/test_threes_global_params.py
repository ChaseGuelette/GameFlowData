"""
Quick diagnostic: Compare calibration using global MLE params vs XGBoost predictions.

If global params produce better calibration, it confirms the XGBoost mu training is wrong.
"""

import numpy as np
from scipy.stats import nbinom
from pathlib import Path
import pandas as pd
from sqlalchemy import text

from src.db.client import get_engine


def truncated_negbin_quantile(q: float, p_zero: float, mu: float, alpha: float) -> int:
    """
    Compute the q-th quantile of the hurdle + truncated NegBin distribution.

    Args:
        q: Quantile (0-1)
        p_zero: P(X = 0) from classifier
        mu: Mean of untruncated NegBin
        alpha: Dispersion parameter

    Returns:
        Integer quantile value
    """
    if q <= p_zero:
        return 0

    # Map q to the positive distribution
    q_pos = (q - p_zero) / (1 - p_zero + 1e-9)
    q_pos = np.clip(q_pos, 0.001, 0.999)

    # NegBin params for scipy
    n = 1.0 / (alpha + 1e-6)
    p = n / (n + mu + 1e-6)
    p = np.clip(p, 0.001, 0.999)

    # P(X=0) under full NegBin
    p_zero_nb = nbinom.pmf(0, n, p)

    # Inverse CDF of truncated distribution
    adjusted_q = q_pos * (1 - p_zero_nb) + p_zero_nb
    adjusted_q = np.clip(adjusted_q, 0.001, 0.999)

    return max(1, int(nbinom.ppf(adjusted_q, n, p)))


def main():
    engine = get_engine()

    # Get holdout data (22025 season)
    query = text("""
        SELECT
            player_id,
            game_id,
            fg3m as actual_threes,
            min as actual_minutes
        FROM player_game_stats
        WHERE season_id = '22025'
          AND min >= 10
          AND fg3m IS NOT NULL
        ORDER BY game_date
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    print(f"Loaded {len(df):,} samples from 22025 season")

    y_actual = df['actual_threes'].values.astype(int)

    # Compute actual zero rate (this is our p_zero estimate)
    actual_zero_rate = (y_actual == 0).mean()
    print(f"Actual zero rate: {actual_zero_rate:.3f}")

    # Positive samples stats
    positive_mask = y_actual > 0
    positive_mean = y_actual[positive_mask].mean()
    positive_var = y_actual[positive_mask].var()
    print(f"Positive samples: n={positive_mask.sum()}, mean={positive_mean:.3f}, var={positive_var:.3f}")

    # Global MLE from training (correct values)
    global_mu_correct = 1.657
    global_alpha_correct = 0.297

    # What the XGBoost predicts (incorrect - too high)
    xgb_mu_wrong = 2.5  # From training log: pred_mean=2.502

    print("\n" + "="*60)
    print("SCENARIO 1: Using CORRECT global MLE params (mu=1.657)")
    print("="*60)

    quantiles = [0.10, 0.25, 0.50, 0.75, 0.90]
    for q in quantiles:
        # Use actual zero rate as p_zero
        pred = truncated_negbin_quantile(q, actual_zero_rate, global_mu_correct, global_alpha_correct)
        coverage = (y_actual <= pred).mean()
        gap = coverage - q
        status = "OK" if abs(gap) <= 0.05 else f"GAP {gap:+.3f}"
        print(f"  Q{q:.2f}: pred={pred}, coverage={coverage:.3f}, target={q:.2f} [{status}]")

    print("\n" + "="*60)
    print("SCENARIO 2: Using WRONG XGBoost mu (mu=2.5)")
    print("="*60)

    for q in quantiles:
        pred = truncated_negbin_quantile(q, actual_zero_rate, xgb_mu_wrong, global_alpha_correct)
        coverage = (y_actual <= pred).mean()
        gap = coverage - q
        status = "OK" if abs(gap) <= 0.05 else f"GAP {gap:+.3f}"
        print(f"  Q{q:.2f}: pred={pred}, coverage={coverage:.3f}, target={q:.2f} [{status}]")

    print("\n" + "="*60)
    print("SCENARIO 3: Compute CORRECT mu from observed mean")
    print("="*60)

    # The key insight: mu should be computed from the truncated mean
    # E[X | X > 0] = mu / (1 - P(X=0))
    # So: mu = E[X | X > 0] * (1 - P(X=0))
    # But P(X=0) here is the NegBin's p_zero, not the classifier's
    # This requires iteration...

    # For now, approximate: if classifier p_zero ≈ NegBin p_zero
    mu_from_mean = positive_mean * (1 - actual_zero_rate)
    print(f"  Estimated mu from truncated mean: {mu_from_mean:.3f}")
    print(f"  Compare to MLE mu: {global_mu_correct:.3f}")

    for q in quantiles:
        pred = truncated_negbin_quantile(q, actual_zero_rate, mu_from_mean, global_alpha_correct)
        coverage = (y_actual <= pred).mean()
        gap = coverage - q
        status = "OK" if abs(gap) <= 0.05 else f"GAP {gap:+.3f}"
        print(f"  Q{q:.2f}: pred={pred}, coverage={coverage:.3f}, target={q:.2f} [{status}]")

    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print("""
The mu model is trained to predict log(observed_count + 0.5), but mu should be
the mean of the UNTRUNCATED distribution, which is lower than observed values.

Fix options:
1. Train mu on log(y * (1 - p_zero)) where p_zero is from the classifier
2. Use MLE to fit mu for each training sample (slow but correct)
3. Use global MLE and train a multiplicative adjustment factor
""")


if __name__ == "__main__":
    main()
