"""
Compare actual threes distribution vs NegBin model prediction.
"""

from collections import Counter

import numpy as np
from scipy.stats import nbinom
from sqlalchemy import text

from src.db.client import get_engine


def main():
    engine = get_engine()

    # Get holdout data
    query = text("""
        SELECT fg3m as actual_threes
        FROM player_game_stats
        WHERE season_id = '22025'
          AND min >= 10
          AND fg3m IS NOT NULL
    """)

    with engine.connect() as conn:
        import pandas as pd
        df = pd.read_sql(query, conn)

    y = df['actual_threes'].values.astype(int)

    # Actual distribution
    counts = Counter(y)
    total = len(y)

    print("="*60)
    print("ACTUAL THREES DISTRIBUTION")
    print("="*60)

    cumulative = 0
    for k in sorted(counts.keys()):
        pct = counts[k] / total
        cumulative += pct
        print(f"  {k}: {counts[k]:5d} ({pct:5.1%})  cumulative: {cumulative:5.1%}")

    print(f"\nTotal: {total:,}")
    print(f"Mean: {y.mean():.3f}")
    print(f"Variance: {y.var():.3f}")
    print(f"Zero rate: {(y == 0).mean():.3f}")

    # NegBin with MLE params
    mu = 1.657
    alpha = 0.297
    n = 1.0 / alpha
    p = n / (n + mu)

    print("\n" + "="*60)
    print(f"NEGBIN DISTRIBUTION (mu={mu}, alpha={alpha})")
    print("="*60)

    cumulative_nb = 0
    for k in range(max(counts.keys()) + 1):
        pmf = nbinom.pmf(k, n, p)
        cumulative_nb += pmf
        print(f"  {k}: {pmf:5.1%}  cumulative: {cumulative_nb:5.1%}")

    print(f"\nNegBin mean: {mu:.3f}")
    print(f"NegBin P(X=0): {nbinom.pmf(0, n, p):.3f}")

    # Truncated NegBin (conditioned on X > 0)
    print("\n" + "="*60)
    print("TRUNCATED NEGBIN (X > 0)")
    print("="*60)

    p_zero_nb = nbinom.pmf(0, n, p)
    cumulative_trunc = 0

    for k in range(1, max(counts.keys()) + 1):
        pmf = nbinom.pmf(k, n, p) / (1 - p_zero_nb)
        cumulative_trunc += pmf
        print(f"  {k}: {pmf:5.1%}  cumulative: {cumulative_trunc:5.1%}")

    trunc_mean = mu / (1 - p_zero_nb)
    print(f"\nTruncated mean: {trunc_mean:.3f}")

    # Compare to actual positive samples
    positive_mean = y[y > 0].mean()
    print(f"Actual positive mean: {positive_mean:.3f}")

    print("\n" + "="*60)
    print("COMPARISON: Where quantiles fall")
    print("="*60)

    p_zero_actual = (y == 0).mean()

    for q in [0.10, 0.25, 0.50, 0.75, 0.90]:
        # Actual quantile
        actual_q = np.percentile(y, q * 100)

        # Model quantile (hurdle + truncated NegBin)
        if q <= p_zero_actual:
            model_q = 0
        else:
            q_pos = (q - p_zero_actual) / (1 - p_zero_actual)
            adjusted_q = q_pos * (1 - p_zero_nb) + p_zero_nb
            model_q = int(nbinom.ppf(np.clip(adjusted_q, 0.01, 0.99), n, p))
            model_q = max(1, model_q)

        print(f"  Q{q:.2f}: actual={int(actual_q)}, model={model_q}")


if __name__ == "__main__":
    main()
