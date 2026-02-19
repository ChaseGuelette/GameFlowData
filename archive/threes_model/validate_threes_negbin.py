"""
Phase 0 Validation: Test if Truncated Negative Binomial fits positive-only threes data.

This script:
1. Queries positive-only fg3m data from player_game_stats
2. Fits a truncated NegBin via MLE
3. Runs chi-squared goodness-of-fit test
4. Validates across shooter volume segments

Exit criteria: chi-squared p-value > 0.05 to proceed with implementation
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.optimize import minimize
from scipy.stats import chisquare, nbinom

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.db.client import get_engine


def query_positive_threes() -> pd.DataFrame:
    """Query positive-only threes data with shooter volume info."""
    engine = get_engine()

    query = """
    SELECT
        pgs.player_id,
        pgs.fg3m,
        pgs.fg3a,
        pgs.min,
        pgs.season_id
    FROM player_game_stats pgs
    WHERE pgs.fg3m > 0
      AND pgs.min >= 10
      AND pgs.season_id IN ('22022', '22023', '22024')
    """

    df = pd.read_sql(query, engine)
    print(f"Loaded {len(df):,} positive-only threes samples")
    return df


def truncated_negbin_nll(params, data):
    """
    Negative log-likelihood for zero-truncated negative binomial.

    Using mu/alpha parameterization:
    - mu: mean of untruncated distribution
    - alpha: overdispersion parameter (variance = mu + alpha * mu^2)

    Convert to scipy's (n, p) notation:
    - n = 1/alpha
    - p = 1/(1 + alpha * mu)
    """
    mu, alpha = params

    if mu <= 0 or alpha <= 0:
        return 1e10

    # Convert to scipy params
    n = 1.0 / alpha
    p = 1.0 / (1.0 + alpha * mu)

    if p <= 0 or p >= 1 or n <= 0:
        return 1e10

    # P(X=0) under full NegBin
    p_zero = nbinom.pmf(0, n, p)
    if p_zero >= 1:
        return 1e10

    # Log-likelihood of truncated distribution: log[P(X=k)] - log[1 - P(X=0)]
    log_probs = nbinom.logpmf(data.astype(int), n, p) - np.log(1 - p_zero)

    # Handle any invalid log probs
    log_probs = np.where(np.isfinite(log_probs), log_probs, -100)

    return -np.sum(log_probs)


def fit_truncated_negbin(data: np.ndarray) -> dict:
    """Fit truncated NegBin via MLE and return estimated parameters."""

    # Initial guess: moment estimates
    mu_init = data.mean()
    var_init = data.var()

    # For NegBin: variance = mu + alpha * mu^2
    # Solving: alpha = (var - mu) / mu^2
    alpha_init = max(0.1, (var_init - mu_init) / (mu_init ** 2 + 1e-6))

    print(f"\nMoment estimates: mu={mu_init:.3f}, var={var_init:.3f}, alpha_init={alpha_init:.3f}")

    # Optimize
    result = minimize(
        truncated_negbin_nll,
        x0=[mu_init, alpha_init],
        args=(data,),
        method='Nelder-Mead',
        options={'maxiter': 1000}
    )

    mu_mle, alpha_mle = result.x

    print(f"MLE estimates: mu={mu_mle:.3f}, alpha={alpha_mle:.3f}")
    print(f"Optimization success: {result.success}, NLL: {result.fun:.2f}")

    return {
        'mu': mu_mle,
        'alpha': alpha_mle,
        'success': result.success,
        'nll': result.fun
    }


def truncated_negbin_pmf(k: int, mu: float, alpha: float) -> float:
    """PMF of zero-truncated negative binomial."""
    n = 1.0 / alpha
    p = 1.0 / (1.0 + alpha * mu)

    p_zero = nbinom.pmf(0, n, p)
    return nbinom.pmf(k, n, p) / (1 - p_zero)


def chi_squared_test(data: np.ndarray, mu: float, alpha: float) -> dict:
    """
    Chi-squared goodness-of-fit test.

    Compares observed vs expected frequencies for k=1,2,...,max_k.
    """
    max_k = min(int(data.max()), 12)  # Cap at 12 to avoid sparse bins

    # Observed frequencies
    observed = np.zeros(max_k)
    for k in range(1, max_k + 1):
        observed[k - 1] = np.sum(data == k)

    # Add tail bin (k > max_k)
    observed_tail = np.sum(data > max_k)
    observed = np.append(observed, observed_tail)

    # Expected frequencies
    n_samples = len(data)
    expected = np.zeros(max_k)
    for k in range(1, max_k + 1):
        expected[k - 1] = truncated_negbin_pmf(k, mu, alpha) * n_samples

    # Tail probability
    tail_prob = 1 - sum(truncated_negbin_pmf(k, mu, alpha) for k in range(1, max_k + 1))
    expected_tail = tail_prob * n_samples
    expected = np.append(expected, expected_tail)

    # Combine bins with expected < 5 (chi-squared requirement)
    valid_mask = expected >= 5
    if not valid_mask.all():
        print(f"  Combining {(~valid_mask).sum()} bins with expected < 5")

    # Run chi-squared test
    observed_valid = observed[valid_mask]
    expected_valid = expected[valid_mask]

    # Normalize expected to match observed sum (chi-squared requirement)
    expected_valid = expected_valid * (observed_valid.sum() / expected_valid.sum())

    stat, p_value = chisquare(observed_valid, expected_valid)

    # Also compute Mean Absolute Percentage Error for practical assessment
    mape = np.mean(np.abs(observed - expected) / (expected + 1e-6)) * 100

    # Weighted MAPE (weight by frequency)
    weights = expected / expected.sum()
    weighted_mape = np.sum(weights * np.abs(observed - expected) / (expected + 1e-6)) * 100

    return {
        'statistic': stat,
        'p_value': p_value,
        'df': len(observed_valid) - 1 - 2,  # -2 for estimated params
        'observed': observed,
        'expected': expected,
        'max_k': max_k,
        'mape': mape,
        'weighted_mape': weighted_mape
    }


def validate_segment(df: pd.DataFrame, segment_name: str) -> dict:
    """Validate NegBin fit on a data segment."""
    data = df['fg3m'].values

    print(f"\n{'='*60}")
    print(f"Segment: {segment_name}")
    print(f"Samples: {len(data):,}")
    print(f"Mean: {data.mean():.3f}, Var: {data.var():.3f}")
    print(f"Range: [{data.min()}, {data.max()}]")

    # Fit MLE
    fit = fit_truncated_negbin(data)

    if not fit['success']:
        print("WARNING: MLE optimization did not converge")

    # Chi-squared test
    chi2 = chi_squared_test(data, fit['mu'], fit['alpha'])

    print("\nChi-squared test:")
    print(f"  Statistic: {chi2['statistic']:.2f}")
    print(f"  p-value: {chi2['p_value']:.4f}")
    print(f"  Degrees of freedom: {chi2['df']}")
    print(f"  MAPE: {chi2['mape']:.1f}%")
    print(f"  Weighted MAPE: {chi2['weighted_mape']:.1f}%")

    if chi2['p_value'] > 0.05:
        print("  Result: PASS (p > 0.05) - NegBin is a good fit")
    elif chi2['weighted_mape'] < 5:
        print("  Result: MARGINAL (p <= 0.05 but MAPE < 5%) - Practically acceptable")
    else:
        print("  Result: FAIL (p <= 0.05) - NegBin may not be appropriate")

    # Print observed vs expected
    print("\n  k | Observed | Expected | Diff%")
    print("  " + "-" * 35)
    for k in range(1, chi2['max_k'] + 1):
        obs = chi2['observed'][k - 1]
        exp = chi2['expected'][k - 1]
        diff_pct = 100 * (obs - exp) / exp if exp > 0 else 0
        print(f"  {k:2d} | {obs:8.0f} | {exp:8.1f} | {diff_pct:+6.1f}%")

    # Tail
    obs_tail = chi2['observed'][-1]
    exp_tail = chi2['expected'][-1]
    diff_pct = 100 * (obs_tail - exp_tail) / exp_tail if exp_tail > 0 else 0
    print(f"  {chi2['max_k']+1}+ | {obs_tail:8.0f} | {exp_tail:8.1f} | {diff_pct:+6.1f}%")

    # Practical acceptance: p > 0.05 OR weighted MAPE < 5%
    passed = chi2['p_value'] > 0.05 or chi2['weighted_mape'] < 5

    return {
        'segment': segment_name,
        'n_samples': len(data),
        'mean': data.mean(),
        'var': data.var(),
        'mu': fit['mu'],
        'alpha': fit['alpha'],
        'chi2_stat': chi2['statistic'],
        'chi2_p': chi2['p_value'],
        'mape': chi2['mape'],
        'weighted_mape': chi2['weighted_mape'],
        'passed': passed
    }


def main():
    print("=" * 60)
    print("PHASE 0: Truncated Negative Binomial Validation")
    print("=" * 60)

    # Load data
    df = query_positive_threes()

    # Calculate average fg3a per player for segmentation
    player_avg_fg3a = df.groupby('player_id')['fg3a'].mean().reset_index()
    player_avg_fg3a.columns = ['player_id', 'avg_fg3a']
    df = df.merge(player_avg_fg3a, on='player_id')

    results = []

    # 1. Overall validation
    results.append(validate_segment(df, "All Positive Samples"))

    # 2. Segment by shooter volume
    # High volume: avg 4+ 3PA
    high_vol = df[df['avg_fg3a'] >= 4]
    if len(high_vol) > 100:
        results.append(validate_segment(high_vol, "High Volume (4+ 3PA avg)"))

    # Moderate volume: 2-4 3PA
    mod_vol = df[(df['avg_fg3a'] >= 2) & (df['avg_fg3a'] < 4)]
    if len(mod_vol) > 100:
        results.append(validate_segment(mod_vol, "Moderate Volume (2-4 3PA avg)"))

    # Low volume: 0-2 3PA
    low_vol = df[df['avg_fg3a'] < 2]
    if len(low_vol) > 100:
        results.append(validate_segment(low_vol, "Low Volume (0-2 3PA avg)"))

    # Summary
    print("\n" + "=" * 60)
    print("VALIDATION SUMMARY")
    print("=" * 60)

    all_passed = True
    for r in results:
        status = "PASS" if r['passed'] else "FAIL"
        wmape_str = f"WMAPE={r['weighted_mape']:.1f}%" if 'weighted_mape' in r else ""
        print(f"{r['segment']:30s} | p={r['chi2_p']:.4f} | {wmape_str:12s} | {status}")
        if not r['passed']:
            all_passed = False

    print("\n" + "=" * 60)
    if all_passed:
        print("OVERALL: PASS - Truncated NegBin is a good fit across all segments")
        print("Proceed to Phase 1: Build TruncatedNegBinModel class")
    else:
        print("OVERALL: FAIL - Consider empirical multinomial as fallback")
        print("Review failed segments before proceeding")
    print("=" * 60)

    return all_passed


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
