# evaluation/calibration.py

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass
class CalibrationReport:
    """Calibration evaluation results."""

    quantile: float
    target_coverage: float
    actual_coverage: float
    gap: float
    n_samples: int

    @property
    def is_calibrated(self, tolerance: float = 0.03) -> bool:
        return abs(self.gap) <= tolerance


class CalibrationEvaluator:
    """Evaluate quantile calibration and betting performance."""

    def __init__(self, predictor, feature_store):
        self.predictor = predictor
        self.feature_store = feature_store

    def evaluate_quantile_calibration(
        self, df: pd.DataFrame, stat: str = "pts"
    ) -> list[CalibrationReport]:
        """
        Check if predicted quantiles match actual coverage.

        For each quantile q, we check: P(actual < predicted_q) ≈ q
        """
        reports = []
        actual_col = f"actual_{stat}"

        for q in [0.10, 0.25, 0.50, 0.75, 0.90]:
            q_col = f"pred_q{int(q * 100):02d}"

            if q_col not in df.columns:
                continue

            # Calculate actual coverage
            coverage = (df[actual_col] <= df[q_col]).mean()

            reports.append(
                CalibrationReport(
                    quantile=q,
                    target_coverage=q,
                    actual_coverage=coverage,
                    gap=coverage - q,
                    n_samples=len(df),
                )
            )

        return reports

    def evaluate_betting_performance(
        self,
        predictions_df: pd.DataFrame,
        lines_df: pd.DataFrame,
        stat: str = "pts",
        edge_threshold: float = 0.05,
    ) -> dict:
        """
        Evaluate betting performance on historical data.

        Args:
            predictions_df: DataFrame with predicted distributions
            lines_df: DataFrame with actual prop lines and results
            stat: Which stat to evaluate
            edge_threshold: Minimum edge to place bet

        Returns:
            Dict with ROI, Sharpe, hit rate, etc.
        """
        # Merge predictions with lines
        merged = predictions_df.merge(lines_df, on=["player_id", "game_id", "stat"], how="inner")

        # Calculate edge for each bet
        merged["over_prob"] = merged.apply(
            lambda r: (r["samples"] > r["line"]).mean()
            if isinstance(r.get("samples"), np.ndarray)
            else self._estimate_prob_over(r, r["line"]),
            axis=1,
        )

        merged["implied_over"] = merged["over_odds"].apply(self._odds_to_prob)
        merged["over_edge"] = merged["over_prob"] - merged["implied_over"]

        merged["implied_under"] = merged["under_odds"].apply(self._odds_to_prob)
        merged["under_edge"] = (1 - merged["over_prob"]) - merged["implied_under"]

        # Identify bets
        merged["bet_over"] = merged["over_edge"] > edge_threshold
        merged["bet_under"] = merged["under_edge"] > edge_threshold

        # Calculate results
        merged["over_hit"] = merged["actual"] > merged["line"]
        merged["under_hit"] = merged["actual"] < merged["line"]

        # ROI calculation
        over_bets = merged[merged["bet_over"]]
        under_bets = merged[merged["bet_under"]]

        over_roi = self._calculate_roi(over_bets["over_hit"], over_bets["over_odds"])
        under_roi = self._calculate_roi(under_bets["under_hit"], under_bets["under_odds"])

        return {
            "total_bets": len(over_bets) + len(under_bets),
            "over_bets": len(over_bets),
            "under_bets": len(under_bets),
            "over_roi": over_roi,
            "under_roi": under_roi,
            "combined_roi": self._combined_roi(over_bets, under_bets),
            "over_hit_rate": over_bets["over_hit"].mean() if len(over_bets) > 0 else 0,
            "under_hit_rate": under_bets["under_hit"].mean() if len(under_bets) > 0 else 0,
            "avg_over_edge": over_bets["over_edge"].mean() if len(over_bets) > 0 else 0,
            "avg_under_edge": under_bets["under_edge"].mean() if len(under_bets) > 0 else 0,
        }

    def _estimate_prob_over(self, row: pd.Series, line: float) -> float:
        """Estimate P(over) from quantiles using linear interpolation."""
        quantiles = np.array([0.10, 0.25, 0.50, 0.75, 0.90])
        values = np.array([row["q10"], row["q25"], row["q50"], row["q75"], row["q90"]])

        # Find where line falls in the distribution
        if line <= values[0]:
            return 0.90 + 0.10 * (values[0] - line) / (values[0] - 0)
        elif line >= values[-1]:
            return 0.10 * (1 - (line - values[-1]) / values[-1])
        else:
            # Linear interpolation
            prob_under = np.interp(line, values, quantiles)
            return 1 - prob_under

    def _odds_to_prob(self, odds: int) -> float:
        """Convert American odds to implied probability."""
        if odds > 0:
            return 100 / (odds + 100)
        else:
            return abs(odds) / (abs(odds) + 100)

    def _calculate_roi(self, hits: pd.Series, odds: pd.Series) -> float:
        """Calculate ROI for a set of bets."""
        if len(hits) == 0:
            return 0.0

        total_wagered = len(hits)  # Assuming unit bets

        winnings = sum(
            (o / 100 if o > 0 else 100 / abs(o)) if h else -1 for h, o in zip(hits, odds)
        )

        return winnings / total_wagered

    def _combined_roi(self, over_bets: pd.DataFrame, under_bets: pd.DataFrame) -> float:
        """Calculate combined ROI across all bets."""
        total_bets = len(over_bets) + len(under_bets)
        if total_bets == 0:
            return 0.0

        over_winnings = (
            sum(
                (o / 100 if o > 0 else 100 / abs(o)) if h else -1
                for h, o in zip(over_bets["over_hit"], over_bets["over_odds"])
            )
            if len(over_bets) > 0
            else 0
        )

        under_winnings = (
            sum(
                (o / 100 if o > 0 else 100 / abs(o)) if h else -1
                for h, o in zip(under_bets["under_hit"], under_bets["under_odds"])
            )
            if len(under_bets) > 0
            else 0
        )

        return (over_winnings + under_winnings) / total_bets
