"""
Performance metrics for backtesting evaluation.
"""

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass
class CalibrationResult:
    """Calibration metrics for a single quantile."""

    quantile: float
    target_coverage: float
    actual_coverage: float
    gap: float
    n_samples: int


@dataclass
class PerformanceMetrics:
    """
    Comprehensive performance metrics from a backtest.
    """

    # Betting metrics
    total_bets: int
    wins: int
    losses: int
    pushes: int
    total_staked: float
    total_profit: float
    roi: float
    hit_rate: float

    # Risk metrics
    sharpe_ratio: float
    max_drawdown: float
    win_streak: int
    loss_streak: int

    # Calibration metrics
    calibration_results: list[CalibrationResult]
    overall_calibration_gap: float

    # By-stat breakdown
    by_stat: dict[str, dict]

    # By-edge breakdown
    by_edge_bucket: dict[str, dict]

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "betting": {
                "total_bets": self.total_bets,
                "wins": self.wins,
                "losses": self.losses,
                "pushes": self.pushes,
                "total_staked": self.total_staked,
                "total_profit": self.total_profit,
                "roi": self.roi,
                "hit_rate": self.hit_rate,
            },
            "risk": {
                "sharpe_ratio": self.sharpe_ratio,
                "max_drawdown": self.max_drawdown,
                "win_streak": self.win_streak,
                "loss_streak": self.loss_streak,
            },
            "calibration": {
                "results": [
                    {
                        "quantile": c.quantile,
                        "target": c.target_coverage,
                        "actual": c.actual_coverage,
                        "gap": c.gap,
                        "n": c.n_samples,
                    }
                    for c in self.calibration_results
                ],
                "overall_gap": self.overall_calibration_gap,
            },
            "by_stat": self.by_stat,
            "by_edge_bucket": self.by_edge_bucket,
        }

    def __str__(self) -> str:
        """Pretty print summary."""
        lines = [
            "=" * 50,
            "BACKTEST PERFORMANCE SUMMARY",
            "=" * 50,
            "",
            "BETTING METRICS",
            f"  Total Bets: {self.total_bets}",
            f"  Wins: {self.wins} | Losses: {self.losses} | Pushes: {self.pushes}",
            f"  Hit Rate: {self.hit_rate:.1%}",
            f"  Total Staked: ${self.total_staked:,.2f}",
            f"  Total Profit: ${self.total_profit:,.2f}",
            f"  ROI: {self.roi:.2%}",
            "",
            "RISK METRICS",
            f"  Sharpe Ratio: {self.sharpe_ratio:.2f}",
            f"  Max Drawdown: {self.max_drawdown:.2%}",
            f"  Best Win Streak: {self.win_streak}",
            f"  Worst Loss Streak: {self.loss_streak}",
            "",
            "CALIBRATION",
            f"  Overall Gap: {self.overall_calibration_gap:.3f}",
        ]

        for cal in self.calibration_results:
            status = "OK" if abs(cal.gap) < 0.05 else "WARNING"
            lines.append(
                f"  Q{int(cal.quantile * 100):02d}: {cal.actual_coverage:.3f} (target: {cal.target_coverage:.3f}) [{status}]"
            )

        lines.extend(["", "BY STAT"])
        for stat, metrics in self.by_stat.items():
            lines.append(f"  {stat.upper()}: {metrics.get('bets', 0)} bets, ROI: {metrics.get('roi', 0):.2%}")

        lines.append("=" * 50)
        return "\n".join(lines)


class MetricsCalculator:
    """
    Calculate performance metrics from backtest results.
    """

    QUANTILES = [0.10, 0.25, 0.50, 0.75, 0.90]

    def calculate(
        self,
        predictions_df: pd.DataFrame,
        bets_df: pd.DataFrame,
        starting_bankroll: float = 0.0,
    ) -> PerformanceMetrics:
        """
        Calculate comprehensive metrics.

        Args:
            predictions_df: All predictions with actuals
                Required columns: player_id, game_id, stat, pred_q10, pred_q25,
                pred_q50, pred_q75, pred_q90, actual
            bets_df: All simulated bets with outcomes
                Required columns: stat, edge, outcome, profit, stake
            starting_bankroll: Optional starting bankroll for equity-based risk metrics.
        """
        betting_metrics = self._calculate_betting_metrics(bets_df)
        risk_metrics = self._calculate_risk_metrics(bets_df, starting_bankroll)
        calibration_results = self._calculate_calibration(predictions_df)
        by_stat = self._calculate_by_stat(bets_df)
        by_edge = self._calculate_by_edge_bucket(bets_df)

        overall_cal_gap = np.mean([abs(c.gap) for c in calibration_results]) if calibration_results else 0.0

        return PerformanceMetrics(
            total_bets=betting_metrics["total_bets"],
            wins=betting_metrics["wins"],
            losses=betting_metrics["losses"],
            pushes=betting_metrics["pushes"],
            total_staked=betting_metrics["total_staked"],
            total_profit=betting_metrics["total_profit"],
            roi=betting_metrics["roi"],
            hit_rate=betting_metrics["hit_rate"],
            sharpe_ratio=risk_metrics["sharpe_ratio"],
            max_drawdown=risk_metrics["max_drawdown"],
            win_streak=risk_metrics["win_streak"],
            loss_streak=risk_metrics["loss_streak"],
            calibration_results=calibration_results,
            overall_calibration_gap=overall_cal_gap,
            by_stat=by_stat,
            by_edge_bucket=by_edge,
        )

    def _calculate_betting_metrics(self, bets_df: pd.DataFrame) -> dict:
        """Calculate basic betting metrics."""
        if bets_df.empty:
            return {
                "total_bets": 0,
                "wins": 0,
                "losses": 0,
                "pushes": 0,
                "total_staked": 0.0,
                "total_profit": 0.0,
                "roi": 0.0,
                "hit_rate": 0.0,
            }

        resolved = bets_df[bets_df["outcome"].notna()]
        wins = (resolved["outcome"] == "win").sum()
        losses = (resolved["outcome"] == "loss").sum()
        pushes = (resolved["outcome"] == "push").sum()
        total_staked = resolved[resolved["outcome"] != "push"]["stake"].sum()
        total_profit = resolved["profit"].sum()

        return {
            "total_bets": len(bets_df),
            "wins": int(wins),
            "losses": int(losses),
            "pushes": int(pushes),
            "total_staked": float(total_staked),
            "total_profit": float(total_profit),
            "roi": float(total_profit / total_staked) if total_staked > 0 else 0.0,
            "hit_rate": float(wins / (wins + losses)) if (wins + losses) > 0 else 0.0,
        }

    def _calculate_risk_metrics(self, bets_df: pd.DataFrame, starting_bankroll: float = 0.0) -> dict:
        """Calculate risk-adjusted metrics."""
        if bets_df.empty or bets_df["profit"].isna().all():
            return {
                "sharpe_ratio": 0.0,
                "max_drawdown": 0.0,
                "win_streak": 0,
                "loss_streak": 0,
            }

        # Filter to resolved bets with profit
        profits = bets_df[bets_df["profit"].notna()]["profit"].values

        if len(profits) == 0:
            return {
                "sharpe_ratio": 0.0,
                "max_drawdown": 0.0,
                "win_streak": 0,
                "loss_streak": 0,
            }

        # Sharpe ratio (annualized assuming ~170 NBA game days per season)
        mean_return = profits.mean()
        std_return = profits.std()
        sharpe = (mean_return / std_return) * np.sqrt(170) if std_return > 0 else 0.0

        # Max drawdown calculation
        cumulative_pl = np.concatenate([[0.0], np.cumsum(profits)])

        if starting_bankroll > 0:
            # Equity curve based
            equity = starting_bankroll + cumulative_pl
            running_max = np.maximum.accumulate(equity)
            drawdowns = equity - running_max  # Always <= 0
            max_drawdown_amount = abs(drawdowns.min()) if len(drawdowns) > 0 else 0.0

            # Drawdown % relative to peak equity
            peak = running_max.max()
            max_dd_pct = max_drawdown_amount / peak if peak > 0 else 0.0
        else:
            # P&L based (legacy/fallback)
            running_max = np.maximum.accumulate(cumulative_pl)
            drawdowns = cumulative_pl - running_max
            max_drawdown_amount = abs(drawdowns.min()) if len(drawdowns) > 0 else 0.0

            # Normalize drawdown by peak P&L (if positive) or total staked
            peak = running_max[np.argmin(drawdowns)] if len(drawdowns) > 0 else 0.0
            total_staked = bets_df[bets_df["outcome"].notna()]["stake"].sum()
            reference = peak if peak > 0 else total_staked
            max_dd_pct = max_drawdown_amount / reference if reference > 0 else 0.0

        # Win/loss streaks
        outcomes = bets_df[bets_df["outcome"].notna()]["outcome"].values
        win_streak, loss_streak = self._calculate_streaks(outcomes)

        return {
            "sharpe_ratio": float(sharpe),
            "max_drawdown": float(max_dd_pct),
            "win_streak": win_streak,
            "loss_streak": loss_streak,
        }

    def _calculate_streaks(self, outcomes: np.ndarray) -> tuple[int, int]:
        """Calculate longest win and loss streaks."""
        if len(outcomes) == 0:
            return 0, 0

        max_win = 0
        max_loss = 0
        current_win = 0
        current_loss = 0

        for outcome in outcomes:
            if outcome == "win":
                current_win += 1
                current_loss = 0
                max_win = max(max_win, current_win)
            elif outcome == "loss":
                current_loss += 1
                current_win = 0
                max_loss = max(max_loss, current_loss)
            else:  # push
                current_win = 0
                current_loss = 0

        return max_win, max_loss

    def _calculate_calibration(self, predictions_df: pd.DataFrame) -> list[CalibrationResult]:
        """Calculate quantile calibration metrics."""
        results = []

        if predictions_df.empty or "actual" not in predictions_df.columns:
            return results

        # Filter to rows with actual values
        df = predictions_df[predictions_df["actual"].notna()].copy()

        if df.empty:
            return results

        for q in self.QUANTILES:
            col = f"pred_q{int(q * 100)}"
            if col not in df.columns:
                continue

            # Coverage: P(actual <= predicted_quantile)
            coverage = (df["actual"] <= df[col]).mean()

            results.append(
                CalibrationResult(
                    quantile=q,
                    target_coverage=q,
                    actual_coverage=float(coverage),
                    gap=float(coverage - q),
                    n_samples=len(df),
                )
            )

        return results

    def _calculate_by_stat(self, bets_df: pd.DataFrame) -> dict[str, dict]:
        """Calculate metrics broken down by stat."""
        by_stat = {}

        if bets_df.empty or "stat" not in bets_df.columns:
            return by_stat

        for stat in bets_df["stat"].unique():
            stat_bets = bets_df[bets_df["stat"] == stat]
            resolved = stat_bets[stat_bets["outcome"].notna()]

            wins = (resolved["outcome"] == "win").sum()
            losses = (resolved["outcome"] == "loss").sum()
            total_staked = resolved[resolved["outcome"] != "push"]["stake"].sum()
            total_profit = resolved["profit"].sum()

            by_stat[stat] = {
                "bets": len(stat_bets),
                "wins": int(wins),
                "losses": int(losses),
                "hit_rate": float(wins / (wins + losses)) if (wins + losses) > 0 else 0.0,
                "profit": float(total_profit),
                "roi": float(total_profit / total_staked) if total_staked > 0 else 0.0,
            }

        return by_stat

    def _calculate_by_edge_bucket(self, bets_df: pd.DataFrame) -> dict[str, dict]:
        """Calculate metrics broken down by edge buckets."""
        buckets = {}

        if bets_df.empty or "edge" not in bets_df.columns:
            return buckets

        # Define edge buckets
        bucket_ranges = [
            ("0.05-0.10", 0.05, 0.10),
            ("0.10-0.15", 0.10, 0.15),
            ("0.15-0.20", 0.15, 0.20),
            ("0.20+", 0.20, float("inf")),
        ]

        for name, low, high in bucket_ranges:
            bucket_bets = bets_df[(bets_df["edge"] >= low) & (bets_df["edge"] < high)]
            resolved = bucket_bets[bucket_bets["outcome"].notna()]

            wins = (resolved["outcome"] == "win").sum()
            losses = (resolved["outcome"] == "loss").sum()
            total_staked = resolved[resolved["outcome"] != "push"]["stake"].sum()
            total_profit = resolved["profit"].sum()

            buckets[name] = {
                "bets": len(bucket_bets),
                "wins": int(wins),
                "losses": int(losses),
                "hit_rate": float(wins / (wins + losses)) if (wins + losses) > 0 else 0.0,
                "profit": float(total_profit),
                "roi": float(total_profit / total_staked) if total_staked > 0 else 0.0,
            }

        return buckets
