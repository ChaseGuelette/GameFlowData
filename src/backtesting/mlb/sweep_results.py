"""Output and serialization helpers for the MLB backtest sweep."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import pandas as pd

from src.backtesting.mlb.sweep_config import SweepConfig
from src.backtesting.performance_metrics import PerformanceMetrics

logger = logging.getLogger("MLBBacktestSweep")


@dataclass
class SweepResult:
    """Results for a single sweep configuration."""

    config: SweepConfig
    metrics: PerformanceMetrics
    bets_df: pd.DataFrame
    predictions_df: pd.DataFrame
    elapsed_seconds: float
    all_edges_df: pd.DataFrame = field(default_factory=pd.DataFrame)


def print_comparison_table(
    results: list[SweepResult],
    start_date: date,
    end_date: date,
    phase01_time: float,
    total_predictions: int,
    total_dates: int,
    starting_bankroll: float = 10000.0,
) -> None:
    header = (
        f"\n{'=' * 120}\n"
        f"MLB BACKTEST SWEEP  ({start_date} to {end_date})\n"
        f"Phase 0-1: {total_dates} dates, {total_predictions} predictions ({phase01_time:.1f}s)\n"
        f"Starting bankroll: ${starting_bankroll:,.0f}\n"
        f"{'=' * 120}\n"
    )
    print(header)

    fmt = "{:>3}  {:<40} {:>5} {:>7} {:>8} {:>9} {:>10} {:>7} {:>7} {:>6}"
    print(fmt.format(
        "#", "Config", "Bets", "HitRate", "ROI", "Profit",
        "Staked", "Sharpe", "MaxDD", "Time",
    ))
    print(fmt.format(
        "---", "-" * 40, "-----", "-------", "--------", "---------",
        "----------", "-------", "-------", "------",
    ))

    for i, r in enumerate(results, 1):
        m = r.metrics
        roi_str = f"{m.roi:+.2%}" if m.roi != 0 else "0.00%"
        profit_str = f"${m.total_profit:+,.0f}" if m.total_profit != 0 else "$0"
        staked_str = f"${m.total_staked:,.0f}" if m.total_staked != 0 else "$0"
        print(fmt.format(
            i,
            r.config.label,
            m.total_bets,
            f"{m.hit_rate:.1%}",
            roi_str,
            profit_str,
            staked_str,
            f"{m.sharpe_ratio:.2f}",
            f"{m.max_drawdown:.1%}",
            f"{r.elapsed_seconds:.1f}s",
        ))

    # Per-stat breakdown
    print(f"\n{'─' * 120}")
    print("PER-STAT BREAKDOWN")
    print(f"{'─' * 120}")
    stat_labels = []
    for r in results:
        if r.metrics.by_stat:
            stat_labels = list(r.metrics.by_stat.keys())
            break

    if stat_labels:
        header_parts = [f"{s.upper():>16}" for s in stat_labels]
        print(f"{'#':>3}  {'Config':<40} " + " ".join(header_parts))
        print(f"{'---':>3}  {'-' * 40} " + " ".join(["-" * 16] * len(stat_labels)))

        for i, r in enumerate(results, 1):
            parts = []
            for s in stat_labels:
                stat_data = r.metrics.by_stat.get(s, {})
                roi = stat_data.get("roi", 0)
                bets = stat_data.get("bets", 0)
                hit = stat_data.get("hit_rate", 0)
                parts.append(f"{roi:+.1%}({bets},{hit:.0%})")
            print(f"{i:>3}  {r.config.label:<40} " + " ".join(f"{p:>16}" for p in parts))

    # Summary
    print(f"\n{'=' * 120}")
    if results:
        best_roi = max(results, key=lambda r: r.metrics.roi)
        best_sharpe = max(results, key=lambda r: r.metrics.sharpe_ratio)
        most_bets = max(results, key=lambda r: r.metrics.total_bets)
        best_roi_idx = results.index(best_roi) + 1
        best_sharpe_idx = results.index(best_sharpe) + 1
        most_bets_idx = results.index(most_bets) + 1
        print(f"Best ROI:    #{best_roi_idx} ({best_roi.config.label}) = {best_roi.metrics.roi:+.2%}")
        print(f"Best Sharpe: #{best_sharpe_idx} ({best_sharpe.config.label}) = {best_sharpe.metrics.sharpe_ratio:.2f}")
        print(f"Most bets:   #{most_bets_idx} ({most_bets.config.label}) = {most_bets.metrics.total_bets}")
    print(f"{'=' * 120}\n")


def save_results(
    results: list[SweepResult],
    output_dir: Path,
    start_date: date,
    end_date: date,
    phase01_time: float,
    total_predictions: int,
    total_dates: int,
    starting_bankroll: float = 10000.0,
    promotion_metadata: dict | None = None,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    json_output = {
        "sweep_metadata": {
            "start_date": str(start_date),
            "end_date": str(end_date),
            "game_dates": total_dates,
            "total_predictions": total_predictions,
            "total_configs": len(results),
            "phase01_time_seconds": round(phase01_time, 1),
        },
        "results": [],
    }
    if promotion_metadata is not None:
        json_output["sweep_metadata"]["promotion_contract"] = promotion_metadata

    csv_rows = []

    for i, r in enumerate(results, 1):
        m = r.metrics

        json_output["results"].append({
            "config": r.config.to_dict(),
            "metrics": m.to_dict(),
            "elapsed_seconds": round(r.elapsed_seconds, 2),
        })

        row = {
            "tau": r.config.tau,
            "z_max": r.config.z_max,
            "max_weight": r.config.max_weight,
            "edge_threshold": r.config.edge_threshold,
            "kelly_fraction": r.config.kelly_fraction,
            "flat_bet_size": r.config.flat_bet_size,
            "total_bets": m.total_bets,
            "wins": m.wins,
            "losses": m.losses,
            "pushes": m.pushes,
            "hit_rate": round(m.hit_rate, 4),
            "roi": round(m.roi, 4),
            "return_on_capital": round(m.return_on_capital, 4),
            "total_profit": round(m.total_profit, 2),
            "total_staked": round(m.total_staked, 2),
            "sharpe_ratio": round(m.sharpe_ratio, 3),
            "max_drawdown": round(m.max_drawdown, 4),
            "win_streak": m.win_streak,
            "loss_streak": m.loss_streak,
            "elapsed_seconds": round(r.elapsed_seconds, 2),
        }

        # Add per-stat columns
        for stat, stat_data in m.by_stat.items():
            row[f"{stat}_bets"] = stat_data.get("bets", 0)
            row[f"{stat}_roi"] = round(stat_data.get("roi", 0), 4)
            row[f"{stat}_hit_rate"] = round(stat_data.get("hit_rate", 0), 4)

        csv_rows.append(row)

        # Per-config subdirectory
        tau_label = "no_BL" if r.config.tau is None else f"tau{r.config.tau}"
        dir_name = f"config_{i:02d}_{tau_label}_edge{r.config.edge_threshold}_kelly{r.config.kelly_fraction}"
        config_dir = output_dir / dir_name
        config_dir.mkdir(parents=True, exist_ok=True)

        if not r.bets_df.empty:
            r.bets_df.to_csv(config_dir / "bets.csv", index=False)
        if not r.predictions_df.empty:
            r.predictions_df.to_csv(config_dir / "predictions.csv", index=False)
        if not r.all_edges_df.empty:
            r.all_edges_df.to_csv(config_dir / "bookmaker_candidate_edges.csv", index=False)
            r.all_edges_df.to_csv(config_dir / "all_bookmaker_edges.csv", index=False)

        metrics_output = m.to_dict()
        metrics_output["config"] = r.config.to_dict()
        with open(config_dir / "metrics.json", "w") as f:
            json.dump(metrics_output, f, indent=2, default=str)

    with open(output_dir / "sweep_results.json", "w") as f:
        json.dump(json_output, f, indent=2, default=str)

    pd.DataFrame(csv_rows).to_csv(output_dir / "sweep_summary.csv", index=False)
    logger.info(f"Results saved to {output_dir}")
