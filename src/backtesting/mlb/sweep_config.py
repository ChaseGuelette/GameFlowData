"""Typed configuration and CLI parsing helpers for MLB backtest sweeps."""

from __future__ import annotations

import argparse
import itertools
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path


@dataclass
class SweepConfig:
    """One point in the parameter sweep grid."""

    tau: float | None  # None = no BL blending (baseline)
    edge_threshold: float
    kelly_fraction: float
    z_max: float = 1.0
    max_weight: float = 0.50
    flat_bet_size: float | None = None
    book_routing_policy: str = "lowest_vig"

    @property
    def label(self) -> str:
        sizing = f"flat=${self.flat_bet_size:g}" if self.flat_bet_size is not None else f"kelly={self.kelly_fraction}"
        if self.tau is None:
            return f"no_BL | edge={self.edge_threshold} | {sizing}"
        mw = f" mw={self.max_weight}" if self.max_weight != 0.50 else ""
        return f"tau={self.tau} z_max={self.z_max}{mw} | edge={self.edge_threshold} | {sizing}"

    def to_dict(self) -> dict:
        return {
            "tau": self.tau,
            "z_max": self.z_max,
            "max_weight": self.max_weight,
            "edge_threshold": self.edge_threshold,
            "kelly_fraction": self.kelly_fraction,
            "flat_bet_size": self.flat_bet_size,
            "book_routing_policy": self.book_routing_policy,
        }


@dataclass(frozen=True)
class QuoteCleanConfig:
    """Quote-clean line-selection options parsed from CLI flags."""

    enabled: bool
    cutoff_time_et: str = "13:30"
    decision_policy: str = "fixed_et"
    relative_minutes: int = 60
    line_source: str = "mlb_raw_player_props"


@dataclass
class SweepCliConfig:
    """Typed representation of the sweep CLI arguments.

    This object is intentionally parse-only. It must not construct engines,
    feature stores, model suites, or perform DB/model work.
    """

    start_date: date
    end_date: date
    tau_values: list[float | None]
    edge_thresholds: list[float]
    kelly_fractions: list[float]
    z_max_values: list[float]
    max_weight_values: list[float]
    sweep_grid: list[SweepConfig]
    model_dir: str
    n_samples: int
    stats: list[str]
    starting_bankroll: float
    max_bet_pct: float | None
    flat_bet: float | None
    output_dir: Path | None
    local: bool
    combined: bool
    direction: str
    cli_allowed_bets: set[tuple[str, str]] | None
    quote_clean: QuoteCleanConfig


def parse_tau_values(raw_tau_values: list[str]) -> list[float | None]:
    """Parse BL tau CLI values, preserving 'none' as the no-BL baseline."""
    tau_values: list[float | None] = []
    for value in raw_tau_values:
        if value.lower() == "none":
            tau_values.append(None)
        else:
            tau_values.append(float(value))
    return tau_values


def build_sweep_grid(
    tau_values: list[float | None],
    edge_thresholds: list[float],
    kelly_fractions: list[float],
    z_max_values: list[float] | None = None,
    max_weight_values: list[float] | None = None,
    flat_bet_size: float | None = None,
    book_routing_policy: str = "lowest_vig",
) -> list[SweepConfig]:
    if z_max_values is None:
        z_max_values = [1.0]
    if max_weight_values is None:
        max_weight_values = [0.50]

    configs = []
    for tau, edge, kelly, z_max, mw in itertools.product(
        tau_values,
        edge_thresholds,
        kelly_fractions,
        z_max_values,
        max_weight_values,
    ):
        if tau is None and (z_max != z_max_values[0] or mw != max_weight_values[0]):
            continue
        configs.append(
            SweepConfig(
                tau=tau,
                edge_threshold=edge,
                kelly_fraction=kelly,
                z_max=z_max,
                max_weight=mw,
                flat_bet_size=flat_bet_size,
                book_routing_policy=book_routing_policy,
            )
        )
    return configs


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="MLB Backtest Parameter Sweep",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("--start", type=str, required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, required=True, help="End date (YYYY-MM-DD)")

    # Sweep grid
    parser.add_argument(
        "--tau",
        type=str,
        nargs="+",
        default=["none", "0.03", "0.05", "0.10", "0.25"],
        help="BL tau values. Use 'none' for no-BL baseline.",
    )
    parser.add_argument("--edge", type=float, nargs="+", default=[0.05, 0.08, 0.10])
    parser.add_argument("--kelly", type=float, nargs="+", default=[0.125])
    parser.add_argument("--z-max", type=float, nargs="+", default=[1.0])
    parser.add_argument(
        "--max-weight",
        type=float,
        nargs="+",
        default=[0.50],
        help="BL max blending weight (0.50=default, higher=more model influence)",
    )

    # Model / data
    parser.add_argument("--model-dir", type=str, default="src/models/mlb/artifacts")
    parser.add_argument("--n-samples", type=int, default=5000, help="Monte Carlo samples")
    parser.add_argument(
        "--stats",
        nargs="+",
        default=["pitcher_strikeouts", "batter_hits", "batter_rbis"],
    )
    parser.add_argument("--starting-bankroll", type=float, default=10000.0)
    parser.add_argument("--max-bet-pct", type=float, default=None)
    parser.add_argument(
        "--flat",
        "--flat-bet",
        dest="flat_bet",
        type=float,
        default=None,
        help="Use fixed dollar stake per bet instead of Kelly sizing (e.g. --flat 100).",
    )
    parser.add_argument("--output-dir", type=str, default=None)
    parser.add_argument(
        "--local",
        action="store_true",
        help="Use local Postgres (LOCAL_DATABASE_URL) instead of Supabase",
    )
    parser.add_argument(
        "--combined",
        action="store_true",
        help=(
            "Run combined backtest using per-stat optimal BL configs from mlb_stat_config.py. "
            "Ignores --tau, --edge, --z-max, --max-weight when set."
        ),
    )
    parser.add_argument(
        "--direction",
        choices=["over", "under", "both"],
        default="both",
        help=(
            "Restrict bet direction for all stats (default: both). "
            "In --combined mode, per-stat allowed_directions from mlb_stat_config.py "
            "are also applied on top of this filter."
        ),
    )
    parser.add_argument(
        "--quote-clean",
        action="store_true",
        help=(
            "Use production-equivalent quote selection: latest snapshot at/before "
            "--quote-cutoff-time-et per book/line/outcome, then lowest-vig line. "
            "Without this flag, preserves legacy optimistic line aggregation."
        ),
    )
    parser.add_argument(
        "--quote-cutoff-time-et",
        type=str,
        default="13:30",
        help="ET cutoff time for --quote-clean historical replay (HH:MM, default 13:30).",
    )
    parser.add_argument(
        "--quote-decision-policy",
        choices=["fixed_et", "skip_early_fixed_et", "relative_to_commence", "slate_or_tminus"],
        default="fixed_et",
        help=(
            "How --quote-clean chooses decision time. fixed_et preserves legacy one-time-per-day behavior; "
            "skip_early_fixed_et drops games already started by the fixed time; relative_to_commence uses "
            "T-minus per game; slate_or_tminus uses 09:30/13:30/17:30 ET slates with T-minus fallback."
        ),
    )
    parser.add_argument(
        "--quote-relative-minutes",
        type=int,
        default=60,
        help="Minutes before commence for relative_to_commence and fallback in slate_or_tminus.",
    )
    parser.add_argument(
        "--line-source",
        choices=["mlb_raw_player_props", "mlb_player_props_clv_snapshots"],
        default="mlb_raw_player_props",
        help="Odds table for quote-clean line selection. Dense CLV table requires linked game_id/player_id.",
    )
    parser.add_argument(
        "--book-routing-policy",
        choices=["lowest_vig", "preferred_book_first"],
        default="lowest_vig",
        help=(
            "Book selection policy after candidate edge calculation. lowest_vig preserves legacy behavior; "
            "preferred_book_first selects the best preferred/reference book that clears edge before fallback books."
        ),
    )
    return parser


def parse_sweep_cli_config(args: argparse.Namespace) -> SweepCliConfig:
    """Translate parsed CLI args into a typed sweep config without side effects."""
    tau_values = parse_tau_values(args.tau)
    start_date = datetime.strptime(args.start, "%Y-%m-%d").date()
    end_date = datetime.strptime(args.end, "%Y-%m-%d").date()

    if args.direction == "both":
        cli_allowed_bets: set[tuple[str, str]] | None = None
    else:
        cli_allowed_bets = {(stat, args.direction) for stat in args.stats}

    sweep_grid = build_sweep_grid(
        tau_values,
        args.edge,
        args.kelly,
        args.z_max,
        args.max_weight,
        flat_bet_size=args.flat_bet,
        book_routing_policy=args.book_routing_policy,
    )

    output_dir = Path(args.output_dir) if args.output_dir else None

    return SweepCliConfig(
        start_date=start_date,
        end_date=end_date,
        tau_values=tau_values,
        edge_thresholds=args.edge,
        kelly_fractions=args.kelly,
        z_max_values=args.z_max,
        max_weight_values=args.max_weight,
        sweep_grid=sweep_grid,
        model_dir=args.model_dir,
        n_samples=args.n_samples,
        stats=args.stats,
        starting_bankroll=args.starting_bankroll,
        max_bet_pct=args.max_bet_pct,
        flat_bet=args.flat_bet,
        output_dir=output_dir,
        local=args.local,
        combined=args.combined,
        direction=args.direction,
        cli_allowed_bets=cli_allowed_bets,
        quote_clean=QuoteCleanConfig(
            enabled=args.quote_clean,
            cutoff_time_et=args.quote_cutoff_time_et,
            decision_policy=args.quote_decision_policy,
            relative_minutes=args.quote_relative_minutes,
            line_source=args.line_source,
        ),
    )
