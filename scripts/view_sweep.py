#!/usr/bin/env python3
"""Pretty-print a sweep_summary.csv in the terminal.

Usage:
    python scripts/view_sweep.py backtest_results/playoffs_sweep_2025/sweep_summary.csv
    python scripts/view_sweep.py backtest_results/playoffs_sweep_2025/sweep_summary.csv --min-bets 100
    python scripts/view_sweep.py backtest_results/playoffs_sweep_2025/sweep_summary.csv --sort sharpe
    python scripts/view_sweep.py backtest_results/playoffs_sweep_2025/sweep_summary.csv --top 30
"""

import argparse
import sys
from pathlib import Path

import pandas as pd
from rich import box
from rich.console import Console
from rich.table import Table
from rich.text import Text

console = Console(width=200)


def color_roi(val: float) -> Text:
    pct = val * 100
    s = f"{pct:+.1f}%"
    if pct >= 15:
        return Text(s, style="bold green")
    elif pct >= 8:
        return Text(s, style="green")
    elif pct >= 0:
        return Text(s, style="yellow")
    else:
        return Text(s, style="red")


def color_hit(val: float) -> Text:
    pct = val * 100
    s = f"{pct:.1f}%"
    if pct >= 63:
        return Text(s, style="bold green")
    elif pct >= 57:
        return Text(s, style="green")
    elif pct >= 53:
        return Text(s, style="yellow")
    else:
        return Text(s, style="red")


def color_sharpe(val: float) -> Text:
    s = f"{val:.2f}"
    if val >= 2.0:
        return Text(s, style="bold green")
    elif val >= 1.0:
        return Text(s, style="green")
    elif val >= 0.5:
        return Text(s, style="yellow")
    else:
        return Text(s, style="red")


def color_dd(val: float) -> Text:
    pct = val * 100
    s = f"{pct:.1f}%"
    if pct <= 10:
        return Text(s, style="bold green")
    elif pct <= 20:
        return Text(s, style="green")
    elif pct <= 35:
        return Text(s, style="yellow")
    else:
        return Text(s, style="red")


def color_stat_roi(val) -> Text:
    if pd.isna(val):
        return Text("—", style="dim")
    pct = val * 100
    s = f"{pct:+.1f}%"
    if pct >= 15:
        return Text(s, style="bold green")
    elif pct >= 5:
        return Text(s, style="green")
    elif pct >= 0:
        return Text(s, style="yellow")
    else:
        return Text(s, style="red")


def fmt_tau(val) -> str:
    if pd.isna(val):
        return "none"
    return str(val)


def main():
    parser = argparse.ArgumentParser(description="Pretty-print sweep_summary.csv")
    parser.add_argument("csv", help="Path to sweep_summary.csv")
    parser.add_argument("--min-bets", type=int, default=50, help="Minimum bets to show (default: 50)")
    parser.add_argument("--sort", choices=["roi", "sharpe", "bets", "drawdown"], default="roi",
                        help="Sort column (default: roi)")
    parser.add_argument("--top", type=int, default=25, help="Number of rows to show (default: 25)")
    parser.add_argument("--all", action="store_true", help="Show all rows (ignores --min-bets and --top)")
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        console.print(f"[red]File not found:[/red] {csv_path}")
        sys.exit(1)

    df = pd.read_csv(csv_path)

    # Filter
    if not args.all:
        df = df[df["total_bets"] >= args.min_bets]

    # Sort
    sort_map = {
        "roi": ("roi", False),
        "sharpe": ("sharpe_ratio", False),
        "bets": ("total_bets", False),
        "drawdown": ("max_drawdown", True),
    }
    sort_col, asc = sort_map[args.sort]
    df = df.sort_values(sort_col, ascending=asc)

    if not args.all:
        df = df.head(args.top)

    total_in_file = len(pd.read_csv(csv_path))

    # Header summary
    console.print()
    console.print(f"[bold cyan]{csv_path.parent.name} / {csv_path.name}[/bold cyan]", justify="center")
    console.print(
        f"[dim]{total_in_file} total configs  •  "
        f"showing top {len(df)} with {args.min_bets}+ bets  •  "
        f"sorted by {args.sort}[/dim]",
        justify="center",
    )
    console.print()

    # Detect stat columns
    stat_cols = sorted({c.split("_")[0] for c in df.columns if c.endswith("_roi") and not c.startswith("total")})

    table = Table(
        box=box.ROUNDED,
        show_header=True,
        header_style="bold white on dark_blue",
        border_style="dim",
        row_styles=["", "dim"],
        pad_edge=True,
    )

    table.add_column("#", style="dim", justify="right", width=3)
    table.add_column("tau", justify="center", width=6)
    table.add_column("z_max", justify="center", width=5)
    table.add_column("mw", justify="center", width=5)
    table.add_column("edge", justify="center", width=5)
    table.add_column("bets", justify="right", width=5)
    table.add_column("hit%", justify="right", width=6)
    table.add_column("ROI", justify="right", width=7)
    table.add_column("profit", justify="right", width=10)
    table.add_column("sharpe", justify="right", width=7)
    table.add_column("maxDD", justify="right", width=7)
    for s in stat_cols:
        table.add_column(f"{s.upper()}", justify="right", width=8)

    for rank, (_, row) in enumerate(df.iterrows(), 1):
        stat_cells = [color_stat_roi(row.get(f"{s}_roi")) for s in stat_cols]
        profit = row["total_profit"]
        profit_str = f"${profit:>+,.0f}"
        profit_text = Text(profit_str, style="bold green" if profit > 0 else "red")

        table.add_row(
            str(rank),
            fmt_tau(row["tau"]),
            str(row["z_max"]),
            str(row["max_weight"]),
            str(row["edge_threshold"]),
            str(int(row["total_bets"])),
            color_hit(row["hit_rate"]),
            color_roi(row["roi"]),
            profit_text,
            color_sharpe(row["sharpe_ratio"]),
            color_dd(row["max_drawdown"]),
            *stat_cells,
        )

    console.print(table)

    # Summary stats
    best_roi = df.iloc[0] if args.sort == "roi" else df.loc[df["roi"].idxmax()]
    best_sharpe = df.loc[df["sharpe_ratio"].idxmax()]
    console.print(
        f"[dim]Best ROI:[/dim] tau={fmt_tau(best_roi['tau'])} "
        f"edge={best_roi['edge_threshold']} → "
        f"[bold green]{best_roi['roi']*100:+.1f}% ROI[/bold green], "
        f"{int(best_roi['total_bets'])} bets, "
        f"Sharpe {best_roi['sharpe_ratio']:.2f}"
    )
    console.print(
        f"[dim]Best Sharpe:[/dim] tau={fmt_tau(best_sharpe['tau'])} "
        f"edge={best_sharpe['edge_threshold']} → "
        f"Sharpe [bold green]{best_sharpe['sharpe_ratio']:.2f}[/bold green], "
        f"{int(best_sharpe['total_bets'])} bets, "
        f"{best_sharpe['roi']*100:+.1f}% ROI"
    )
    console.print()


if __name__ == "__main__":
    main()
