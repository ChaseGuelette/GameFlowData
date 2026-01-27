"""
Visualize backtest results using Plotly.
Generates an interactive HTML chart showing bankroll growth and daily P&L.

Usage:
    python src/backtesting/visualize_results.py --results-dir backtest_results/bt_20250126_120000
"""

import argparse
import json
import webbrowser
from pathlib import Path

import pandas as pd

try:
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
except ImportError:
    print("Error: 'plotly' is required for this script.")
    print("Please install it using: pip install plotly")
    exit(1)


def load_data(results_dir: Path) -> tuple[pd.DataFrame, float]:
    """Load bets.csv and metrics.json."""
    bets_path = results_dir / "bets.csv"
    metrics_path = results_dir / "metrics.json"

    if not bets_path.exists():
        raise FileNotFoundError(f"Could not find bets.csv in {results_dir}")

    # Load bets
    df = pd.read_csv(bets_path)
    df["game_date"] = pd.to_datetime(df["game_date"])

    # Load starting bankroll from metrics config if available
    starting_bankroll = 10000.0  # Default
    if metrics_path.exists():
        with open(metrics_path, "r") as f:
            metrics = json.load(f)
            # Try to find starting bankroll in nested config or root
            if "config" in metrics and "starting_bankroll" in metrics["config"]:
                starting_bankroll = metrics["config"]["starting_bankroll"]
            elif "starting_bankroll" in metrics:
                starting_bankroll = metrics["starting_bankroll"]

    return df, starting_bankroll


def generate_chart(df: pd.DataFrame, starting_bankroll: float, output_path: Path):
    """Generate interactive Plotly chart."""
    # 1. Aggregate Daily P&L
    daily_stats = df.groupby("game_date")["profit"].sum().reset_index()
    daily_stats = daily_stats.sort_values("game_date")

    # 2. Calculate Cumulative Bankroll
    # We need a starting point. Let's create a row for "Day 0"
    start_date = daily_stats["game_date"].min() - pd.Timedelta(days=1)
    
    # Calculate cumulative profit
    daily_stats["cumulative_profit"] = daily_stats["profit"].cumsum()
    daily_stats["bankroll"] = starting_bankroll + daily_stats["cumulative_profit"]

    # Prepend start point
    start_row = pd.DataFrame(
        {
            "game_date": [start_date],
            "profit": [0.0],
            "cumulative_profit": [0.0],
            "bankroll": [starting_bankroll],
        }
    )
    chart_data = pd.concat([start_row, daily_stats], ignore_index=True)

    # 3. Create Plot
    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=[0.7, 0.3],
        subplot_titles=("Bankroll Growth", "Daily P&L"),
    )

    # Upper Chart: Bankroll Curve
    fig.add_trace(
        go.Scatter(
            x=chart_data["game_date"],
            y=chart_data["bankroll"],
            mode="lines+markers",
            name="Bankroll",
            line=dict(color="#1f77b4", width=2),
            marker=dict(size=4),
            fill="tozeroy",
            fillcolor="rgba(31, 119, 180, 0.1)",
        ),
        row=1,
        col=1,
    )

    # Lower Chart: Daily P&L Bars
    colors = ["#2ca02c" if p >= 0 else "#d62728" for p in chart_data["profit"]]
    fig.add_trace(
        go.Bar(
            x=chart_data["game_date"],
            y=chart_data["profit"],
            name="Daily P&L",
            marker_color=colors,
        ),
        row=2,
        col=1,
    )

    # Layout Styling
    total_return = (chart_data["bankroll"].iloc[-1] - starting_bankroll) / starting_bankroll
    title_text = f"Backtest Results | Start: ${starting_bankroll:,.0f} | End: ${chart_data['bankroll'].iloc[-1]:,.0f} | ROI: {total_return:+.1%}"

    fig.update_layout(
        title=title_text,
        template="plotly_white",
        height=800,
        showlegend=False,
        hovermode="x unified",
    )

    # Y-Axes Formatting
    fig.update_yaxes(title_text="Account Value ($)", tickprefix="$", row=1, col=1)
    fig.update_yaxes(title_text="Daily P&L ($)", tickprefix="$", row=2, col=1)

    # Save
    fig.write_html(output_path)
    print(f"Chart saved to: {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Visualize Backtest Results")
    parser.add_argument("--results-dir", type=str, required=True, help="Path to backtest results directory")
    args = parser.parse_args()

    results_dir = Path(args.results_dir)
    if not results_dir.exists():
        print(f"Error: Directory not found: {results_dir}")
        return

    print(f"Processing results from {results_dir}...")
    
    try:
        df, starting_bankroll = load_data(results_dir)
        
        output_file = results_dir / "bankroll_chart.html"
        generate_chart(df, starting_bankroll, output_file)
        
        # Open in browser
        webbrowser.open(output_file.absolute().as_uri())
        
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
