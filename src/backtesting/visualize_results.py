"""
Visualize backtest results as a comprehensive HTML dashboard.

Generates an interactive single-page HTML file with:
- Portfolio performance charts (bankroll growth + daily P&L)
- Metrics summary (betting, risk, calibration, by-stat, by-edge)
- Enriched bet log with player/team names and game matchups
- Other bookmaker lines comparison (when available)

Usage:
    python src/backtesting/visualize_results.py --results-dir backtest_results/bt_20250126_120000
"""

import argparse
import json
import logging
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

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data Loading
# ---------------------------------------------------------------------------


def load_bets(results_dir: Path) -> pd.DataFrame:
    """Load bets.csv and parse dates."""
    bets_path = results_dir / "bets.csv"
    if not bets_path.exists():
        raise FileNotFoundError(f"Could not find bets.csv in {results_dir}")
    df = pd.read_csv(bets_path)
    df["game_date"] = pd.to_datetime(df["game_date"])
    return df


def load_metrics(results_dir: Path) -> dict:
    """Load metrics.json. Returns empty dict if absent."""
    metrics_path = results_dir / "metrics.json"
    if not metrics_path.exists():
        return {}
    with open(metrics_path) as f:
        return json.load(f)


def load_all_bookmaker_edges(results_dir: Path) -> pd.DataFrame | None:
    """Load all_bookmaker_edges.csv if it exists."""
    path = results_dir / "all_bookmaker_edges.csv"
    if not path.exists():
        return None
    return pd.read_csv(path)


def load_data(results_dir: Path) -> tuple[pd.DataFrame, float]:
    """Load bets.csv and metrics.json (backward-compatible API)."""
    df = load_bets(results_dir)
    metrics = load_metrics(results_dir)
    starting_bankroll = 10000.0
    if "config" in metrics and "starting_bankroll" in metrics["config"]:
        starting_bankroll = metrics["config"]["starting_bankroll"]
    elif "starting_bankroll" in metrics:
        starting_bankroll = metrics["starting_bankroll"]
    return df, starting_bankroll


# ---------------------------------------------------------------------------
# Data Enrichment
# ---------------------------------------------------------------------------


def _get_db_engine():
    """Try to get DB engine; returns None on any failure."""
    try:
        from src.db.client import get_engine

        engine = get_engine()
        # Quick connectivity check
        with engine.connect() as conn:
            conn.execute(__import__("sqlalchemy").text("SELECT 1"))
        return engine
    except Exception:
        return None


def enrich_bets(bets_df: pd.DataFrame, results_dir: Path) -> pd.DataFrame:
    """Add player_name, team_name, matchup columns to bets_df.

    Tries DB first, falls back to predictions.csv, worst case shows IDs.
    """
    df = bets_df.copy()

    # Ensure columns exist with defaults
    for col in ("player_name", "team_name", "matchup"):
        if col not in df.columns:
            df[col] = None

    player_ids = df["player_id"].unique().tolist()
    game_ids = df["game_id"].unique().tolist()

    # Try DB enrichment
    engine = _get_db_engine()
    if engine is not None:
        try:
            return _enrich_from_db(df, engine, player_ids, game_ids)
        except Exception as e:
            logger.warning("DB enrichment failed: %s. Falling back to CSV.", e)

    # Fallback: predictions.csv
    return _enrich_from_csv(df, results_dir)


def _enrich_from_db(
    df: pd.DataFrame, engine, player_ids: list, game_ids: list
) -> pd.DataFrame:
    """Enrich bets from database."""
    from sqlalchemy import text

    query = text("""
        SELECT DISTINCT pgs.player_id, pgs.game_id,
               p.player_name, t.team_name, pgs.matchup
        FROM player_game_stats pgs
        JOIN players p ON pgs.player_id = p.player_id
        LEFT JOIN teams t ON pgs.team_id = t.team_id
        WHERE pgs.player_id = ANY(:player_ids)
          AND pgs.game_id = ANY(:game_ids)
    """)
    with engine.connect() as conn:
        result = conn.execute(
            query, {"player_ids": player_ids, "game_ids": game_ids}
        )
        rows = result.fetchall()

    if not rows:
        return df

    lookup = {}
    for r in rows:
        lookup[(r.player_id, r.game_id)] = {
            "player_name": r.player_name,
            "team_name": r.team_name,
            "matchup": r.matchup,
        }

    for idx, row in df.iterrows():
        key = (row["player_id"], row["game_id"])
        if key in lookup:
            df.at[idx, "player_name"] = lookup[key]["player_name"]
            df.at[idx, "team_name"] = lookup[key]["team_name"]
            df.at[idx, "matchup"] = lookup[key]["matchup"]

    return df


def _enrich_from_csv(df: pd.DataFrame, results_dir: Path) -> pd.DataFrame:
    """Fallback: enrich from predictions.csv."""
    pred_path = results_dir / "predictions.csv"
    if not pred_path.exists():
        return df

    preds = pd.read_csv(pred_path)
    if "player_name" not in preds.columns:
        return df

    # Build lookup from predictions
    name_lookup = {}
    for _, row in preds.drop_duplicates(subset=["player_id", "game_id"]).iterrows():
        name_lookup[(row["player_id"], row["game_id"])] = row.get("player_name", "")

    for idx, row in df.iterrows():
        key = (row["player_id"], row["game_id"])
        if key in name_lookup and pd.isna(df.at[idx, "player_name"]):
            df.at[idx, "player_name"] = name_lookup[key]

    return df


def get_other_bookmaker_lines(
    bets_df: pd.DataFrame, edges_df: pd.DataFrame | None
) -> dict:
    """Build dict of other bookmaker lines for each placed bet.

    Returns: {(player_id, game_id, stat): DataFrame of all bookmaker rows}
    """
    if edges_df is None:
        return {}

    result = {}
    bet_keys = set()
    for _, row in bets_df.iterrows():
        bet_keys.add((row["player_id"], row["game_id"], row["stat"]))

    for key in bet_keys:
        pid, gid, stat = key
        mask = (
            (edges_df["player_id"] == pid)
            & (edges_df["game_id"] == gid)
            & (edges_df["stat"] == stat)
        )
        subset = edges_df[mask]
        if not subset.empty:
            result[key] = subset

    return result


# ---------------------------------------------------------------------------
# HTML / CSS / JS Rendering
# ---------------------------------------------------------------------------


def _render_css() -> str:
    return """
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #f5f6fa; color: #2c3e50; line-height: 1.5;
        }
        .nav {
            position: sticky; top: 0; z-index: 1000;
            background: #2c3e50; padding: 12px 24px;
            display: flex; gap: 24px; align-items: center;
        }
        .nav a { color: #ecf0f1; text-decoration: none; font-weight: 500; font-size: 14px; }
        .nav a:hover { color: #3498db; }
        .nav .title { font-size: 18px; font-weight: 700; margin-right: 32px; }
        .container { max-width: 1400px; margin: 0 auto; padding: 24px; }
        .section { margin-bottom: 32px; }
        .section-title { font-size: 22px; font-weight: 700; margin-bottom: 16px; color: #2c3e50;
                         border-bottom: 2px solid #3498db; padding-bottom: 8px; }

        /* Metric Cards */
        .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 16px; }
        .card {
            background: #fff; border-radius: 8px; padding: 20px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        }
        .card h3 { font-size: 14px; text-transform: uppercase; color: #7f8c8d;
                    letter-spacing: 0.5px; margin-bottom: 12px; }
        .card .metric { display: flex; justify-content: space-between; padding: 4px 0;
                        font-size: 14px; border-bottom: 1px solid #f0f0f0; }
        .card .metric:last-child { border-bottom: none; }
        .card .metric .label { color: #7f8c8d; }
        .card .metric .value { font-weight: 600; }
        .positive { color: #27ae60; }
        .negative { color: #e74c3c; }

        /* Config card */
        .config-grid { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 8px; }
        .config-tag {
            background: #ecf0f1; border-radius: 4px; padding: 4px 10px;
            font-size: 13px; color: #2c3e50;
        }

        /* Bet Table */
        .table-controls { margin-bottom: 12px; display: flex; gap: 12px; align-items: center; }
        .table-controls input {
            padding: 8px 12px; border: 1px solid #ddd; border-radius: 4px;
            font-size: 14px; width: 300px;
        }
        .table-controls .count { font-size: 13px; color: #7f8c8d; }
        table.bet-table {
            width: 100%; border-collapse: collapse; background: #fff;
            border-radius: 8px; overflow: hidden;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08); font-size: 13px;
        }
        table.bet-table th {
            background: #2c3e50; color: #fff; padding: 10px 8px;
            text-align: left; cursor: pointer; user-select: none;
            white-space: nowrap; position: sticky; top: 0;
        }
        table.bet-table th:hover { background: #34495e; }
        table.bet-table th .sort-arrow { margin-left: 4px; opacity: 0.5; }
        table.bet-table td { padding: 8px; border-bottom: 1px solid #f0f0f0; }
        table.bet-table tr:hover { background: #f8f9fa; }
        table.bet-table tr.win { background: #eafaf1; }
        table.bet-table tr.loss { background: #fdedec; }
        table.bet-table tr.push { background: #fef9e7; }
        table.bet-table tr.win:hover { background: #d5f5e3; }
        table.bet-table tr.loss:hover { background: #fadbd8; }

        /* Expandable sub-rows */
        tr.sub-row { display: none; }
        tr.sub-row td { padding: 8px 8px 8px 32px; background: #fafbfc; }
        tr.sub-row table { width: 100%; font-size: 12px; border-collapse: collapse; }
        tr.sub-row table th { background: #ecf0f1; color: #2c3e50; padding: 6px 8px; position: static; }
        tr.sub-row table td { padding: 6px 8px; border-bottom: 1px solid #eee; }
        .expand-btn {
            cursor: pointer; width: 22px; height: 22px; border: 1px solid #bdc3c7;
            border-radius: 3px; background: #fff; font-size: 14px; line-height: 20px;
            text-align: center; color: #7f8c8d;
        }
        .expand-btn:hover { background: #ecf0f1; }

        /* Responsive */
        .table-wrapper { overflow-x: auto; }
    </style>
    """


def _render_table_js() -> str:
    return """
    <script>
    function sortTable(colIdx, dataType) {
        const table = document.getElementById('betTable');
        const tbody = table.querySelector('tbody');
        const rows = Array.from(tbody.querySelectorAll('tr.bet-row'));
        const th = table.querySelectorAll('thead th')[colIdx];
        const asc = th.dataset.sortDir !== 'asc';
        th.dataset.sortDir = asc ? 'asc' : 'desc';

        // Reset all arrows
        table.querySelectorAll('thead th .sort-arrow').forEach(a => a.textContent = '\\u2195');
        th.querySelector('.sort-arrow').textContent = asc ? '\\u2191' : '\\u2193';

        rows.sort((a, b) => {
            let va = a.cells[colIdx].dataset.val || a.cells[colIdx].textContent.trim();
            let vb = b.cells[colIdx].dataset.val || b.cells[colIdx].textContent.trim();
            if (dataType === 'num') {
                va = parseFloat(va) || 0;
                vb = parseFloat(vb) || 0;
            }
            if (va < vb) return asc ? -1 : 1;
            if (va > vb) return asc ? 1 : -1;
            return 0;
        });

        rows.forEach(r => {
            const subId = r.dataset.subrow;
            tbody.appendChild(r);
            if (subId) {
                const sub = document.getElementById(subId);
                if (sub) tbody.appendChild(sub);
            }
        });
    }

    function filterTable() {
        const input = document.getElementById('tableFilter');
        const filter = input.value.toLowerCase();
        const rows = document.querySelectorAll('#betTable tbody tr.bet-row');
        let visible = 0;
        rows.forEach(r => {
            const text = r.textContent.toLowerCase();
            const show = text.includes(filter);
            r.style.display = show ? '' : 'none';
            const subId = r.dataset.subrow;
            if (subId) {
                document.getElementById(subId).style.display = 'none';
            }
            if (show) visible++;
        });
        document.getElementById('visibleCount').textContent = visible + ' of ' + rows.length + ' bets';
    }

    function toggleSubRow(btn, subId) {
        const row = document.getElementById(subId);
        if (row.style.display === 'table-row') {
            row.style.display = 'none';
            btn.textContent = '+';
        } else {
            row.style.display = 'table-row';
            btn.textContent = '\\u2212';
        }
    }
    </script>
    """


# ---------------------------------------------------------------------------
# Section Renderers
# ---------------------------------------------------------------------------


def _render_bankroll_chart(bets_df: pd.DataFrame, starting_bankroll: float) -> str:
    """Render the bankroll growth + daily P&L charts. Returns Plotly HTML div."""
    daily_stats = bets_df.groupby("game_date")["profit"].sum().reset_index()
    daily_stats = daily_stats.sort_values("game_date")
    start_date = daily_stats["game_date"].min() - pd.Timedelta(days=1)

    daily_stats["cumulative_profit"] = daily_stats["profit"].cumsum()
    daily_stats["bankroll"] = starting_bankroll + daily_stats["cumulative_profit"]

    start_row = pd.DataFrame({
        "game_date": [start_date],
        "profit": [0.0],
        "cumulative_profit": [0.0],
        "bankroll": [starting_bankroll],
    })
    chart_data = pd.concat([start_row, daily_stats], ignore_index=True)

    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.03,
        row_heights=[0.7, 0.3],
        subplot_titles=("Bankroll Growth", "Daily P&L"),
    )

    fig.add_trace(
        go.Scatter(
            x=chart_data["game_date"], y=chart_data["bankroll"],
            mode="lines+markers", name="Bankroll",
            line=dict(color="#1f77b4", width=2), marker=dict(size=4),
            fill="tozeroy", fillcolor="rgba(31, 119, 180, 0.1)",
        ),
        row=1, col=1,
    )

    colors = ["#2ca02c" if p >= 0 else "#d62728" for p in chart_data["profit"]]
    fig.add_trace(
        go.Bar(
            x=chart_data["game_date"], y=chart_data["profit"],
            name="Daily P&L", marker_color=colors,
        ),
        row=2, col=1,
    )

    total_return = (chart_data["bankroll"].iloc[-1] - starting_bankroll) / starting_bankroll
    title = (
        f"Start: ${starting_bankroll:,.0f} | "
        f"End: ${chart_data['bankroll'].iloc[-1]:,.0f} | "
        f"ROI: {total_return:+.1%}"
    )
    fig.update_layout(
        title=title, template="plotly_white", height=600,
        showlegend=False, hovermode="x unified",
        margin=dict(l=60, r=20, t=60, b=20),
    )
    fig.update_yaxes(title_text="Account Value ($)", tickprefix="$", row=1, col=1)
    fig.update_yaxes(title_text="Daily P&L ($)", tickprefix="$", row=2, col=1)

    return fig.to_html(full_html=False, include_plotlyjs=False)


def _fmt_val(v, fmt="default") -> str:
    """Format a value for display."""
    if v is None:
        return "N/A"
    if fmt == "pct":
        return f"{v:+.2%}" if v != 0 else "0.00%"
    if fmt == "pct_abs":
        return f"{v:.2%}"
    if fmt == "dollar":
        cls = "positive" if v >= 0 else "negative"
        return f'<span class="{cls}">${v:+,.2f}</span>'
    if fmt == "int":
        return f"{int(v):,}"
    if fmt == "float2":
        return f"{v:.2f}"
    if fmt == "float4":
        return f"{v:.4f}"
    return str(v)


def _metric_row(label: str, value, fmt="default") -> str:
    return f'<div class="metric"><span class="label">{label}</span><span class="value">{_fmt_val(value, fmt)}</span></div>'


def _render_metrics_section(metrics: dict) -> str:
    """Render metrics.json as styled cards."""
    if not metrics:
        return '<div class="card"><p>No metrics data available.</p></div>'

    html_parts = ['<div class="cards">']

    # Betting stats
    bet = metrics.get("betting", {})
    if bet:
        html_parts.append('<div class="card"><h3>Betting</h3>')
        html_parts.append(_metric_row("Total Bets", bet.get("total_bets"), "int"))
        html_parts.append(_metric_row("Wins", bet.get("wins"), "int"))
        html_parts.append(_metric_row("Losses", bet.get("losses"), "int"))
        pushes = bet.get("pushes", 0)
        if pushes:
            html_parts.append(_metric_row("Pushes", pushes, "int"))
        html_parts.append(_metric_row("Hit Rate", bet.get("hit_rate"), "pct_abs"))
        html_parts.append(_metric_row("ROI", bet.get("roi"), "pct"))
        html_parts.append(_metric_row("Return on Capital", bet.get("return_on_capital"), "pct"))
        html_parts.append(_metric_row("Total Staked", bet.get("total_staked"), "dollar"))
        html_parts.append(_metric_row("Total Profit", bet.get("total_profit"), "dollar"))
        html_parts.append("</div>")

    # Risk stats
    risk = metrics.get("risk", {})
    if risk:
        html_parts.append('<div class="card"><h3>Risk</h3>')
        html_parts.append(_metric_row("Sharpe Ratio", risk.get("sharpe_ratio"), "float2"))
        html_parts.append(_metric_row("Max Drawdown", risk.get("max_drawdown"), "pct_abs"))
        html_parts.append(_metric_row("Win Streak", risk.get("win_streak"), "int"))
        html_parts.append(_metric_row("Loss Streak", risk.get("loss_streak"), "int"))
        html_parts.append("</div>")

    # Calibration
    cal = metrics.get("calibration", {})
    if cal:
        html_parts.append('<div class="card"><h3>Calibration</h3>')
        html_parts.append(_metric_row("Overall Gap", cal.get("overall_gap"), "float4"))
        results = cal.get("results", [])
        if results:
            html_parts.append('<table style="width:100%;margin-top:8px;font-size:13px;border-collapse:collapse;">')
            html_parts.append('<tr style="border-bottom:1px solid #eee;">'
                              '<th style="text-align:left;padding:4px;">Quantile</th>'
                              '<th style="text-align:right;padding:4px;">Target</th>'
                              '<th style="text-align:right;padding:4px;">Actual</th>'
                              '<th style="text-align:right;padding:4px;">Gap</th></tr>')
            for r in results:
                gap = r.get("gap", 0)
                gap_cls = "positive" if abs(gap) < 0.02 else "negative"
                html_parts.append(
                    f'<tr style="border-bottom:1px solid #f5f5f5;">'
                    f'<td style="padding:4px;">Q{int(r["quantile"]*100)}</td>'
                    f'<td style="text-align:right;padding:4px;">{r["target"]:.2f}</td>'
                    f'<td style="text-align:right;padding:4px;">{r["actual"]:.3f}</td>'
                    f'<td style="text-align:right;padding:4px;" class="{gap_cls}">{gap:+.4f}</td></tr>'
                )
            html_parts.append("</table>")
        html_parts.append("</div>")

    # By-Stat
    by_stat = metrics.get("by_stat", {})
    if by_stat:
        html_parts.append('<div class="card"><h3>By Stat</h3>')
        html_parts.append('<table style="width:100%;font-size:13px;border-collapse:collapse;">')
        html_parts.append('<tr style="border-bottom:1px solid #eee;">'
                          '<th style="text-align:left;padding:4px;">Stat</th>'
                          '<th style="text-align:right;padding:4px;">Bets</th>'
                          '<th style="text-align:right;padding:4px;">Wins</th>'
                          '<th style="text-align:right;padding:4px;">Hit Rate</th>'
                          '<th style="text-align:right;padding:4px;">ROI</th>'
                          '<th style="text-align:right;padding:4px;">Profit</th></tr>')
        for stat, data in sorted(by_stat.items()):
            roi = data.get("roi", 0)
            roi_cls = "positive" if roi >= 0 else "negative"
            profit = data.get("profit", 0)
            profit_cls = "positive" if profit >= 0 else "negative"
            html_parts.append(
                f'<tr style="border-bottom:1px solid #f5f5f5;">'
                f'<td style="padding:4px;font-weight:600;">{stat.upper()}</td>'
                f'<td style="text-align:right;padding:4px;">{data.get("bets", 0)}</td>'
                f'<td style="text-align:right;padding:4px;">{data.get("wins", 0)}</td>'
                f'<td style="text-align:right;padding:4px;">{data.get("hit_rate", 0):.1%}</td>'
                f'<td style="text-align:right;padding:4px;" class="{roi_cls}">{roi:+.2%}</td>'
                f'<td style="text-align:right;padding:4px;" class="{profit_cls}">${profit:+,.2f}</td></tr>'
            )
        html_parts.append("</table>")
        html_parts.append("</div>")

    # By Edge Bucket
    by_edge = metrics.get("by_edge_bucket", {})
    if by_edge:
        html_parts.append('<div class="card"><h3>By Edge Bucket</h3>')
        html_parts.append('<table style="width:100%;font-size:13px;border-collapse:collapse;">')
        html_parts.append('<tr style="border-bottom:1px solid #eee;">'
                          '<th style="text-align:left;padding:4px;">Edge</th>'
                          '<th style="text-align:right;padding:4px;">Bets</th>'
                          '<th style="text-align:right;padding:4px;">Hit Rate</th>'
                          '<th style="text-align:right;padding:4px;">ROI</th>'
                          '<th style="text-align:right;padding:4px;">Profit</th></tr>')
        for bucket, data in by_edge.items():
            bets = data.get("bets", 0)
            if bets == 0:
                continue
            roi = data.get("roi", 0)
            roi_cls = "positive" if roi >= 0 else "negative"
            profit = data.get("profit", 0)
            profit_cls = "positive" if profit >= 0 else "negative"
            html_parts.append(
                f'<tr style="border-bottom:1px solid #f5f5f5;">'
                f'<td style="padding:4px;font-weight:600;">{bucket}</td>'
                f'<td style="text-align:right;padding:4px;">{bets}</td>'
                f'<td style="text-align:right;padding:4px;">{data.get("hit_rate", 0):.1%}</td>'
                f'<td style="text-align:right;padding:4px;" class="{roi_cls}">{roi:+.2%}</td>'
                f'<td style="text-align:right;padding:4px;" class="{profit_cls}">${profit:+,.2f}</td></tr>'
            )
        html_parts.append("</table>")
        html_parts.append("</div>")

    # Config
    config = metrics.get("config", {})
    if config:
        html_parts.append('<div class="card"><h3>Config</h3>')
        if "edge_threshold" in config:
            html_parts.append(_metric_row("Edge Threshold", config["edge_threshold"], "pct_abs"))
        if "starting_bankroll" in config:
            html_parts.append(_metric_row("Starting Bankroll", config["starting_bankroll"], "dollar"))
        if "kelly_fraction" in config:
            html_parts.append(_metric_row("Kelly Fraction", config["kelly_fraction"], "float4"))
        if "bl_tau" in config:
            html_parts.append(_metric_row("BL Tau", config["bl_tau"], "float4"))
        if "bookmakers" in config:
            html_parts.append('<div style="margin-top:8px;"><span class="label">Bookmakers</span>'
                              '<div class="config-grid">')
            for b in config["bookmakers"]:
                html_parts.append(f'<span class="config-tag">{b}</span>')
            html_parts.append("</div></div>")
        if "stats" in config:
            html_parts.append('<div style="margin-top:8px;"><span class="label">Stats</span>'
                              '<div class="config-grid">')
            for s in config["stats"]:
                html_parts.append(f'<span class="config-tag">{s.upper()}</span>')
            html_parts.append("</div></div>")
        if "allowed_bets" in config and config["allowed_bets"]:
            html_parts.append('<div style="margin-top:8px;"><span class="label">Allowed Bets</span>'
                              '<div class="config-grid">')
            for ab in config["allowed_bets"]:
                html_parts.append(f'<span class="config-tag">{ab}</span>')
            html_parts.append("</div></div>")
        html_parts.append("</div>")

    html_parts.append("</div>")  # close cards grid
    return "\n".join(html_parts)


def _render_bet_table(bets_df: pd.DataFrame, other_lines: dict) -> str:
    """Render the full bet log as an HTML table."""
    if bets_df.empty:
        return '<p style="color:#7f8c8d;">No bets placed during this backtest period.</p>'

    has_bookmaker = "bookmaker" in bets_df.columns and bets_df["bookmaker"].notna().any()
    has_posterior = "posterior_prob" in bets_df.columns and bets_df["posterior_prob"].notna().any()
    has_expand = bool(other_lines)

    # Columns config: (header, data_key, data_type, format_fn)
    headers = []
    if has_expand:
        headers.append(("", None, "str"))  # expand button column
    headers.extend([
        ("Date", "game_date", "str"),
        ("Player", "player_name", "str"),
        ("Team", "team_name", "str"),
        ("Matchup", "matchup", "str"),
        ("Stat", "stat", "str"),
        ("Side", "side", "str"),
        ("Line", "line", "num"),
        ("Odds", "odds", "num"),
    ])
    if has_bookmaker:
        headers.append(("Bookmaker", "bookmaker", "str"))
    headers.extend([
        ("Stake", "stake", "num"),
        ("Model Prob", "model_prob", "num"),
        ("Implied Prob", "implied_prob", "num"),
        ("Edge", "edge", "num"),
    ])
    if has_posterior:
        headers.append(("Posterior Prob", "posterior_prob", "num"))
    headers.extend([
        ("Actual", "actual", "num"),
        ("Outcome", "outcome", "str"),
        ("Profit", "profit", "num"),
    ])

    html = []
    # Controls
    html.append('<div class="table-controls">')
    html.append('<input type="text" id="tableFilter" placeholder="Filter bets..." oninput="filterTable()">')
    html.append(f'<span class="count" id="visibleCount">{len(bets_df)} of {len(bets_df)} bets</span>')
    html.append("</div>")

    # Table
    html.append('<div class="table-wrapper">')
    html.append('<table class="bet-table" id="betTable"><thead><tr>')
    for i, (label, key, dtype) in enumerate(headers):
        if key is None:
            html.append("<th></th>")
        else:
            html.append(f'<th onclick="sortTable({i}, \'{dtype}\')">{label}<span class="sort-arrow">\u2195</span></th>')
    html.append("</tr></thead><tbody>")

    # Sort by date descending by default
    sorted_df = bets_df.sort_values("game_date", ascending=False).reset_index(drop=True)

    for idx, row in sorted_df.iterrows():
        outcome = str(row.get("outcome", "")).lower()
        row_cls = outcome if outcome in ("win", "loss", "push") else ""
        bet_key = (row["player_id"], row["game_id"], row["stat"])
        sub_id = f"sub_{idx}" if has_expand and bet_key in other_lines else ""

        html.append(f'<tr class="bet-row {row_cls}" data-subrow="{sub_id}">')
        for label, key, dtype in headers:
            if key is None:
                # Expand button
                if sub_id:
                    html.append(f'<td><button class="expand-btn" onclick="toggleSubRow(this, \'{sub_id}\')">+</button></td>')
                else:
                    html.append("<td></td>")
                continue

            val = row.get(key, "")
            display = ""
            data_val = ""

            if key == "game_date":
                display = pd.Timestamp(val).strftime("%Y-%m-%d") if pd.notna(val) else ""
                data_val = display
            elif key in ("player_name", "team_name", "matchup"):
                display = str(val) if pd.notna(val) else str(row.get("player_id", "")) if key == "player_name" else "N/A"
                data_val = display
            elif key == "stat":
                display = str(val).upper()
                data_val = display
            elif key == "side":
                display = str(val).capitalize() if pd.notna(val) else ""
                data_val = display
            elif key == "line":
                display = f"{val:.1f}" if pd.notna(val) else ""
                data_val = f"{val}" if pd.notna(val) else "0"
            elif key == "odds":
                display = f"{int(val):+d}" if pd.notna(val) else ""
                data_val = f"{val}" if pd.notna(val) else "0"
            elif key == "bookmaker":
                display = str(val) if pd.notna(val) else "N/A"
                data_val = display
            elif key == "stake":
                display = f"${val:.2f}" if pd.notna(val) else ""
                data_val = f"{val}" if pd.notna(val) else "0"
            elif key in ("model_prob", "implied_prob", "posterior_prob"):
                display = f"{val:.1%}" if pd.notna(val) else "N/A"
                data_val = f"{val}" if pd.notna(val) else "0"
            elif key == "edge":
                if pd.notna(val):
                    cls = "positive" if val > 0 else "negative"
                    display = f'<span class="{cls}">{val:+.1%}</span>'
                    data_val = f"{val}"
                else:
                    display = "N/A"
                    data_val = "0"
            elif key == "actual":
                display = f"{val:.0f}" if pd.notna(val) else ""
                data_val = f"{val}" if pd.notna(val) else "0"
            elif key == "outcome":
                display = str(val).upper() if pd.notna(val) else ""
                data_val = display
            elif key == "profit":
                if pd.notna(val):
                    cls = "positive" if val >= 0 else "negative"
                    display = f'<span class="{cls}">${val:+,.2f}</span>'
                    data_val = f"{val}"
                else:
                    display = ""
                    data_val = "0"
            else:
                display = str(val) if pd.notna(val) else ""
                data_val = display

            html.append(f'<td data-val="{data_val}">{display}</td>')
        html.append("</tr>")

        # Sub-row for other bookmaker lines
        if sub_id and bet_key in other_lines:
            edges_subset = other_lines[bet_key]
            colspan = len(headers)
            html.append(f'<tr class="sub-row" id="{sub_id}"><td colspan="{colspan}">')
            html.append('<table><thead><tr>'
                        '<th>Bookmaker</th><th>Line</th>'
                        '<th>Over Odds</th><th>Under Odds</th>'
                        '<th>Over Edge</th><th>Under Edge</th>'
                        '</tr></thead><tbody>')
            for _, erow in edges_subset.iterrows():
                bk = erow.get("bookmaker", "N/A")
                line_val = erow.get("line", "")
                over_odds = erow.get("over_odds", "")
                under_odds = erow.get("under_odds", "")
                over_edge = erow.get("over_edge", 0)
                under_edge = erow.get("under_edge", 0)

                oe_cls = "positive" if over_edge > 0 else "negative"
                ue_cls = "positive" if under_edge > 0 else "negative"

                over_odds_fmt = f"{int(over_odds):+d}" if pd.notna(over_odds) and over_odds != "" else "N/A"
                under_odds_fmt = f"{int(under_odds):+d}" if pd.notna(under_odds) and under_odds != "" else "N/A"

                html.append(
                    f'<tr><td>{bk}</td><td>{line_val}</td>'
                    f'<td>{over_odds_fmt}</td><td>{under_odds_fmt}</td>'
                    f'<td class="{oe_cls}">{over_edge:+.2%}</td>'
                    f'<td class="{ue_cls}">{under_edge:+.2%}</td></tr>'
                )
            html.append("</tbody></table></td></tr>")

    html.append("</tbody></table></div>")
    return "\n".join(html)


# ---------------------------------------------------------------------------
# Dashboard Assembly
# ---------------------------------------------------------------------------


def generate_dashboard(
    bets_df: pd.DataFrame,
    metrics: dict,
    starting_bankroll: float,
    other_lines: dict,
    output_path: Path,
) -> None:
    """Assemble and write the full HTML dashboard."""
    # Render each section, catching errors gracefully
    def _safe(fn, *args, fallback=""):
        try:
            return fn(*args)
        except Exception as e:
            return f'<div class="card"><p style="color:#e74c3c;">Error: {e}</p></div>'

    chart_html = _safe(_render_bankroll_chart, bets_df, starting_bankroll)
    metrics_html = _safe(_render_metrics_section, metrics)
    table_html = _safe(_render_bet_table, bets_df, other_lines)

    # Determine backtest directory name for title
    dir_name = output_path.parent.name if output_path.parent.name.startswith("bt_") else "Backtest"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Dashboard - {dir_name}</title>
    <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
    {_render_css()}
</head>
<body>
    <nav class="nav">
        <span class="title">Backtest Dashboard</span>
        <a href="#performance">Performance</a>
        <a href="#metrics">Metrics</a>
        <a href="#bets">Bet Log</a>
    </nav>

    <div class="container">
        <div class="section" id="performance">
            <h2 class="section-title">Portfolio Performance</h2>
            {chart_html}
        </div>

        <div class="section" id="metrics">
            <h2 class="section-title">Metrics Summary</h2>
            {metrics_html}
        </div>

        <div class="section" id="bets">
            <h2 class="section-title">Bet Log</h2>
            {table_html}
        </div>
    </div>

    {_render_table_js()}
</body>
</html>"""

    output_path.write_text(html, encoding="utf-8")
    print(f"Dashboard saved to: {output_path}")


def generate_chart(df: pd.DataFrame, starting_bankroll: float, output_path: Path):
    """Generate bankroll chart only (backward-compatible API)."""
    chart_html = _render_bankroll_chart(df, starting_bankroll)
    html = f"""<!DOCTYPE html>
<html><head>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
</head><body>{chart_html}</body></html>"""
    output_path.write_text(html, encoding="utf-8")
    print(f"Chart saved to: {output_path}")


# ---------------------------------------------------------------------------
# CLI Entry Point
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="Visualize Backtest Results")
    parser.add_argument("--results-dir", type=str, required=True, help="Path to backtest results directory")
    parser.add_argument("--no-browser", action="store_true", help="Don't auto-open in browser")
    args = parser.parse_args()

    results_dir = Path(args.results_dir)
    if not results_dir.exists():
        print(f"Error: Directory not found: {results_dir}")
        return

    print(f"Processing results from {results_dir}...")

    try:
        # Load data
        bets_df = load_bets(results_dir)
        metrics = load_metrics(results_dir)
        edges_df = load_all_bookmaker_edges(results_dir)

        starting_bankroll = 10000.0
        config = metrics.get("config", {})
        if "starting_bankroll" in config:
            starting_bankroll = config["starting_bankroll"]
        elif "starting_bankroll" in metrics:
            starting_bankroll = metrics["starting_bankroll"]

        # Enrich
        print("Enriching bet data...")
        bets_df = enrich_bets(bets_df, results_dir)
        other_lines = get_other_bookmaker_lines(bets_df, edges_df)

        # Generate
        output_file = results_dir / "dashboard.html"
        generate_dashboard(bets_df, metrics, starting_bankroll, other_lines, output_file)

        if not args.no_browser:
            webbrowser.open(output_file.absolute().as_uri())

    except Exception as e:
        print(f"An error occurred: {e}")
        raise


if __name__ == "__main__":
    main()
