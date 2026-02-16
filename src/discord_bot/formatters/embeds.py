"""Discord embed builders for GameFlowData bot."""

from datetime import date, datetime

import discord


# Color scheme
COLORS = {
    "success": discord.Color.green(),
    "warning": discord.Color.orange(),
    "info": discord.Color.blue(),
    "gold": discord.Color.gold(),
    "error": discord.Color.red(),
}


def _format_edge(edge: float | None) -> str:
    """Format edge as percentage string."""
    if edge is None:
        return "—"
    return f"{edge:+.1%}"


def _format_prob(prob: float | None) -> str:
    """Format probability as percentage string."""
    if prob is None:
        return "—"
    return f"{prob:.1%}"


def _get_best_side(prediction: dict) -> tuple[str, float, float]:
    """Determine the best side to bet.

    Returns:
        Tuple of (side, edge, prob)
    """
    over_edge = prediction.get("over_edge") or 0
    under_edge = prediction.get("under_edge") or 0

    if over_edge >= under_edge:
        return "Over", over_edge, prediction.get("over_prob") or 0.5
    else:
        return "Under", under_edge, prediction.get("under_prob") or 0.5


def _format_game_time(game_time) -> str:
    """Format game time for display."""
    if game_time is None:
        return ""

    if isinstance(game_time, datetime):
        return game_time.strftime("%I:%M %p ET")
    elif isinstance(game_time, str):
        try:
            dt = datetime.fromisoformat(game_time.replace("Z", "+00:00"))
            return dt.strftime("%I:%M %p ET")
        except (ValueError, AttributeError):
            return game_time

    return str(game_time)


def create_picks_embed(picks: list[dict], prediction_date: date | None = None) -> discord.Embed:
    """Create an embed for daily picks.

    Args:
        picks: List of prediction dicts
        prediction_date: Date of predictions

    Returns:
        Discord embed with picks
    """
    if prediction_date is None:
        prediction_date = date.today()

    embed = discord.Embed(
        title=f"Top Picks for {prediction_date.strftime('%b %d, %Y')}",
        color=COLORS["success"],
        timestamp=datetime.now(),
    )

    if not picks:
        embed.description = "No predictions available for this date."
        return embed

    for pick in picks[:10]:
        side, edge, prob = _get_best_side(pick)
        line = pick.get("line", "—")
        stat = pick.get("stat", "").upper()
        player = pick.get("player_name", "Unknown")
        team = pick.get("team", "")
        opponent = pick.get("opponent", "")
        game_time = _format_game_time(pick.get("game_time"))

        matchup = f"{team} vs {opponent}" if team and opponent else ""

        # Field name: Player - STAT
        name = f"{player} — {stat}"

        # Field value: Side Line | Edge | Matchup
        value_parts = [f"**{side} {line}** | Edge: {_format_edge(edge)}"]
        if matchup:
            value_parts.append(matchup)
        if game_time:
            value_parts.append(game_time)

        embed.add_field(
            name=name,
            value=" | ".join(value_parts),
            inline=False,
        )

    embed.set_footer(text=f"Showing top {len(picks)} picks by edge")
    return embed


def create_player_embed(predictions: list[dict]) -> discord.Embed:
    """Create an embed for player-specific predictions.

    Args:
        predictions: List of prediction dicts for a single player

    Returns:
        Discord embed with player predictions
    """
    if not predictions:
        embed = discord.Embed(
            title="Player Not Found",
            description="No predictions found for this player today.",
            color=COLORS["warning"],
        )
        return embed

    player = predictions[0].get("player_name") or predictions[0].get("matched_name", "Unknown")
    team = predictions[0].get("team", "")
    opponent = predictions[0].get("opponent", "")
    game_time = _format_game_time(predictions[0].get("game_time"))

    matchup = f"{team} vs {opponent}" if team and opponent else "Today's Game"

    embed = discord.Embed(
        title=f"{player}",
        description=f"{matchup}" + (f" | {game_time}" if game_time else ""),
        color=COLORS["info"],
        timestamp=datetime.now(),
    )

    for pred in predictions:
        stat = pred.get("stat", "").upper()
        line = pred.get("line", "—")
        side, edge, prob = _get_best_side(pred)

        q50 = pred.get("pred_q50")
        q10 = pred.get("pred_q10")
        q90 = pred.get("pred_q90")

        # Build value string
        value_lines = [
            f"**{side} {line}** | Edge: {_format_edge(edge)}",
            f"Model Prob: {_format_prob(prob)}",
        ]

        if q50 is not None:
            value_lines.append(f"Projection: {q50:.1f}")

        if q10 is not None and q90 is not None:
            value_lines.append(f"Range: {q10:.1f} - {q90:.1f}")

        embed.add_field(
            name=f"{stat}",
            value="\n".join(value_lines),
            inline=True,
        )

    return embed


def create_bankroll_embed(summary: dict) -> discord.Embed:
    """Create an embed for bankroll summary.

    Args:
        summary: Dict with balance, daily_pnl, total_pnl

    Returns:
        Discord embed with bankroll info
    """
    balance = summary.get("balance", 1000.0)
    daily_pnl = summary.get("daily_pnl", 0.0)
    total_pnl = summary.get("total_pnl", 0.0)

    # Choose color based on total P&L
    if total_pnl > 0:
        color = COLORS["success"]
    elif total_pnl < 0:
        color = COLORS["error"]
    else:
        color = COLORS["gold"]

    embed = discord.Embed(
        title="Paper Trading Bankroll",
        color=color,
        timestamp=datetime.now(),
    )

    embed.add_field(
        name="Current Balance",
        value=f"**${balance:,.2f}**",
        inline=True,
    )

    embed.add_field(
        name="Today's P&L",
        value=f"${daily_pnl:+,.2f}",
        inline=True,
    )

    embed.add_field(
        name="Total P&L",
        value=f"${total_pnl:+,.2f}",
        inline=True,
    )

    # Calculate ROI from starting bankroll of $1000
    roi = (balance - 1000) / 1000 * 100
    embed.set_footer(text=f"ROI: {roi:+.1f}% from $1,000 start")

    return embed


def create_performance_embed(stats: dict) -> discord.Embed:
    """Create an embed for performance statistics.

    Args:
        stats: Dict with win_rate, roi, total_bets, etc.

    Returns:
        Discord embed with performance stats
    """
    win_rate = stats.get("win_rate", 0.0)
    roi = stats.get("roi", 0.0)
    total_bets = stats.get("total_bets", 0)
    wins = stats.get("wins", 0)
    losses = stats.get("losses", 0)
    pushes = stats.get("pushes", 0)
    total_pnl = stats.get("total_pnl", 0.0)
    best_stat = stats.get("best_stat")
    best_stat_roi = stats.get("best_stat_roi", 0.0)

    # Choose color based on ROI
    if roi > 0.05:
        color = COLORS["success"]
    elif roi > 0:
        color = COLORS["gold"]
    elif roi > -0.05:
        color = COLORS["warning"]
    else:
        color = COLORS["error"]

    embed = discord.Embed(
        title="Model Performance (Last 30 Days)",
        color=color,
        timestamp=datetime.now(),
    )

    embed.add_field(
        name="Win Rate",
        value=f"**{win_rate:.1%}**",
        inline=True,
    )

    embed.add_field(
        name="ROI",
        value=f"**{roi:+.1%}**",
        inline=True,
    )

    embed.add_field(
        name="Total P&L",
        value=f"${total_pnl:+,.2f}",
        inline=True,
    )

    embed.add_field(
        name="Record",
        value=f"{wins}W - {losses}L - {pushes}P",
        inline=True,
    )

    embed.add_field(
        name="Total Bets",
        value=str(total_bets),
        inline=True,
    )

    if best_stat:
        embed.add_field(
            name="Best Stat",
            value=f"{best_stat.upper()} ({best_stat_roi:+.1%})",
            inline=True,
        )

    return embed


def create_alert_embed(picks: list[dict], prediction_date: date | None = None) -> discord.Embed:
    """Create an embed for daily prediction alerts.

    Args:
        picks: List of top prediction dicts
        prediction_date: Date of predictions

    Returns:
        Discord embed for alert
    """
    if prediction_date is None:
        prediction_date = date.today()

    embed = discord.Embed(
        title="Daily Predictions Ready!",
        description=f"Top picks for {prediction_date.strftime('%B %d, %Y')}",
        color=COLORS["gold"],
        timestamp=datetime.now(),
    )

    if not picks:
        embed.add_field(
            name="Status",
            value="No high-edge predictions found for today.",
            inline=False,
        )
        return embed

    for i, pick in enumerate(picks[:5], 1):
        side, edge, prob = _get_best_side(pick)
        line = pick.get("line", "—")
        stat = pick.get("stat", "").upper()
        player = pick.get("player_name", "Unknown")
        team = pick.get("team", "")
        opponent = pick.get("opponent", "")

        matchup = f"{team} vs {opponent}" if team and opponent else ""

        embed.add_field(
            name=f"#{i} {player} — {stat}",
            value=f"**{side} {line}** | Edge: {_format_edge(edge)}" +
                  (f"\n{matchup}" if matchup else ""),
            inline=False,
        )

    embed.set_footer(text="Use /picks for full list | /player <name> for details")
    return embed


def create_error_embed(message: str) -> discord.Embed:
    """Create an embed for error messages.

    Args:
        message: Error message to display

    Returns:
        Discord embed with error
    """
    embed = discord.Embed(
        title="Error",
        description=message,
        color=COLORS["error"],
    )
    return embed


def create_no_data_embed(title: str, message: str) -> discord.Embed:
    """Create an embed for no data scenarios.

    Args:
        title: Embed title
        message: Description message

    Returns:
        Discord embed
    """
    embed = discord.Embed(
        title=title,
        description=message,
        color=COLORS["warning"],
    )
    return embed
