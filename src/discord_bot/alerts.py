"""
Discord alert functions for GameFlowData.

These functions send alerts directly via Discord REST API,
allowing alerts to be sent from scheduled jobs without requiring
the bot process to be running.
"""

import logging
import os
from datetime import date, datetime

import aiohttp
import pandas as pd
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Discord API base URL
DISCORD_API_BASE = "https://discord.com/api/v10"


def _get_best_side(row: pd.Series) -> tuple[str, float]:
    """Determine best side to bet from prediction row."""
    over_edge = row.get("over_edge") or 0
    under_edge = row.get("under_edge") or 0

    if over_edge >= under_edge:
        return "Over", over_edge
    else:
        return "Under", under_edge


def _build_alert_embed(
    predictions_df: pd.DataFrame,
    prediction_date: date,
    sport: str = "nba",
) -> dict:
    """Build Discord embed payload for alert.

    Args:
        predictions_df: DataFrame with predictions
        prediction_date: Date of predictions
        sport: Sport identifier ("nba" or "mlb")

    Returns:
        Discord embed dict
    """
    sport_upper = sport.upper()
    sport_colors = {"nba": 0xFFD700, "mlb": 0x002D72}  # Gold / MLB blue
    embed = {
        "title": f"{sport_upper} Predictions Ready!",
        "description": f"Top {sport_upper} picks for {prediction_date.strftime('%B %d, %Y')}",
        "color": sport_colors.get(sport, 0xFFD700),
        "timestamp": datetime.utcnow().isoformat(),
        "fields": [],
        "footer": {
            "text": f"{sport_upper} | /picks for full list | /player <name> for details",
        },
    }

    if predictions_df.empty:
        embed["fields"].append({
            "name": "Status",
            "value": "No high-edge predictions found for today.",
            "inline": False,
        })
        return embed

    # Sort by max edge and take top 5
    df = predictions_df.copy()
    df["max_edge"] = df[["over_edge", "under_edge"]].max(axis=1)
    df = df.nlargest(5, "max_edge")

    for i, (_, row) in enumerate(df.iterrows(), 1):
        side, edge = _get_best_side(row)
        player = row.get("player_name", "Unknown")
        stat = str(row.get("stat", "")).upper()
        line = row.get("line", "—")

        # Try to get team info
        team = row.get("team", "")
        opponent = row.get("opponent", "")
        matchup = f"{team} vs {opponent}" if team and opponent else ""

        embed["fields"].append({
            "name": f"#{i} {player} — {stat}",
            "value": f"**{side} {line}** | Edge: {edge:+.1%}" +
                     (f"\n{matchup}" if matchup else ""),
            "inline": False,
        })

    return embed


async def send_predictions_alert(
    predictions_df: pd.DataFrame,
    prediction_date: date | None = None,
    channel_id: str | None = None,
    sport: str = "nba",
) -> bool:
    """Send predictions alert to Discord channel.

    This function uses the Discord REST API directly, so it can be called
    from scheduled jobs without requiring the bot process to be running.

    Args:
        predictions_df: DataFrame with predictions (must have player_name, stat, line, over_edge, under_edge)
        prediction_date: Date of predictions (defaults to today)
        channel_id: Discord channel ID (defaults to sport-specific predictions channel)
        sport: Sport identifier ("nba" or "mlb")

    Returns:
        True if alert was sent successfully, False otherwise
    """
    load_dotenv()

    if prediction_date is None:
        prediction_date = date.today()

    # Get configuration
    bot_token = os.getenv("DISCORD_BOT_TOKEN")
    if not bot_token:
        logger.warning("DISCORD_BOT_TOKEN not configured, skipping alert")
        return False

    # Route to sport-specific channel (fall back to shared channel)
    if not channel_id:
        if sport == "mlb":
            channel_id = os.getenv("DISCORD_MLB_CHANNEL_PREDICTIONS") or os.getenv("DISCORD_CHANNEL_PREDICTIONS")
        else:
            channel_id = os.getenv("DISCORD_CHANNEL_PREDICTIONS")
    if not channel_id:
        logger.warning("No predictions channel configured, skipping alert")
        return False

    # Filter to high-edge predictions (>=9%)
    min_edge = 0.09
    filtered = predictions_df[
        (predictions_df["over_edge"] >= min_edge) |
        (predictions_df["under_edge"] >= min_edge)
    ].copy() if not predictions_df.empty else predictions_df

    # Build embed
    embed = _build_alert_embed(filtered, prediction_date, sport=sport)

    # Send via Discord API
    url = f"{DISCORD_API_BASE}/channels/{channel_id}/messages"
    headers = {
        "Authorization": f"Bot {bot_token}",
        "Content-Type": "application/json",
    }
    payload = {"embeds": [embed]}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as response:
                if response.status == 200 or response.status == 201:
                    logger.info(f"Sent Discord alert with {len(filtered)} predictions")
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"Discord API error {response.status}: {error_text}")
                    return False

    except Exception as e:
        logger.exception(f"Failed to send Discord alert: {e}")
        return False


def send_predictions_alert_sync(
    predictions_df: pd.DataFrame,
    prediction_date: date | None = None,
    channel_id: str | None = None,
    sport: str = "nba",
) -> bool:
    """Synchronous wrapper for send_predictions_alert.

    Use this in synchronous code (like the inference job).

    Args:
        predictions_df: DataFrame with predictions
        prediction_date: Date of predictions (defaults to today)
        channel_id: Discord channel ID
        sport: Sport identifier ("nba" or "mlb")

    Returns:
        True if alert was sent successfully, False otherwise
    """
    import asyncio

    try:
        # Check if we're already in an event loop
        try:
            loop = asyncio.get_running_loop()
            # We're in an async context, need to use run_coroutine_threadsafe
            future = asyncio.run_coroutine_threadsafe(
                send_predictions_alert(predictions_df, prediction_date, channel_id, sport=sport),
                loop,
            )
            return future.result(timeout=30)
        except RuntimeError:
            # No event loop running, use asyncio.run
            return asyncio.run(
                send_predictions_alert(predictions_df, prediction_date, channel_id, sport=sport)
            )

    except Exception as e:
        logger.exception(f"Failed to send alert synchronously: {e}")
        return False


# =============================================================================
# Job Status Alerts
# =============================================================================


def _format_duration(seconds: float) -> str:
    """Format duration in seconds to human-readable string."""
    if seconds < 60:
        return f"{seconds:.0f}s"
    minutes = int(seconds // 60)
    secs = int(seconds % 60)
    if minutes < 60:
        return f"{minutes}m {secs}s"
    hours = int(minutes // 60)
    mins = int(minutes % 60)
    return f"{hours}h {mins}m"


def _build_job_alert_embed(
    job_name: str,
    success: bool,
    duration_seconds: float,
    metrics: dict | None = None,
    error_message: str | None = None,
) -> dict:
    """Build Discord embed for job status alert.

    Args:
        job_name: Human-readable job name
        success: Whether job completed successfully
        duration_seconds: Job runtime in seconds
        metrics: Optional dict of job-specific metrics
        error_message: Error message if job failed

    Returns:
        Discord embed dict
    """
    if success:
        title = f"Job Completed: {job_name}"
        color = 0x2ECC71  # Green
        status_emoji = ""
    else:
        title = f"Job Failed: {job_name}"
        color = 0xE74C3C  # Red
        status_emoji = ""

    embed = {
        "title": f"{status_emoji} {title}",
        "color": color,
        "timestamp": datetime.utcnow().isoformat(),
        "fields": [
            {
                "name": "Duration",
                "value": _format_duration(duration_seconds),
                "inline": True,
            },
            {
                "name": "Status",
                "value": "Success" if success else "Failed",
                "inline": True,
            },
        ],
        "footer": {
            "text": "GameFlowData Scheduler",
        },
    }

    # Add metrics if provided
    if metrics:
        for key, value in metrics.items():
            # Format key as title case with underscores replaced
            field_name = key.replace("_", " ").title()
            embed["fields"].append({
                "name": field_name,
                "value": str(value),
                "inline": True,
            })

    # Add error message if present
    if error_message:
        # Truncate long error messages
        truncated = error_message[:500] + "..." if len(error_message) > 500 else error_message
        embed["fields"].append({
            "name": "Error",
            "value": f"```{truncated}```",
            "inline": False,
        })

    return embed


async def send_job_alert(
    job_name: str,
    success: bool,
    duration_seconds: float,
    metrics: dict | None = None,
    error_message: str | None = None,
    channel_id: str | None = None,
) -> bool:
    """Send job status alert to Discord channel.

    This function uses the Discord REST API directly, so it can be called
    from the scheduler without requiring the bot process to be running.

    Args:
        job_name: Human-readable job name (e.g., "Daily Stats", "Lines Scraper")
        success: Whether job completed successfully
        duration_seconds: Job runtime in seconds
        metrics: Optional dict of job-specific metrics to display
        error_message: Error message if job failed
        channel_id: Discord channel ID (defaults to DISCORD_CHANNEL_ALERTS env var)

    Returns:
        True if alert was sent successfully, False otherwise
    """
    load_dotenv()

    # Get configuration
    bot_token = os.getenv("DISCORD_BOT_TOKEN")
    if not bot_token:
        logger.warning("DISCORD_BOT_TOKEN not configured, skipping job alert")
        return False

    channel_id = channel_id or os.getenv("DISCORD_CHANNEL_ALERTS")
    if not channel_id:
        logger.warning("DISCORD_CHANNEL_ALERTS not configured, skipping job alert")
        return False

    # Build embed
    embed = _build_job_alert_embed(
        job_name=job_name,
        success=success,
        duration_seconds=duration_seconds,
        metrics=metrics,
        error_message=error_message,
    )

    # Send via Discord API
    url = f"{DISCORD_API_BASE}/channels/{channel_id}/messages"
    headers = {
        "Authorization": f"Bot {bot_token}",
        "Content-Type": "application/json",
    }
    payload = {"embeds": [embed]}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as response:
                if response.status == 200 or response.status == 201:
                    logger.info(f"Sent job alert for '{job_name}' (success={success})")
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"Discord API error {response.status}: {error_text}")
                    return False

    except Exception as e:
        logger.exception(f"Failed to send job alert: {e}")
        return False


def send_job_alert_sync(
    job_name: str,
    success: bool,
    duration_seconds: float,
    metrics: dict | None = None,
    error_message: str | None = None,
    channel_id: str | None = None,
) -> bool:
    """Synchronous wrapper for send_job_alert.

    Use this from the scheduler which runs in synchronous context.

    Args:
        job_name: Human-readable job name
        success: Whether job completed successfully
        duration_seconds: Job runtime in seconds
        metrics: Optional dict of job-specific metrics
        error_message: Error message if job failed
        channel_id: Discord channel ID

    Returns:
        True if alert was sent successfully, False otherwise
    """
    import asyncio

    try:
        # Check if we're already in an event loop
        try:
            loop = asyncio.get_running_loop()
            # We're in an async context, need to use run_coroutine_threadsafe
            future = asyncio.run_coroutine_threadsafe(
                send_job_alert(
                    job_name, success, duration_seconds, metrics, error_message, channel_id
                ),
                loop,
            )
            return future.result(timeout=30)
        except RuntimeError:
            # No event loop running, use asyncio.run
            return asyncio.run(
                send_job_alert(
                    job_name, success, duration_seconds, metrics, error_message, channel_id
                )
            )

    except Exception as e:
        logger.exception(f"Failed to send job alert synchronously: {e}")
        return False


# =============================================================================
# Daily P&L Summary (Performance Channel)
# =============================================================================


def _build_pnl_summary_embed(
    resolution_result: dict,
    bankroll: float,
    daily_pnl: float,
    total_pnl: float,
    sport: str = "nba",
) -> dict:
    """Build Discord embed for daily P&L summary.

    Args:
        resolution_result: Dict from PaperTrader.resolve_all_pending()
        bankroll: Current bankroll balance
        daily_pnl: Today's P&L
        total_pnl: Cumulative P&L

    Returns:
        Discord embed dict
    """
    wins = resolution_result.get("total_won", 0)
    losses = resolution_result.get("total_lost", 0)
    pushes = resolution_result.get("total_push", 0)
    total_resolved = resolution_result.get("total_resolved", 0)

    # Determine color based on daily P&L
    if daily_pnl > 0:
        color = 0x2ECC71  # Green
        pnl_emoji = "📈"
    elif daily_pnl < 0:
        color = 0xE74C3C  # Red
        pnl_emoji = "📉"
    else:
        color = 0x95A5A6  # Gray
        pnl_emoji = "➖"

    # Build record string
    record = f"{wins}W-{losses}L"
    if pushes > 0:
        record += f"-{pushes}P"

    sport_upper = sport.upper()
    embed = {
        "title": f"{pnl_emoji} {sport_upper} Daily Performance Summary",
        "color": color,
        "timestamp": datetime.utcnow().isoformat(),
        "fields": [
            {
                "name": "Today's Record",
                "value": record if total_resolved > 0 else "No bets resolved",
                "inline": True,
            },
            {
                "name": "Daily P&L",
                "value": f"${daily_pnl:+,.2f}",
                "inline": True,
            },
            {
                "name": "Bankroll",
                "value": f"${bankroll:,.2f}",
                "inline": True,
            },
            {
                "name": "Total P&L",
                "value": f"${total_pnl:+,.2f}",
                "inline": True,
            },
        ],
        "footer": {
            "text": f"{sport_upper} Paper Trading | GameFlowData",
        },
    }

    # Add win rate if we have resolved bets
    if wins + losses > 0:
        win_rate = wins / (wins + losses)
        embed["fields"].append({
            "name": "Win Rate (Today)",
            "value": f"{win_rate:.1%}",
            "inline": True,
        })

    return embed


async def send_pnl_summary(
    resolution_result: dict,
    bankroll: float,
    daily_pnl: float,
    total_pnl: float,
    channel_id: str | None = None,
    sport: str = "nba",
) -> bool:
    """Send daily P&L summary to Discord performance channel.

    Args:
        resolution_result: Dict from PaperTrader.resolve_all_pending()
        bankroll: Current bankroll balance
        daily_pnl: Today's P&L
        total_pnl: Cumulative P&L
        channel_id: Discord channel ID (defaults to sport-specific performance channel)
        sport: Sport identifier ("nba" or "mlb")

    Returns:
        True if alert was sent successfully, False otherwise
    """
    load_dotenv()

    # Get configuration
    bot_token = os.getenv("DISCORD_BOT_TOKEN")
    if not bot_token:
        logger.warning("DISCORD_BOT_TOKEN not configured, skipping P&L summary")
        return False

    if not channel_id:
        if sport == "mlb":
            channel_id = os.getenv("DISCORD_MLB_CHANNEL_PERFORMANCE") or os.getenv("DISCORD_CHANNEL_PERFORMANCE")
        else:
            channel_id = os.getenv("DISCORD_CHANNEL_PERFORMANCE")
    if not channel_id:
        logger.warning("No performance channel configured, skipping P&L summary")
        return False

    # Build embed
    embed = _build_pnl_summary_embed(
        resolution_result=resolution_result,
        bankroll=bankroll,
        daily_pnl=daily_pnl,
        total_pnl=total_pnl,
        sport=sport,
    )

    # Send via Discord API
    url = f"{DISCORD_API_BASE}/channels/{channel_id}/messages"
    headers = {
        "Authorization": f"Bot {bot_token}",
        "Content-Type": "application/json",
    }
    payload = {"embeds": [embed]}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as response:
                if response.status == 200 or response.status == 201:
                    logger.info("Sent daily P&L summary to performance channel")
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"Discord API error {response.status}: {error_text}")
                    return False

    except Exception as e:
        logger.exception(f"Failed to send P&L summary: {e}")
        return False


def send_pnl_summary_sync(
    resolution_result: dict,
    bankroll: float,
    daily_pnl: float,
    total_pnl: float,
    channel_id: str | None = None,
    sport: str = "nba",
) -> bool:
    """Synchronous wrapper for send_pnl_summary.

    Args:
        resolution_result: Dict from PaperTrader.resolve_all_pending()
        bankroll: Current bankroll balance
        daily_pnl: Today's P&L
        total_pnl: Cumulative P&L
        channel_id: Discord channel ID
        sport: Sport identifier ("nba" or "mlb")

    Returns:
        True if alert was sent successfully, False otherwise
    """
    import asyncio

    try:
        try:
            loop = asyncio.get_running_loop()
            future = asyncio.run_coroutine_threadsafe(
                send_pnl_summary(
                    resolution_result, bankroll, daily_pnl, total_pnl, channel_id, sport=sport
                ),
                loop,
            )
            return future.result(timeout=30)
        except RuntimeError:
            return asyncio.run(
                send_pnl_summary(
                    resolution_result, bankroll, daily_pnl, total_pnl, channel_id, sport=sport
                )
            )

    except Exception as e:
        logger.exception(f"Failed to send P&L summary synchronously: {e}")
        return False


# =============================================================================
# Calibration Drift Alerts (Performance Channel)
# =============================================================================


def _build_calibration_embed(metrics) -> dict:
    """Build Discord embed for calibration drift report.

    Args:
        metrics: CalibrationMetrics instance from calibration_monitor.

    Returns:
        Discord embed dict
    """
    severity = metrics.severity
    if severity == "healthy":
        color = 0x2ECC71  # Green
        title = "Calibration Check — Healthy"
    elif severity == "warning":
        color = 0xF39C12  # Amber
        title = "Calibration Check — Drift Detected"
    else:
        color = 0xE74C3C  # Red
        title = "Calibration Check — Significant Drift"

    embed = {
        "title": title,
        "color": color,
        "timestamp": datetime.utcnow().isoformat(),
        "fields": [],
        "footer": {
            "text": f"Paper Trading | {metrics.n_bets} bets ({metrics.date_range[0]} to {metrics.date_range[1]})",
        },
    }

    # Quantile coverage summary
    global_cov = metrics.quantile_coverage.get("GLOBAL", {})
    if global_cov:
        parts = [f"Q{int(q*100)}: {c:.0%}" for q, c in sorted(global_cov.items())]
        embed["fields"].append({
            "name": "Quantile Coverage",
            "value": " | ".join(parts),
            "inline": False,
        })

    # Prob calibration
    embed["fields"].append({
        "name": "ECE",
        "value": f"{metrics.ece:.3f}",
        "inline": True,
    })
    embed["fields"].append({
        "name": "Brier Score",
        "value": f"{metrics.brier_score:.3f}",
        "inline": True,
    })

    # Bias by stat (compact)
    bias_parts = []
    for stat, b in metrics.bias_by_stat.items():
        if stat == "GLOBAL":
            continue
        bias_parts.append(f"{stat.upper()}: {b['rel_bias_pct']:+.1f}%")
    if bias_parts:
        embed["fields"].append({
            "name": "Bias by Stat",
            "value": " | ".join(bias_parts),
            "inline": False,
        })

    # Edge accuracy
    if metrics.edge_accuracy:
        edge_parts = []
        for ea in metrics.edge_accuracy:
            edge_parts.append(
                f"{ea['bucket']}: {ea['actual_win_rate']:.0%} "
                f"(exp {ea['expected_win_rate']:.0%}, n={ea['n']})"
            )
        embed["fields"].append({
            "name": "Edge Accuracy",
            "value": "\n".join(edge_parts),
            "inline": False,
        })

    # Drift alerts
    if metrics.all_alerts:
        alert_text = "\n".join(f"- {a}" for a in metrics.all_alerts[:5])
        if len(metrics.all_alerts) > 5:
            alert_text += f"\n... and {len(metrics.all_alerts) - 5} more"
        embed["fields"].append({
            "name": "Drift Alerts",
            "value": alert_text,
            "inline": False,
        })

    return embed


async def send_calibration_alert(
    metrics,
    channel_id: str | None = None,
) -> bool:
    """Send calibration drift alert to Discord performance channel.

    Args:
        metrics: CalibrationMetrics instance from calibration_monitor.
        channel_id: Discord channel ID (defaults to DISCORD_CHANNEL_PERFORMANCE)

    Returns:
        True if alert was sent successfully, False otherwise
    """
    load_dotenv()

    bot_token = os.getenv("DISCORD_BOT_TOKEN")
    if not bot_token:
        logger.warning("DISCORD_BOT_TOKEN not configured, skipping calibration alert")
        return False

    channel_id = channel_id or os.getenv("DISCORD_CHANNEL_PERFORMANCE")
    if not channel_id:
        logger.warning("DISCORD_CHANNEL_PERFORMANCE not configured, skipping calibration alert")
        return False

    embed = _build_calibration_embed(metrics)

    url = f"{DISCORD_API_BASE}/channels/{channel_id}/messages"
    headers = {
        "Authorization": f"Bot {bot_token}",
        "Content-Type": "application/json",
    }
    payload = {"embeds": [embed]}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as response:
                if response.status in (200, 201):
                    logger.info(
                        f"Sent calibration alert (severity={metrics.severity}, "
                        f"{len(metrics.all_alerts)} alerts)"
                    )
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"Discord API error {response.status}: {error_text}")
                    return False

    except Exception as e:
        logger.exception(f"Failed to send calibration alert: {e}")
        return False


def send_calibration_alert_sync(
    metrics,
    channel_id: str | None = None,
) -> bool:
    """Synchronous wrapper for send_calibration_alert.

    Args:
        metrics: CalibrationMetrics instance from calibration_monitor.
        channel_id: Discord channel ID

    Returns:
        True if alert was sent successfully, False otherwise
    """
    import asyncio

    try:
        try:
            loop = asyncio.get_running_loop()
            future = asyncio.run_coroutine_threadsafe(
                send_calibration_alert(metrics, channel_id),
                loop,
            )
            return future.result(timeout=30)
        except RuntimeError:
            return asyncio.run(
                send_calibration_alert(metrics, channel_id)
            )

    except Exception as e:
        logger.exception(f"Failed to send calibration alert synchronously: {e}")
        return False


# =============================================================================
# Kalshi Prediction Market Alerts
# =============================================================================


def _build_kalshi_alert_embed(
    markets: list[dict],
    target_date: date,
    sport: str = "nba",
) -> dict:
    """Build Discord embed for Kalshi high-edge market alert.

    Args:
        markets: List of market dicts with edge data (pre-sorted by edge).
        target_date: Date of the markets.
        sport: Sport identifier.

    Returns:
        Discord embed dict.
    """
    sport_upper = sport.upper()
    embed = {
        "title": f"Kalshi {sport_upper} — High-Edge Markets",
        "description": f"Top prediction market edges for {target_date.strftime('%B %d, %Y')}",
        "color": 0x7C3AED,  # Violet
        "timestamp": datetime.utcnow().isoformat(),
        "fields": [],
        "footer": {
            "text": f"Kalshi {sport_upper} | Fee-adjusted maker edges | NO contracts are API-only",
        },
    }

    if not markets:
        embed["fields"].append({
            "name": "Status",
            "value": "No high-edge Kalshi markets found.",
            "inline": False,
        })
        return embed

    # Take top 5 by edge
    top = sorted(markets, key=lambda m: m.get("maker_fee_adjusted_edge", 0) or 0, reverse=True)[:5]

    for i, m in enumerate(top, 1):
        player = m.get("player_name", "Unknown")
        stat = str(m.get("stat_type", "")).upper()
        line = m.get("line", "—")
        yes_price = m.get("yes_price", 0)
        spread = m.get("bid_ask_spread", 0)
        edge = m.get("maker_fee_adjusted_edge", 0) or 0
        volume = m.get("volume", 0)
        oi = m.get("open_interest", 0)
        close_time = m.get("close_time", "")

        # Determine if NO side is better (under)
        model_prob = m.get("model_prob", 0.5) or 0.5
        kalshi_implied = m.get("kalshi_implied", 0.5) or 0.5
        is_under = model_prob < kalshi_implied

        side_label = "UNDER (API only)" if is_under else "OVER"
        price_label = f"{'NO' if is_under else 'YES'} {100 - yes_price if is_under else yes_price}c"

        # Format close time
        close_str = ""
        if close_time:
            try:
                ct = datetime.fromisoformat(str(close_time).replace("Z", "+00:00"))
                close_str = ct.strftime("%I:%M %p ET")
            except (ValueError, TypeError):
                close_str = str(close_time)

        embed["fields"].append({
            "name": f"#{i} {player} — {stat} {side_label}",
            "value": (
                f"**{price_label}** | Line: {line} | Spread: {spread}c\n"
                f"Edge: **{edge:+.1%}** | Vol: {volume:,} | OI: {oi:,}"
                + (f" | Closes: {close_str}" if close_str else "")
            ),
            "inline": False,
        })

    return embed


async def send_kalshi_alert(
    markets: list[dict],
    target_date: date | None = None,
    channel_id: str | None = None,
    sport: str = "nba",
) -> bool:
    """Send Kalshi high-edge alert to Discord.

    Args:
        markets: List of market dicts with edge data.
        target_date: Date of the markets (defaults to today).
        channel_id: Discord channel ID (defaults to DISCORD_CHANNEL_KALSHI or DISCORD_CHANNEL_PREDICTIONS).
        sport: Sport identifier.

    Returns:
        True if alert was sent successfully.
    """
    load_dotenv()

    if target_date is None:
        target_date = date.today()

    bot_token = os.getenv("DISCORD_BOT_TOKEN")
    if not bot_token:
        logger.warning("DISCORD_BOT_TOKEN not configured, skipping Kalshi alert")
        return False

    if not channel_id:
        channel_id = os.getenv("DISCORD_CHANNEL_KALSHI") or os.getenv("DISCORD_CHANNEL_PREDICTIONS")
    if not channel_id:
        logger.warning("No Kalshi/predictions channel configured, skipping alert")
        return False

    embed = _build_kalshi_alert_embed(markets, target_date, sport=sport)

    url = f"{DISCORD_API_BASE}/channels/{channel_id}/messages"
    headers = {
        "Authorization": f"Bot {bot_token}",
        "Content-Type": "application/json",
    }
    payload = {"embeds": [embed]}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as response:
                if response.status in (200, 201):
                    logger.info(f"Sent Kalshi alert with {len(markets)} markets")
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"Discord API error {response.status}: {error_text}")
                    return False

    except Exception as e:
        logger.exception(f"Failed to send Kalshi alert: {e}")
        return False


def send_kalshi_alert_sync(
    markets: list[dict],
    target_date: date | None = None,
    channel_id: str | None = None,
    sport: str = "nba",
) -> bool:
    """Synchronous wrapper for send_kalshi_alert.

    Args:
        markets: List of market dicts with edge data.
        target_date: Date of the markets.
        channel_id: Discord channel ID.
        sport: Sport identifier.

    Returns:
        True if alert was sent successfully.
    """
    import asyncio

    try:
        try:
            loop = asyncio.get_running_loop()
            future = asyncio.run_coroutine_threadsafe(
                send_kalshi_alert(markets, target_date, channel_id, sport=sport),
                loop,
            )
            return future.result(timeout=30)
        except RuntimeError:
            return asyncio.run(
                send_kalshi_alert(markets, target_date, channel_id, sport=sport)
            )

    except Exception as e:
        logger.exception(f"Failed to send Kalshi alert synchronously: {e}")
        return False


# =============================================================================
# Kalshi Live Trading Alerts
# =============================================================================


def _build_kalshi_trade_placed_embed(trade: dict, mode: str = "live") -> dict:
    """Build Discord embed for a Kalshi trade placement."""
    player = trade.get("player_name", "Unknown")
    stat = str(trade.get("stat_type", "")).upper()
    line = trade.get("line", "—")
    side = trade.get("side", "yes").upper()
    fill_price = trade.get("fill_price", 0)
    contracts = trade.get("contracts", 0)
    edge = trade.get("fee_adjusted_edge", 0)
    total_cost = trade.get("total_cost", 0)
    balance = trade.get("balance_after", 0)

    mode_upper = mode.upper()
    allow_yes = os.environ.get("KALSHI_ALLOW_YES_BETS", "false").lower() == "true"
    no_only_badge = "" if allow_yes else " [NO-ONLY]"
    return {
        "title": f"KALSHI {mode_upper} TRADE PLACED{no_only_badge}",
        "description": f"**{player}** {stat} {'OVER' if side == 'YES' else 'UNDER'} {line}",
        "color": 0x2ECC71 if mode == "live" else 0x3498DB,  # Green / Blue
        "timestamp": datetime.utcnow().isoformat(),
        "fields": [
            {"name": "Side", "value": side, "inline": True},
            {"name": "Price", "value": f"{fill_price}c", "inline": True},
            {"name": "Contracts", "value": str(contracts), "inline": True},
            {"name": "Cost", "value": f"${total_cost:.2f}", "inline": True},
            {"name": "Edge", "value": f"{edge:.1%}", "inline": True},
            {"name": "Balance", "value": f"${balance:.2f}", "inline": True},
        ],
        "footer": {"text": f"Kalshi {mode.title()} Trading | GameFlowData"},
    }


def _build_kalshi_trade_resolved_embed(trade: dict, mode: str = "live") -> dict:
    """Build Discord embed for a resolved Kalshi trade."""
    player = trade.get("player_name", "Unknown")
    stat = str(trade.get("stat_type", "")).upper()
    line = trade.get("line", "—")
    side = trade.get("side", "yes").upper()
    actual = trade.get("actual_value")
    pnl = trade.get("pnl", 0)
    balance = trade.get("balance_after", 0)
    won = trade.get("status") == "won"

    mode_upper = mode.upper()
    allow_yes = os.environ.get("KALSHI_ALLOW_YES_BETS", "false").lower() == "true"
    no_only_badge = "" if allow_yes else " [NO-ONLY]"
    return {
        "title": f"KALSHI {mode_upper} TRADE {'WON' if won else 'LOST'}{no_only_badge}",
        "description": (
            f"**{player}** {stat} {'OVER' if side == 'YES' else 'UNDER'} {line}"
            + (f" — Actual: {actual}" if actual is not None else "")
        ),
        "color": 0x2ECC71 if won else 0xE74C3C,
        "timestamp": datetime.utcnow().isoformat(),
        "fields": [
            {"name": "P&L", "value": f"${pnl:+.2f}", "inline": True},
            {"name": "Balance", "value": f"${balance:.2f}", "inline": True},
        ],
        "footer": {"text": f"Kalshi {mode.title()} Trading | GameFlowData"},
    }


def _build_kalshi_circuit_breaker_embed(reason: str, details: dict) -> dict:
    """Build Discord embed for a circuit breaker trigger."""
    balance = details.get("balance", 0)
    return {
        "title": "CIRCUIT BREAKER TRIGGERED",
        "description": reason,
        "color": 0xE74C3C,  # Red
        "timestamp": datetime.utcnow().isoformat(),
        "fields": [
            {"name": "Balance", "value": f"${balance:.2f}", "inline": True},
            {"name": "Action", "value": details.get("action", "Trading halted"), "inline": True},
        ],
        "footer": {"text": "Kalshi Live Trading | GameFlowData"},
    }


async def send_kalshi_trade_alert(
    embed: dict,
    channel_id: str | None = None,
) -> bool:
    """Send a Kalshi trading alert embed to Discord.

    Routes to DISCORD_CHANNEL_KALSHI (shared for paper + live),
    falling back to DISCORD_CHANNEL_PREDICTIONS.
    """
    load_dotenv()

    bot_token = os.getenv("DISCORD_BOT_TOKEN")
    if not bot_token:
        logger.warning("DISCORD_BOT_TOKEN not configured, skipping Kalshi trade alert")
        return False

    if not channel_id:
        channel_id = (
            os.getenv("DISCORD_CHANNEL_KALSHI")
            or os.getenv("DISCORD_CHANNEL_PREDICTIONS")
        )
    if not channel_id:
        logger.warning("No Kalshi channel configured, skipping trade alert")
        return False

    url = f"{DISCORD_API_BASE}/channels/{channel_id}/messages"
    headers = {
        "Authorization": f"Bot {bot_token}",
        "Content-Type": "application/json",
    }
    payload = {"embeds": [embed]}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as response:
                if response.status in (200, 201):
                    logger.info("Sent Kalshi live trade alert")
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"Discord API error {response.status}: {error_text}")
                    return False
    except Exception as e:
        logger.exception(f"Failed to send Kalshi trade alert: {e}")
        return False


def send_kalshi_trade_alert_sync(
    alert_type: str,
    data: dict,
    channel_id: str | None = None,
    mode: str = "live",
) -> bool:
    """Send a Kalshi trading alert synchronously.

    Args:
        alert_type: "placed", "resolved", or "circuit_breaker".
        data: Dict with trade/breaker details.
        channel_id: Optional Discord channel override.
        mode: "live" or "paper" — controls embed title and color.

    Returns:
        True if alert was sent successfully.
    """
    import asyncio

    if alert_type == "placed":
        embed = _build_kalshi_trade_placed_embed(data, mode=mode)
    elif alert_type == "resolved":
        embed = _build_kalshi_trade_resolved_embed(data, mode=mode)
    elif alert_type == "circuit_breaker":
        embed = _build_kalshi_circuit_breaker_embed(
            data.get("reason", "Unknown"), data,
        )
    else:
        logger.error(f"Unknown Kalshi alert type: {alert_type}")
        return False

    try:
        try:
            loop = asyncio.get_running_loop()
            future = asyncio.run_coroutine_threadsafe(
                send_kalshi_trade_alert(embed, channel_id), loop,
            )
            return future.result(timeout=30)
        except RuntimeError:
            return asyncio.run(send_kalshi_trade_alert(embed, channel_id))
    except Exception as e:
        logger.exception(f"Failed to send Kalshi trade alert synchronously: {e}")
        return False


# =============================================================================
# Kalshi Paper Trading Daily Summary (Performance Channel)
# =============================================================================


def _build_kalshi_pnl_summary_embed(
    resolution_result: dict,
    bankroll: float,
    daily_pnl: float,
    total_pnl: float,
) -> dict:
    """Build Discord embed for Kalshi daily P&L summary.

    Args:
        resolution_result: Dict from KalshiPaperTrader.resolve_all_pending()
        bankroll: Current Kalshi paper bankroll
        daily_pnl: Yesterday's P&L (from daily log)
        total_pnl: Cumulative P&L (from daily log)

    Returns:
        Discord embed dict
    """
    wins = resolution_result.get("total_won", 0)
    losses = resolution_result.get("total_lost", 0)
    total_resolved = resolution_result.get("total_resolved", 0)

    if daily_pnl > 0:
        color = 0x2ECC71  # Green
        pnl_emoji = "📈"
    elif daily_pnl < 0:
        color = 0xE74C3C  # Red
        pnl_emoji = "📉"
    else:
        color = 0x95A5A6  # Gray
        pnl_emoji = "➖"

    record = f"{wins}W-{losses}L"

    allow_yes = os.environ.get("KALSHI_ALLOW_YES_BETS", "false").lower() == "true"
    mode_badge = "YES+NO" if allow_yes else "NO-ONLY"

    embed = {
        "title": f"{pnl_emoji} Kalshi Daily Performance Summary",
        "color": color,
        "timestamp": datetime.utcnow().isoformat(),
        "fields": [
            {
                "name": "Today's Record",
                "value": record if total_resolved > 0 else "No bets resolved",
                "inline": True,
            },
            {
                "name": "Daily P&L",
                "value": f"${daily_pnl:+,.2f}",
                "inline": True,
            },
            {
                "name": "Bankroll",
                "value": f"${bankroll:,.2f}",
                "inline": True,
            },
            {
                "name": "Total P&L",
                "value": f"${total_pnl:+,.2f}",
                "inline": True,
            },
            {
                "name": "Mode",
                "value": mode_badge,
                "inline": True,
            },
        ],
        "footer": {
            "text": "Kalshi Paper Trading | GameFlowData",
        },
    }

    if wins + losses > 0:
        win_rate = wins / (wins + losses)
        embed["fields"].append({
            "name": "Win Rate (Today)",
            "value": f"{win_rate:.1%}",
            "inline": True,
        })

    return embed


async def send_kalshi_pnl_summary(
    resolution_result: dict,
    bankroll: float,
    daily_pnl: float,
    total_pnl: float,
    channel_id: str | None = None,
) -> bool:
    """Send Kalshi daily P&L summary to Discord performance channel."""
    load_dotenv()

    bot_token = os.getenv("DISCORD_BOT_TOKEN")
    if not bot_token:
        logger.warning("DISCORD_BOT_TOKEN not configured, skipping Kalshi P&L summary")
        return False

    channel_id = channel_id or os.getenv("DISCORD_CHANNEL_PERFORMANCE")
    if not channel_id:
        logger.warning("DISCORD_CHANNEL_PERFORMANCE not configured, skipping Kalshi P&L summary")
        return False

    embed = _build_kalshi_pnl_summary_embed(resolution_result, bankroll, daily_pnl, total_pnl)

    url = f"{DISCORD_API_BASE}/channels/{channel_id}/messages"
    headers = {
        "Authorization": f"Bot {bot_token}",
        "Content-Type": "application/json",
    }
    payload = {"embeds": [embed]}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as response:
                if response.status in (200, 201):
                    logger.info("Sent Kalshi daily P&L summary to performance channel")
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"Discord API error {response.status}: {error_text}")
                    return False
    except Exception as e:
        logger.exception(f"Failed to send Kalshi P&L summary: {e}")
        return False


def send_kalshi_pnl_summary_sync(
    resolution_result: dict,
    bankroll: float,
    daily_pnl: float,
    total_pnl: float,
    channel_id: str | None = None,
) -> bool:
    """Synchronous wrapper for send_kalshi_pnl_summary."""
    import asyncio

    try:
        try:
            loop = asyncio.get_running_loop()
            future = asyncio.run_coroutine_threadsafe(
                send_kalshi_pnl_summary(resolution_result, bankroll, daily_pnl, total_pnl, channel_id),
                loop,
            )
            return future.result(timeout=30)
        except RuntimeError:
            return asyncio.run(
                send_kalshi_pnl_summary(resolution_result, bankroll, daily_pnl, total_pnl, channel_id)
            )
    except Exception as e:
        logger.exception(f"Failed to send Kalshi P&L summary synchronously: {e}")
        return False


def _build_kalshi_analysis_embed(metrics) -> dict:
    """Build Discord embed for Kalshi paper trading analysis report.

    Args:
        metrics: KalshiAnalysisMetrics instance from kalshi_analysis module.

    Returns:
        Discord embed dict
    """
    import math

    severity = metrics.severity
    verdict = metrics.verdict

    if severity == "healthy":
        color = 0x2ECC71   # Green
    elif severity == "warning":
        color = 0xF39C12   # Amber
    else:
        color = 0xE74C3C   # Red

    z_str = f"{metrics.z_score:.1f}σ" if not math.isnan(metrics.z_score) else "—"

    embed = {
        "title": f"📊 Kalshi Analysis — {verdict}",
        "color": color,
        "timestamp": datetime.utcnow().isoformat(),
        "fields": [],
        "footer": {
            "text": (
                f"Kalshi Paper Trading | {metrics.n_bets} NO bets "
                f"({metrics.date_range[0]} to {metrics.date_range[1]})"
            ),
        },
    }

    # Overall
    embed["fields"].append({
        "name": "Overall (14d)",
        "value": (
            f"{metrics.n_bets} bets | "
            f"{metrics.win_rate:.1%} win | "
            f"{metrics.break_even:.1%} BE | "
            f"{metrics.alpha:+.1%} alpha"
        ),
        "inline": False,
    })

    # Z-score (14d window + all-time)
    z_alltime_str = (
        f"{metrics.z_score_alltime:.1f}σ"
        if not math.isnan(metrics.z_score_alltime)
        else "—"
    )
    alltime_label = (
        f" (n={metrics.n_bets_alltime})" if metrics.n_bets_alltime > 0 else ""
    )
    embed["fields"].append({
        "name": "Z-Score",
        "value": (
            f"14d: **{z_str}** ({verdict})\n"
            f"All-time: **{z_alltime_str}**{alltime_label}"
        ),
        "inline": True,
    })

    # P&L & ROI
    roi_parts = [f"${metrics.total_pnl:+,.0f}", f"ROI (stake): {metrics.roi:+.1%}"]
    if metrics.bankroll_roi is not None:
        roi_parts.append(f"ROI (bankroll): {metrics.bankroll_roi:+.1%}")
    embed["fields"].append({
        "name": "P&L & ROI",
        "value": " | ".join(roi_parts),
        "inline": False,
    })

    # 95% CI
    ci_lo, ci_hi = metrics.ci_95
    embed["fields"].append({
        "name": "95% CI",
        "value": f"[{ci_lo:.1%}, {ci_hi:.1%}]",
        "inline": True,
    })

    # By stat
    if metrics.by_stat:
        stat_parts = []
        for s in metrics.by_stat[:6]:
            stat_parts.append(
                f"{s['stat'].upper()}: {s['win_rate']:.0%} win (n={s['total']})"
            )
        embed["fields"].append({
            "name": "By Stat",
            "value": " | ".join(stat_parts),
            "inline": False,
        })

    # By edge bucket
    if metrics.by_edge_bucket:
        bucket_lines = []
        for b in metrics.by_edge_bucket:
            bucket_lines.append(
                f"{b['bucket']}: {b['win_rate']:.0%} (n={b['total']})"
            )
        embed["fields"].append({
            "name": "By Edge Bucket",
            "value": "\n".join(bucket_lines),
            "inline": False,
        })

    return embed


async def send_kalshi_analysis_alert(
    metrics,
    channel_id: str | None = None,
) -> bool:
    """Send Kalshi analysis embed to Discord performance channel."""
    load_dotenv()

    bot_token = os.getenv("DISCORD_BOT_TOKEN")
    if not bot_token:
        logger.warning("DISCORD_BOT_TOKEN not configured, skipping Kalshi analysis alert")
        return False

    channel_id = channel_id or os.getenv("DISCORD_CHANNEL_PERFORMANCE")
    if not channel_id:
        logger.warning("DISCORD_CHANNEL_PERFORMANCE not configured, skipping Kalshi analysis alert")
        return False

    embed = _build_kalshi_analysis_embed(metrics)

    url = f"{DISCORD_API_BASE}/channels/{channel_id}/messages"
    headers = {
        "Authorization": f"Bot {bot_token}",
        "Content-Type": "application/json",
    }
    payload = {"embeds": [embed]}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as response:
                if response.status in (200, 201):
                    logger.info(
                        f"Sent Kalshi analysis alert (verdict={metrics.verdict}, "
                        f"n={metrics.n_bets} bets)"
                    )
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"Discord API error {response.status}: {error_text}")
                    return False
    except Exception as e:
        logger.exception(f"Failed to send Kalshi analysis alert: {e}")
        return False


def send_kalshi_analysis_alert_sync(
    metrics,
    channel_id: str | None = None,
) -> bool:
    """Synchronous wrapper for send_kalshi_analysis_alert."""
    import asyncio

    try:
        try:
            loop = asyncio.get_running_loop()
            future = asyncio.run_coroutine_threadsafe(
                send_kalshi_analysis_alert(metrics, channel_id),
                loop,
            )
            return future.result(timeout=30)
        except RuntimeError:
            return asyncio.run(send_kalshi_analysis_alert(metrics, channel_id))
    except Exception as e:
        logger.exception(f"Failed to send Kalshi analysis alert synchronously: {e}")
        return False


# =============================================================================
# Arbitrage Scanner Alerts
# =============================================================================


def _build_arb_alert_embed(opportunities: list, sport: str = "nba") -> dict:
    """Build Discord embed for arbitrage/mispricing opportunities.

    Color coding:
      Pure arb → orange (0xFF8C00)
      Soft arb → yellow (0xFFD700)
      Sportsbook mispricing → blue (0x3498DB)

    Args:
        opportunities: List of ArbOpportunity instances.
        sport: Sport identifier.

    Returns:
        Discord embed dict.
    """
    sport_upper = sport.upper()

    # Determine dominant type for embed color
    arb_types = [getattr(o, "arb_type", "") for o in opportunities]
    if "pure" in arb_types:
        color = 0xFF8C00  # Orange — guaranteed profit
        title_prefix = "PURE ARB DETECTED"
    elif "soft" in arb_types:
        color = 0xFFD700  # Yellow — soft arb
        title_prefix = "Soft Arb Opportunity"
    else:
        color = 0x3498DB  # Blue — market intel
        title_prefix = "Market Mispricing"

    embed = {
        "title": f"{title_prefix} — {sport_upper}",
        "description": "Cross-platform price discrepancies detected",
        "color": color,
        "timestamp": datetime.utcnow().isoformat(),
        "fields": [],
        "footer": {
            "text": f"Polymarket-Kalshi Arb Scanner | {sport_upper} | Phase 1 (paper only)",
        },
    }

    if not opportunities:
        embed["fields"].append({
            "name": "Status",
            "value": "No significant opportunities found.",
            "inline": False,
        })
        return embed

    # Show top 5 opportunities
    shown = opportunities[:5]
    for i, opp in enumerate(shown, 1):
        arb_type = getattr(opp, "arb_type", "unknown")
        player = getattr(opp, "player_name", None) or "Unknown"
        stat = str(getattr(opp, "stat_type", "") or "").upper()
        line = getattr(opp, "line", None)
        poly_price = getattr(opp, "poly_price", 0) or 0
        poly_side = getattr(opp, "poly_side", "yes") or "yes"
        poly_liq = getattr(opp, "poly_liquidity", 0) or 0
        disc = getattr(opp, "price_discrepancy", None)

        if arb_type in ("pure", "soft"):
            k_side = getattr(opp, "kalshi_side", "?") or "?"
            k_price = getattr(opp, "kalshi_price", 0) or 0
            k_vol = getattr(opp, "kalshi_volume", 0) or 0
            net_margin = getattr(opp, "net_margin", None)
            est_profit = getattr(opp, "estimated_profit", None)

            type_badge = "PURE ARB" if arb_type == "pure" else "Soft Arb"
            value_parts = [
                f"Kalshi {k_side.upper()} {k_price}c | Poly {poly_side.upper()} {poly_price:.0f}c",
                f"Discrepancy: **{disc:.1%}**" if disc else "",
            ]
            if net_margin is not None:
                value_parts.append(f"Net margin: **{net_margin:.1f}c**")
            if est_profit is not None:
                value_parts.append(f"Est. profit: ${est_profit:.2f}")
            value_parts.append(f"Kalshi vol: {k_vol:,} | Poly liq: ${poly_liq:,.0f}")

            embed["fields"].append({
                "name": f"#{i} [{type_badge}] {player} — {stat} {line or ''}",
                "value": "\n".join(p for p in value_parts if p),
                "inline": False,
            })

        else:
            # Sportsbook mispricing
            sb_implied = getattr(opp, "sportsbook_implied", None)
            sb_str = f"{sb_implied:.1%}" if sb_implied is not None else "?"
            embed["fields"].append({
                "name": f"#{i} [SB Mispricing] {player} — {stat} {line or ''}",
                "value": (
                    f"Poly {poly_side.upper()} {poly_price:.0f}c vs SB {sb_str}\n"
                    f"Discrepancy: **{disc:.1%}**" if disc else
                    f"Poly {poly_side.upper()} {poly_price:.0f}c vs SB {sb_str}"
                ),
                "inline": False,
            })

    return embed


async def send_arb_alert(
    opportunities: list,
    sport: str = "nba",
    channel_id: str | None = None,
) -> bool:
    """Send arbitrage opportunity alert to Discord.

    Routes to DISCORD_CHANNEL_ARB, falling back to DISCORD_CHANNEL_KALSHI
    then DISCORD_CHANNEL_PREDICTIONS.

    Args:
        opportunities: List of ArbOpportunity instances.
        sport: Sport identifier.
        channel_id: Optional channel override.

    Returns:
        True if alert was sent successfully.
    """
    load_dotenv()

    bot_token = os.getenv("DISCORD_BOT_TOKEN")
    if not bot_token:
        logger.warning("DISCORD_BOT_TOKEN not configured, skipping arb alert")
        return False

    if not channel_id:
        channel_id = (
            os.getenv("DISCORD_CHANNEL_ARB")
            or os.getenv("DISCORD_CHANNEL_KALSHI")
            or os.getenv("DISCORD_CHANNEL_PREDICTIONS")
        )
    if not channel_id:
        logger.warning("No arb/Kalshi/predictions channel configured, skipping arb alert")
        return False

    embed = _build_arb_alert_embed(opportunities, sport=sport)

    url = f"{DISCORD_API_BASE}/channels/{channel_id}/messages"
    headers = {
        "Authorization": f"Bot {bot_token}",
        "Content-Type": "application/json",
    }
    payload = {"embeds": [embed]}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as response:
                if response.status in (200, 201):
                    logger.info(f"Sent arb alert with {len(opportunities)} opportunities")
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"Discord API error {response.status}: {error_text}")
                    return False
    except Exception as e:
        logger.exception(f"Failed to send arb alert: {e}")
        return False


def send_arb_alert_sync(
    opportunities: list,
    sport: str = "nba",
    channel_id: str | None = None,
) -> bool:
    """Synchronous wrapper for send_arb_alert.

    Args:
        opportunities: List of ArbOpportunity instances.
        sport: Sport identifier.
        channel_id: Optional Discord channel override.

    Returns:
        True if alert was sent successfully.
    """
    import asyncio

    try:
        try:
            loop = asyncio.get_running_loop()
            future = asyncio.run_coroutine_threadsafe(
                send_arb_alert(opportunities, sport, channel_id),
                loop,
            )
            return future.result(timeout=30)
        except RuntimeError:
            return asyncio.run(send_arb_alert(opportunities, sport, channel_id))
    except Exception as e:
        logger.exception(f"Failed to send arb alert synchronously: {e}")
        return False
