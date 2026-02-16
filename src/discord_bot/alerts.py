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


def _build_alert_embed(predictions_df: pd.DataFrame, prediction_date: date) -> dict:
    """Build Discord embed payload for alert.

    Args:
        predictions_df: DataFrame with predictions
        prediction_date: Date of predictions

    Returns:
        Discord embed dict
    """
    embed = {
        "title": "Daily Predictions Ready!",
        "description": f"Top picks for {prediction_date.strftime('%B %d, %Y')}",
        "color": 0xFFD700,  # Gold
        "timestamp": datetime.utcnow().isoformat(),
        "fields": [],
        "footer": {
            "text": "Use /picks for full list | /player <name> for details",
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
) -> bool:
    """Send predictions alert to Discord channel.

    This function uses the Discord REST API directly, so it can be called
    from scheduled jobs without requiring the bot process to be running.

    Args:
        predictions_df: DataFrame with predictions (must have player_name, stat, line, over_edge, under_edge)
        prediction_date: Date of predictions (defaults to today)
        channel_id: Discord channel ID (defaults to DISCORD_CHANNEL_ALERTS env var)

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

    channel_id = channel_id or os.getenv("DISCORD_CHANNEL_PREDICTIONS")
    if not channel_id:
        logger.warning("DISCORD_CHANNEL_PREDICTIONS not configured, skipping alert")
        return False

    # Filter to high-edge predictions (>=9%)
    min_edge = 0.09
    filtered = predictions_df[
        (predictions_df["over_edge"] >= min_edge) |
        (predictions_df["under_edge"] >= min_edge)
    ].copy() if not predictions_df.empty else predictions_df

    # Build embed
    embed = _build_alert_embed(filtered, prediction_date)

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
) -> bool:
    """Synchronous wrapper for send_predictions_alert.

    Use this in synchronous code (like the inference job).

    Args:
        predictions_df: DataFrame with predictions
        prediction_date: Date of predictions (defaults to today)
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
            import concurrent.futures
            future = asyncio.run_coroutine_threadsafe(
                send_predictions_alert(predictions_df, prediction_date, channel_id),
                loop,
            )
            return future.result(timeout=30)
        except RuntimeError:
            # No event loop running, use asyncio.run
            return asyncio.run(
                send_predictions_alert(predictions_df, prediction_date, channel_id)
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
