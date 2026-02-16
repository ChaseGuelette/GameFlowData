#!/usr/bin/env python3
"""
GameFlowData Discord Bot - Entry Point

Run the bot:
    python src/discord_bot/run_bot.py

Environment variables required:
    DISCORD_BOT_TOKEN - Bot token from Discord Developer Portal
    DISCORD_CHANNEL_PREDICTIONS - Channel ID for prediction posts
    DISCORD_CHANNEL_ALERTS - Channel ID for daily alerts
    DISCORD_CHANNEL_PERFORMANCE - Channel ID for performance updates
"""

import asyncio
import logging
import os
import signal
import sys

from dotenv import load_dotenv

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.discord_bot.bot import create_bot

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Reduce noise from discord.py
logging.getLogger("discord").setLevel(logging.WARNING)
logging.getLogger("discord.http").setLevel(logging.WARNING)


def main():
    """Main entry point for the Discord bot."""
    # Load environment variables
    load_dotenv()

    # Get bot token
    token = os.getenv("DISCORD_BOT_TOKEN")
    if not token:
        logger.error("DISCORD_BOT_TOKEN not found in environment")
        sys.exit(1)

    # Validate channel configuration
    predictions_channel = os.getenv("DISCORD_CHANNEL_PREDICTIONS")
    alerts_channel = os.getenv("DISCORD_CHANNEL_ALERTS")

    if not predictions_channel:
        logger.warning("DISCORD_CHANNEL_PREDICTIONS not configured")
    if not alerts_channel:
        logger.warning("DISCORD_CHANNEL_ALERTS not configured")

    # Create bot instance
    bot = create_bot()

    # Graceful shutdown handler (Railway compatible)
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        asyncio.create_task(bot.close())

    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Run the bot
    logger.info("Starting GameFlowData Discord Bot...")
    logger.info(f"Predictions channel: {predictions_channel or 'Not configured'}")
    logger.info(f"Alerts channel: {alerts_channel or 'Not configured'}")

    try:
        bot.run(token, log_handler=None)  # We handle logging ourselves
    except KeyboardInterrupt:
        logger.info("Bot stopped by keyboard interrupt")
    except Exception as e:
        logger.exception(f"Bot crashed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
