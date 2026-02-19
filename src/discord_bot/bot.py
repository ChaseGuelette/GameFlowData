"""GameFlowData Discord Bot - Main bot class with slash commands."""

import logging
import os
from datetime import date

import discord
from discord import app_commands
from discord.ext import commands

from src.discord_bot.formatters.embeds import (
    create_alert_embed,
    create_bankroll_embed,
    create_error_embed,
    create_no_data_embed,
    create_performance_embed,
    create_picks_embed,
    create_player_embed,
)
from src.discord_bot.services.paper_trading import (
    get_bankroll_summary,
    get_performance_stats,
)
from src.discord_bot.services.predictions import (
    get_player_predictions,
    get_predictions_for_date,
    get_top_picks,
    search_players,
)

logger = logging.getLogger(__name__)


class GameFlowBot(commands.Bot):
    """Discord bot for GameFlowData predictions and paper trading."""

    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True

        super().__init__(
            command_prefix="!",
            intents=intents,
            description="GameFlowData NBA Props Prediction Bot",
        )

        # Store channel IDs from environment
        self.predictions_channel_id = int(os.getenv("DISCORD_CHANNEL_PREDICTIONS", "0"))
        self.alerts_channel_id = int(os.getenv("DISCORD_CHANNEL_ALERTS", "0"))
        self.performance_channel_id = int(os.getenv("DISCORD_CHANNEL_PERFORMANCE", "0"))

    async def setup_hook(self):
        """Called when bot is ready to set up slash commands."""
        # Register slash commands
        self._register_commands()

        # Sync commands to Discord
        logger.info("Syncing slash commands...")
        await self.tree.sync()
        logger.info("Slash commands synced!")

    async def on_ready(self):
        """Called when bot is fully connected."""
        logger.info(f"Bot connected as {self.user} (ID: {self.user.id})")
        logger.info(f"Connected to {len(self.guilds)} guild(s)")

        # Log channel configuration
        if self.predictions_channel_id:
            logger.info(f"Predictions channel: {self.predictions_channel_id}")
        if self.alerts_channel_id:
            logger.info(f"Alerts channel: {self.alerts_channel_id}")

    def _register_commands(self):
        """Register all slash commands."""

        # /picks command
        @self.tree.command(
            name="picks",
            description="Get today's top predictions",
        )
        @app_commands.describe(
            stat="Filter by stat type",
            min_edge="Minimum edge threshold (default: 5%)",
        )
        @app_commands.choices(stat=[
            app_commands.Choice(name="All Stats", value="all"),
            app_commands.Choice(name="Points", value="pts"),
            app_commands.Choice(name="Rebounds", value="reb"),
            app_commands.Choice(name="Assists", value="ast"),
        ])
        async def picks(
            interaction: discord.Interaction,
            stat: str = "all",
            min_edge: float = 0.05,
        ):
            await interaction.response.defer()

            try:
                predictions = await get_predictions_for_date(
                    prediction_date=date.today(),
                    min_edge=min_edge,
                    stat_filter=stat if stat != "all" else None,
                    limit=10,
                )

                if not predictions:
                    embed = create_no_data_embed(
                        "No Predictions",
                        f"No predictions found with edge >= {min_edge:.0%} for today.",
                    )
                else:
                    embed = create_picks_embed(predictions, date.today())

                await interaction.followup.send(embed=embed)

            except Exception as e:
                logger.exception("Error in /picks command")
                embed = create_error_embed(f"Failed to fetch predictions: {str(e)}")
                await interaction.followup.send(embed=embed)

        # /player command
        @self.tree.command(
            name="player",
            description="Get predictions for a specific player",
        )
        @app_commands.describe(
            name="Player name (fuzzy matching supported)",
        )
        async def player(
            interaction: discord.Interaction,
            name: str,
        ):
            await interaction.response.defer()

            try:
                predictions = await get_player_predictions(
                    player_name=name,
                    prediction_date=date.today(),
                )

                if not predictions:
                    # Try to find similar players
                    similar = await search_players(name, limit=3)
                    if similar:
                        suggestions = ", ".join([p["player_name"] for p in similar])
                        embed = create_no_data_embed(
                            "Player Not Found",
                            f"No predictions found for '{name}'.\n\nDid you mean: {suggestions}?",
                        )
                    else:
                        embed = create_no_data_embed(
                            "Player Not Found",
                            f"No predictions found for '{name}' today.",
                        )
                else:
                    embed = create_player_embed(predictions)

                await interaction.followup.send(embed=embed)

            except Exception as e:
                logger.exception("Error in /player command")
                embed = create_error_embed(f"Failed to fetch player predictions: {str(e)}")
                await interaction.followup.send(embed=embed)

        # /bankroll command
        @self.tree.command(
            name="bankroll",
            description="Show paper trading balance",
        )
        async def bankroll(interaction: discord.Interaction):
            await interaction.response.defer()

            try:
                summary = await get_bankroll_summary()

                if not summary:
                    embed = create_no_data_embed(
                        "No Data",
                        "No paper trading history found.",
                    )
                else:
                    embed = create_bankroll_embed(summary)

                await interaction.followup.send(embed=embed)

            except Exception as e:
                logger.exception("Error in /bankroll command")
                embed = create_error_embed(f"Failed to fetch bankroll: {str(e)}")
                await interaction.followup.send(embed=embed)

        # /performance command
        @self.tree.command(
            name="performance",
            description="Show model performance stats",
        )
        @app_commands.describe(
            days="Number of days to look back (default: 30)",
        )
        async def performance(
            interaction: discord.Interaction,
            days: int = 30,
        ):
            await interaction.response.defer()

            try:
                stats = await get_performance_stats(days=days)

                if stats.get("total_bets", 0) == 0:
                    embed = create_no_data_embed(
                        "No Data",
                        f"No resolved bets found in the last {days} days.",
                    )
                else:
                    embed = create_performance_embed(stats)

                await interaction.followup.send(embed=embed)

            except Exception as e:
                logger.exception("Error in /performance command")
                embed = create_error_embed(f"Failed to fetch performance stats: {str(e)}")
                await interaction.followup.send(embed=embed)

        # /toppicks command (for alerts channel testing)
        @self.tree.command(
            name="toppicks",
            description="Get today's top 5 high-edge picks",
        )
        async def toppicks(interaction: discord.Interaction):
            await interaction.response.defer()

            try:
                picks = await get_top_picks(
                    prediction_date=date.today(),
                    n=5,
                    min_edge=0.09,
                )

                embed = create_alert_embed(picks, date.today())
                await interaction.followup.send(embed=embed)

            except Exception as e:
                logger.exception("Error in /toppicks command")
                embed = create_error_embed(f"Failed to fetch top picks: {str(e)}")
                await interaction.followup.send(embed=embed)

    async def send_daily_alert(self):
        """Send daily prediction alert to the alerts channel."""
        if not self.alerts_channel_id:
            logger.warning("No alerts channel configured")
            return

        channel = self.get_channel(self.alerts_channel_id)
        if not channel:
            logger.warning(f"Could not find alerts channel {self.alerts_channel_id}")
            return

        try:
            picks = await get_top_picks(
                prediction_date=date.today(),
                n=5,
                min_edge=0.09,
            )

            embed = create_alert_embed(picks, date.today())
            await channel.send(embed=embed)
            logger.info(f"Sent daily alert with {len(picks)} picks")

        except Exception:
            logger.exception("Failed to send daily alert")


def create_bot() -> GameFlowBot:
    """Factory function to create bot instance."""
    return GameFlowBot()
