"""
Paper Trading Module for NBA Player Props.

Converts daily predictions into paper bets with Kelly-based stake sizing,
and resolves bets against actual game results.

Supports Black-Litterman probability blending to anchor model probabilities
to market priors, improving edge calibration.

Configuration via environment variables:
    PAPER_TRADING_BANKROLL: Starting bankroll (default: 10000)
    PAPER_TRADING_KELLY_FRACTION: Kelly fraction (default: 0.125)
    PAPER_TRADING_EDGE_THRESHOLD: Edge threshold (default: 0.09)
    PAPER_TRADING_MAX_BET_PCT: Max bet as % of bankroll (default: None = no cap)
    PAPER_TRADING_BL_TAU: Black-Litterman tau (default: 0.5)
"""

from __future__ import annotations

import gzip
import logging
import os
from dataclasses import dataclass, field
from datetime import date
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd
from sqlalchemy import text

from src.db.client import get_engine
from src.models.black_litterman import BlackLittermanBlender, BLConfig

if TYPE_CHECKING:
    from src.config.stat_config import StatConfigSet

logger = logging.getLogger(__name__)

# Supported stat types
SUPPORTED_STATS = {"pts", "reb", "ast"}


def _get_env_float(name: str, default: float) -> float:
    """Get float from environment variable."""
    val = os.environ.get(name)
    if val is None:
        return default
    try:
        return float(val)
    except ValueError:
        logger.warning(f"Invalid {name}={val}, using default {default}")
        return default


def _get_env_float_or_none(name: str, default: float | None) -> float | None:
    """Get float or None from environment variable."""
    val = os.environ.get(name)
    if val is None:
        return default
    if val.lower() in ("none", "null", ""):
        return None
    try:
        return float(val)
    except ValueError:
        logger.warning(f"Invalid {name}={val}, using default {default}")
        return default


# Default configuration (can be overridden via environment variables)
DEFAULT_BANKROLL = _get_env_float("PAPER_TRADING_BANKROLL", 10000.0)
DEFAULT_KELLY_FRACTION = _get_env_float("PAPER_TRADING_KELLY_FRACTION", 0.125)
DEFAULT_EDGE_THRESHOLD = _get_env_float("PAPER_TRADING_EDGE_THRESHOLD", 0.09)
DEFAULT_MAX_BET_PCT = _get_env_float_or_none("PAPER_TRADING_MAX_BET_PCT", None)
DEFAULT_BL_TAU = _get_env_float_or_none("PAPER_TRADING_BL_TAU", 0.5)


@dataclass
class PaperTrader:
    """
    Converts daily predictions into paper bets and tracks P&L.

    Supports standalone operation for dashboard integration.
    Supports per-stat edge thresholds via stat_config.
    Supports Black-Litterman blending via bl_tau and bl_z_max parameters.

    Default configuration matches optimal backtest sweep results:
    - BL tau=0.5, z_max=1.0, edge_threshold=0.09 (9% BL edge)
    - Kelly fraction=0.125, no bet size cap (true Kelly sizing)

    Configuration can be overridden via environment variables:
    - PAPER_TRADING_BANKROLL: Starting bankroll
    - PAPER_TRADING_KELLY_FRACTION: Kelly fraction
    - PAPER_TRADING_EDGE_THRESHOLD: Edge threshold
    - PAPER_TRADING_MAX_BET_PCT: Max bet % (set to "none" for no cap)
    - PAPER_TRADING_BL_TAU: Black-Litterman tau
    """

    edge_threshold: float = field(default_factory=lambda: DEFAULT_EDGE_THRESHOLD)
    kelly_fraction: float = field(default_factory=lambda: DEFAULT_KELLY_FRACTION)
    max_bet_pct: float | None = field(default_factory=lambda: DEFAULT_MAX_BET_PCT)
    starting_bankroll: float = field(default_factory=lambda: DEFAULT_BANKROLL)
    min_odds: int = -200  # Don't bet heavy favorites
    max_odds: int = 200  # Don't bet long shots
    stat_config: StatConfigSet | None = None  # Per-stat edge thresholds
    bl_tau: float | None = field(default_factory=lambda: DEFAULT_BL_TAU)
    bl_z_max: float = 1.0  # BL z-score saturation threshold
    _bl_blender: BlackLittermanBlender | None = field(init=False, default=None)

    def __post_init__(self):
        self.engine = get_engine()

        # Log active configuration
        logger.info(
            f"PaperTrader initialized: bankroll=${self.starting_bankroll:,.0f}, "
            f"kelly={self.kelly_fraction}, edge_threshold={self.edge_threshold}, "
            f"max_bet_pct={self.max_bet_pct or 'None (no cap)'}"
        )

        # Initialize BL blender if tau is set
        if self.bl_tau is not None:
            self._bl_blender = BlackLittermanBlender(
                BLConfig(tau=self.bl_tau, z_max=self.bl_z_max)
            )
            logger.info(f"BL blending enabled: tau={self.bl_tau}, z_max={self.bl_z_max}")

    def _load_samples_for_date(self, game_date: date) -> dict[tuple, np.ndarray]:
        """Load all MC samples for a date from daily_prediction_samples.

        Returns:
            Dict mapping (player_id, game_id, stat) -> np.ndarray of samples
        """
        query = text("""
            SELECT player_id, game_id, stat, n_samples, samples_gz
            FROM daily_prediction_samples
            WHERE prediction_date = :game_date
        """)

        samples_dict: dict[tuple, np.ndarray] = {}
        with self.engine.connect() as conn:
            results = conn.execute(query, {"game_date": game_date}).fetchall()

        for row in results:
            player_id = int(row[0])
            game_id = str(row[1])
            stat = str(row[2])
            n_samples = int(row[3])
            blob = row[4]

            # Handle memoryview (psycopg2 returns bytea as memoryview)
            if isinstance(blob, memoryview):
                blob = bytes(blob)

            try:
                samples = np.frombuffer(
                    gzip.decompress(blob), dtype=np.float64, count=n_samples
                )
                samples_dict[(player_id, game_id, stat)] = samples
            except Exception as e:
                logger.warning(f"Failed to decompress samples for {player_id}/{stat}: {e}")

        logger.info(f"Loaded {len(samples_dict)} sample arrays for {game_date}")
        return samples_dict

    def _get_edge_threshold(self, stat: str) -> float:
        """Get edge threshold for a stat, using per-stat config if available."""
        if self.stat_config is not None:
            return self.stat_config.get_edge_threshold(stat)
        return self.edge_threshold

    def _american_to_decimal(self, odds: float) -> float:
        """Convert American odds to decimal odds."""
        if odds is None or pd.isna(odds):
            return 0.0
        if odds > 0:
            return (odds / 100.0) + 1.0
        else:
            return (100.0 / abs(odds)) + 1.0

    def _calculate_kelly_stake(
        self, odds: float, model_prob: float, bankroll: float
    ) -> float:
        """Calculate stake using fractional Kelly Criterion."""
        if odds is None or pd.isna(odds) or odds == 0:
            return 0.0

        # Convert to net fractional odds (b in Kelly formula)
        if odds > 0:
            b = odds / 100.0
        else:
            b = 100.0 / abs(odds)

        # Kelly: f = (p(b + 1) - 1) / b
        f = (model_prob * (b + 1) - 1) / b

        # Apply fraction
        f_fractional = f * self.kelly_fraction

        if f_fractional <= 0:
            return 0.0

        stake = f_fractional * bankroll

        # Cap at max_bet_pct if set (None = no cap, true Kelly sizing)
        if self.max_bet_pct is not None:
            max_stake = self.max_bet_pct * bankroll
            stake = min(stake, max_stake)

        return min(stake, bankroll)

    def _get_current_bankroll(self) -> float:
        """Get current bankroll from most recent daily log entry."""
        query = text("""
            SELECT bankroll_after
            FROM paper_trading_daily_log
            ORDER BY game_date DESC
            LIMIT 1
        """)
        with self.engine.connect() as conn:
            result = conn.execute(query).fetchone()

        if result is None:
            return self.starting_bankroll
        return float(result[0])

    def _get_started_game_ids(self, game_date: date) -> set[str]:
        """Get game_ids for games that have already started (commence_time < now).

        Uses raw_player_props_combined which has commence_time per game.
        Returns set of game_id strings (10-digit zero-padded).
        """
        from datetime import datetime, timezone

        query = text("""
            SELECT DISTINCT game_id, commence_time
            FROM raw_player_props_combined
            WHERE game_id IS NOT NULL
              AND commence_time IS NOT NULL
              AND commence_time < :now
              AND snapshot_time > :cutoff
        """)

        now = datetime.now(timezone.utc)
        # Only look at recent snapshots (last 24h) to avoid scanning old data
        cutoff = now - pd.Timedelta(hours=24)

        try:
            with self.engine.connect() as conn:
                rows = conn.execute(query, {"now": now, "cutoff": cutoff}).fetchall()

            started = set()
            for row in rows:
                gid = str(row[0]).zfill(10)
                started.add(gid)
                started.add(gid.lstrip("0"))  # Also add unpadded version
            return started
        except Exception as e:
            logger.warning(f"Could not check game start times: {e}")
            return set()

    def select_bets(self, game_date: date) -> list[dict[str, Any]]:
        """
        Query daily_predictions for game_date, filter by edge threshold,
        and calculate Kelly stakes.

        When BL blending is enabled (bl_tau is set), loads MC samples and
        recalculates probabilities/edges using Black-Litterman blending.

        Skips games that have already started (no live betting).

        Returns list of bet dictionaries ready for placement.
        """
        bankroll = self._get_current_bankroll()

        # Identify games already in progress — skip them
        started_game_ids = self._get_started_game_ids(game_date)
        if started_game_ids:
            logger.info(f"Excluding {len(started_game_ids) // 2} started games from bet selection")

        # Load MC samples if BL blending is enabled
        samples_dict: dict[tuple, np.ndarray] = {}
        if self._bl_blender is not None:
            samples_dict = self._load_samples_for_date(game_date)
            if not samples_dict:
                logger.warning(f"BL enabled but no samples found for {game_date}")

        # Query predictions with edge
        query = text("""
            SELECT
                id as prediction_id,
                prediction_date,
                player_id,
                player_name,
                game_id,
                stat,
                line,
                over_odds,
                under_odds,
                over_prob,
                under_prob,
                implied_over,
                implied_under,
                over_edge,
                under_edge
            FROM daily_predictions
            WHERE prediction_date = :game_date
              AND stat IN ('pts', 'reb', 'ast')
              AND line IS NOT NULL
            ORDER BY player_name, stat
        """)

        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"game_date": game_date})

        if df.empty:
            logger.warning(f"No predictions found for {game_date}")
            return []

        bets = []
        skipped_live = 0
        for _, row in df.iterrows():
            stat = str(row["stat"])
            threshold = self._get_edge_threshold(stat)
            player_id = int(row["player_id"])
            game_id = str(row["game_id"])
            line = float(row["line"]) if pd.notna(row["line"]) else None

            # Skip games already in progress
            if game_id in started_game_ids or game_id.lstrip("0") in started_game_ids:
                skipped_live += 1
                continue

            if line is None:
                continue

            over_odds = row["over_odds"]
            under_odds = row["under_odds"]

            # Skip if missing odds
            if pd.isna(over_odds) or pd.isna(under_odds):
                continue

            over_odds = int(over_odds)
            under_odds = int(under_odds)

            # Apply BL blending if enabled and samples available
            if self._bl_blender is not None:
                samples = samples_dict.get((player_id, game_id, stat))
                if samples is not None and len(samples) > 0:
                    # Use BL blending
                    bl_result = self._bl_blender.blend_prediction(
                        samples=samples,
                        line=line,
                        over_odds=over_odds,
                        under_odds=under_odds,
                    )
                    over_prob = bl_result["posterior_over"]
                    under_prob = bl_result["posterior_under"]
                    implied_over = bl_result["market_over"]
                    implied_under = bl_result["market_under"]
                    over_edge = over_prob - implied_over
                    under_edge = under_prob - implied_under
                else:
                    # No samples - skip this prediction
                    continue
            else:
                # No BL - use stored values
                over_edge = float(row["over_edge"]) if pd.notna(row["over_edge"]) else 0
                under_edge = float(row["under_edge"]) if pd.notna(row["under_edge"]) else 0
                over_prob = float(row["over_prob"]) if pd.notna(row["over_prob"]) else 0.5
                under_prob = float(row["under_prob"]) if pd.notna(row["under_prob"]) else 0.5
                implied_over = float(row["implied_over"]) if pd.notna(row["implied_over"]) else 0.5
                implied_under = float(row["implied_under"]) if pd.notna(row["implied_under"]) else 0.5

            # Select the direction with higher edge that meets threshold
            direction = None
            edge = 0.0
            odds = 0
            model_prob = 0.0
            implied_prob = 0.0

            if over_edge > under_edge and over_edge >= threshold:
                direction = "over"
                edge = over_edge
                odds = over_odds
                model_prob = over_prob
                implied_prob = implied_over
            elif under_edge >= threshold:
                direction = "under"
                edge = under_edge
                odds = under_odds
                model_prob = under_prob
                implied_prob = implied_under

            if direction is None:
                continue

            # Filter extreme odds
            if pd.isna(odds) or odds < self.min_odds or odds > self.max_odds:
                continue

            # Calculate stake
            stake = self._calculate_kelly_stake(odds, model_prob, bankroll)
            if stake <= 0:
                continue

            bet = {
                "prediction_id": int(row["prediction_id"]),
                "game_date": game_date,
                "player_id": int(row["player_id"]),
                "player_name": row["player_name"],
                "stat_type": row["stat"],
                "line": float(row["line"]),
                "bet_direction": direction,
                "odds_at_bet": float(odds),
                "implied_prob": float(implied_prob),
                "model_prob": float(model_prob),
                "edge": float(edge),
                "stake": round(stake, 2),
                "kelly_fraction": self.kelly_fraction,
            }
            bets.append(bet)

        if skipped_live > 0:
            logger.info(f"Skipped {skipped_live} predictions for games already in progress")
        logger.info(f"Selected {len(bets)} bets for {game_date}")
        return bets

    def place_bets(self, bets: list[dict[str, Any]]) -> int:
        """
        Insert bets into paper_bets table (UPSERT for idempotency).

        Returns count of bets placed.
        """
        if not bets:
            return 0

        # UPSERT query
        query = text("""
            INSERT INTO paper_bets (
                prediction_id, game_date, player_id, player_name, stat_type,
                line, bet_direction, odds_at_bet, implied_prob, model_prob,
                edge, stake, kelly_fraction, status
            ) VALUES (
                :prediction_id, :game_date, :player_id, :player_name, :stat_type,
                :line, :bet_direction, :odds_at_bet, :implied_prob, :model_prob,
                :edge, :stake, :kelly_fraction, 'pending'
            )
            ON CONFLICT (game_date, player_id, stat_type, bet_direction)
            DO UPDATE SET
                prediction_id = EXCLUDED.prediction_id,
                line = EXCLUDED.line,
                odds_at_bet = EXCLUDED.odds_at_bet,
                implied_prob = EXCLUDED.implied_prob,
                model_prob = EXCLUDED.model_prob,
                edge = EXCLUDED.edge,
                stake = EXCLUDED.stake,
                kelly_fraction = EXCLUDED.kelly_fraction,
                placed_at = NOW()
            WHERE paper_bets.status = 'pending'
        """)

        with self.engine.connect() as conn:
            for bet in bets:
                conn.execute(query, bet)
            conn.commit()

        logger.info(f"Placed {len(bets)} bets")
        return len(bets)

    def resolve_bets(self, game_date: date) -> dict[str, Any]:
        """
        Resolve pending bets for game_date using actual stats from player_game_stats.

        Updates paper_bets with status/pnl and paper_trading_daily_log with summary.

        Returns resolution summary.
        """
        # Get pending bets for date
        bets_query = text("""
            SELECT id, player_id, stat_type, line, bet_direction, odds_at_bet, stake
            FROM paper_bets
            WHERE game_date = :game_date AND status = 'pending'
        """)

        with self.engine.connect() as conn:
            bets_df = pd.read_sql(bets_query, conn, params={"game_date": game_date})

        if bets_df.empty:
            logger.info(f"No pending bets to resolve for {game_date}")
            return {"resolved": 0, "won": 0, "lost": 0, "push": 0, "cancelled": 0}

        # Get actual stats - join with team_game_stats to get game_date
        actuals_query = text("""
            SELECT
                pgs.player_id,
                pgs.pts,
                pgs.reb,
                pgs.ast,
                pgs.min,
                pgs.did_not_play
            FROM player_game_stats pgs
            JOIN team_game_stats tgs ON pgs.game_id = tgs.game_id AND pgs.team_id = tgs.team_id
            WHERE tgs.game_date = :game_date
        """)

        with self.engine.connect() as conn:
            actuals_df = pd.read_sql(
                actuals_query, conn, params={"game_date": game_date}
            )

        # Build actuals lookup: player_id -> {stat -> value}
        # Void (None) for DNPs and 0-minute players — sportsbooks void these bets
        actuals_lookup: dict[int, dict[str, float | None]] = {}
        for _, row in actuals_df.iterrows():
            player_id = int(row["player_id"])
            did_not_play = row.get("did_not_play", False) or False
            minutes = row.get("min", None)
            zero_minutes = minutes is not None and (minutes == 0 or (pd.notna(minutes) and float(minutes) == 0))

            if did_not_play or zero_minutes:
                actuals_lookup[player_id] = {"pts": None, "reb": None, "ast": None}
            else:
                actuals_lookup[player_id] = {
                    "pts": float(row["pts"]) if pd.notna(row["pts"]) else None,
                    "reb": float(row["reb"]) if pd.notna(row["reb"]) else None,
                    "ast": float(row["ast"]) if pd.notna(row["ast"]) else None,
                }

        # Resolve each bet
        results = {"won": 0, "lost": 0, "push": 0, "cancelled": 0}
        updates = []

        for _, bet in bets_df.iterrows():
            player_id = int(bet["player_id"])
            stat_type = bet["stat_type"]
            line = float(bet["line"])
            direction = bet["bet_direction"]
            odds = float(bet["odds_at_bet"])
            stake = float(bet["stake"])
            bet_id = int(bet["id"])

            # Get actual value
            player_actuals = actuals_lookup.get(player_id, {})
            actual = player_actuals.get(stat_type)

            # Determine outcome
            if actual is None:
                status = "cancelled"
                pnl = 0.0
                results["cancelled"] += 1
            elif actual > line:
                if direction == "over":
                    status = "won"
                    decimal_odds = self._american_to_decimal(odds)
                    pnl = stake * (decimal_odds - 1)
                    results["won"] += 1
                else:
                    status = "lost"
                    pnl = -stake
                    results["lost"] += 1
            elif actual < line:
                if direction == "under":
                    status = "won"
                    decimal_odds = self._american_to_decimal(odds)
                    pnl = stake * (decimal_odds - 1)
                    results["won"] += 1
                else:
                    status = "lost"
                    pnl = -stake
                    results["lost"] += 1
            else:  # actual == line
                status = "push"
                pnl = 0.0
                results["push"] += 1

            updates.append(
                {
                    "bet_id": bet_id,
                    "status": status,
                    "actual_value": actual,
                    "pnl": round(pnl, 2),
                }
            )

        # Update bets in database
        update_query = text("""
            UPDATE paper_bets
            SET status = :status,
                actual_value = :actual_value,
                pnl = :pnl,
                resolved_at = NOW()
            WHERE id = :bet_id
        """)

        with self.engine.connect() as conn:
            for update in updates:
                conn.execute(update_query, update)
            conn.commit()

        # Update daily log
        self._update_daily_log(game_date)

        results["resolved"] = len(updates)
        logger.info(
            f"Resolved {results['resolved']} bets: "
            f"{results['won']}W {results['lost']}L {results['push']}P {results['cancelled']}C"
        )
        return results

    def resolve_all_pending(self, exclude_today: bool = True) -> dict[str, Any]:
        """
        Resolve ALL pending bets where game results are available.

        This handles multi-day catchup (weekend gaps, job failures).
        Safe to run multiple times - only resolves bets with available stats.

        Args:
            exclude_today: If True (default), skip today's bets to avoid
                false resolution of games that haven't finished yet.
                The team_game_stats check provides a secondary guard, but
                this prevents any edge case where partial stats exist.

        Returns:
            dict with {dates_processed: int, dates_skipped: int,
                       total_resolved: int, by_date: {...}}
        """
        # Get all unique dates with pending bets (exclude today if requested)
        if exclude_today:
            dates_query = text("""
                SELECT DISTINCT game_date
                FROM paper_bets
                WHERE status = 'pending'
                  AND game_date < :today
                ORDER BY game_date ASC
            """)
            query_params = {"today": date.today()}
        else:
            dates_query = text("""
                SELECT DISTINCT game_date
                FROM paper_bets
                WHERE status = 'pending'
                ORDER BY game_date ASC
            """)
            query_params = {}

        with self.engine.connect() as conn:
            dates_result = conn.execute(dates_query, query_params).fetchall()

        if not dates_result:
            logger.info("No pending bets to resolve")
            return {
                "dates_processed": 0,
                "dates_skipped": 0,
                "total_resolved": 0,
                "total_won": 0,
                "total_lost": 0,
                "total_push": 0,
                "total_cancelled": 0,
                "by_date": {},
            }

        pending_dates = [row[0] for row in dates_result]
        logger.info(f"Found {len(pending_dates)} dates with pending bets: {pending_dates}")

        # Check which dates have game stats available
        dates_processed = 0
        dates_skipped = 0
        total_resolved = 0
        total_won = 0
        total_lost = 0
        total_push = 0
        total_cancelled = 0
        by_date: dict[str, dict[str, Any]] = {}

        for game_date in pending_dates:
            # Check if we have stats for this date
            stats_check_query = text("""
                SELECT COUNT(*)
                FROM team_game_stats
                WHERE game_date = :game_date
            """)

            with self.engine.connect() as conn:
                stats_count = conn.execute(
                    stats_check_query, {"game_date": game_date}
                ).scalar()

            if stats_count == 0:
                logger.info(f"Skipping {game_date}: no game stats available yet")
                dates_skipped += 1
                by_date[str(game_date)] = {"skipped": True, "reason": "no_stats"}
                continue

            # Resolve bets for this date
            logger.info(f"Resolving bets for {game_date}...")
            result = self.resolve_bets(game_date)

            dates_processed += 1
            total_resolved += result["resolved"]
            total_won += result["won"]
            total_lost += result["lost"]
            total_push += result["push"]
            total_cancelled += result["cancelled"]
            by_date[str(game_date)] = result

        summary = {
            "dates_processed": dates_processed,
            "dates_skipped": dates_skipped,
            "total_resolved": total_resolved,
            "total_won": total_won,
            "total_lost": total_lost,
            "total_push": total_push,
            "total_cancelled": total_cancelled,
            "by_date": by_date,
        }

        logger.info(
            f"Resolution complete: {dates_processed} dates processed, "
            f"{dates_skipped} skipped, {total_resolved} total bets resolved "
            f"({total_won}W {total_lost}L {total_push}P {total_cancelled}C)"
        )

        return summary

    def _update_daily_log(self, game_date: date) -> None:
        """Update or create daily log entry with aggregated bet stats."""
        # Aggregate stats for the date
        agg_query = text("""
            SELECT
                COUNT(*) as total_bets,
                COUNT(*) FILTER (WHERE status = 'won') as bets_won,
                COUNT(*) FILTER (WHERE status = 'lost') as bets_lost,
                COUNT(*) FILTER (WHERE status = 'push') as bets_push,
                COUNT(*) FILTER (WHERE status = 'pending') as bets_pending,
                COALESCE(SUM(stake) FILTER (WHERE status IN ('won', 'lost')), 0) as total_staked,
                COALESCE(SUM(pnl), 0) as total_pnl
            FROM paper_bets
            WHERE game_date = :game_date
        """)

        with self.engine.connect() as conn:
            result = conn.execute(agg_query, {"game_date": game_date}).fetchone()

        if result is None or result[0] == 0:
            return

        total_bets = result[0]
        bets_won = result[1]
        bets_lost = result[2]
        bets_push = result[3]
        bets_pending = result[4]
        total_staked = float(result[5])
        total_pnl = float(result[6])

        roi_pct = (total_pnl / total_staked * 100) if total_staked > 0 else 0

        # Get previous cumulative P&L
        prev_query = text("""
            SELECT cumulative_pnl, bankroll_after
            FROM paper_trading_daily_log
            WHERE game_date < :game_date
            ORDER BY game_date DESC
            LIMIT 1
        """)

        with self.engine.connect() as conn:
            prev_result = conn.execute(prev_query, {"game_date": game_date}).fetchone()

        if prev_result is None:
            prev_cumulative = 0.0
            prev_bankroll = self.starting_bankroll
        else:
            prev_cumulative = float(prev_result[0])
            prev_bankroll = float(prev_result[1])

        cumulative_pnl = prev_cumulative + total_pnl
        bankroll_after = prev_bankroll + total_pnl

        # UPSERT daily log
        upsert_query = text("""
            INSERT INTO paper_trading_daily_log (
                game_date, total_bets, bets_won, bets_lost, bets_push, bets_pending,
                total_staked, total_pnl, roi_pct, cumulative_pnl, bankroll_after
            ) VALUES (
                :game_date, :total_bets, :bets_won, :bets_lost, :bets_push, :bets_pending,
                :total_staked, :total_pnl, :roi_pct, :cumulative_pnl, :bankroll_after
            )
            ON CONFLICT (game_date) DO UPDATE SET
                total_bets = EXCLUDED.total_bets,
                bets_won = EXCLUDED.bets_won,
                bets_lost = EXCLUDED.bets_lost,
                bets_push = EXCLUDED.bets_push,
                bets_pending = EXCLUDED.bets_pending,
                total_staked = EXCLUDED.total_staked,
                total_pnl = EXCLUDED.total_pnl,
                roi_pct = EXCLUDED.roi_pct,
                cumulative_pnl = EXCLUDED.cumulative_pnl,
                bankroll_after = EXCLUDED.bankroll_after,
                updated_at = NOW()
        """)

        with self.engine.connect() as conn:
            conn.execute(
                upsert_query,
                {
                    "game_date": game_date,
                    "total_bets": total_bets,
                    "bets_won": bets_won,
                    "bets_lost": bets_lost,
                    "bets_push": bets_push,
                    "bets_pending": bets_pending,
                    "total_staked": total_staked,
                    "total_pnl": round(total_pnl, 2),
                    "roi_pct": round(roi_pct, 2),
                    "cumulative_pnl": round(cumulative_pnl, 2),
                    "bankroll_after": round(bankroll_after, 2),
                },
            )
            conn.commit()

        logger.info(f"Updated daily log for {game_date}: P&L={total_pnl:.2f}")

    def get_pending_bets(self, game_date: date | None = None) -> pd.DataFrame:
        """Query paper_bets with status='pending'."""
        if game_date:
            query = text("""
                SELECT * FROM paper_bets
                WHERE status = 'pending' AND game_date = :game_date
                ORDER BY player_name, stat_type
            """)
            params = {"game_date": game_date}
        else:
            query = text("""
                SELECT * FROM paper_bets
                WHERE status = 'pending'
                ORDER BY game_date, player_name, stat_type
            """)
            params = {}

        with self.engine.connect() as conn:
            return pd.read_sql(query, conn, params=params)

    def get_daily_summary(self, game_date: date) -> dict[str, Any] | None:
        """Query paper_trading_daily_log for date."""
        query = text("""
            SELECT * FROM paper_trading_daily_log
            WHERE game_date = :game_date
        """)

        with self.engine.connect() as conn:
            result = conn.execute(query, {"game_date": game_date}).fetchone()

        if result is None:
            return None

        return {
            "game_date": result[1],
            "total_bets": result[2],
            "bets_won": result[3],
            "bets_lost": result[4],
            "bets_push": result[5],
            "bets_pending": result[6],
            "total_staked": float(result[7]),
            "total_pnl": float(result[8]),
            "roi_pct": float(result[9]) if result[9] else 0,
            "cumulative_pnl": float(result[10]),
            "bankroll_after": float(result[11]),
        }

    def get_bets_for_date(self, game_date: date) -> pd.DataFrame:
        """Get all bets for a specific date."""
        query = text("""
            SELECT * FROM paper_bets
            WHERE game_date = :game_date
            ORDER BY player_name, stat_type
        """)

        with self.engine.connect() as conn:
            return pd.read_sql(query, conn, params={"game_date": game_date})

    def get_recent_performance(self, days: int = 7) -> pd.DataFrame:
        """Get P&L summary for last N days."""
        query = text("""
            SELECT * FROM paper_trading_daily_log
            ORDER BY game_date DESC
            LIMIT :days
        """)

        with self.engine.connect() as conn:
            return pd.read_sql(query, conn, params={"days": days})
