"""
Paper Trading Module for MLB Player Props.

Converts MLB daily predictions into paper bets with Kelly-based stake sizing,
and resolves bets against actual game results from pitching/batting stats tables.

Supports Black-Litterman probability blending, per-stat edge thresholds via
mlb_stat_config, and multi-day catchup resolution.

Configuration via environment variables:
    MLB_PAPER_TRADING_BANKROLL: Starting bankroll (default: 5000)
    MLB_PAPER_TRADING_KELLY_FRACTION: Kelly fraction (default: 0.125)
"""

from __future__ import annotations

import gzip
import logging
import os
from dataclasses import dataclass, field
from datetime import date
from typing import Any

import numpy as np
import pandas as pd
from sqlalchemy import text

from src.db.client import get_engine
from src.models.black_litterman import BlackLittermanBlender, BLConfig
from src.models.mlb.mlb_stat_config import MLB_STATS

logger = logging.getLogger(__name__)

# Stat resolution: stat_type -> (table, column) for resolving actual values
MLB_STAT_RESOLUTION: dict[str, tuple[str, str]] = {
    "pitcher_strikeouts": ("mlb_player_game_stats_pitching", "so"),
    "pitcher_outs":       ("mlb_player_game_stats_pitching", "outs_recorded"),
    "batter_hits":        ("mlb_player_game_stats_batting",  "h"),
    "batter_rbis":        ("mlb_player_game_stats_batting",  "rbi"),
}

# DNP detection columns per table
MLB_DNP_COLUMNS: dict[str, str] = {
    "mlb_player_game_stats_pitching": "did_not_play",
    "mlb_player_game_stats_batting": "did_not_play",
}

SUPPORTED_STATS = set(MLB_STATS.keys())


def _get_env_float(name: str, default: float) -> float:
    val = os.environ.get(name)
    if val is None:
        return default
    try:
        return float(val)
    except ValueError:
        logger.warning(f"Invalid {name}={val}, using default {default}")
        return default


DEFAULT_BANKROLL = _get_env_float("MLB_PAPER_TRADING_BANKROLL", 5000.0)
DEFAULT_KELLY_FRACTION = _get_env_float("MLB_PAPER_TRADING_KELLY_FRACTION", 0.125)


@dataclass
class MLBPaperTrader:
    """
    Converts MLB daily predictions into paper bets and tracks P&L.

    Uses per-stat edge thresholds from mlb_stat_config.py.
    Supports Black-Litterman blending via bl_tau.
    """

    kelly_fraction: float = field(default_factory=lambda: DEFAULT_KELLY_FRACTION)
    max_bet_pct: float | None = None
    starting_bankroll: float = field(default_factory=lambda: DEFAULT_BANKROLL)
    min_odds: int = -200
    max_odds: int = 200
    bl_tau: float | None = 0.5
    bl_z_max: float = 1.0
    _bl_blender: BlackLittermanBlender | None = field(init=False, default=None)

    def __post_init__(self):
        self.engine = get_engine()
        logger.info(
            f"MLBPaperTrader initialized: bankroll=${self.starting_bankroll:,.0f}, "
            f"kelly={self.kelly_fraction}"
        )
        if self.bl_tau is not None:
            self._bl_blender = BlackLittermanBlender(
                BLConfig(tau=self.bl_tau, z_max=self.bl_z_max)
            )
            logger.info(f"BL blending enabled: tau={self.bl_tau}, z_max={self.bl_z_max}")

    def _load_samples_for_date(self, game_date: date) -> dict[tuple, np.ndarray]:
        """Load all MC samples for a date from mlb_daily_prediction_samples."""
        query = text("""
            SELECT player_id, game_id, stat, n_samples, samples_gz
            FROM mlb_daily_prediction_samples
            WHERE prediction_date = :game_date
        """)

        samples_dict: dict[tuple, np.ndarray] = {}
        with self.engine.connect() as conn:
            results = conn.execute(query, {"game_date": game_date}).fetchall()

        for row in results:
            player_id = int(row[0])
            game_id = int(row[1])
            stat = str(row[2])
            n_samples = int(row[3])
            blob = row[4]

            if isinstance(blob, memoryview):
                blob = bytes(blob)

            try:
                samples = np.frombuffer(
                    gzip.decompress(blob), dtype=np.float64, count=n_samples
                )
                samples_dict[(player_id, game_id, stat)] = samples
            except Exception as e:
                logger.warning(f"Failed to decompress samples for {player_id}/{stat}: {e}")

        logger.info(f"Loaded {len(samples_dict)} MLB sample arrays for {game_date}")
        return samples_dict

    def _get_edge_threshold(self, stat: str) -> float:
        """Get edge threshold for a stat from MLB stat config."""
        cfg = MLB_STATS.get(stat)
        if cfg is not None:
            return cfg.get("edge_threshold", 0.08)
        return 0.08

    def _american_to_decimal(self, odds: float) -> float:
        if odds is None or pd.isna(odds):
            return 0.0
        if odds > 0:
            return (odds / 100.0) + 1.0
        else:
            return (100.0 / abs(odds)) + 1.0

    def _calculate_kelly_stake(
        self, odds: float, model_prob: float, bankroll: float
    ) -> float:
        if odds is None or pd.isna(odds) or odds == 0:
            return 0.0
        if odds > 0:
            b = odds / 100.0
        else:
            b = 100.0 / abs(odds)

        f = (model_prob * (b + 1) - 1) / b
        f_fractional = f * self.kelly_fraction

        if f_fractional <= 0:
            return 0.0

        stake = f_fractional * bankroll

        if self.max_bet_pct is not None:
            max_stake = self.max_bet_pct * bankroll
            stake = min(stake, max_stake)

        return min(stake, bankroll)

    def _get_current_bankroll(self) -> float:
        query = text("""
            SELECT bankroll_after
            FROM mlb_paper_trading_daily_log
            ORDER BY game_date DESC
            LIMIT 1
        """)
        with self.engine.connect() as conn:
            result = conn.execute(query).fetchone()

        if result is None:
            return self.starting_bankroll
        return float(result[0])

    def select_bets(self, game_date: date) -> list[dict[str, Any]]:
        """
        Query mlb_daily_predictions for game_date, filter by edge threshold,
        and calculate Kelly stakes.
        """
        bankroll = self._get_current_bankroll()

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
                model_type,
                line,
                over_odds,
                under_odds,
                over_prob,
                under_prob,
                implied_over,
                implied_under,
                over_edge,
                under_edge
            FROM mlb_daily_predictions
            WHERE prediction_date = :game_date
              AND line IS NOT NULL
            ORDER BY player_name, stat
        """)

        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"game_date": game_date})

        if df.empty:
            logger.warning(f"No MLB predictions found for {game_date}")
            return []

        bets = []
        for _, row in df.iterrows():
            stat = str(row["stat"])
            if stat not in SUPPORTED_STATS:
                continue

            threshold = self._get_edge_threshold(stat)
            player_id = int(row["player_id"])
            game_id = int(row["game_id"])
            line = float(row["line"]) if pd.notna(row["line"]) else None

            if line is None:
                continue

            over_odds = row["over_odds"]
            under_odds = row["under_odds"]

            if pd.isna(over_odds) or pd.isna(under_odds):
                continue

            over_odds = int(over_odds)
            under_odds = int(under_odds)

            # Apply BL blending if enabled and samples available
            if self._bl_blender is not None:
                samples = samples_dict.get((player_id, game_id, stat))
                if samples is not None and len(samples) > 0:
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
                    continue
            else:
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

            if pd.isna(odds) or odds < self.min_odds or odds > self.max_odds:
                continue

            stake = self._calculate_kelly_stake(odds, model_prob, bankroll)
            if stake <= 0:
                continue

            bet = {
                "prediction_id": int(row["prediction_id"]),
                "game_date": game_date,
                "player_id": player_id,
                "player_name": row["player_name"],
                "stat_type": stat,
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

        logger.info(f"Selected {len(bets)} MLB bets for {game_date}")
        return bets

    def place_bets(self, bets: list[dict[str, Any]]) -> int:
        """Insert bets into mlb_paper_bets table (UPSERT for idempotency)."""
        if not bets:
            return 0

        query = text("""
            INSERT INTO mlb_paper_bets (
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
            WHERE mlb_paper_bets.status = 'pending'
        """)

        with self.engine.connect() as conn:
            for bet in bets:
                conn.execute(query, bet)
            conn.commit()

        logger.info(f"Placed {len(bets)} MLB bets")
        return len(bets)

    def resolve_bets(self, game_date: date) -> dict[str, Any]:
        """Resolve pending MLB bets for game_date using actual stats."""
        bets_query = text("""
            SELECT id, player_id, stat_type, line, bet_direction, odds_at_bet, stake
            FROM mlb_paper_bets
            WHERE game_date = :game_date AND status = 'pending'
        """)

        with self.engine.connect() as conn:
            bets_df = pd.read_sql(bets_query, conn, params={"game_date": game_date})

        if bets_df.empty:
            logger.info(f"No pending MLB bets to resolve for {game_date}")
            return {"resolved": 0, "won": 0, "lost": 0, "push": 0, "cancelled": 0}

        # Build actuals lookup by dispatching to correct stats table per stat
        actuals_lookup: dict[tuple[int, str], float | None] = {}
        stats_needed = bets_df["stat_type"].unique()

        for stat_type in stats_needed:
            resolution = MLB_STAT_RESOLUTION.get(stat_type)
            if resolution is None:
                logger.warning(f"No resolution table for stat: {stat_type}")
                continue

            table_name, column_name = resolution
            dnp_col = MLB_DNP_COLUMNS.get(table_name, "did_not_play")

            actuals_query = text(f"""
                SELECT
                    s.player_id,
                    s.{column_name} as actual_value,
                    s.{dnp_col} as did_not_play
                FROM {table_name} s
                WHERE s.game_date = :game_date
            """)

            with self.engine.connect() as conn:
                actuals_df = pd.read_sql(
                    actuals_query, conn, params={"game_date": game_date}
                )

            for _, row in actuals_df.iterrows():
                player_id = int(row["player_id"])
                did_not_play = row.get("did_not_play", False) or False

                if did_not_play:
                    actuals_lookup[(player_id, stat_type)] = None
                else:
                    val = row["actual_value"]
                    actuals_lookup[(player_id, stat_type)] = (
                        float(val) if pd.notna(val) else None
                    )

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

            actual = actuals_lookup.get((player_id, stat_type))

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
            else:
                status = "push"
                pnl = 0.0
                results["push"] += 1

            updates.append({
                "bet_id": bet_id,
                "status": status,
                "actual_value": actual,
                "pnl": round(pnl, 2),
            })

        update_query = text("""
            UPDATE mlb_paper_bets
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

        self._update_daily_log(game_date)

        results["resolved"] = len(updates)
        logger.info(
            f"Resolved {results['resolved']} MLB bets: "
            f"{results['won']}W {results['lost']}L {results['push']}P {results['cancelled']}C"
        )
        return results

    def resolve_all_pending(self, exclude_today: bool = True) -> dict[str, Any]:
        """Resolve ALL pending MLB bets where game results are available."""
        if exclude_today:
            dates_query = text("""
                SELECT DISTINCT game_date
                FROM mlb_paper_bets
                WHERE status = 'pending'
                  AND game_date < :today
                ORDER BY game_date ASC
            """)
            query_params = {"today": date.today()}
        else:
            dates_query = text("""
                SELECT DISTINCT game_date
                FROM mlb_paper_bets
                WHERE status = 'pending'
                ORDER BY game_date ASC
            """)
            query_params = {}

        with self.engine.connect() as conn:
            dates_result = conn.execute(dates_query, query_params).fetchall()

        if not dates_result:
            logger.info("No pending MLB bets to resolve")
            return {
                "dates_processed": 0, "dates_skipped": 0,
                "total_resolved": 0, "total_won": 0, "total_lost": 0,
                "total_push": 0, "total_cancelled": 0, "by_date": {},
            }

        pending_dates = [row[0] for row in dates_result]
        logger.info(f"Found {len(pending_dates)} dates with pending MLB bets")

        dates_processed = 0
        dates_skipped = 0
        total_resolved = 0
        total_won = 0
        total_lost = 0
        total_push = 0
        total_cancelled = 0
        by_date: dict[str, dict[str, Any]] = {}

        for game_date in pending_dates:
            # Check if we have stats for this date (check both pitching and batting tables)
            stats_check_query = text("""
                SELECT
                    (SELECT COUNT(*) FROM mlb_player_game_stats_pitching WHERE game_date = :game_date) +
                    (SELECT COUNT(*) FROM mlb_player_game_stats_batting WHERE game_date = :game_date)
                    as total_stats
            """)

            with self.engine.connect() as conn:
                stats_count = conn.execute(
                    stats_check_query, {"game_date": game_date}
                ).scalar()

            if stats_count == 0:
                logger.info(f"Skipping {game_date}: no MLB game stats available yet")
                dates_skipped += 1
                by_date[str(game_date)] = {"skipped": True, "reason": "no_stats"}
                continue

            logger.info(f"Resolving MLB bets for {game_date}...")
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
            f"MLB resolution complete: {dates_processed} dates processed, "
            f"{dates_skipped} skipped, {total_resolved} total bets resolved "
            f"({total_won}W {total_lost}L {total_push}P {total_cancelled}C)"
        )
        return summary

    def _update_daily_log(self, game_date: date) -> None:
        """Update or create daily log entry with aggregated bet stats."""
        agg_query = text("""
            SELECT
                COUNT(*) as total_bets,
                COUNT(*) FILTER (WHERE status = 'won') as bets_won,
                COUNT(*) FILTER (WHERE status = 'lost') as bets_lost,
                COUNT(*) FILTER (WHERE status = 'push') as bets_push,
                COUNT(*) FILTER (WHERE status = 'pending') as bets_pending,
                COALESCE(SUM(stake) FILTER (WHERE status IN ('won', 'lost')), 0) as total_staked,
                COALESCE(SUM(pnl), 0) as total_pnl
            FROM mlb_paper_bets
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

        prev_query = text("""
            SELECT cumulative_pnl, bankroll_after
            FROM mlb_paper_trading_daily_log
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

        upsert_query = text("""
            INSERT INTO mlb_paper_trading_daily_log (
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

        logger.info(f"Updated MLB daily log for {game_date}: P&L={total_pnl:.2f}")
