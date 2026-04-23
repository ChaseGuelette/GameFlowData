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

import logging
import os
from dataclasses import dataclass, field
from datetime import date
from typing import Any

import pandas as pd
from sqlalchemy import text

from src.db.client import get_engine
from src.models.mlb.mlb_stat_config import MLB_STATS

logger = logging.getLogger(__name__)

# Stat resolution: stat_type -> (table, columns) for resolving actual values.
# columns is a list of column names; compound stats use multiple columns summed as
# `s.col1 + s.col2 + ...` in queries.
MLB_STAT_RESOLUTION: dict[str, tuple[str, list[str]]] = {
    "pitcher_strikeouts": ("mlb_player_game_stats_pitching", ["so"]),
    "pitcher_outs":       ("mlb_player_game_stats_pitching", ["outs_recorded"]),
    "batter_hits":        ("mlb_player_game_stats_batting",  ["h"]),
    "batter_rbis":        ("mlb_player_game_stats_batting",  ["rbi"]),
    "batter_hrr":         ("mlb_player_game_stats_batting",  ["h", "r", "rbi"]),
}

# DNP detection columns per table
MLB_DNP_COLUMNS: dict[str, str] = {
    "mlb_player_game_stats_pitching": "did_not_play",
    "mlb_player_game_stats_batting": "did_not_play",
}

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
DEFAULT_MAX_BET_PCT = _get_env_float("MLB_PAPER_TRADING_MAX_BET_PCT", 0.03)  # 3% of bankroll cap per bet


@dataclass
class MLBPaperTrader:
    """
    Converts MLB daily predictions into paper bets and tracks P&L.

    Selects bets that match exactly what the Model Picks page shows:
    any prediction with is_recommended=True (set by the inference job's
    BL blending). Uses pre-computed bl_over_prob/bl_under_prob for Kelly sizing.
    """

    kelly_fraction: float = field(default_factory=lambda: DEFAULT_KELLY_FRACTION)
    max_bet_pct: float = field(default_factory=lambda: DEFAULT_MAX_BET_PCT)
    starting_bankroll: float = field(default_factory=lambda: DEFAULT_BANKROLL)

    def __post_init__(self):
        self.engine = get_engine()
        logger.info(
            f"MLBPaperTrader initialized: bankroll=${self.starting_bankroll:,.0f}, "
            f"kelly={self.kelly_fraction}, max_bet_pct={self.max_bet_pct:.1%}"
        )

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
        Select bets matching exactly what the Model Picks page shows.

        Queries mlb_daily_predictions for rows with is_recommended=True and uses
        the pre-computed BL edges/probs stored by the inference job. Direction is
        whichever of bl_over_edge / bl_under_edge is higher (mirrors inference logic).
        """
        bankroll = self._get_current_bankroll()

        query = text("""
            SELECT
                id as prediction_id,
                player_id,
                player_name,
                stat,
                line,
                over_odds,
                under_odds,
                bl_over_prob,
                bl_under_prob,
                bl_over_edge,
                bl_under_edge,
                implied_over,
                implied_under
            FROM mlb_daily_predictions
            WHERE prediction_date = :game_date
              AND is_recommended = true
              AND stat IN ('pitcher_strikeouts', 'batter_hits')
              AND line IS NOT NULL
              AND bl_over_edge IS NOT NULL
              AND bl_under_edge IS NOT NULL
            ORDER BY player_name, stat
        """)

        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"game_date": game_date})

        if df.empty:
            logger.warning(f"No recommended MLB predictions found for {game_date}")
            return []

        bets = []
        for _, row in df.iterrows():
            over_odds = row["over_odds"]
            under_odds = row["under_odds"]
            if pd.isna(over_odds) or pd.isna(under_odds):
                continue

            bl_over_edge = float(row["bl_over_edge"])
            bl_under_edge = float(row["bl_under_edge"])

            # Determine direction respecting allowed_directions from config
            stat = str(row["stat"])
            allowed_dirs = MLB_STATS.get(stat, {}).get("allowed_directions")

            if bl_over_edge >= bl_under_edge:
                raw_dir = "over"
            else:
                raw_dir = "under"

            if allowed_dirs and raw_dir not in allowed_dirs:
                # Override to the allowed direction
                direction = "under" if raw_dir == "over" else "over"
            else:
                direction = raw_dir

            if direction == "over":
                edge = bl_over_edge
                odds = int(over_odds)
                model_prob = float(row["bl_over_prob"])
                implied_prob = float(row["implied_over"])
            else:
                edge = bl_under_edge
                odds = int(under_odds)
                model_prob = float(row["bl_under_prob"])
                implied_prob = float(row["implied_under"])

            stake = self._calculate_kelly_stake(odds, model_prob, bankroll)
            if stake <= 0:
                continue

            bets.append({
                "prediction_id": int(row["prediction_id"]),
                "game_date": game_date,
                "player_id": int(row["player_id"]),
                "player_name": row["player_name"],
                "stat_type": str(row["stat"]),
                "line": float(row["line"]),
                "bet_direction": direction,
                "odds_at_bet": float(odds),
                "implied_prob": implied_prob,
                "model_prob": model_prob,
                "edge": edge,
                "stake": round(stake, 2),
                "kelly_fraction": self.kelly_fraction,
            })

        # Deduplicate by (player_id, stat_type) — keep highest-edge bet.
        # Multiple bookmakers can each produce an is_recommended=True row for the
        # same player in opposite directions. We only want one bet per player per stat.
        seen: dict[tuple, dict] = {}
        for bet in bets:
            key = (bet["player_id"], bet["stat_type"])
            if key not in seen or abs(bet["edge"]) > abs(seen[key]["edge"]):
                seen[key] = bet
        bets = list(seen.values())

        logger.info(f"Selected {len(bets)} MLB bets for {game_date} (is_recommended=True, deduped by player+stat)")
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
            ON CONFLICT (game_date, player_id, stat_type)
            DO UPDATE SET
                prediction_id = EXCLUDED.prediction_id,
                bet_direction = EXCLUDED.bet_direction,
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

            table_name, columns = resolution
            col_expr = " + ".join(f"s.{c}" for c in columns)
            dnp_col = MLB_DNP_COLUMNS.get(table_name, "did_not_play")

            actuals_query = text(f"""
                SELECT
                    s.player_id,
                    {col_expr} as actual_value,
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
