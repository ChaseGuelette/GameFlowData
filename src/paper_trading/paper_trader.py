"""
Paper Trading Module for NBA Player Props.

Converts daily predictions into frozen $10 automatic evaluation entries,
and resolves bets against actual game results.

Supports Black-Litterman probability blending to anchor model probabilities
to market priors, improving edge calibration.

Configuration via environment variables:
    PAPER_TRADING_BANKROLL: Starting bankroll (default: 10000)
    PAPER_TRADING_KELLY_FRACTION: Kelly fraction (default: 0.125)
    PAPER_TRADING_EDGE_THRESHOLD: Diagnostic threshold; placement uses promoted policy
    PAPER_TRADING_MAX_BET_PCT: Max bet as % of bankroll (default: None = no cap)
    PAPER_TRADING_BL_TAU: Diagnostic only; BL is applied by prediction producers
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import date
from typing import TYPE_CHECKING, Any

import pandas as pd
from sqlalchemy import text

from src.db.client import get_engine
from src.models.nba_paper_policy import NBA_PAPER_POLICY, apply_recommendation_policy, is_placeable_selection
from src.paper_trading.nba_paper_entry import (
    EVALUATION_RUN_ID,
    FIXED_STAKE,
    eligible_at_boundary,
    insert_bet,
    valid_snapshot,
)
from src.paper_trading.nba_settlement import resolve_date, resolve_pending

if TYPE_CHECKING:
    from src.config.stat_config import StatConfigSet

logger = logging.getLogger(__name__)

# This forward paper policy covers the three evaluated base stats.
SUPPORTED_STATS = set(NBA_PAPER_POLICY.stats)


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
DEFAULT_EDGE_THRESHOLD = _get_env_float("PAPER_TRADING_EDGE_THRESHOLD", NBA_PAPER_POLICY.edge_threshold)
DEFAULT_MAX_BET_PCT = _get_env_float_or_none("PAPER_TRADING_MAX_BET_PCT", None)
DEFAULT_BL_TAU = _get_env_float_or_none("PAPER_TRADING_BL_TAU", NBA_PAPER_POLICY.bl_tau)


@dataclass
class PaperTrader:
    """
    Converts daily predictions into paper bets and tracks P&L.

    Supports standalone operation for dashboard integration.
    Supports per-stat edge thresholds via stat_config.

    Uses stored BL-blended edges (bl_over_edge/bl_under_edge) from daily_predictions,
    which are computed by the inference_job and edge_refresh_job. New entries
    require captured producer evidence and use fixed $10 units. Kelly settings
    remain diagnostic only; absent BL evidence is not a placement fallback.

    Configuration can be overridden via environment variables:
    - PAPER_TRADING_BANKROLL: Starting bankroll
    - PAPER_TRADING_KELLY_FRACTION: Kelly fraction
    - PAPER_TRADING_EDGE_THRESHOLD: Edge threshold
    - PAPER_TRADING_MAX_BET_PCT: Max bet % (set to "none" for no cap)
    - PAPER_TRADING_BL_TAU: Accepted but unused (BL already applied by inference job)
    """

    edge_threshold: float = field(default_factory=lambda: DEFAULT_EDGE_THRESHOLD)
    kelly_fraction: float = field(default_factory=lambda: DEFAULT_KELLY_FRACTION)
    max_bet_pct: float | None = field(default_factory=lambda: DEFAULT_MAX_BET_PCT)
    starting_bankroll: float = field(default_factory=lambda: DEFAULT_BANKROLL)
    min_odds: int = -200  # Don't bet heavy favorites
    max_odds: int = 200  # Don't bet long shots
    stat_config: StatConfigSet | None = None  # Per-stat edge thresholds
    bl_tau: float | None = field(default_factory=lambda: DEFAULT_BL_TAU)  # Kept for compat
    bl_z_max: float = NBA_PAPER_POLICY.bl_z_max  # Kept for compat

    def __post_init__(self):
        self.engine = get_engine()

        # Log active configuration
        logger.info(
            f"PaperTrader initialized: bankroll=${self.starting_bankroll:,.0f}, "
            f"kelly={self.kelly_fraction}, edge_threshold={self.edge_threshold}, "
            f"max_bet_pct={self.max_bet_pct or 'None (no cap)'}"
        )

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
        """Diagnostic fractional Kelly sizing; never used for new evaluation entries."""
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

    def select_bets(self, game_date: date) -> list[dict[str, Any]]:
        """Reapply the promoted policy before freezing producer evidence."""
        with self.engine.connect() as conn:
            df = pd.read_sql(text("""
                SELECT id AS prediction_id, player_id, player_name, game_id, stat,
                    line, over_odds, under_odds, bookmaker, bl_over_prob, bl_under_prob,
                    bl_over_edge, bl_under_edge, implied_over, implied_under,
                    quote_snapshot, model_snapshot
                FROM daily_predictions
                WHERE prediction_date = :game_date
                  AND line IS NOT NULL AND bl_over_edge IS NOT NULL AND bl_under_edge IS NOT NULL
                ORDER BY player_name, stat
            """), conn, params={"game_date": game_date})
        apply_recommendation_policy(df)
        bets = []
        for record in df.loc[df["is_recommended"].eq(True)].to_dict(orient="records"):
            row: dict[str, Any] = {str(k): (None if isinstance(v, float) and pd.isna(v) else v) for k, v in record.items()}
            row["game_date"] = game_date.isoformat()
            if row.get("stat") not in SUPPORTED_STATS or not valid_snapshot(row):
                logger.info("Paper candidate %s blocked: missing/conflicting producer evidence", row.get("prediction_id"))
                continue
            direction = "under"
            values = [row.get(k) for k in (f"bl_{direction}_prob", f"implied_{direction}", f"bl_{direction}_edge")]
            if any(v is None or not pd.notna(v) for v in values):
                continue
            probability, implied, edge = (float(v) for v in values if v is not None)
            if not (0 <= probability <= 1 and 0 <= implied <= 1 and edge > 0):
                continue
            with self.engine.begin() as conn:
                if not eligible_at_boundary(conn, row):
                    continue
            bets.append({
                "prediction_id": int(row["prediction_id"]), "game_date": game_date,
                "game_id": row["game_id"], "player_id": int(row["player_id"]),
                "player_name": row["player_name"], "stat_type": row["stat"],
                "line": float(row["line"]), "bet_direction": direction,
                "odds_at_bet": float(row[f"{direction}_odds"]), "implied_prob": implied,
                "model_prob": probability, "edge": edge, "stake": FIXED_STAKE, "kelly_fraction": 0,
                "evaluation_run_id": EVALUATION_RUN_ID,
                "entry_snapshot": {"policy": EVALUATION_RUN_ID, "fixed_stake": FIXED_STAKE, "prediction": row},
            })
        return bets

    def place_bets(self, bets: list[dict[str, Any]]) -> int:
        """Freeze first eligible entry; count RETURNING rows, never attempts."""
        if not bets:
            return 0
        inserted = 0
        with self.engine.begin() as conn:
            for bet in bets:
                snapshot = bet.get("entry_snapshot") or {}
                row = snapshot.get("prediction") or {}
                side = bet.get("bet_direction")
                if (bet.get("evaluation_run_id") != EVALUATION_RUN_ID
                        or bet.get("stake") != FIXED_STAKE or not valid_snapshot(row)
                        or not is_placeable_selection(row, direction=side, edge=bet.get("edge"))):
                    continue
                frozen = {"game_id": row["game_id"], "player_id": row["player_id"],
                          "prediction_id": row["prediction_id"], "stat_type": row["stat"],
                          "line": row["line"], "odds_at_bet": row[f"{side}_odds"],
                          "model_prob": row[f"bl_{side}_prob"], "implied_prob": row[f"implied_{side}"],
                          "edge": row[f"bl_{side}_edge"]}
                if (str(bet["game_date"]) != row["game_date"]
                        or any(bet.get(k) != v for k, v in frozen.items())):
                    continue
                inserted += int(insert_bet(conn, bet))
        logger.info("Inserted %d automatic paper bets", inserted)
        return inserted

    def resolve_bets(self, game_date: date) -> dict[str, Any]:
        result = resolve_date(self.engine, "paper_bets", game_date)
        self._update_daily_log(game_date)
        logger.info("Automatic NBA settlement %s: %s", game_date, result)
        return result

    def resolve_all_pending(self, exclude_today: bool = True) -> dict[str, Any]:
        result = resolve_pending(self.engine, "paper_bets", self.resolve_bets, exclude_today=exclude_today)
        # Retry summary writes even when the previous run settled the final row.
        self._update_daily_log(date.today())
        return result

    def _update_daily_log(self, game_date: date) -> None:
        """Rebuild all dates from ledger so late settlements repair cumulative totals."""
        with self.engine.begin() as conn:
            # Serialize summary writers before taking the ledger snapshot.
            conn.execute(text("SELECT pg_advisory_xact_lock(74290111)"))
            conn.execute(text("""
                WITH daily AS (
                    SELECT game_date, COUNT(*) AS total_bets,
                        COUNT(*) FILTER (WHERE status = 'won') AS bets_won,
                        COUNT(*) FILTER (WHERE status = 'lost') AS bets_lost,
                        COUNT(*) FILTER (WHERE status = 'push') AS bets_push,
                        COUNT(*) FILTER (WHERE status = 'pending') AS bets_pending,
                        COALESCE(SUM(stake) FILTER (WHERE status IN ('won', 'lost')), 0) AS total_staked,
                        COALESCE(SUM(pnl) FILTER (WHERE status <> 'pending'), 0) AS total_pnl
                    FROM paper_bets GROUP BY game_date
                ), cumulative AS (
                    SELECT *, SUM(total_pnl) OVER (ORDER BY game_date) AS cumulative_pnl FROM daily
                )
                INSERT INTO paper_trading_daily_log (
                    game_date, total_bets, bets_won, bets_lost, bets_push, bets_pending,
                    total_staked, total_pnl, roi_pct, cumulative_pnl, bankroll_after
                ) SELECT game_date, total_bets, bets_won, bets_lost, bets_push, bets_pending,
                    total_staked, total_pnl,
                    CASE WHEN total_staked > 0 THEN ROUND(total_pnl / total_staked * 100, 2) ELSE 0 END,
                    cumulative_pnl, :bankroll + cumulative_pnl FROM cumulative WHERE true
                ON CONFLICT (game_date) DO UPDATE SET
                    total_bets = EXCLUDED.total_bets, bets_won = EXCLUDED.bets_won,
                    bets_lost = EXCLUDED.bets_lost, bets_push = EXCLUDED.bets_push,
                    bets_pending = EXCLUDED.bets_pending, total_staked = EXCLUDED.total_staked,
                    total_pnl = EXCLUDED.total_pnl, roi_pct = EXCLUDED.roi_pct,
                    cumulative_pnl = EXCLUDED.cumulative_pnl, bankroll_after = EXCLUDED.bankroll_after,
                    updated_at = NOW()
            """), {"bankroll": self.starting_bankroll})

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
