"""Side-effectful Kalshi trade-selection input loading.

This module owns the DB/API/environment loading needed before the pure strategy
service can select trades.  Keep deterministic policy in ``strategy.py``; this
loader only converts live state into strategy inputs.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Callable

import pandas as pd
from sqlalchemy import text

from src.models.daily_runner import should_skip_recommendation
from src.scrapers.kalshi.kalshi_utils import fee_adjusted_edge
from src.trading.kalshi.live_trading_config import SPORTSBOOK_LINE_FALLBACK_GAP
from src.trading.kalshi.strategy import ExistingPosition, StrategyConfig, TradeCandidate

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TradeSelectionInputs:
    """All preloaded inputs required by the pure Kalshi strategy service."""

    candidates: list[TradeCandidate]
    config: StrategyConfig
    existing_player_stats: set[tuple[int, str]]
    queued_player_stats: set[tuple[int, str]]
    held_positions: dict[str, ExistingPosition]
    mode_str: str
    effective_daily_exposure_cap: float


def _env_float(name: str, default: float) -> float:
    val = os.environ.get(name)
    if val is None:
        return default
    try:
        return float(val)
    except ValueError:
        logger.warning(f"Invalid {name}={val}, using default {default}")
        return default


class KalshiSelectionInputLoader:
    """Load side-effectful state for ``select_trades`` without selecting policy."""

    def __init__(
        self,
        *,
        engine: Any,
        client: Any,
        supported_stats: dict[str, set[str]],
        get_game_start_time: Callable[[str, dict[str, datetime | None]], datetime | None],
        sportsbook_line_fallback_gap: float = SPORTSBOOK_LINE_FALLBACK_GAP,
    ):
        self.engine = engine
        self.client = client
        self.supported_stats = supported_stats
        self.get_game_start_time = get_game_start_time
        self.sportsbook_line_fallback_gap = sportsbook_line_fallback_gap

    def load_inputs(
        self,
        target_date: date,
        *,
        sport: str,
        prior_exposure: float,
        strategy_knobs: dict[str, Any],
    ) -> TradeSelectionInputs | None:
        """Load API/DB/env state and convert it to pure strategy inputs.

        Returns ``None`` when trading should abort early, preserving the legacy
        ``select_trades`` behavior for disabled sports, balance failures, and
        empty market pools.
        """
        sport_gate_var = f"{sport.upper()}_TRADING_ENABLED"
        if os.getenv(sport_gate_var, "false").lower() != "true":
            logger.info(f"Live trading disabled for {sport} ({sport_gate_var}!=true)")
            return None

        balance_data = self.client.get_balance()
        if balance_data is None:
            logger.error(
                "LIVE TRADE ABORT: get_balance() returned None — "
                "Kalshi portfolio API unreachable or API key lacks portfolio permissions. "
                "Check KALSHI_API_KEY scope and account status."
            )
            return None
        bankroll = balance_data.get("balance", 0) / 100.0
        logger.info(f"Kalshi balance: ${bankroll:.2f}")

        daily_exposure_pct = _env_float("KALSHI_DAILY_EXPOSURE_PCT", 0.60)
        min_exposure = _env_float("KALSHI_MIN_DAILY_EXPOSURE", 80.0)
        max_exposure = _env_float("KALSHI_MAX_DAILY_EXPOSURE", 500.0)
        dynamic_cap = bankroll * daily_exposure_pct
        effective_cap = max(min_exposure, min(dynamic_cap, max_exposure))
        allow_yes = os.environ.get("KALSHI_ALLOW_YES_BETS", "false").lower() == "true"
        mode_str = "YES+NO" if allow_yes else "NO-only"
        logger.info(
            f"Mode: {mode_str} | Exposure cap: ${bankroll:.2f} × {daily_exposure_pct:.0%} "
            f"= ${dynamic_cap:.2f} → effective ${effective_cap:.2f} "
            f"(floor ${min_exposure:.0f}, ceiling ${max_exposure:.0f})"
        )

        positions = self.client.get_positions(settlement_status="open")
        held_positions = {
            p.get("ticker"): ExistingPosition(total_traded=int(p.get("total_traded", 0) or 0))
            for p in positions
            if p.get("ticker")
        }

        df = self._load_market_rows(target_date, sport)
        if df.empty:
            logger.info(f"No Kalshi markets with model probs for {target_date}")
            return None

        existing_player_stats = self._load_existing_player_stats(target_date)
        queued_player_stats = self._load_queued_player_stats(target_date, sport)
        if queued_player_stats:
            logger.info(f"Skipping {len(queued_player_stats)} player+stat combos already pending approval")

        existing_exposure = self._load_existing_exposure(target_date)
        total_prior_exposure = float(existing_exposure or 0) + prior_exposure

        allowed_directions_by_stat = self._load_allowed_directions_by_stat(sport)
        game_start_times = self._lookup_game_start_times(target_date, sport)
        candidates, pool_no_edges = self._build_candidates(
            df,
            target_date=target_date,
            sport=sport,
            allowed_directions_by_stat=allowed_directions_by_stat,
            game_start_times=game_start_times,
        )

        tiers = [0, 3, 5, 10, 15]
        tier_str = ", ".join(
            f">{t}%: {sum(1 for e in pool_no_edges if e >= t / 100.0)}"
            for t in tiers
        )
        logger.info(f"BET POOL ({target_date} {mode_str}): {tier_str}")

        config = StrategyConfig(
            min_edge=strategy_knobs["min_edge"],
            max_edge=_env_float("KALSHI_LIVE_MAX_EDGE", 0.40),
            min_price=strategy_knobs["min_price"],
            max_contracts=strategy_knobs["max_contracts"],
            kelly_fraction=strategy_knobs["kelly_fraction"],
            bankroll=bankroll,
            effective_daily_exposure_cap=effective_cap,
            prior_exposure=total_prior_exposure,
            allow_yes=allow_yes,
            star_hits_yes_price=int(os.environ.get("KALSHI_STAR_HITS_YES_PRICE", "72")),
            sportsbook_line_fallback_gap=self.sportsbook_line_fallback_gap,
            supported_stats=self.supported_stats,
        )
        return TradeSelectionInputs(
            candidates=candidates,
            config=config,
            existing_player_stats=existing_player_stats,
            queued_player_stats=queued_player_stats,
            held_positions=held_positions,
            mode_str=mode_str,
            effective_daily_exposure_cap=effective_cap,
        )

    def _load_market_rows(self, target_date: date, sport: str) -> pd.DataFrame:
        query = text("""
            SELECT DISTINCT ON (ticker)
                ticker, sport, player_id, player_name, stat_type, line,
                yes_price, model_prob, kalshi_implied,
                raw_edge, maker_fee_adjusted_edge,
                volume, bid_ask_spread, market_status,
                bl_model_prob, bl_edge,
                sportsbook_consensus_line, line_vs_sportsbook
            FROM kalshi_markets
            WHERE sport = :sport
              AND (snapshot_time AT TIME ZONE 'America/New_York')::date = :target_date
              AND market_status = 'open'
              AND model_prob IS NOT NULL
            ORDER BY ticker, snapshot_time DESC
        """)
        with self.engine.connect() as conn:
            return pd.read_sql(query, conn, params={"sport": sport, "target_date": target_date})

    def _load_existing_player_stats(self, target_date: date) -> set[tuple[int, str]]:
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT DISTINCT player_id, stat_type
                FROM kalshi_live_orders
                WHERE game_date = :d AND status != 'cancelled'
                  AND player_id IS NOT NULL
            """), {"d": target_date}).fetchall()
        return {(int(row[0]), row[1]) for row in rows}

    def _load_queued_player_stats(self, target_date: date, sport: str) -> set[tuple[int, str]]:
        with self.engine.connect() as conn:
            queued_rows = conn.execute(text("""
                SELECT player_id, stat_type FROM kalshi_trade_queue
                WHERE game_date = :d AND sport = :sport
                  AND status = 'pending_approval'
                  AND expires_at > now()
                  AND player_id IS NOT NULL
            """), {"d": target_date, "sport": sport}).fetchall()
        return {(int(r[0]), r[1]) for r in queued_rows}

    def _load_existing_exposure(self, target_date: date) -> float:
        with self.engine.connect() as conn:
            return conn.execute(text("""
                SELECT COALESCE(SUM(total_cost), 0)
                FROM kalshi_live_orders
                WHERE game_date = :d AND status != 'cancelled'
            """), {"d": target_date}).scalar()

    def _lookup_game_start_times(self, target_date: date, sport: str) -> dict[str, datetime | None]:
        start_times: dict[str, datetime | None] = {}
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(text("""
                    SELECT DISTINCT ON (ticker) ticker, close_time
                    FROM kalshi_markets
                    WHERE sport = :sport
                      AND (snapshot_time AT TIME ZONE 'America/New_York')::date = :d
                      AND close_time IS NOT NULL
                    ORDER BY ticker, snapshot_time DESC
                """), {"d": target_date, "sport": sport}).fetchall()
                for row in rows:
                    start_times[row[0]] = row[1]
        except Exception as e:
            logger.debug(f"Game start time lookup failed (non-fatal): {e}")
        return start_times

    def _load_allowed_directions_by_stat(self, sport: str) -> dict[str, tuple[str, ...] | None]:
        if sport != "mlb":
            return {}
        from src.models.mlb.mlb_stat_config import MLB_STATS as _MLB_STATS

        return {
            stat: tuple(config.get("allowed_directions", ["over", "under"]))
            for stat, config in _MLB_STATS.items()
        }

    def _build_candidates(
        self,
        df: pd.DataFrame,
        *,
        target_date: date,
        sport: str,
        allowed_directions_by_stat: dict[str, tuple[str, ...] | None],
        game_start_times: dict[str, datetime | None],
    ) -> tuple[list[TradeCandidate], list[float]]:
        candidates: list[TradeCandidate] = []
        pool_no_edges: list[float] = []

        for _, row in df.iterrows():
            yes_price = int(row["yes_price"])
            model_prob = float(row["bl_model_prob"]) if pd.notna(row.get("bl_model_prob")) else float(row["model_prob"])
            no_edge = fee_adjusted_edge(model_prob, yes_price, is_yes=False, is_maker=False)
            if no_edge > 0:
                pool_no_edges.append(no_edge)

            stat_type = row["stat_type"]
            line_val = float(row["line"]) if pd.notna(row.get("line")) else None
            structural_reason: str | None = None
            structural_skip_directions: list[str] = []
            for direction in ("over", "under"):
                skip, reason = should_skip_recommendation(stat=stat_type, direction=direction, line=line_val)
                if skip:
                    structural_skip_directions.append(direction)
                    structural_reason = reason if structural_reason is None else structural_reason
                    logger.debug(f"SKIP {row['player_name']} {stat_type} {direction}: {reason}")

            candidate = TradeCandidate(
                game_date=target_date,
                ticker=row["ticker"],
                sport=sport,
                player_id=int(row["player_id"]) if pd.notna(row["player_id"]) else None,
                player_name=row["player_name"],
                stat_type=stat_type,
                line=line_val,
                yes_price=yes_price,
                model_prob=model_prob,
                kalshi_implied=float(row["kalshi_implied"]) if pd.notna(row.get("kalshi_implied")) else yes_price / 100.0,
                raw_edge=float(row["raw_edge"]) if pd.notna(row.get("raw_edge")) else 0.0,
                volume=int(row["volume"] or 0),
                bid_ask_spread=int(row["bid_ask_spread"] or 100),
                sportsbook_consensus_line=(
                    float(row["sportsbook_consensus_line"])
                    if pd.notna(row.get("sportsbook_consensus_line"))
                    else None
                ),
                game_start_time=self.get_game_start_time(row["ticker"], game_start_times),
                allowed_directions=allowed_directions_by_stat.get(stat_type),
                structural_skip_reason=structural_reason,
                structural_skip_directions=tuple(structural_skip_directions),
            )
            candidates.append(candidate)

        return candidates, pool_no_edges
