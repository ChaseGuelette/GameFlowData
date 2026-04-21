"""
Paper Trading Module for Kalshi Player Prop Markets.

Mirrors the live trader 1:1 (same edge thresholds, fees, sizing, filters)
but uses DB-tracked bankroll instead of real API balance.

Key features:
  - Taker fee structure (7% base) matching live execution
  - 15% minimum fee-adjusted edge threshold (sniper mode)
  - Position accumulation awareness (dedup same-day same-ticker)
  - Discord notifications on every bet placed and resolved
  - Kelly sizing in contracts with daily exposure cap

Configuration via environment variables:
    KALSHI_PAPER_TRADING_BANKROLL: Starting bankroll (default: 100)
    KALSHI_PAPER_TRADING_KELLY_FRACTION: Kelly fraction (default: 0.125)
"""

from __future__ import annotations

import json
import logging
import math
import os
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

import pandas as pd
from sqlalchemy import text

from src.db.client import get_engine
from src.models.daily_runner import should_skip_recommendation
from src.paper_trading.mlb_paper_trader import MLB_STAT_RESOLUTION
from src.scrapers.kalshi.kalshi_utils import fee_adjusted_edge, kalshi_taker_fee

logger = logging.getLogger(__name__)

# When two lines for the same player+stat are within this edge gap,
# prefer the line closest to the sportsbook consensus (more reliable signal).
_LINE_TIEBREAK_THRESHOLD = 0.03  # 3 percentage points

# NBA stat resolution: stat_type -> (table, [columns to sum])
NBA_STAT_RESOLUTION: dict[str, tuple[str, list[str]]] = {
    "pts": ("player_game_stats", ["pts"]),
    "reb": ("player_game_stats", ["reb"]),
    "ast": ("player_game_stats", ["ast"]),
    "stl": ("player_game_stats", ["stl"]),
    "blk": ("player_game_stats", ["blk"]),
    "3pm": ("player_game_stats", ["fg3m"]),
    "pra": ("player_game_stats", ["pts", "reb", "ast"]),
    "pr":  ("player_game_stats", ["pts", "reb"]),
    "pa":  ("player_game_stats", ["pts", "ast"]),
    "ra":  ("player_game_stats", ["reb", "ast"]),
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


# Supported stat types per sport (skip markets without trained models)
SUPPORTED_STATS: dict[str, set[str]] = {
    "nba": {"pts", "reb", "ast", "pra", "pr", "pa", "ra", "stl", "blk", "3pm"},
    "mlb": {"pitcher_strikeouts", "batter_hits", "batter_hrr"},
}

DEFAULT_BANKROLL = _get_env_float("KALSHI_PAPER_TRADING_BANKROLL", 100.0)
DEFAULT_KELLY_FRACTION = _get_env_float("KALSHI_PAPER_TRADING_KELLY_FRACTION", 0.125)


@dataclass
class KalshiPaperTrader:
    """
    Simulates paper trading on Kalshi binary contract markets.

    Reads edges from kalshi_markets, sizes positions via fractional Kelly,
    places paper bets, and resolves against actual player stats.
    """

    min_fee_adjusted_edge: float = 0.15  # Sniper mode — matches live trader
    max_contracts_per_market: int = 50
    max_daily_exposure: float = 80.0
    min_volume: int = 20  # Matches live trader (taker fills anyway)
    max_spread: int = 15  # Matches live trader
    min_price: int = 5  # Skip extreme prices (below 5c or above 95c)
    starting_bankroll: float = field(default_factory=lambda: DEFAULT_BANKROLL)
    kelly_fraction: float = field(default_factory=lambda: DEFAULT_KELLY_FRACTION)

    def __post_init__(self):
        self.engine = get_engine()
        logger.info(
            f"KalshiPaperTrader initialized: bankroll=${self.starting_bankroll:,.0f}, "
            f"kelly={self.kelly_fraction}, min_edge={self.min_fee_adjusted_edge:.1%}"
        )

    # ------------------------------------------------------------------
    # Bankroll
    # ------------------------------------------------------------------

    def get_bankroll(self) -> float:
        """Get current bankroll from latest daily log entry."""
        query = text("""
            SELECT bankroll_after
            FROM kalshi_paper_trading_daily_log
            ORDER BY game_date DESC
            LIMIT 1
        """)
        with self.engine.connect() as conn:
            result = conn.execute(query).fetchone()
        if result is None:
            return self.starting_bankroll
        return float(result[0])

    # ------------------------------------------------------------------
    # Kelly sizing
    # ------------------------------------------------------------------

    def _kelly_contracts(
        self, model_prob: float, price_cents: int, side: str, bankroll: float
    ) -> int:
        """Calculate Kelly-optimal number of contracts.

        For YES at price P (cents):
          cost_per = P/100, win_per = (100-P)/100
          f = (model_prob_yes - cost_per) / win_per

        For NO at price P (yes_price P means NO costs (100-P)):
          cost_per = (100-P)/100, win_per = P/100
          f = (model_prob_no - cost_per) / win_per
        """
        if side == "yes":
            cost_per = price_cents / 100.0
            win_per = (100 - price_cents) / 100.0
            prob = model_prob  # P(over) = P(YES wins)
        else:
            cost_per = (100 - price_cents) / 100.0
            win_per = price_cents / 100.0
            prob = 1.0 - model_prob  # P(under) = P(NO wins)

        if win_per <= 0 or cost_per <= 0:
            return 0

        f = (prob - cost_per) / win_per
        f_fractional = f * self.kelly_fraction

        if f_fractional <= 0:
            return 0

        # Deduct expected taker fee from effective return
        fee_per = kalshi_taker_fee(price_cents if side == "yes" else 100 - price_cents)
        net_win_per = win_per - fee_per
        if net_win_per <= 0:
            return 0

        f_net = (prob - cost_per) / net_win_per
        f_net_fractional = f_net * self.kelly_fraction
        if f_net_fractional <= 0:
            return 0

        contracts = int(math.floor(f_net_fractional * bankroll / cost_per))
        return min(contracts, self.max_contracts_per_market)

    # ------------------------------------------------------------------
    # Select bets
    # ------------------------------------------------------------------

    def select_bets(self, target_date: date, sport: str = "nba", prior_exposure: float = 0.0) -> list[dict[str, Any]]:
        """Select paper bets from kalshi_markets with sufficient taker-fee-adjusted edge.

        Mirrors live trader logic: taker fees, position accumulation awareness,
        same-day dedup, daily exposure cap.

        MLB priority rule: to ensure MLB bets take precedence over NBA within the
        shared daily exposure cap, call with sport="mlb" first, then pass the
        resulting total cost as prior_exposure= when calling with sport="nba".
        """
        bankroll = self.get_bankroll()

        # Bankroll-proportional exposure cap
        daily_exposure_pct = _get_env_float("KALSHI_DAILY_EXPOSURE_PCT", 0.90)
        min_exposure = _get_env_float("KALSHI_MIN_DAILY_EXPOSURE", 80.0)
        max_exposure = _get_env_float("KALSHI_MAX_DAILY_EXPOSURE", 500.0)
        dynamic_cap = bankroll * daily_exposure_pct
        effective_cap = max(min_exposure, min(dynamic_cap, max_exposure))
        # NO-only mode control
        _allow_yes = os.environ.get("KALSHI_ALLOW_YES_BETS", "false").lower() == "true"
        _mode_str = "YES+NO" if _allow_yes else "NO-only"
        logger.info(
            f"Mode: {_mode_str} | Exposure cap: ${bankroll:.2f} × {daily_exposure_pct:.0%} "
            f"= ${dynamic_cap:.2f} → effective ${effective_cap:.2f} "
            f"(floor ${min_exposure:.0f}, ceiling ${max_exposure:.0f})"
        )

        # Get latest snapshot per ticker for the date
        query = text("""
            SELECT DISTINCT ON (ticker)
                ticker, sport, player_id, player_name, stat_type, line,
                yes_price, model_prob, kalshi_implied,
                raw_edge, taker_fee_adjusted_edge,
                volume, bid_ask_spread, market_status,
                bl_model_prob, bl_edge, bl_confidence,
                sportsbook_consensus_line, line_vs_sportsbook, market_title,
                close_time
            FROM kalshi_markets
            WHERE sport = :sport
              AND snapshot_time::date = :target_date
              AND market_status = 'open'
              AND model_prob IS NOT NULL
            ORDER BY ticker, snapshot_time DESC
        """)

        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn, params={
                "sport": sport, "target_date": target_date,
            })

        if df.empty:
            logger.info(f"No Kalshi markets with edges for {target_date}")
            return []

        # Position accumulation: check what we already bet on today (by player+stat)
        today_positions: set[tuple[int, str]] = set()
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT DISTINCT player_id, stat_type
                FROM kalshi_paper_bets
                WHERE game_date = :d
                  AND status NOT IN ('cancelled', 'overflow', 'overflow_won', 'overflow_lost', 'overflow_cancelled')
                  AND player_id IS NOT NULL
            """), {"d": target_date}).fetchall()
            today_positions = {(int(row[0]), row[1]) for row in rows}

        # Check today's existing exposure (real bets only, not overflow)
        existing_exposure = 0.0
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT COALESCE(SUM(
                    CASE WHEN side = 'yes' THEN contracts * fill_price / 100.0
                         ELSE contracts * (100 - fill_price) / 100.0
                    END
                ), 0)
                FROM kalshi_paper_bets
                WHERE game_date = :d
                  AND status NOT IN ('cancelled', 'overflow', 'overflow_won', 'overflow_lost', 'overflow_cancelled')
            """), {"d": target_date}).scalar()
            existing_exposure = float(result or 0) + prior_exposure

        # First pass: find best-edge candidate per (player_id, stat_type)
        run_candidates: dict[tuple[int, str], dict] = {}
        _pool_no_edges: list[float] = []  # All NO edges (for pool logging)

        for _, row in df.iterrows():
            player_id = int(row["player_id"]) if pd.notna(row["player_id"]) else None
            stat_type = row["stat_type"]

            if player_id is None:
                continue

            pos_key = (player_id, stat_type)

            # Stat whitelist: skip unsupported stat types
            supported = SUPPORTED_STATS.get(sport, set())
            if supported and stat_type not in supported:
                continue

            # Skip if already bet on today (position accumulation awareness)
            if pos_key in today_positions:
                continue

            volume = int(row["volume"] or 0)
            spread = int(row["bid_ask_spread"] or 100)

            # Liquidity filters (match live trader)
            if volume < self.min_volume:
                continue
            if spread > self.max_spread:
                continue

            yes_price = int(row["yes_price"])

            # Price floor: skip extreme prices
            if yes_price < self.min_price or yes_price > (100 - self.min_price):
                continue

            # Use BL-blended probability if available, else fall back to raw
            if pd.notna(row.get("bl_model_prob")):
                model_prob = float(row["bl_model_prob"])
            else:
                model_prob = float(row["model_prob"])

            # Calculate taker fee-adjusted edges from (BL-blended) prob
            yes_edge = fee_adjusted_edge(model_prob, yes_price, is_yes=True, is_maker=False)
            no_edge = fee_adjusted_edge(model_prob, yes_price, is_yes=False, is_maker=False)

            # Track all NO edges for bet pool logging (before threshold filter)
            if no_edge > 0:
                _pool_no_edges.append(no_edge)

            # NO-only by default; re-enable YES via KALSHI_ALLOW_YES_BETS=true
            if _allow_yes and yes_edge >= no_edge and yes_edge >= self.min_fee_adjusted_edge:
                side = "yes"
                edge = yes_edge
            elif no_edge >= self.min_fee_adjusted_edge:
                side = "no"
                edge = no_edge
            else:
                continue

            # Per-stat direction restriction (matches daily runner allowed_directions)
            if sport == "mlb":
                from src.models.mlb.mlb_stat_config import MLB_STATS as _MLB_STATS
                _allowed = _MLB_STATS.get(stat_type, {}).get("allowed_directions", ["over", "under"])
                _direction = "over" if side == "yes" else "under"
                if _direction not in _allowed:
                    continue

            # Structural filters — same rules as daily_runner / edge_refresh_job
            direction = "over" if side == "yes" else "under"
            line_val = float(row["line"]) if pd.notna(row.get("line")) else None
            skip, reason = should_skip_recommendation(
                stat=stat_type,
                direction=direction,
                line=line_val,
            )
            if skip:
                logger.debug(f"SKIP {row['player_name']} {stat_type} {direction}: {reason}")
                continue

            # Sportsbook-proximity tiebreaker
            sb_line = float(row["sportsbook_consensus_line"]) if pd.notna(row.get("sportsbook_consensus_line")) else None
            kalshi_line = float(row["line"])
            sb_dist = abs(kalshi_line - sb_line) if sb_line is not None else float("inf")

            # Same-run dedup: keep best edge per (player_id, stat_type)
            if pos_key in run_candidates:
                existing = run_candidates[pos_key]
                existing_edge = existing["fee_adjusted_edge"]
                edge_gap = edge - existing_edge  # positive = new is better
                if edge_gap > _LINE_TIEBREAK_THRESHOLD:
                    pass  # new candidate clearly better — replace
                elif edge_gap >= -_LINE_TIEBREAK_THRESHOLD:
                    # Edges within threshold — prefer sportsbook-aligned line
                    if sb_dist >= existing.get("sb_dist", float("inf")):
                        continue  # existing is closer to sportsbook, keep it
                    # else: new candidate is closer to sportsbook, replace
                    logger.info(
                        f"Line tiebreak: {row['player_name']} {stat_type} — "
                        f"replacing {existing['line']} (edge={existing_edge:.3f}, sb_dist={existing.get('sb_dist', float('inf')):.1f}) "
                        f"with {kalshi_line} (edge={edge:.3f}, sb_dist={sb_dist:.1f}) "
                        f"[gap={edge_gap:.3f}]"
                    )
                else:
                    continue  # existing candidate is clearly better

            kalshi_implied = float(row["kalshi_implied"]) if pd.notna(row["kalshi_implied"]) else yes_price / 100.0

            run_candidates[pos_key] = {
                "ticker": row["ticker"],
                "player_name": row["player_name"],
                "line": kalshi_line,
                "sb_dist": sb_dist,
                "side": side,
                "yes_price": yes_price,
                "model_prob": model_prob,
                "kalshi_implied": kalshi_implied,
                "raw_edge": float(row["raw_edge"]) if pd.notna(row["raw_edge"]) else 0,
                "fee_adjusted_edge": edge,
                # Extra market context for bet_reasoning
                "bl_model_prob": float(row["bl_model_prob"]) if pd.notna(row.get("bl_model_prob")) else None,
                "bl_edge": float(row["bl_edge"]) if pd.notna(row.get("bl_edge")) else None,
                "bl_confidence": float(row["bl_confidence"]) if pd.notna(row.get("bl_confidence")) else None,
                "sportsbook_line": float(row["sportsbook_consensus_line"]) if pd.notna(row.get("sportsbook_consensus_line")) else None,
                "line_vs_sportsbook": float(row["line_vs_sportsbook"]) if pd.notna(row.get("line_vs_sportsbook")) else None,
                "market_title": str(row["market_title"]) if pd.notna(row.get("market_title")) else None,
                "volume": int(row["volume"]) if pd.notna(row.get("volume")) else None,
                "close_time": row["close_time"] if pd.notna(row.get("close_time")) else None,
            }

        # Enrich candidates with prediction context (quantiles, features)
        prediction_ctx = self._fetch_prediction_context(
            target_date, list(run_candidates.keys())
        )
        for pos_key, cand in run_candidates.items():
            ctx = prediction_ctx.get(pos_key, {})
            cand["_pred_ctx"] = ctx

        # Log bet pool statistics (all NO edges, before threshold filter)
        _tiers = [0, 3, 5, 10, 15]
        _tier_str = ", ".join(
            f">{t}%: {sum(1 for e in _pool_no_edges if e >= t / 100.0)}"
            for t in _tiers
        )
        logger.info(f"BET POOL ({target_date} {_mode_str}): {_tier_str}")

        # Second pass: Kelly-size ALL candidates (sorted by edge, best first)
        # Then scale proportionally if total Kelly cost exceeds the remaining cap.
        # This ensures every qualifying bet gets placed at a fair fraction of its
        # Kelly-optimal size, rather than the top few getting full Kelly and the
        # rest getting nothing.
        kelly_info: list[dict[str, Any]] = []
        for pos_key, cand in sorted(
            run_candidates.items(),
            key=lambda kv: kv[1]["fee_adjusted_edge"],
            reverse=True,
        ):
            yes_price = cand["yes_price"]
            side = cand["side"]
            model_prob = cand["model_prob"]

            kelly_contracts = self._kelly_contracts(model_prob, yes_price, side, bankroll)
            if kelly_contracts <= 0:
                continue

            cost_per = (yes_price / 100.0) if side == "yes" else ((100 - yes_price) / 100.0)
            fee_per = kalshi_taker_fee(yes_price if side == "yes" else 100 - yes_price)

            kelly_info.append({
                "pos_key": pos_key,
                "cand": cand,
                "kelly_contracts": kelly_contracts,
                "kelly_cost": kelly_contracts * cost_per,
                "cost_per": cost_per,
                "fee_per": fee_per,
            })

        # Greedy allocation by edge priority (replaces proportional scaling).
        # kelly_info is already sorted best-edge first.
        # For each bet: place at full Kelly if cap allows, otherwise fit as many
        # contracts as the remaining cap permits (min 1), otherwise overflow.
        # This eliminates the floor-to-zero problem that hit proportional scaling
        # hard when bankroll was small or many candidates competed for the cap.
        remaining_cap = max(0.0, effective_cap - existing_exposure)
        total_kelly_cost = sum(k["kelly_cost"] for k in kelly_info)

        logger.info(
            f"Greedy sizing: {len(kelly_info)} candidates, "
            f"Kelly total=${total_kelly_cost:.2f}, remaining cap=${remaining_cap:.2f}"
        )

        # Third pass: greedy allocation
        bets: list[dict[str, Any]] = []
        overflow_bets: list[dict[str, Any]] = []
        running_exposure = existing_exposure

        for k in kelly_info:
            pos_key = k["pos_key"]
            cand = k["cand"]
            yes_price = cand["yes_price"]
            side = cand["side"]
            cost_per = k["cost_per"]
            fee_per = k["fee_per"]
            kelly_contracts = k["kelly_contracts"]

            ctx = cand.get("_pred_ctx", {})
            bet_reasoning: dict[str, Any] = {
                # Quantiles from daily_predictions (the model's full distribution)
                "q10": ctx.get("q10"),
                "q25": ctx.get("q25"),
                "q50": ctx.get("q50"),
                "q75": ctx.get("q75"),
                "q90": ctx.get("q90"),
                "pred_mean": ctx.get("pred_mean"),
                "l5_avg": ctx.get("l5_avg"),
                "l3_avg": ctx.get("l3_avg"),
                # Game context
                "opp_abbrev": ctx.get("opp_abbrev"),
                "rest_days": ctx.get("rest_days"),
                "is_back_to_back": ctx.get("is_back_to_back"),
                "team_out_count": ctx.get("team_out_count"),
                # Model probability chain
                "model_prob_raw": round(cand["model_prob"], 4),
                "bl_model_prob": cand.get("bl_model_prob"),
                "bl_edge": cand.get("bl_edge"),
                "bl_confidence": cand.get("bl_confidence") or ctx.get("bl_confidence"),
                # Market context
                "kalshi_implied": round(cand["kalshi_implied"], 4),
                "sportsbook_line": cand.get("sportsbook_line"),
                "line_vs_sportsbook": cand.get("line_vs_sportsbook"),
                "market_title": cand.get("market_title"),
                "volume": cand.get("volume"),
                # Kelly context
                "kelly_contracts_full": kelly_contracts,
            }

            bet_dict: dict[str, Any] = {
                "game_date": target_date,
                "ticker": cand["ticker"],
                "sport": sport,
                "player_id": pos_key[0],
                "player_name": cand["player_name"],
                "stat_type": pos_key[1],
                "line": cand["line"],
                "side": side,
                "price": yes_price,
                "contracts": kelly_contracts,  # updated below if capped
                "is_maker": False,
                "expected_fee": round(fee_per * kelly_contracts, 4),
                "model_prob": round(cand["model_prob"], 4),
                "kalshi_implied": round(cand["kalshi_implied"], 4),
                "edge": round(cand["raw_edge"], 4),
                "fee_adjusted_edge": round(cand["fee_adjusted_edge"], 4),
                "bet_reasoning": bet_reasoning,
                "close_time": cand.get("close_time"),
            }

            remaining = max(0.0, effective_cap - running_exposure)

            if remaining <= 0:
                # Cap exhausted — all remaining bets overflow
                overflow_bets.append(bet_dict)
                continue

            kelly_full_cost = kelly_contracts * cost_per

            if kelly_full_cost <= remaining:
                # Full Kelly fits within remaining cap
                actual_contracts = min(kelly_contracts, self.max_contracts_per_market)
            else:
                # Partial cap remaining: take as many contracts as fit, minimum 1
                actual_contracts = min(
                    int(math.floor(remaining / cost_per)),
                    kelly_contracts,
                    self.max_contracts_per_market,
                )
                if actual_contracts <= 0:
                    # Even 1 contract doesn't fit — overflow
                    overflow_bets.append(bet_dict)
                    continue

            bet_dict["contracts"] = actual_contracts
            bet_dict["expected_fee"] = round(fee_per * actual_contracts, 4)
            running_exposure += actual_contracts * cost_per
            bets.append(bet_dict)

        logger.info(
            f"Selected {len(bets)} Kalshi bets [{_mode_str}] for {target_date} "
            f"(exposure: ${running_exposure:.2f}/${effective_cap:.2f} cap, "
            f"{len(overflow_bets)} overflow)"
        )

        # Log overflow bets that would have been taken without the exposure cap
        if overflow_bets:
            overflow_exposure = sum(
                b["contracts"] * ((b["price"] / 100.0) if b["side"] == "yes" else ((100 - b["price"]) / 100.0))
                for b in overflow_bets
            )
            logger.info(
                f"OVERFLOW: {len(overflow_bets)} additional bets would have been taken "
                f"(${overflow_exposure:.2f} extra exposure). Logging to DB."
            )
            for b in overflow_bets:
                cost = b["contracts"] * ((b["price"] / 100.0) if b["side"] == "yes" else ((100 - b["price"]) / 100.0))
                logger.info(
                    f"  OVERFLOW: {b['player_name']} {b['stat_type']} "
                    f"{b['side'].upper()} @ {b['price']}c x{b['contracts']} "
                    f"= ${cost:.2f} (edge: {b['fee_adjusted_edge']:.1%})"
                )
            self._store_overflow_bets(overflow_bets)

        return bets

    # ------------------------------------------------------------------
    # Place bets
    # ------------------------------------------------------------------

    def place_bets(self, bets: list[dict[str, Any]]) -> int:
        """Insert paper bets into kalshi_paper_bets (UPSERT for idempotency).

        Assumes immediate fill at market price for paper trading.
        Sends Discord notification for each bet placed.
        """
        if not bets:
            return 0

        query = text("""
            INSERT INTO kalshi_paper_bets (
                game_date, ticker, sport, player_id, player_name,
                stat_type, line, side, price, contracts,
                is_maker, expected_fee, model_prob, kalshi_implied,
                edge, fee_adjusted_edge, status, fill_price, bet_reasoning,
                close_time
            ) VALUES (
                :game_date, :ticker, :sport, :player_id, :player_name,
                :stat_type, :line, :side, :price, :contracts,
                :is_maker, :expected_fee, :model_prob, :kalshi_implied,
                :edge, :fee_adjusted_edge, 'pending', :price,
                CAST(:bet_reasoning AS jsonb),
                :close_time
            )
            ON CONFLICT (game_date, ticker, side)
            DO UPDATE SET
                player_id = EXCLUDED.player_id,
                contracts = EXCLUDED.contracts,
                price = EXCLUDED.price,
                expected_fee = EXCLUDED.expected_fee,
                model_prob = EXCLUDED.model_prob,
                kalshi_implied = EXCLUDED.kalshi_implied,
                edge = EXCLUDED.edge,
                fee_adjusted_edge = EXCLUDED.fee_adjusted_edge,
                fill_price = EXCLUDED.fill_price,
                bet_reasoning = EXCLUDED.bet_reasoning,
                placed_at = NOW()
            WHERE kalshi_paper_bets.status = 'pending'
        """)

        bankroll = self.get_bankroll()

        with self.engine.connect() as conn:
            for bet in bets:
                params = dict(bet)
                params["bet_reasoning"] = (
                    json.dumps(params["bet_reasoning"])
                    if params.get("bet_reasoning") else None
                )
                conn.execute(query, params)
            conn.commit()

        # Discord notifications for each placed bet
        for bet in bets:
            cost_per = (bet["price"] / 100.0) if bet["side"] == "yes" else ((100 - bet["price"]) / 100.0)
            total_cost = bet["contracts"] * cost_per
            bankroll -= total_cost
            self._send_trade_alert("placed", {
                "player_name": bet.get("player_name", "Unknown"),
                "stat_type": bet["stat_type"],
                "line": bet["line"],
                "side": bet["side"],
                "fill_price": bet["price"],
                "contracts": bet["contracts"],
                "total_cost": total_cost,
                "fee_adjusted_edge": bet["fee_adjusted_edge"],
                "balance_after": bankroll,
                "bet_reasoning": bet.get("bet_reasoning"),
            })

        logger.info(f"Placed {len(bets)} Kalshi paper bets")
        return len(bets)

    # ------------------------------------------------------------------
    # Resolve bets
    # ------------------------------------------------------------------

    def resolve_bets(self, target_date: date, sport: str = "nba") -> dict[str, Any]:
        """Resolve pending Kalshi paper bets for a date using actual stats.

        Resolution logic:
          - actual >= line → YES wins
          - actual < line  → NO wins
          - No data / DNP  → cancelled

        P&L (per contract):
          - YES winner: (100 - fill_price)/100 - fee
          - NO winner:  fill_price/100 - fee
          - YES loser:  -(fill_price/100)
          - NO loser:   -((100 - fill_price)/100)
        """
        bets_query = text("""
            SELECT id, player_id, player_name, stat_type, line,
                   side, fill_price, contracts, expected_fee, sport, status
            FROM kalshi_paper_bets
            WHERE game_date = :game_date AND status IN ('pending', 'overflow')
        """)

        with self.engine.connect() as conn:
            bets_df = pd.read_sql(bets_query, conn, params={"game_date": target_date})

        if bets_df.empty:
            logger.info(f"No pending Kalshi bets to resolve for {target_date}")
            return {"resolved": 0, "won": 0, "lost": 0, "cancelled": 0,
                    "overflow_resolved": 0, "overflow_won": 0, "overflow_lost": 0}

        # Build actuals lookup
        actuals = self._fetch_actuals(target_date, bets_df, sport)

        results = {"won": 0, "lost": 0, "cancelled": 0, "skipped": 0,
                   "overflow_won": 0, "overflow_lost": 0, "overflow_cancelled": 0}
        updates = []

        # Staleness threshold: cancel bets only if >48h old with no stats (DNP/postponed)
        staleness_cutoff = date.today() - timedelta(days=2)

        for _, bet in bets_df.iterrows():
            player_id = int(bet["player_id"]) if pd.notna(bet["player_id"]) else None
            stat_type = bet["stat_type"]
            line = float(bet["line"])
            side = bet["side"]
            fill_price = int(bet["fill_price"])
            contracts = int(bet["contracts"])
            fee = float(bet["expected_fee"])
            is_overflow = bet["status"] == "overflow"
            prefix = "overflow_" if is_overflow else ""

            actual = actuals.get((player_id, stat_type)) if player_id else None

            if actual is None:
                if target_date <= staleness_cutoff:
                    # Stale bet (>48h) — likely DNP or postponed, cancel it
                    status = f"{prefix}cancelled"
                    pnl = 0.0
                    results[f"{prefix}cancelled"] += 1
                else:
                    # No stats yet — leave as pending for next resolution attempt
                    results["skipped"] += 1
                    continue
            else:
                yes_wins = actual >= line

                if side == "yes":
                    if yes_wins:
                        pnl = contracts * (100 - fill_price) / 100.0 - fee
                        status = f"{prefix}won"
                        results[f"{prefix}won"] += 1
                    else:
                        pnl = -(contracts * fill_price / 100.0)
                        status = f"{prefix}lost"
                        results[f"{prefix}lost"] += 1
                else:  # side == "no"
                    if not yes_wins:
                        pnl = contracts * fill_price / 100.0 - fee
                        status = f"{prefix}won"
                        results[f"{prefix}won"] += 1
                    else:
                        pnl = -(contracts * (100 - fill_price) / 100.0)
                        status = f"{prefix}lost"
                        results[f"{prefix}lost"] += 1

            updates.append({
                "bet_id": int(bet["id"]),
                "status": status,
                "actual_value": actual,
                "pnl": round(pnl, 2),
                "player_name": bet["player_name"],
                "stat_type": bet["stat_type"],
                "line": float(bet["line"]),
                "side": bet["side"],
                "is_overflow": is_overflow,
            })

        update_query = text("""
            UPDATE kalshi_paper_bets
            SET status = :status,
                actual_value = :actual_value,
                pnl = :pnl,
                resolved_at = NOW()
            WHERE id = :bet_id
        """)

        with self.engine.connect() as conn:
            for update in updates:
                conn.execute(update_query, {
                    "bet_id": update["bet_id"],
                    "status": update["status"],
                    "actual_value": update["actual_value"],
                    "pnl": update["pnl"],
                })
            conn.commit()

        self._update_daily_log(target_date)

        # Discord notifications for resolved bets (not overflow — those are hypothetical)
        running_balance = self.get_bankroll()
        for update in updates:
            if update["status"] in ("won", "lost"):
                running_balance += update["pnl"]
                self._send_trade_alert("resolved", {
                    "player_name": update["player_name"],
                    "stat_type": update["stat_type"],
                    "line": update["line"],
                    "side": update["side"],
                    "actual_value": update["actual_value"],
                    "pnl": update["pnl"],
                    "status": update["status"],
                    "balance_after": running_balance,
                })

        real_resolved = sum(1 for u in updates if not u["is_overflow"])
        overflow_resolved = sum(1 for u in updates if u["is_overflow"])
        results["resolved"] = real_resolved
        results["overflow_resolved"] = overflow_resolved

        msg = (
            f"Resolved {real_resolved} Kalshi bets for {target_date}: "
            f"{results['won']}W {results['lost']}L {results['cancelled']}C "
            f"{results['skipped']}S(pending)"
        )
        if overflow_resolved > 0:
            overflow_pnl = sum(u["pnl"] for u in updates if u["is_overflow"])
            msg += (
                f" | {overflow_resolved} overflow: "
                f"{results['overflow_won']}W {results['overflow_lost']}L "
                f"(hypothetical P&L: ${overflow_pnl:+.2f})"
            )
        logger.info(msg)
        return results

    def _fetch_actuals(
        self, game_date: date, bets_df: pd.DataFrame, sport: str
    ) -> dict[tuple[int, str], float | None]:
        """Fetch actual stat values for resolution.

        Supports NBA (player_game_stats) and MLB (pitching/batting tables).
        """
        actuals: dict[tuple[int, str], float | None] = {}
        stats_needed = bets_df["stat_type"].unique()

        for stat_type in stats_needed:
            # Try NBA resolution first
            nba_res = NBA_STAT_RESOLUTION.get(stat_type)
            if nba_res is not None:
                table, columns = nba_res
                col_expr = " + ".join(f"s.{c}" for c in columns)

                actuals_query = text(f"""
                    SELECT s.player_id, ({col_expr}) as actual_value
                    FROM {table} s
                    WHERE s.game_date = :game_date
                      AND s.player_id IS NOT NULL
                      AND s.min > 0
                """)

                with self.engine.connect() as conn:
                    rows = conn.execute(actuals_query, {"game_date": game_date}).fetchall()

                for row in rows:
                    pid = int(row[0])
                    val = row[1]
                    actuals[(pid, stat_type)] = float(val) if val is not None else None
                continue

            # Try MLB resolution
            mlb_res = MLB_STAT_RESOLUTION.get(stat_type)
            if mlb_res is not None:
                table, columns = mlb_res
                col_expr = " + ".join(f"s.{c}" for c in columns)

                actuals_query = text(f"""
                    SELECT s.player_id, {col_expr} as actual_value,
                           s.did_not_play
                    FROM {table} s
                    WHERE s.game_date = :game_date
                """)

                with self.engine.connect() as conn:
                    rows = conn.execute(actuals_query, {"game_date": game_date}).fetchall()

                for row in rows:
                    pid = int(row[0])
                    val = row[1]
                    dnp = row[2] if len(row) > 2 else False
                    if dnp:
                        actuals[(pid, stat_type)] = None
                    else:
                        actuals[(pid, stat_type)] = float(val) if val is not None else None
                continue

            logger.warning(f"No resolution mapping for stat_type: {stat_type}")

        return actuals

    # ------------------------------------------------------------------
    # Multi-day catchup
    # ------------------------------------------------------------------

    def resolve_all_pending(self, exclude_today: bool = True) -> dict[str, Any]:
        """Resolve ALL pending Kalshi bets where game results are available."""
        if exclude_today:
            dates_query = text("""
                SELECT DISTINCT game_date
                FROM kalshi_paper_bets
                WHERE status IN ('pending', 'overflow')
                  AND game_date < :today
                ORDER BY game_date ASC
            """)
            params: dict[str, Any] = {"today": date.today()}
        else:
            dates_query = text("""
                SELECT DISTINCT game_date
                FROM kalshi_paper_bets
                WHERE status IN ('pending', 'overflow')
                ORDER BY game_date ASC
            """)
            params = {}

        with self.engine.connect() as conn:
            dates_result = conn.execute(dates_query, params).fetchall()

        if not dates_result:
            logger.info("No pending Kalshi bets to resolve")
            return {
                "dates_processed": 0, "dates_skipped": 0,
                "total_resolved": 0, "total_won": 0, "total_lost": 0,
                "total_cancelled": 0, "by_date": {},
            }

        pending_dates = [row[0] for row in dates_result]
        logger.info(f"Found {len(pending_dates)} dates with pending Kalshi bets")

        totals = {"dates_processed": 0, "dates_skipped": 0,
                  "total_resolved": 0, "total_won": 0, "total_lost": 0,
                  "total_cancelled": 0}
        by_date: dict[str, dict[str, Any]] = {}

        for game_date in pending_dates:
            # Determine sport from the bets
            sport_query = text("""
                SELECT DISTINCT sport FROM kalshi_paper_bets
                WHERE game_date = :game_date AND status IN ('pending', 'overflow')
            """)
            with self.engine.connect() as conn:
                sports = [r[0] for r in conn.execute(sport_query, {"game_date": game_date}).fetchall()]

            any_resolved = False
            for sport in sports:
                # Check if sufficient stats exist per-sport
                if not self._has_stats_for_date(game_date, sport=sport):
                    logger.info(f"Skipping {game_date}/{sport}: insufficient stats (< threshold)")
                    continue

                result = self.resolve_bets(game_date, sport=sport)
                any_resolved = True
                totals["dates_processed"] += 1
                totals["total_resolved"] += result["resolved"]
                totals["total_won"] += result["won"]
                totals["total_lost"] += result["lost"]
                totals["total_cancelled"] += result["cancelled"]
                by_date[str(game_date)] = result

            if not any_resolved:
                totals["dates_skipped"] += 1
                by_date[str(game_date)] = {"skipped": True, "reason": "no_stats"}

        totals["by_date"] = by_date
        logger.info(
            f"Kalshi resolution complete: {totals['dates_processed']} dates, "
            f"{totals['total_resolved']} bets resolved "
            f"({totals['total_won']}W {totals['total_lost']}L {totals['total_cancelled']}C)"
        )
        return totals

    def _has_stats_for_date(self, game_date: date, sport: str | None = None) -> bool:
        """Check if sufficient game stats exist for the given date.

        Requires a minimum number of stat rows to prevent partial early-game
        stats from triggering premature resolution of ALL bets.
        """
        MIN_STATS_THRESHOLD = 50  # At least 50 player stat rows

        if sport == "mlb":
            query = text("""
                SELECT
                    (SELECT COUNT(*) FROM mlb_player_game_stats_pitching WHERE game_date = :game_date) +
                    (SELECT COUNT(*) FROM mlb_player_game_stats_batting WHERE game_date = :game_date)
                    as total_stats
            """)
        elif sport == "nba":
            query = text("""
                SELECT COUNT(*) as total_stats
                FROM player_game_stats WHERE game_date = :game_date
            """)
        else:
            # No sport specified — check both
            query = text("""
                SELECT
                    (SELECT COUNT(*) FROM player_game_stats WHERE game_date = :game_date) +
                    (SELECT COUNT(*) FROM mlb_player_game_stats_pitching WHERE game_date = :game_date) +
                    (SELECT COUNT(*) FROM mlb_player_game_stats_batting WHERE game_date = :game_date)
                    as total_stats
            """)

        with self.engine.connect() as conn:
            count = conn.execute(query, {"game_date": game_date}).scalar()
        return (count or 0) >= MIN_STATS_THRESHOLD

    # ------------------------------------------------------------------
    # Daily log
    # ------------------------------------------------------------------

    def _update_daily_log(self, game_date: date) -> None:
        """Aggregate bet results and upsert daily log entry."""
        agg_query = text("""
            SELECT
                COUNT(*) as total_bets,
                COUNT(*) FILTER (WHERE status = 'won') as bets_won,
                COUNT(*) FILTER (WHERE status = 'lost') as bets_lost,
                COUNT(*) FILTER (WHERE status = 'cancelled') as bets_cancelled,
                COUNT(*) FILTER (WHERE status = 'pending') as bets_pending,
                COALESCE(SUM(
                    CASE WHEN side = 'yes' THEN contracts * fill_price / 100.0
                         ELSE contracts * (100 - fill_price) / 100.0
                    END
                ) FILTER (WHERE status IN ('won', 'lost')), 0) as total_cost,
                COALESCE(SUM(pnl) FILTER (WHERE status IN ('won', 'lost', 'pending', 'cancelled')), 0) as total_pnl
            FROM kalshi_paper_bets
            WHERE game_date = :game_date
              AND status NOT LIKE 'overflow%'
        """)

        with self.engine.connect() as conn:
            result = conn.execute(agg_query, {"game_date": game_date}).fetchone()

        if result is None or result[0] == 0:
            return

        total_bets = result[0]
        bets_won = result[1]
        bets_lost = result[2]
        bets_cancelled = result[3]
        bets_pending = result[4]
        total_cost = float(result[5])
        total_pnl = float(result[6])

        roi_pct = (total_pnl / total_cost * 100) if total_cost > 0 else 0

        # Get previous day's cumulative values
        prev_query = text("""
            SELECT cumulative_pnl, bankroll_after
            FROM kalshi_paper_trading_daily_log
            WHERE game_date < :game_date
            ORDER BY game_date DESC
            LIMIT 1
        """)

        with self.engine.connect() as conn:
            prev = conn.execute(prev_query, {"game_date": game_date}).fetchone()

        if prev is None:
            prev_cumulative = 0.0
            prev_bankroll = self.starting_bankroll
        else:
            prev_cumulative = float(prev[0])
            prev_bankroll = float(prev[1])

        cumulative_pnl = prev_cumulative + total_pnl
        bankroll_after = prev_bankroll + total_pnl

        upsert_query = text("""
            INSERT INTO kalshi_paper_trading_daily_log (
                game_date, total_bets, bets_won, bets_lost, bets_cancelled,
                bets_pending, total_cost, total_pnl, roi_pct,
                cumulative_pnl, bankroll_after
            ) VALUES (
                :game_date, :total_bets, :bets_won, :bets_lost, :bets_cancelled,
                :bets_pending, :total_cost, :total_pnl, :roi_pct,
                :cumulative_pnl, :bankroll_after
            )
            ON CONFLICT (game_date) DO UPDATE SET
                total_bets = EXCLUDED.total_bets,
                bets_won = EXCLUDED.bets_won,
                bets_lost = EXCLUDED.bets_lost,
                bets_cancelled = EXCLUDED.bets_cancelled,
                bets_pending = EXCLUDED.bets_pending,
                total_cost = EXCLUDED.total_cost,
                total_pnl = EXCLUDED.total_pnl,
                roi_pct = EXCLUDED.roi_pct,
                cumulative_pnl = EXCLUDED.cumulative_pnl,
                bankroll_after = EXCLUDED.bankroll_after,
                updated_at = NOW()
        """)

        with self.engine.connect() as conn:
            conn.execute(upsert_query, {
                "game_date": game_date,
                "total_bets": total_bets,
                "bets_won": bets_won,
                "bets_lost": bets_lost,
                "bets_cancelled": bets_cancelled,
                "bets_pending": bets_pending,
                "total_cost": round(total_cost, 2),
                "total_pnl": round(total_pnl, 2),
                "roi_pct": round(roi_pct, 2),
                "cumulative_pnl": round(cumulative_pnl, 2),
                "bankroll_after": round(bankroll_after, 2),
            })
            conn.commit()

        logger.info(
            f"Updated Kalshi daily log for {game_date}: "
            f"P&L=${total_pnl:.2f}, bankroll=${bankroll_after:.2f}"
        )

    # ------------------------------------------------------------------
    # Overflow bet storage
    # ------------------------------------------------------------------

    def _store_overflow_bets(self, overflow_bets: list[dict[str, Any]]) -> None:
        """Store overflow bets (skipped due to exposure cap) for hypothetical tracking.

        These are stored with status='overflow' and resolved alongside pending bets.
        Their P&L is NOT included in the daily log — tracked separately for analysis.
        """
        if not overflow_bets:
            return

        query = text("""
            INSERT INTO kalshi_paper_bets (
                game_date, ticker, sport, player_id, player_name,
                stat_type, line, side, price, contracts,
                is_maker, expected_fee, model_prob, kalshi_implied,
                edge, fee_adjusted_edge, status, fill_price, bet_reasoning,
                close_time
            ) VALUES (
                :game_date, :ticker, :sport, :player_id, :player_name,
                :stat_type, :line, :side, :price, :contracts,
                :is_maker, :expected_fee, :model_prob, :kalshi_implied,
                :edge, :fee_adjusted_edge, 'overflow', :price,
                CAST(:bet_reasoning AS jsonb),
                :close_time
            )
            ON CONFLICT (game_date, ticker, side) DO NOTHING
        """)

        stored = 0
        with self.engine.connect() as conn:
            for bet in overflow_bets:
                params = dict(bet)
                params["bet_reasoning"] = (
                    json.dumps(params["bet_reasoning"])
                    if params.get("bet_reasoning") else None
                )
                result = conn.execute(query, params)
                stored += result.rowcount
            conn.commit()

        logger.info(f"Stored {stored}/{len(overflow_bets)} overflow bets for tracking")

    # ------------------------------------------------------------------
    # Prediction context
    # ------------------------------------------------------------------

    def _fetch_prediction_context(
        self, target_date: date, pos_keys: list[tuple[int, str]]
    ) -> dict[tuple[int, str], dict[str, Any]]:
        """Fetch model prediction context (quantiles + game features) for bet_reasoning.

        Queries daily_predictions for the given (player_id, stat_type) pairs and
        returns a dict mapping each key to a context dict.  Missing entries are
        silently omitted — callers should use .get(key, {}).
        """
        if not pos_keys:
            return {}

        pos_keys_set = set(pos_keys)
        player_ids = list({pk[0] for pk in pos_keys})
        stat_types = list({pk[1] for pk in pos_keys})

        try:
            query = text("""
                SELECT DISTINCT ON (player_id, stat)
                    player_id, stat,
                    pred_q10, pred_q25, pred_q50, pred_q75, pred_q90,
                    pred_mean,
                    feat_player_avg_stat_l5, feat_player_avg_stat_l3,
                    feat_opp_abbrev, feat_rest_days, feat_is_back_to_back,
                    feat_team_out_count, feat_is_home,
                    feat_player_avg_usg_pct_l5, feat_player_min_floor_l5,
                    bl_confidence
                FROM daily_predictions
                WHERE prediction_date = :pred_date
                  AND player_id = ANY(:player_ids)
                  AND stat = ANY(:stats)
                ORDER BY player_id, stat,
                         is_recommended DESC NULLS LAST,
                         created_at DESC
            """)

            with self.engine.connect() as conn:
                rows = conn.execute(query, {
                    "pred_date": target_date,
                    "player_ids": player_ids,
                    "stats": stat_types,
                }).fetchall()

            result: dict[tuple[int, str], dict[str, Any]] = {}
            for row in rows:
                pid = int(row[0])
                stat = row[1]
                key = (pid, stat)
                if key not in pos_keys_set:
                    continue
                result[key] = {
                    "q10": float(row[2]) if row[2] is not None else None,
                    "q25": float(row[3]) if row[3] is not None else None,
                    "q50": float(row[4]) if row[4] is not None else None,
                    "q75": float(row[5]) if row[5] is not None else None,
                    "q90": float(row[6]) if row[6] is not None else None,
                    "pred_mean": float(row[7]) if row[7] is not None else None,
                    "l5_avg": float(row[8]) if row[8] is not None else None,
                    "l3_avg": float(row[9]) if row[9] is not None else None,
                    "opp_abbrev": row[10],
                    "rest_days": int(row[11]) if row[11] is not None else None,
                    "is_back_to_back": bool(row[12]) if row[12] is not None else None,
                    "team_out_count": int(row[13]) if row[13] is not None else None,
                    "is_home": bool(row[14]) if row[14] is not None else None,
                    "usg_pct_l5": float(row[15]) if row[15] is not None else None,
                    "min_floor_l5": float(row[16]) if row[16] is not None else None,
                    "bl_confidence": float(row[17]) if row[17] is not None else None,
                }

            logger.info(
                f"Fetched prediction context for {len(result)}/{len(pos_keys)} "
                f"candidates from daily_predictions"
            )
            return result

        except Exception as e:
            logger.warning(f"Failed to fetch prediction context: {e}")
            return {}

    # ------------------------------------------------------------------
    # Discord alerts
    # ------------------------------------------------------------------

    def _send_trade_alert(self, alert_type: str, data: dict) -> None:
        """Send Discord alert for a paper trade (placed or resolved).

        Uses the same alert infrastructure as the live trader but with
        PAPER prefix in the embed title.
        """
        try:
            from src.discord_bot.alerts import send_kalshi_trade_alert_sync
            send_kalshi_trade_alert_sync(alert_type, data, mode="paper")
        except Exception as e:
            logger.warning(f"Failed to send Kalshi paper trade alert: {e}")
