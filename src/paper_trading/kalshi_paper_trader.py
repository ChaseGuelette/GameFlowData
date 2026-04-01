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

import logging
import math
import os
from dataclasses import dataclass, field
from datetime import date
from typing import Any

import pandas as pd
from sqlalchemy import text

from src.db.client import get_engine
from src.paper_trading.mlb_paper_trader import MLB_STAT_RESOLUTION
from src.scrapers.kalshi.kalshi_utils import fee_adjusted_edge, kalshi_taker_fee

logger = logging.getLogger(__name__)

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

# Combined MLB stats: stat_type -> (table, [columns to sum])
COMBINED_STAT_RESOLUTION: dict[str, tuple[str, list[str]]] = {
    "batter_hits_runs_rbis": ("mlb_player_game_stats_batting", ["h", "r", "rbi"]),
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

    def select_bets(self, target_date: date, sport: str = "nba") -> list[dict[str, Any]]:
        """Select paper bets from kalshi_markets with sufficient taker-fee-adjusted edge.

        Mirrors live trader logic: taker fees, position accumulation awareness,
        same-day dedup, daily exposure cap.
        """
        bankroll = self.get_bankroll()

        # Get latest snapshot per ticker for the date
        query = text("""
            SELECT DISTINCT ON (ticker)
                ticker, sport, player_id, player_name, stat_type, line,
                yes_price, model_prob, kalshi_implied,
                raw_edge, taker_fee_adjusted_edge,
                volume, bid_ask_spread, market_status
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

        # Position accumulation: check what we already bet on today (exclude overflow)
        today_tickers: set[str] = set()
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT DISTINCT ticker
                FROM kalshi_paper_bets
                WHERE game_date = :d
                  AND status NOT IN ('cancelled', 'overflow', 'overflow_won', 'overflow_lost', 'overflow_cancelled')
            """), {"d": target_date}).fetchall()
            today_tickers = {row[0] for row in rows}

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
            existing_exposure = float(result or 0)

        bets: list[dict[str, Any]] = []
        overflow_bets: list[dict[str, Any]] = []
        total_exposure = existing_exposure

        for _, row in df.iterrows():
            ticker = row["ticker"]

            # Skip if already bet on today (position accumulation awareness)
            if ticker in today_tickers:
                continue

            volume = int(row["volume"] or 0)
            spread = int(row["bid_ask_spread"] or 100)

            # Liquidity filters (match live trader)
            if volume < self.min_volume:
                continue
            if spread > self.max_spread:
                continue

            yes_price = int(row["yes_price"])
            model_prob = float(row["model_prob"])

            # Calculate taker fee-adjusted edges for both sides
            yes_edge = fee_adjusted_edge(model_prob, yes_price, is_yes=True, is_maker=False)
            no_edge = fee_adjusted_edge(model_prob, yes_price, is_yes=False, is_maker=False)

            if yes_edge >= no_edge and yes_edge >= self.min_fee_adjusted_edge:
                side = "yes"
                edge = yes_edge
            elif no_edge >= self.min_fee_adjusted_edge:
                side = "no"
                edge = no_edge
            else:
                continue

            contracts = self._kelly_contracts(model_prob, yes_price, side, bankroll)
            if contracts <= 0:
                continue

            # Cost for this bet
            cost_per = (yes_price / 100.0) if side == "yes" else ((100 - yes_price) / 100.0)
            bet_cost = contracts * cost_per

            fee_per = kalshi_taker_fee(yes_price if side == "yes" else 100 - yes_price)
            expected_fee = fee_per * contracts
            kalshi_implied = float(row["kalshi_implied"]) if pd.notna(row["kalshi_implied"]) else yes_price / 100.0

            bet_dict = {
                "game_date": target_date,
                "ticker": ticker,
                "sport": sport,
                "player_id": int(row["player_id"]) if pd.notna(row["player_id"]) else None,
                "player_name": row["player_name"],
                "stat_type": row["stat_type"],
                "line": float(row["line"]),
                "side": side,
                "price": yes_price,
                "contracts": contracts,
                "is_maker": False,
                "expected_fee": round(expected_fee, 4),
                "model_prob": round(model_prob, 4),
                "kalshi_implied": round(kalshi_implied, 4),
                "edge": round(float(row["raw_edge"]) if pd.notna(row["raw_edge"]) else 0, 4),
                "fee_adjusted_edge": round(edge, 4),
            }

            # Enforce daily exposure cap
            if total_exposure + bet_cost > self.max_daily_exposure:
                remaining = self.max_daily_exposure - total_exposure
                partial_filled = False
                if remaining > cost_per:
                    partial_contracts = int(math.floor(remaining / cost_per))
                    if partial_contracts > 0:
                        partial_dict = dict(bet_dict)  # Copy — don't mutate original
                        partial_dict["contracts"] = partial_contracts
                        partial_dict["expected_fee"] = round(fee_per * partial_contracts, 4)
                        total_exposure += partial_contracts * cost_per
                        bets.append(partial_dict)
                        partial_filled = True

                # If no partial fill, this bet was entirely skipped — track as overflow
                if not partial_filled:
                    overflow_bets.append(bet_dict)
                continue

            total_exposure += bet_cost
            bets.append(bet_dict)

        logger.info(
            f"Selected {len(bets)} Kalshi bets for {target_date} "
            f"(total exposure: ${total_exposure:.2f}, bankroll: ${bankroll:.2f})"
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
                edge, fee_adjusted_edge, status, fill_price
            ) VALUES (
                :game_date, :ticker, :sport, :player_id, :player_name,
                :stat_type, :line, :side, :price, :contracts,
                :is_maker, :expected_fee, :model_prob, :kalshi_implied,
                :edge, :fee_adjusted_edge, 'pending', :price
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
                placed_at = NOW()
            WHERE kalshi_paper_bets.status = 'pending'
        """)

        bankroll = self.get_bankroll()

        with self.engine.connect() as conn:
            for bet in bets:
                conn.execute(query, bet)
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

        results = {"won": 0, "lost": 0, "cancelled": 0,
                   "overflow_won": 0, "overflow_lost": 0, "overflow_cancelled": 0}
        updates = []

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
                status = f"{prefix}cancelled"
                pnl = 0.0
                results[f"{prefix}cancelled"] += 1
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
        bankroll = self.get_bankroll()
        for update in updates:
            if update["status"] in ("won", "lost"):
                self._send_trade_alert("resolved", {
                    "player_name": update["player_name"],
                    "stat_type": update["stat_type"],
                    "line": update["line"],
                    "side": update["side"],
                    "actual_value": update["actual_value"],
                    "pnl": update["pnl"],
                    "status": update["status"],
                    "balance_after": bankroll,
                })

        real_resolved = sum(1 for u in updates if not u["is_overflow"])
        overflow_resolved = sum(1 for u in updates if u["is_overflow"])
        results["resolved"] = real_resolved
        results["overflow_resolved"] = overflow_resolved

        msg = (
            f"Resolved {real_resolved} Kalshi bets for {target_date}: "
            f"{results['won']}W {results['lost']}L {results['cancelled']}C"
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
                table, column = mlb_res

                actuals_query = text(f"""
                    SELECT s.player_id, s.{column} as actual_value,
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

            # Try combined stat resolution (e.g., batter_hits_runs_rbis)
            combined_res = COMBINED_STAT_RESOLUTION.get(stat_type)
            if combined_res is not None:
                table, columns = combined_res
                col_expr = " + ".join(f"s.{c}" for c in columns)

                actuals_query = text(f"""
                    SELECT s.player_id, ({col_expr}) as actual_value,
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
            # Check if stats exist for this date
            if not self._has_stats_for_date(game_date):
                logger.info(f"Skipping {game_date}: no game stats available yet")
                totals["dates_skipped"] += 1
                by_date[str(game_date)] = {"skipped": True, "reason": "no_stats"}
                continue

            # Determine sport from the bets
            sport_query = text("""
                SELECT DISTINCT sport FROM kalshi_paper_bets
                WHERE game_date = :game_date AND status IN ('pending', 'overflow')
            """)
            with self.engine.connect() as conn:
                sports = [r[0] for r in conn.execute(sport_query, {"game_date": game_date}).fetchall()]

            for sport in sports:
                result = self.resolve_bets(game_date, sport=sport)
                totals["dates_processed"] += 1
                totals["total_resolved"] += result["resolved"]
                totals["total_won"] += result["won"]
                totals["total_lost"] += result["lost"]
                totals["total_cancelled"] += result["cancelled"]
                by_date[str(game_date)] = result

        totals["by_date"] = by_date
        logger.info(
            f"Kalshi resolution complete: {totals['dates_processed']} dates, "
            f"{totals['total_resolved']} bets resolved "
            f"({totals['total_won']}W {totals['total_lost']}L {totals['total_cancelled']}C)"
        )
        return totals

    def _has_stats_for_date(self, game_date: date) -> bool:
        """Check if any game stats exist for the given date (NBA or MLB)."""
        query = text("""
            SELECT
                (SELECT COUNT(*) FROM player_game_stats WHERE game_date = :game_date) +
                (SELECT COUNT(*) FROM mlb_player_game_stats_pitching WHERE game_date = :game_date) +
                (SELECT COUNT(*) FROM mlb_player_game_stats_batting WHERE game_date = :game_date)
                as total_stats
        """)
        with self.engine.connect() as conn:
            count = conn.execute(query, {"game_date": game_date}).scalar()
        return (count or 0) > 0

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
                edge, fee_adjusted_edge, status, fill_price
            ) VALUES (
                :game_date, :ticker, :sport, :player_id, :player_name,
                :stat_type, :line, :side, :price, :contracts,
                :is_maker, :expected_fee, :model_prob, :kalshi_implied,
                :edge, :fee_adjusted_edge, 'overflow', :price
            )
            ON CONFLICT (game_date, ticker, side) DO NOTHING
        """)

        stored = 0
        with self.engine.connect() as conn:
            for bet in overflow_bets:
                result = conn.execute(query, bet)
                stored += result.rowcount
            conn.commit()

        logger.info(f"Stored {stored}/{len(overflow_bets)} overflow bets for tracking")

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
