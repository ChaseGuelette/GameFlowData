"""
Paper Trading Module for Kalshi Player Prop Markets.

Simulates trades against Kalshi binary contracts using model edges,
Kelly-based position sizing in contracts, and cents-based P&L accounting.

Key differences from NBA/MLB paper traders:
  - Cents-based pricing (0-99) instead of American odds
  - Binary YES/NO sides instead of over/under
  - Position sizing in contracts (integer) instead of dollar stakes
  - Maker/taker fee structure instead of vig
  - Liquidity filters (volume, spread)
  - No push outcome — strictly binary resolution

Configuration via environment variables:
    KALSHI_PAPER_TRADING_BANKROLL: Starting bankroll (default: 5000)
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
from src.scrapers.kalshi.kalshi_utils import kalshi_maker_fee

logger = logging.getLogger(__name__)

# NBA stat resolution: stat_type -> (table, [columns to sum])
NBA_STAT_RESOLUTION: dict[str, tuple[str, list[str]]] = {
    "pts": ("player_game_stats", ["pts"]),
    "reb": ("player_game_stats", ["reb"]),
    "ast": ("player_game_stats", ["ast"]),
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


DEFAULT_BANKROLL = _get_env_float("KALSHI_PAPER_TRADING_BANKROLL", 5000.0)
DEFAULT_KELLY_FRACTION = _get_env_float("KALSHI_PAPER_TRADING_KELLY_FRACTION", 0.125)


@dataclass
class KalshiPaperTrader:
    """
    Simulates paper trading on Kalshi binary contract markets.

    Reads edges from kalshi_markets, sizes positions via fractional Kelly,
    places paper bets, and resolves against actual player stats.
    """

    min_fee_adjusted_edge: float = 0.05
    max_contracts_per_market: int = 100
    max_daily_exposure: float = 1000.0  # Max $ spent per day
    min_volume: int = 50
    max_spread: int = 10  # Max bid-ask spread in cents
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

        # Deduct expected maker fee from effective return
        fee_per = kalshi_maker_fee(price_cents if side == "yes" else 100 - price_cents)
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
        """Select paper bets from kalshi_markets with sufficient edge.

        Queries the latest snapshot per ticker for the target date,
        filters by edge/volume/spread, determines YES or NO side,
        and sizes via Kelly.
        """
        bankroll = self.get_bankroll()

        # Get latest snapshot per ticker for the date
        query = text("""
            SELECT DISTINCT ON (ticker)
                ticker, sport, player_id, player_name, stat_type, line,
                yes_price, model_prob, kalshi_implied,
                raw_edge, maker_fee_adjusted_edge,
                volume, bid_ask_spread, market_status
            FROM kalshi_markets
            WHERE sport = :sport
              AND snapshot_time::date = :target_date
              AND market_status = 'open'
              AND model_prob IS NOT NULL
              AND maker_fee_adjusted_edge IS NOT NULL
            ORDER BY ticker, snapshot_time DESC
        """)

        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn, params={
                "sport": sport, "target_date": target_date,
            })

        if df.empty:
            logger.info(f"No Kalshi markets with edges for {target_date}")
            return []

        bets: list[dict[str, Any]] = []
        total_exposure = 0.0

        for _, row in df.iterrows():
            volume = int(row["volume"] or 0)
            spread = int(row["bid_ask_spread"] or 100)

            # Liquidity filters
            if volume < self.min_volume:
                continue
            if spread > self.max_spread:
                continue

            yes_price = int(row["yes_price"])
            model_prob = float(row["model_prob"])
            kalshi_implied = float(row["kalshi_implied"])

            # Determine which side has the edge
            # YES edge: model_prob - kalshi_implied (after fees)
            # NO edge: (1 - model_prob) - (1 - kalshi_implied) (after fees)
            from src.scrapers.kalshi.kalshi_utils import fee_adjusted_edge

            yes_edge = fee_adjusted_edge(model_prob, yes_price, is_yes=True, is_maker=True)
            no_edge = fee_adjusted_edge(model_prob, yes_price, is_yes=False, is_maker=True)

            if yes_edge >= no_edge and yes_edge >= self.min_fee_adjusted_edge:
                side = "yes"
                edge = yes_edge
                price = yes_price
            elif no_edge >= self.min_fee_adjusted_edge:
                side = "no"
                edge = no_edge
                price = yes_price  # Store original yes_price; NO cost = 100 - yes_price
            else:
                continue

            contracts = self._kelly_contracts(model_prob, yes_price, side, bankroll)
            if contracts <= 0:
                continue

            # Cost for this bet
            cost_per = (price / 100.0) if side == "yes" else ((100 - price) / 100.0)
            bet_cost = contracts * cost_per

            # Enforce daily exposure cap
            if total_exposure + bet_cost > self.max_daily_exposure:
                remaining = self.max_daily_exposure - total_exposure
                if remaining <= cost_per:
                    continue
                contracts = int(math.floor(remaining / cost_per))
                if contracts <= 0:
                    continue
                bet_cost = contracts * cost_per

            total_exposure += bet_cost

            fee_per = kalshi_maker_fee(price if side == "yes" else 100 - price)
            expected_fee = fee_per * contracts

            bets.append({
                "game_date": target_date,
                "ticker": row["ticker"],
                "sport": sport,
                "player_id": int(row["player_id"]) if pd.notna(row["player_id"]) else None,
                "player_name": row["player_name"],
                "stat_type": row["stat_type"],
                "line": float(row["line"]),
                "side": side,
                "price": yes_price,
                "contracts": contracts,
                "is_maker": True,
                "expected_fee": round(expected_fee, 4),
                "model_prob": round(model_prob, 4),
                "kalshi_implied": round(kalshi_implied, 4),
                "edge": round(float(row["raw_edge"]), 4),
                "fee_adjusted_edge": round(edge, 4),
            })

        logger.info(
            f"Selected {len(bets)} Kalshi bets for {target_date} "
            f"(total exposure: ${total_exposure:.2f})"
        )
        return bets

    # ------------------------------------------------------------------
    # Place bets
    # ------------------------------------------------------------------

    def place_bets(self, bets: list[dict[str, Any]]) -> int:
        """Insert paper bets into kalshi_paper_bets (UPSERT for idempotency).

        Assumes immediate fill at market price for paper trading.
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

        with self.engine.connect() as conn:
            for bet in bets:
                conn.execute(query, bet)
            conn.commit()

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
                   side, fill_price, contracts, expected_fee, sport
            FROM kalshi_paper_bets
            WHERE game_date = :game_date AND status = 'pending'
        """)

        with self.engine.connect() as conn:
            bets_df = pd.read_sql(bets_query, conn, params={"game_date": target_date})

        if bets_df.empty:
            logger.info(f"No pending Kalshi bets to resolve for {target_date}")
            return {"resolved": 0, "won": 0, "lost": 0, "cancelled": 0}

        # Build actuals lookup
        actuals = self._fetch_actuals(target_date, bets_df, sport)

        results = {"won": 0, "lost": 0, "cancelled": 0}
        updates = []

        for _, bet in bets_df.iterrows():
            player_id = int(bet["player_id"]) if pd.notna(bet["player_id"]) else None
            stat_type = bet["stat_type"]
            line = float(bet["line"])
            side = bet["side"]
            fill_price = int(bet["fill_price"])
            contracts = int(bet["contracts"])
            fee = float(bet["expected_fee"])

            actual = actuals.get((player_id, stat_type)) if player_id else None

            if actual is None:
                status = "cancelled"
                pnl = 0.0
                results["cancelled"] += 1
            else:
                yes_wins = actual >= line

                if side == "yes":
                    if yes_wins:
                        # Won YES: profit = contracts * (100 - fill) / 100 - fee
                        pnl = contracts * (100 - fill_price) / 100.0 - fee
                        status = "won"
                        results["won"] += 1
                    else:
                        # Lost YES: lose cost = contracts * fill / 100
                        pnl = -(contracts * fill_price / 100.0)
                        status = "lost"
                        results["lost"] += 1
                else:  # side == "no"
                    if not yes_wins:
                        # Won NO: profit = contracts * fill / 100 - fee
                        pnl = contracts * fill_price / 100.0 - fee
                        status = "won"
                        results["won"] += 1
                    else:
                        # Lost NO: lose cost = contracts * (100 - fill) / 100
                        pnl = -(contracts * (100 - fill_price) / 100.0)
                        status = "lost"
                        results["lost"] += 1

            updates.append({
                "bet_id": int(bet["id"]),
                "status": status,
                "actual_value": actual,
                "pnl": round(pnl, 2),
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
                conn.execute(update_query, update)
            conn.commit()

        self._update_daily_log(target_date)

        results["resolved"] = len(updates)
        logger.info(
            f"Resolved {results['resolved']} Kalshi bets for {target_date}: "
            f"{results['won']}W {results['lost']}L {results['cancelled']}C"
        )
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
                WHERE status = 'pending'
                  AND game_date < :today
                ORDER BY game_date ASC
            """)
            params: dict[str, Any] = {"today": date.today()}
        else:
            dates_query = text("""
                SELECT DISTINCT game_date
                FROM kalshi_paper_bets
                WHERE status = 'pending'
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
                WHERE game_date = :game_date AND status = 'pending'
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
                COALESCE(SUM(pnl), 0) as total_pnl
            FROM kalshi_paper_bets
            WHERE game_date = :game_date
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
