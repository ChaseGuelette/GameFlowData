"""
Arb Paper Trader
================
Paper trades arbitrage opportunities detected between Kalshi and Polymarket.

Only trades pure arbs (guaranteed profit) by default. Soft arbs are skipped
because their P&L depends on outcome and is harder to track cleanly.

Pure arb P&L is deterministic: both legs pay $1/contract together regardless
of outcome. P&L per contract = net_margin / 100.

Usage:
    from src.paper_trading.arb_paper_trader import ArbPaperTrader
    trader = ArbPaperTrader()
    arbs = trader.select_arbs(sport="mlb")
    trader.place_arbs(arbs)
    trader.resolve_all_pending()
"""

import logging
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from sqlalchemy import text

logger = logging.getLogger(__name__)


@dataclass
class ArbPaperTrader:
    """Paper trader for Kalshi↔Polymarket arbitrage opportunities.

    Attributes:
        contracts_per_arb: Number of contracts to paper-trade per arb.
        max_daily_arbs: Maximum arbs to place per day (cap).
        min_net_margin: Minimum net margin in cents to trade (pure arbs only).
        skip_season_futures: Skip season_future market type (phantom-prone).
    """
    contracts_per_arb: int = 10
    max_daily_arbs: int = 50
    min_net_margin: float = 0.5       # cents; must be clearly profitable
    skip_season_futures: bool = True

    def __post_init__(self):
        from src.db.client import get_engine
        self.engine = get_engine()

    # ------------------------------------------------------------------
    # Select arbs
    # ------------------------------------------------------------------

    def select_arbs(self, sport: str = "mlb") -> list[dict[str, Any]]:
        """Select new arb opportunities to paper-trade.

        Queries arb_opportunities from the last 2 hours (current scan cycle).
        Deduplicates against already-placed bets using the unique key
        (kalshi_ticker, poly_condition_id, kalshi_side).

        Filters applied:
        - pure arbs only
        - net_margin >= min_net_margin
        - skip season_future if skip_season_futures=True
        - daily cap enforcement

        Returns:
            List of arb opportunity dicts ready for place_arbs().
        """
        # Count already placed today
        today = date.today()
        count_q = text("""
            SELECT COUNT(*) FROM arb_paper_bets
            WHERE (placed_at AT TIME ZONE 'America/New_York')::date = :today AND sport = :sport
        """)
        with self.engine.connect() as conn:
            placed_today = conn.execute(count_q, {"today": today, "sport": sport}).scalar() or 0

        slots_remaining = self.max_daily_arbs - placed_today
        if slots_remaining <= 0:
            logger.info(f"Daily arb cap reached ({self.max_daily_arbs}), skipping select")
            return []

        season_future_filter = "AND ao.market_type != 'season_future'" if self.skip_season_futures else ""

        # Fetch recent pure arbs not already placed
        query = text(f"""
            SELECT DISTINCT ON (ao.kalshi_ticker, ao.poly_condition_id, ao.kalshi_side)
                ao.id AS arb_opportunity_id,
                ao.sport,
                ao.arb_type,
                ao.market_type,
                ao.team1,
                ao.team2,
                ao.description,
                ao.kalshi_ticker,
                ao.kalshi_side,
                ao.kalshi_price,
                ao.kalshi_fee,
                ao.kalshi_volume,
                ao.poly_condition_id,
                ao.poly_side,
                ao.poly_price,
                ao.poly_liquidity,
                ao.poly_fee,
                ao.combined_cost,
                ao.net_margin,
                ao.scan_time
            FROM arb_opportunities ao
            LEFT JOIN arb_paper_bets pb ON (
                pb.kalshi_ticker = ao.kalshi_ticker
                AND pb.poly_condition_id = ao.poly_condition_id
                AND pb.kalshi_side = ao.kalshi_side
            )
            WHERE ao.sport = :sport
              AND ao.arb_type = 'pure'
              AND ao.net_margin >= :min_margin
              AND ao.scan_time >= NOW() - INTERVAL '2 hours'
              AND pb.id IS NULL
              {season_future_filter}
            ORDER BY ao.kalshi_ticker, ao.poly_condition_id, ao.kalshi_side,
                     ao.net_margin DESC
            LIMIT :limit
        """)

        with self.engine.connect() as conn:
            rows = conn.execute(query, {
                "sport": sport,
                "min_margin": self.min_net_margin,
                "limit": slots_remaining,
            }).fetchall()

        results = [dict(row._mapping) for row in rows]
        logger.info(f"Selected {len(results)} new arb opportunities for paper trading")
        return results

    # ------------------------------------------------------------------
    # Place arbs
    # ------------------------------------------------------------------

    def place_arbs(self, arbs: list[dict[str, Any]]) -> int:
        """Insert arb paper bets into arb_paper_bets.

        Uses ON CONFLICT DO NOTHING for idempotency — duplicate key
        (kalshi_ticker, poly_condition_id, kalshi_side) is silently skipped.

        Also sends a Discord alert per placed bet.

        Returns:
            Count of actually inserted rows.
        """
        if not arbs:
            return 0

        from src.arbitrage.market_matcher import _extract_date_from_kalshi_ticker

        stmt = text("""
            INSERT INTO arb_paper_bets (
                sport, arb_type, market_type,
                team1, team2, description,
                kalshi_ticker, kalshi_side, kalshi_price, kalshi_fee,
                kalshi_contracts,
                poly_condition_id, poly_side, poly_price, poly_fee,
                poly_contracts,
                combined_cost, net_margin,
                game_date,
                arb_opportunity_id,
                status
            ) VALUES (
                :sport, :arb_type, :market_type,
                :team1, :team2, :description,
                :kalshi_ticker, :kalshi_side, :kalshi_price, :kalshi_fee,
                :kalshi_contracts,
                :poly_condition_id, :poly_side, :poly_price, :poly_fee,
                :poly_contracts,
                :combined_cost, :net_margin,
                :game_date,
                :arb_opportunity_id,
                'placed'
            )
            ON CONFLICT (kalshi_ticker, poly_condition_id, kalshi_side) DO NOTHING
        """)

        count = 0
        with self.engine.begin() as conn:
            for arb in arbs:
                ticker = arb.get("kalshi_ticker") or ""
                game_date_val = _extract_date_from_kalshi_ticker(ticker)

                result = conn.execute(stmt, {
                    "sport": arb.get("sport") or "mlb",
                    "arb_type": arb.get("arb_type") or "pure",
                    "market_type": arb.get("market_type") or "moneyline",
                    "team1": arb.get("team1"),
                    "team2": arb.get("team2"),
                    "description": arb.get("description"),
                    "kalshi_ticker": ticker,
                    "kalshi_side": arb.get("kalshi_side") or "yes",
                    "kalshi_price": int(arb.get("kalshi_price") or 0),
                    "kalshi_fee": float(arb.get("kalshi_fee") or 0),
                    "kalshi_contracts": self.contracts_per_arb,
                    "poly_condition_id": arb.get("poly_condition_id") or "",
                    "poly_side": arb.get("poly_side") or "no",
                    "poly_price": float(arb.get("poly_price") or 0),
                    "poly_fee": float(arb.get("poly_fee") or 0),
                    "poly_contracts": self.contracts_per_arb,
                    "combined_cost": float(arb.get("combined_cost") or 0),
                    "net_margin": float(arb.get("net_margin") or 0),
                    "game_date": game_date_val,
                    "arb_opportunity_id": arb.get("arb_opportunity_id"),
                })
                if result.rowcount > 0:
                    count += 1
                    # Send Discord alert (non-fatal)
                    try:
                        from src.discord_bot.alerts import send_arb_paper_trade_alert_sync
                        bet_dict = {**arb, "game_date": game_date_val, "kalshi_contracts": self.contracts_per_arb, "poly_contracts": self.contracts_per_arb}
                        send_arb_paper_trade_alert_sync("placed", bet_dict)
                    except Exception as e:
                        logger.warning(f"Discord alert failed for placed arb: {e}")

        logger.info(f"Placed {count} arb paper bets ({len(arbs) - count} skipped as duplicates)")
        return count

    # ------------------------------------------------------------------
    # Resolve arbs
    # ------------------------------------------------------------------

    def resolve_arbs(self, target_date: date | None = None, sport: str = "mlb") -> dict[str, Any]:
        """Resolve placed arbs for a specific game date.

        For pure arbs (moneyline): determines winner from mlb_game_schedule,
        confirms both legs collectively paid $1/contract, marks as resolved_profit.
        Pure arbs are always profitable by construction — both legs pay $1 together
        regardless of which team wins. Resolution is just record-keeping.

        For unresolvable games (not Final yet): leaves as 'placed'.

        Args:
            target_date: Game date to resolve. Defaults to yesterday.
            sport: Sport identifier.

        Returns:
            Dict with counts: resolved, profit, loss, cancelled.
        """
        if target_date is None:
            target_date = date.today() - timedelta(days=1)

        # Fetch placed bets for this game date
        fetch_q = text("""
            SELECT id, kalshi_ticker, kalshi_side, kalshi_price, kalshi_contracts,
                   poly_condition_id, poly_side, poly_price, poly_contracts,
                   net_margin, market_type, team1, team2
            FROM arb_paper_bets
            WHERE status = 'placed'
              AND game_date = :game_date
              AND sport = :sport
        """)
        with self.engine.connect() as conn:
            rows = conn.execute(fetch_q, {"game_date": target_date, "sport": sport}).fetchall()

        if not rows:
            logger.info(f"No placed arbs to resolve for {target_date} {sport}")
            return {"resolved": 0, "profit": 0, "loss": 0, "cancelled": 0}

        bets = [dict(r._mapping) for r in rows]
        logger.info(f"Resolving {len(bets)} arb bets for {target_date} {sport}")

        # For pure arbs: winner doesn't matter (both legs combined always pay $1).
        # We just need to confirm the game finished (status = 'Final').
        final_games = self._load_final_games(target_date, sport)

        result = {"resolved": 0, "profit": 0, "loss": 0, "cancelled": 0}

        with self.engine.begin() as conn:
            for bet in bets:
                team1 = (bet.get("team1") or "").upper()
                team2 = (bet.get("team2") or "").upper()

                # Check if game is Final
                is_final = self._is_game_final(team1, team2, final_games)

                if not is_final:
                    # Game not done yet — skip for now
                    continue

                # Pure arb: guaranteed profit regardless of winner
                # Both legs combined pay exactly $1/contract
                net_margin = float(bet.get("net_margin") or 0)
                contracts = int(bet.get("kalshi_contracts") or self.contracts_per_arb)
                pnl = net_margin * contracts / 100  # convert cents to dollars

                conn.execute(text("""
                    UPDATE arb_paper_bets
                    SET status = 'resolved_profit',
                        resolved_at = NOW(),
                        pnl = :pnl
                    WHERE id = :id
                """), {"pnl": pnl, "id": bet["id"]})

                result["resolved"] += 1
                result["profit"] += 1

        # Cancel stale bets older than 5 days with no resolution
        stale_cutoff = date.today() - timedelta(days=5)
        with self.engine.begin() as conn:
            cancelled = conn.execute(text("""
                UPDATE arb_paper_bets
                SET status = 'cancelled', resolved_at = NOW(), pnl = 0
                WHERE status = 'placed'
                  AND game_date <= :cutoff
                  AND sport = :sport
            """), {"cutoff": stale_cutoff, "sport": sport})
            stale_count = cancelled.rowcount
            result["cancelled"] += stale_count

        if result["resolved"] > 0 or result["cancelled"] > 0:
            self._update_daily_log(target_date, sport)

        logger.info(
            f"Resolved {result['resolved']} arbs ({result['profit']} profit, "
            f"{result['cancelled']} cancelled) for {target_date} {sport}"
        )
        return result

    def resolve_all_pending(self, sport: str = "mlb") -> dict[str, Any]:
        """Resolve all placed arbs where games may now be Final.

        Finds all unique game_dates with pending arbs (before today),
        then resolves each.

        Returns:
            Aggregated results across all resolved dates.
        """
        dates_q = text("""
            SELECT DISTINCT game_date FROM arb_paper_bets
            WHERE status = 'placed'
              AND sport = :sport
              AND game_date < CURRENT_DATE
            ORDER BY game_date
        """)
        with self.engine.connect() as conn:
            dates = [r[0] for r in conn.execute(dates_q, {"sport": sport}).fetchall()]

        if not dates:
            logger.info(f"No pending arbs to resolve for {sport}")
            return {"resolved": 0, "profit": 0, "loss": 0, "cancelled": 0}

        logger.info(f"Resolving pending arbs for {len(dates)} dates: {dates}")

        totals: dict[str, Any] = {"resolved": 0, "profit": 0, "loss": 0, "cancelled": 0}
        for d in dates:
            try:
                result = self.resolve_arbs(target_date=d, sport=sport)
                for k in totals:
                    totals[k] += result.get(k, 0)
            except Exception as e:
                logger.error(f"Failed to resolve arbs for {d}: {e}")

        return totals

    # ------------------------------------------------------------------
    # Game resolution helpers
    # ------------------------------------------------------------------

    def _load_final_games(self, game_date: date, sport: str) -> list[dict]:
        """Load Final games from the schedule for a given date."""
        if sport == "mlb":
            query = text("""
                SELECT
                    ht.team_abbreviation AS home_team,
                    at2.team_abbreviation AS away_team,
                    g.home_score,
                    g.away_score,
                    g.status
                FROM mlb_game_schedule g
                JOIN mlb_teams ht ON g.home_team_id = ht.team_id
                JOIN mlb_teams at2 ON g.away_team_id = at2.team_id
                WHERE g.game_date = :game_date
                  AND g.status = 'Final'
            """)
        else:
            # NBA (future expansion)
            return []

        try:
            with self.engine.connect() as conn:
                rows = conn.execute(query, {"game_date": game_date}).fetchall()
            return [dict(r._mapping) for r in rows]
        except Exception as e:
            logger.warning(f"Could not load final games for {game_date}: {e}")
            return []

    def _is_game_final(self, team1: str, team2: str, final_games: list[dict]) -> bool:
        """Check if a game between team1 and team2 is in the Final games list."""
        pair = {team1.upper(), team2.upper()}
        for game in final_games:
            home = (game.get("home_team") or "").upper()
            away = (game.get("away_team") or "").upper()
            if {home, away} == pair:
                return True
        return False

    # ------------------------------------------------------------------
    # Daily log
    # ------------------------------------------------------------------

    def _update_daily_log(self, log_date: date, sport: str = "mlb") -> None:
        """Aggregate resolved arb results and upsert daily log entry."""
        agg_q = text("""
            SELECT
                COUNT(*) AS arbs_placed,
                COUNT(*) FILTER (WHERE status IN ('resolved_profit', 'resolved_loss', 'cancelled')) AS arbs_resolved,
                COUNT(*) FILTER (WHERE status = 'resolved_profit') AS arbs_profit,
                COUNT(*) FILTER (WHERE status = 'resolved_loss') AS arbs_loss,
                COUNT(*) FILTER (WHERE status = 'cancelled') AS arbs_cancelled,
                COALESCE(SUM(pnl) FILTER (WHERE status IN ('resolved_profit', 'resolved_loss')), 0) AS total_pnl
            FROM arb_paper_bets
            WHERE game_date = :log_date AND sport = :sport
        """)
        with self.engine.connect() as conn:
            row = conn.execute(agg_q, {"log_date": log_date, "sport": sport}).fetchone()

        if row is None:
            return

        agg = dict(row._mapping)

        # Compute cumulative P&L up to and including log_date
        cum_q = text("""
            SELECT COALESCE(SUM(total_pnl), 0) FROM arb_paper_trading_daily_log
            WHERE log_date <= :log_date AND sport = :sport
        """)
        with self.engine.connect() as conn:
            prev_cum = float(conn.execute(cum_q, {"log_date": log_date - timedelta(days=1), "sport": sport}).scalar() or 0)

        cumulative_pnl = prev_cum + float(agg.get("total_pnl") or 0)

        upsert_q = text("""
            INSERT INTO arb_paper_trading_daily_log (
                log_date, sport, arbs_placed, arbs_resolved,
                arbs_profit, arbs_loss, arbs_cancelled,
                total_pnl, cumulative_pnl
            ) VALUES (
                :log_date, :sport, :arbs_placed, :arbs_resolved,
                :arbs_profit, :arbs_loss, :arbs_cancelled,
                :total_pnl, :cumulative_pnl
            )
            ON CONFLICT (log_date, sport) DO UPDATE SET
                arbs_placed = EXCLUDED.arbs_placed,
                arbs_resolved = EXCLUDED.arbs_resolved,
                arbs_profit = EXCLUDED.arbs_profit,
                arbs_loss = EXCLUDED.arbs_loss,
                arbs_cancelled = EXCLUDED.arbs_cancelled,
                total_pnl = EXCLUDED.total_pnl,
                cumulative_pnl = EXCLUDED.cumulative_pnl
        """)
        with self.engine.begin() as conn:
            conn.execute(upsert_q, {
                "log_date": log_date,
                "sport": sport,
                "arbs_placed": int(agg.get("arbs_placed") or 0),
                "arbs_resolved": int(agg.get("arbs_resolved") or 0),
                "arbs_profit": int(agg.get("arbs_profit") or 0),
                "arbs_loss": int(agg.get("arbs_loss") or 0),
                "arbs_cancelled": int(agg.get("arbs_cancelled") or 0),
                "total_pnl": float(agg.get("total_pnl") or 0),
                "cumulative_pnl": cumulative_pnl,
            })

        # Send daily summary Discord alert
        try:
            from src.discord_bot.alerts import send_arb_paper_trade_alert_sync
            summary = {
                "log_date": str(log_date),
                "sport": sport,
                "arbs_placed": int(agg.get("arbs_placed") or 0),
                "arbs_resolved": int(agg.get("arbs_resolved") or 0),
                "arbs_profit": int(agg.get("arbs_profit") or 0),
                "arbs_loss": int(agg.get("arbs_loss") or 0),
                "arbs_cancelled": int(agg.get("arbs_cancelled") or 0),
                "total_pnl": float(agg.get("total_pnl") or 0),
                "cumulative_pnl": cumulative_pnl,
            }
            send_arb_paper_trade_alert_sync("daily_summary", summary)
        except Exception as e:
            logger.warning(f"Discord daily summary alert failed: {e}")

        logger.info(f"Updated arb daily log for {log_date} {sport}: pnl={agg.get('total_pnl'):.2f}, cum={cumulative_pnl:.2f}")
