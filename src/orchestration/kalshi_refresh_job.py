"""
Kalshi Refresh Job
==================
Orchestrates the full Kalshi pipeline:
  1. Scrape markets (or skip if no credentials)
  2. Fetch orderbook snapshots
  3. Compute edges against MC samples
  4. Paper trading (select/place/resolve bets)
  5. Send Discord alert for high-edge markets

Follows the edge_refresh_job.py pattern: CLI-driven, subprocess-friendly,
graceful degradation when credentials are missing.

Usage:
    python src/orchestration/kalshi_refresh_job.py
    python src/orchestration/kalshi_refresh_job.py --mock --dry-run
    python src/orchestration/kalshi_refresh_job.py --sport nba --date 2026-03-31
    python src/orchestration/kalshi_refresh_job.py --skip-discord
    python src/orchestration/kalshi_refresh_job.py --skip-paper
"""

import argparse
import logging
import os
import sys
from datetime import date
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("KalshiRefreshJob")


def run(
    target_date: date,
    sport: str = "nba",
    dry_run: bool = False,
    mock: bool = False,
    skip_discord: bool = False,
    skip_paper: bool = False,
    skip_live: bool = False,
    resolve_only: bool = False,
) -> dict:
    """Run the full Kalshi refresh pipeline.

    Args:
        target_date: Date to process.
        sport: Target sport ("nba" or "mlb").
        dry_run: Print results without DB writes.
        mock: Use synthetic data instead of API.
        skip_discord: Skip Discord alerts.

    Returns:
        Summary dict.
    """
    summary: dict = {"scrape": {}, "edges": {}, "paper_trading": {}, "live_trading": {}, "alerts_sent": False}

    # Resolve-only mode: skip scrape/edges/paper/live, just resolve + reconcile
    if resolve_only:
        logger.info("Resolve-only mode: resolving/reconciling live orders only")
        try:
            from src.paper_trading.kalshi_live_trader import KalshiLiveTrader

            resolver = KalshiLiveTrader(resolve_only=True)
            resolver.reconcile_fills()
            resolve_result = resolver.resolve_settled()
            summary["live_resolution"] = resolve_result
            logger.info(f"Resolve-only result: {resolve_result}")
        except Exception as e:
            logger.error(f"Resolve-only failed: {e}", exc_info=True)
            summary["live_resolution"] = {"error": str(e)}
        return summary

    # Step 1: Scrape markets
    logger.info("Step 1: Scraping Kalshi markets...")
    try:
        from src.scrapers.kalshi.kalshi_market_scraper import scrape_and_store

        scrape_stats = scrape_and_store(sport=sport, dry_run=dry_run, mock=mock)
        summary["scrape"] = scrape_stats
        logger.info(f"Scrape: {scrape_stats.get('parsed', 0)} parsed, {scrape_stats.get('stored', 0)} stored")
    except Exception as e:
        logger.error(f"Market scrape failed: {e}", exc_info=True)
        summary["scrape"] = {"error": str(e)}

    # Step 2: Fetch orderbook snapshots (skip in dry-run/mock)
    if not dry_run and not mock:
        logger.info("Step 2: Fetching orderbook snapshots...")
        try:
            _fetch_orderbooks(target_date, sport)
        except Exception as e:
            logger.warning(f"Orderbook fetch failed (non-fatal): {e}")
    else:
        logger.info("Step 2: Skipping orderbook fetch (dry-run/mock)")

    # Step 3: Compute edges
    if not dry_run:
        logger.info("Step 3: Computing edges...")
        try:
            from src.models.kalshi_edge import KalshiEdgeCalculator

            calc = KalshiEdgeCalculator()
            edge_stats = calc.compute_edges(target_date, sport=sport)
            summary["edges"] = edge_stats
            logger.info(
                f"Edges: {edge_stats.get('matched', 0)} matched, "
                f"{edge_stats.get('updated', 0)} updated"
            )
        except Exception as e:
            logger.error(f"Edge computation failed: {e}", exc_info=True)
            summary["edges"] = {"error": str(e)}
    else:
        logger.info("Step 3: Skipping edge computation (dry-run)")

    # Step 4: Paper trading
    if not skip_paper and not dry_run and not mock:
        logger.info("Step 4: Paper trading...")
        try:
            from src.paper_trading.kalshi_paper_trader import KalshiPaperTrader

            trader = KalshiPaperTrader()
            # Resolve pending bets from previous days first
            resolve_result = trader.resolve_all_pending()
            # Select and place new bets
            bets = trader.select_bets(target_date, sport=sport)
            placed = trader.place_bets(bets) if bets else 0
            summary["paper_trading"] = {
                "resolved": resolve_result.get("total_resolved", 0),
                "selected": len(bets),
                "placed": placed,
            }
        except Exception as e:
            logger.warning(f"Paper trading failed (non-fatal): {e}")
            summary["paper_trading"] = {"error": str(e)}
    else:
        logger.info("Step 4: Skipping paper trading")

    # Step 4.5a: ALWAYS resolve + reconcile pending live orders (even if trading disabled)
    if not dry_run and not mock:
        logger.info("Step 4.5a: Resolving/reconciling live orders...")
        try:
            from src.paper_trading.kalshi_live_trader import KalshiLiveTrader

            resolver = KalshiLiveTrader(resolve_only=True)
            resolver.reconcile_fills(target_date)
            resolve_result = resolver.resolve_settled()
            summary["live_resolution"] = resolve_result
            logger.info(f"Live resolution: {resolve_result}")
        except Exception as e:
            logger.warning(f"Live resolution failed: {e}")
            summary["live_resolution"] = {"error": str(e)}
    else:
        logger.info("Step 4.5a: Skipping live resolution (dry-run/mock)")

    # Step 4.5b: Live trading — NEW trades only (gated by env var)
    if not skip_live and not dry_run and not mock:
        live_enabled = os.getenv("KALSHI_LIVE_TRADING_ENABLED", "false").lower() == "true"
        if live_enabled:
            logger.info("Step 4.5b: Live trading...")
            try:
                from src.paper_trading.kalshi_live_trader import KalshiLiveTrader

                trader = KalshiLiveTrader()

                # Check circuit breakers first
                can_trade, reason = trader.check_circuit_breakers()
                if not can_trade:
                    logger.warning(f"Live trading halted: {reason}")
                    summary["live_trading"] = {"halted": True, "reason": reason}
                else:
                    # Carry forward any trades that expired without action
                    # (silently extends their timer if markets are still open)
                    renewed = trader.renew_expired_queue_trades(target_date, sport=sport)

                    # Select NEW trades (skips player+stat combos already pending)
                    trades = trader.select_trades(target_date, sport=sport)
                    if trades:
                        proposed = trader.propose_trades(trades)
                        _send_trade_approval_alert(trades, sport)
                        summary["live_trading"] = {
                            "selected": len(trades),
                            "proposed": proposed,
                            "renewed": renewed,
                        }
                    else:
                        summary["live_trading"] = {"selected": 0, "proposed": 0, "renewed": renewed}
            except RuntimeError as e:
                logger.warning(f"Live trading not available: {e}")
                summary["live_trading"] = {"error": str(e)}
            except Exception as e:
                logger.error(f"Live trading failed: {e}", exc_info=True)
                summary["live_trading"] = {"error": str(e)}
        else:
            logger.info("Step 4.5b: Live trading not enabled (KALSHI_LIVE_TRADING_ENABLED != true)")
    else:
        logger.info("Step 4.5b: Skipping live trading")

    # Step 5: Discord alerts
    if not skip_discord and not dry_run:
        logger.info("Step 5: Checking for high-edge markets...")
        try:
            alerts_sent = _send_high_edge_alerts(target_date, sport)
            summary["alerts_sent"] = alerts_sent
        except Exception as e:
            logger.warning(f"Discord alert failed (non-fatal): {e}")
    else:
        logger.info("Step 5: Skipping Discord alerts")

    return summary


def _send_trade_approval_alert(trades: list, sport: str) -> None:
    """Send Discord notification that trades are pending approval."""
    try:
        from src.discord_bot.alerts import send_kalshi_trade_alert_sync

        total_exposure = sum(t.get("expected_cost", 0) for t in trades)
        edges = [t.get("fee_adjusted_edge", 0) for t in trades if t.get("fee_adjusted_edge")]
        edge_range = f"{min(edges):.0%}-{max(edges):.0%}" if edges else "N/A"

        send_kalshi_trade_alert_sync("approval_needed", {
            "sport": sport.upper(),
            "count": len(trades),
            "total_exposure": total_exposure,
            "edge_range": edge_range,
            "trades": [
                {
                    "player_name": t.get("player_name", "Unknown"),
                    "stat_type": t["stat_type"],
                    "side": t["side"],
                    "contracts": t["contracts"],
                    "expected_cost": t["expected_cost"],
                    "fee_adjusted_edge": t.get("fee_adjusted_edge", 0),
                }
                for t in trades[:10]  # Cap at 10 for Discord embed limits
            ],
        })
    except Exception as e:
        logger.warning(f"Failed to send trade approval alert: {e}")


def _fetch_orderbooks(target_date: date, sport: str) -> int:
    """Fetch and store orderbook snapshots for open markets.

    Returns:
        Number of orderbooks fetched.
    """
    from datetime import UTC, datetime

    from sqlalchemy import text

    from src.db.client import get_engine
    from src.scrapers.kalshi.kalshi_client import KalshiClient

    client = KalshiClient()
    if not client.is_authenticated:
        logger.info("No Kalshi credentials — skipping orderbook fetch")
        return 0

    engine = get_engine()

    # Get open market tickers
    query = text("""
        SELECT DISTINCT ticker FROM kalshi_markets
        WHERE sport = :sport
          AND (snapshot_time AT TIME ZONE 'America/New_York')::date = :target_date
          AND market_status = 'open'
    """)
    with engine.connect() as conn:
        tickers = [row[0] for row in conn.execute(query, {"sport": sport, "target_date": target_date}).fetchall()]

    if not tickers:
        logger.info("No open markets to fetch orderbooks for")
        return 0

    snapshot_time = datetime.now(UTC)
    insert_stmt = text("""
        INSERT INTO kalshi_orderbook_snapshots (
            ticker, snapshot_time, yes_bid, yes_ask, yes_bid_size, yes_ask_size,
            depth, mid_price, spread, total_bid_depth, total_ask_depth
        ) VALUES (
            :ticker, :snapshot_time, :yes_bid, :yes_ask, :yes_bid_size, :yes_ask_size,
            :depth, :mid_price, :spread, :total_bid_depth, :total_ask_depth
        )
    """)

    from concurrent.futures import ThreadPoolExecutor, as_completed
    import json

    def _fetch_orderbook(ticker):
        try:
            return ticker, client.get_orderbook(ticker)
        except Exception as e:
            logger.warning(f"Orderbook fetch failed for {ticker}: {e}")
            return ticker, None

    orderbook_results = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(_fetch_orderbook, t): t for t in tickers}
        for future in as_completed(futures):
            ticker, result = future.result()
            if result is None:
                continue
            orderbook_results[ticker] = result

    count = 0
    with engine.begin() as conn:
        for ticker, result in orderbook_results.items():
            ob = result.get("orderbook", {})
            yes_bids = ob.get("yes", [])
            no_bids = ob.get("no", [])

            best_bid = yes_bids[0][0] if yes_bids else 0
            best_ask = (100 - no_bids[0][0]) if no_bids else 100
            best_bid_size = yes_bids[0][1] if yes_bids else 0
            best_ask_size = no_bids[0][1] if no_bids else 0

            mid = (best_bid + best_ask) / 2.0
            spread = best_ask - best_bid
            total_bid_depth = sum(level[1] for level in yes_bids) if yes_bids else 0
            total_ask_depth = sum(level[1] for level in no_bids) if no_bids else 0

            conn.execute(insert_stmt, {
                "ticker": ticker,
                "snapshot_time": snapshot_time,
                "yes_bid": best_bid,
                "yes_ask": best_ask,
                "yes_bid_size": best_bid_size,
                "yes_ask_size": best_ask_size,
                "depth": json.dumps({"yes": yes_bids, "no": no_bids}),
                "mid_price": mid,
                "spread": spread,
                "total_bid_depth": total_bid_depth,
                "total_ask_depth": total_ask_depth,
            })
            count += 1

    logger.info(f"Fetched {count} orderbook snapshots")
    return count


def _send_high_edge_alerts(target_date: date, sport: str, min_edge: float = 0.05) -> bool:
    """Send Discord alert if any markets have fee-adjusted edge >= threshold.

    Returns:
        True if alert was sent.
    """
    from sqlalchemy import text

    from src.db.client import get_engine

    engine = get_engine()

    query = text("""
        SELECT DISTINCT ON (ticker)
            ticker, player_name, stat_type, line,
            yes_price, bid_ask_spread, volume, open_interest,
            maker_fee_adjusted_edge, close_time, model_prob, kalshi_implied
        FROM kalshi_markets
        WHERE sport = :sport
          AND (snapshot_time AT TIME ZONE 'America/New_York')::date = :target_date
          AND market_status = 'open'
          AND maker_fee_adjusted_edge IS NOT NULL
          AND maker_fee_adjusted_edge >= :min_edge
          AND volume >= 20
          AND bid_ask_spread <= 15
        ORDER BY ticker, snapshot_time DESC
    """)

    with engine.connect() as conn:
        rows = conn.execute(query, {
            "sport": sport, "target_date": target_date, "min_edge": min_edge,
        }).fetchall()

    if not rows:
        logger.info("No high-edge Kalshi markets found")
        return False

    logger.info(f"Found {len(rows)} Kalshi markets with edge >= {min_edge:.0%}")

    try:
        from src.discord_bot.alerts import send_kalshi_alert_sync

        return send_kalshi_alert_sync(
            markets=[dict(row._mapping) for row in rows],
            target_date=target_date,
            sport=sport,
        )
    except Exception as e:
        logger.warning(f"Failed to send Kalshi Discord alert: {e}")
        return False


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kalshi market refresh pipeline")
    parser.add_argument("--date", type=str, default=date.today().isoformat(), help="Target date (YYYY-MM-DD)")
    parser.add_argument("--sport", type=str, default="nba", choices=["nba", "mlb"])
    parser.add_argument("--dry-run", action="store_true", help="No DB writes or API calls")
    parser.add_argument("--mock", action="store_true", help="Use synthetic data")
    parser.add_argument("--skip-discord", action="store_true", help="Skip Discord alerts")
    parser.add_argument("--skip-paper", action="store_true", help="Skip paper trading")
    parser.add_argument("--skip-live", action="store_true", help="Skip live trading")
    parser.add_argument("--resolve-only", action="store_true", help="Only resolve/reconcile live orders, no new trades")
    parser.add_argument("--yes-bets", action="store_true", help="Allow YES bets (sets KALSHI_ALLOW_YES_BETS=true)")
    return parser.parse_args()


def main():
    args = parse_args()
    target_date = date.fromisoformat(args.date)

    if args.yes_bets:
        os.environ["KALSHI_ALLOW_YES_BETS"] = "true"
    allow_yes = os.environ.get("KALSHI_ALLOW_YES_BETS", "false").lower() == "true"

    logger.info("=" * 60)
    logger.info("Kalshi Refresh Job")
    logger.info(f"  Date: {target_date}")
    logger.info(f"  Sport: {args.sport.upper()}")
    logger.info(f"  Mode: {'YES+NO' if allow_yes else 'NO-only (default)'}")
    logger.info(f"  Mock: {args.mock}")
    logger.info(f"  Dry run: {args.dry_run}")
    logger.info(f"  Skip Discord: {args.skip_discord}")
    logger.info(f"  Skip Paper: {args.skip_paper}")
    logger.info(f"  Skip Live: {args.skip_live}")
    logger.info(f"  Resolve Only: {args.resolve_only}")
    logger.info("=" * 60)

    # Check credentials early
    has_creds = bool(os.getenv("KALSHI_API_KEY"))
    if not has_creds and not args.mock:
        logger.info("KALSHI_API_KEY not set — exiting gracefully (use --mock for testing)")
        sys.exit(0)

    summary = run(
        target_date=target_date,
        sport=args.sport,
        dry_run=args.dry_run,
        mock=args.mock,
        skip_discord=args.skip_discord,
        skip_paper=args.skip_paper,
        skip_live=args.skip_live,
        resolve_only=args.resolve_only,
    )

    logger.info("=" * 60)
    logger.info("KALSHI REFRESH COMPLETE")
    logger.info(f"  Scrape: {summary.get('scrape', {})}")
    logger.info(f"  Edges: {summary.get('edges', {})}")
    logger.info(f"  Paper trading: {summary.get('paper_trading', {})}")
    logger.info(f"  Live resolution: {summary.get('live_resolution', {})}")
    logger.info(f"  Live trading: {summary.get('live_trading', {})}")
    logger.info(f"  Alerts sent: {summary.get('alerts_sent', False)}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
