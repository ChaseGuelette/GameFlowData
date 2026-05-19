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

from src.db.client import get_engine
from src.scrapers.kalshi.kalshi_client import KalshiClient
from src.trading.kalshi.actuals_adapter import KalshiActualsAdapter
from src.trading.kalshi.alert_adapter import KalshiAlertAdapter
from src.trading.kalshi.daily_ledger_service import KalshiDailyLedgerService
from src.trading.kalshi.events import (
    CircuitBreakerTripped,
    HighEdgeMarketsFound,
    OrderResolved,
    TradeApprovalNeeded,
    TradeApprovalReminder,
)
from src.trading.kalshi.live_trading_config import (
    SPORTSBOOK_LINE_FALLBACK_GAP,
    SUPPORTED_STATS,
    get_game_start_time,
)
from src.trading.kalshi.queue_service import KalshiQueueService
from src.trading.kalshi.reconciliation_service import KalshiReconciliationService
from src.trading.kalshi.risk_service import KalshiRiskService
from src.trading.kalshi.selection_loader import KalshiSelectionInputLoader
from src.trading.kalshi.settlement_service import KalshiSettlementService
from src.trading.kalshi.strategy import select_trade_intents

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("KalshiRefreshJob")

def _env_float(name: str, default: float) -> float:
    val = os.environ.get(name)
    if val is None:
        return default
    try:
        return float(val)
    except ValueError:
        logger.warning(f"Invalid {name}={val}, using default {default}")
        return default


def _env_int(name: str, default: int) -> int:
    val = os.environ.get(name)
    if val is None:
        return default
    try:
        return int(val)
    except ValueError:
        logger.warning(f"Invalid {name}={val}, using default {default}")
        return default


def _send_resolution_alert(order, status: str, actual: float | None, pnl: float, balance: float) -> None:
    KalshiAlertAdapter().send(OrderResolved(
        order=order,
        status=status,
        actual=actual,
        pnl=pnl,
        balance=balance,
    ))


def _send_circuit_breaker_alert(reason: str, balance: float, action: str) -> None:
    KalshiAlertAdapter().send(CircuitBreakerTripped(
        reason=reason,
        balance=balance,
        action=action,
        dedupe=True,
    ))


def _build_risk_service(engine, client) -> KalshiRiskService:
    return KalshiRiskService(
        engine=engine,
        client=client,
        starting_bankroll=_env_float("KALSHI_LIVE_STARTING_BANKROLL", 100.0),
        drawdown_limit=_env_float("KALSHI_LIVE_DRAWDOWN_LIMIT", 0.30),
        daily_loss_limit=_env_float("KALSHI_LIVE_DAILY_LOSS_LIMIT", 15.0),
        consec_loss_limit=_env_int("KALSHI_LIVE_CONSEC_LOSS_LIMIT", 5),
        send_circuit_breaker_alert=_send_circuit_breaker_alert,
        force_resume=os.getenv("KALSHI_LIVE_FORCE_RESUME", "").lower() == "true",
    )


def _run_live_resolution(target_date: date | None = None) -> dict:
    engine = get_engine()
    client = KalshiClient()

    reconciliation_service = KalshiReconciliationService(engine=engine, client=client)
    reconciliation_service.reconcile_fills(target_date)

    actuals_adapter = KalshiActualsAdapter(engine)
    ledger_service = KalshiDailyLedgerService(
        engine=engine,
        starting_bankroll=_env_float("KALSHI_LIVE_STARTING_BANKROLL", 100.0),
    )
    risk_service = _build_risk_service(engine, client)
    settlement_service = KalshiSettlementService(
        engine=engine,
        client=client,
        fetch_actuals=actuals_adapter.fetch_actuals,
        send_resolution_alert=_send_resolution_alert,
        update_daily_log=ledger_service.update_daily_log,
        get_consecutive_losses=risk_service.get_consecutive_losses,
        update_streak=risk_service.update_streak,
    )
    return settlement_service.resolve_settled()


def _select_live_trades(engine, client, target_date: date, sport: str) -> list[dict]:
    inputs = KalshiSelectionInputLoader(
        engine=engine,
        client=client,
        supported_stats=SUPPORTED_STATS,
        get_game_start_time=get_game_start_time,
        sportsbook_line_fallback_gap=SPORTSBOOK_LINE_FALLBACK_GAP,
    ).load_inputs(
        target_date,
        sport=sport,
        prior_exposure=0.0,
        strategy_knobs={
            "min_edge": _env_float("KALSHI_LIVE_MIN_EDGE", 0.15),
            "min_price": _env_int("KALSHI_LIVE_MIN_PRICE", 5),
            "max_contracts": _env_int("KALSHI_LIVE_MAX_CONTRACTS", 50),
            "kelly_fraction": _env_float("KALSHI_LIVE_KELLY_FRACTION", 0.125),
        },
    )
    if inputs is None:
        return []

    intents = select_trade_intents(
        inputs.candidates,
        config=inputs.config,
        existing_player_stats=inputs.existing_player_stats,
        queued_player_stats=inputs.queued_player_stats,
        held_positions=inputs.held_positions,
    )
    trades = [intent.as_legacy_dict() for intent in intents]
    total_exposure = inputs.config.prior_exposure + sum(float(trade["expected_cost"]) for trade in trades)
    logger.info(
        f"Selected {len(trades)} Kalshi live trades [{inputs.mode_str}] for {target_date} "
        f"(exposure: ${total_exposure:.2f}/${inputs.effective_daily_exposure_cap:.2f} cap)"
    )
    return trades


def _run_live_trading(target_date: date, sport: str) -> dict:
    engine = get_engine()
    client = KalshiClient()
    if not getattr(client, "is_authenticated", False):
        raise RuntimeError("Kalshi API credentials not configured for live trading")

    risk_service = _build_risk_service(engine, client)
    risk_service.ensure_config()

    can_trade, reason = risk_service.check_circuit_breakers()
    if not can_trade:
        logger.warning(f"Live trading halted: {reason}")
        return {"halted": True, "reason": reason}

    sport_gate_var = f"{sport.upper()}_TRADING_ENABLED"
    sport_enabled = os.getenv(sport_gate_var, "false").lower() == "true"
    if not sport_enabled:
        logger.info(f"Step 4.5b: Live trading disabled for {sport} ({sport_gate_var}!=true) — skipping")
        return {"selected": 0, "proposed": 0, "renewed": 0}

    queue_service = KalshiQueueService(engine)
    renewed = queue_service.renew_expired_pending_trades(target_date, sport)
    if renewed > 0:
        logger.info(f"Renewed {renewed} expired pending trades for {sport.upper()} (markets still open)")

    already_pending = queue_service.fetch_pending_approval_trades(target_date, sport)
    trades = _select_live_trades(engine, client, target_date, sport)
    if trades:
        proposed = queue_service.propose_trades(trades)
        if proposed:
            logger.info(f"Proposed {proposed} trades to approval queue")
        _send_trade_approval_alert(trades, sport, already_pending=len(already_pending))
        return {"selected": len(trades), "proposed": proposed, "renewed": renewed}

    if already_pending:
        _send_reminder_alert(already_pending, sport)
    return {"selected": 0, "proposed": 0, "renewed": renewed}


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
            resolve_result = _run_live_resolution()
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
            resolve_result = _run_live_resolution(target_date)
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
                summary["live_trading"] = _run_live_trading(target_date, sport)
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


def _get_pending_queue_trades(engine, target_date, sport: str) -> list[dict]:
    from sqlalchemy import text as sa_text
    try:
        with engine.connect() as conn:
            rows = conn.execute(sa_text("""
                SELECT player_name, stat_type, side, contracts,
                       expected_cost, fee_adjusted_edge
                FROM kalshi_trade_queue
                WHERE sport = :sport
                  AND game_date = :d
                  AND status = 'pending_approval'
                  AND expires_at > now()
                ORDER BY proposed_at ASC
            """), {"sport": sport, "d": target_date}).fetchall()
        return [dict(r._mapping) for r in rows]
    except Exception as e:
        logger.warning(f"Failed to query pending queue trades: {e}")
        return []


def _send_trade_approval_alert(trades: list, sport: str, already_pending: int = 0) -> None:
    """Emit a notification that trades are pending approval."""
    KalshiAlertAdapter().send(TradeApprovalNeeded(
        trades=trades,
        sport=sport,
        already_pending=already_pending,
    ))


def _send_reminder_alert(pending_trades: list, sport: str) -> None:
    """Emit a notification that trades are still pending approval."""
    KalshiAlertAdapter().send(TradeApprovalReminder(
        pending_trades=pending_trades,
        sport=sport,
    ))


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

    import json
    from concurrent.futures import ThreadPoolExecutor, as_completed

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

    return KalshiAlertAdapter().send(HighEdgeMarketsFound(
        markets=[dict(row._mapping) for row in rows],
        target_date=target_date,
        sport=sport,
    ))


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
