#!/usr/bin/env python3
"""
Kalshi Execute Approved Trades Job
===================================
Polls `kalshi_trade_queue` for trades with status='approved' and executes
them via the Kalshi API. Designed to run on a short interval (every 2 min)
during trading hours so human approvals on the dashboard get placed quickly.

Usage:
    python src/orchestration/kalshi_execute_approved_job.py
    python src/orchestration/kalshi_execute_approved_job.py --dry-run
"""

import argparse
import logging
import math
import os
import sys
import time
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from dotenv import load_dotenv
from sqlalchemy import text

from src.db.client import get_engine
from src.scrapers.kalshi.kalshi_client import KalshiClient
from src.scrapers.kalshi.kalshi_utils import kalshi_taker_fee
from src.trading.kalshi.alert_adapter import KalshiAlertAdapter
from src.trading.kalshi.events import CircuitBreakerTripped, TradeExecutionFailed, TradePlaced
from src.trading.kalshi.execution_service import KalshiExecutionService
from src.trading.kalshi.queue_service import KalshiQueueService, split_executable_approved_rows
from src.trading.kalshi.risk_service import KalshiRiskService

load_dotenv()

LOG_DIR = Path(__file__).resolve().parents[2] / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "kalshi_execute_approved.log"),
    ],
)
logger = logging.getLogger("KalshiExecuteApproved")


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


def _row_value(row, index: int, key: str):
    if hasattr(row, "_mapping"):
        return row._mapping[key]
    if isinstance(row, dict):
        return row[key]
    return row[index]


def _get_best_available_price(client: KalshiClient, ticker: str, side: str, target_cents: int) -> int | None:
    """Get the taker fill price in YES-equivalent cents from the live orderbook."""
    try:
        ob = client.get_orderbook(ticker, depth=10)
        if ob is None:
            return None
        orderbook = ob.get("orderbook", {})

        if side == "yes":
            no_bids = orderbook.get("no", [])
            for no_bid, qty in no_bids:
                if qty > 0:
                    return 100 - no_bid
            return None

        yes_bids = orderbook.get("yes", [])
        for yes_bid, qty in yes_bids:
            if qty > 0:
                return yes_bid
        return None
    except Exception as e:
        logger.warning(f"Orderbook query failed for {ticker}: {e}")
        return None


def _calculate_kelly_contracts(model_prob: float, price_cents: int, side: str, bankroll: float) -> int:
    """Calculate Kelly-optimal contracts using the legacy live-trader formula."""
    max_contracts = _env_int("KALSHI_LIVE_MAX_CONTRACTS", 50)
    kelly_fraction = _env_float("KALSHI_LIVE_KELLY_FRACTION", 0.125)

    if side == "yes":
        cost_per = price_cents / 100.0
        win_per = (100 - price_cents) / 100.0
        prob = model_prob
    else:
        cost_per = (100 - price_cents) / 100.0
        win_per = price_cents / 100.0
        prob = 1.0 - model_prob

    if win_per <= 0 or cost_per <= 0:
        return 0

    fee_per = kalshi_taker_fee(price_cents if side == "yes" else 100 - price_cents)
    net_win_per = win_per - fee_per
    if net_win_per <= 0:
        return 0

    f_net = (prob - cost_per) / net_win_per
    f_fractional = f_net * kelly_fraction
    if f_fractional <= 0:
        return 0

    contracts = int(math.floor(f_fractional * bankroll / cost_per))
    return min(contracts, max_contracts)


def _send_circuit_breaker_alert(reason: str, balance: float, action: str) -> None:
    KalshiAlertAdapter().send(CircuitBreakerTripped(
        reason=reason,
        balance=balance,
        action=action,
        dedupe=True,
    ))


def _send_trade_placed_alert(
    trade: dict,
    fill_price: int | None,
    contracts: int,
    total_cost: float,
    balance: float,
    swept_from: int | None = None,
    swept_to: int | None = None,
    recalc_edge: float | None = None,
) -> None:
    KalshiAlertAdapter().send(TradePlaced(
        trade=trade,
        fill_price=fill_price,
        contracts=contracts,
        total_cost=total_cost,
        balance=balance,
        swept_from=swept_from,
        swept_to=swept_to,
        recalc_edge=recalc_edge,
    ))


def _alert_channel() -> str | None:
    return os.getenv("DISCORD_CHANNEL_KALSHI") or os.getenv("DISCORD_CHANNEL_PREDICTIONS")


def _send_trade_execution_failed_alert(trade: dict, error_msg: str) -> None:
    KalshiAlertAdapter().send(TradeExecutionFailed(
        trade=trade,
        error_msg=error_msg,
        channel_id=_alert_channel(),
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


def _fetch_preview_rows(engine):
    with engine.connect() as conn:
        return conn.execute(text("""
            SELECT id, ticker, player_name, stat_type, line, side, contracts, expected_cost,
                   fee_adjusted_edge
            FROM kalshi_trade_queue
            WHERE status = 'approved'
              AND expires_at > now()
            ORDER BY approved_at ASC NULLS LAST
        """)).fetchall()


def main():
    parser = argparse.ArgumentParser(description="Execute approved Kalshi trades from the queue")
    parser.add_argument("--dry-run", action="store_true", help="Log approved trades but don't execute")
    args = parser.parse_args()

    live_enabled = os.getenv("KALSHI_LIVE_TRADING_ENABLED", "false").lower() == "true"
    if not live_enabled:
        logger.info("KALSHI_LIVE_TRADING_ENABLED != true — skipping")
        return

    start = time.time()
    engine = get_engine()

    rows = _fetch_preview_rows(engine)
    if not rows:
        logger.info("No approved trades in queue")
        return

    trade_ids = [_row_value(row, 0, "id") for row in rows]
    logger.info(f"Found {len(trade_ids)} approved trades ready to execute")
    for row in rows:
        logger.info(
            f"  id={_row_value(row, 0, 'id')} {_row_value(row, 1, 'ticker')} "
            f"{_row_value(row, 2, 'player_name')} {_row_value(row, 3, 'stat_type')} "
            f"{_row_value(row, 4, 'line')} {_row_value(row, 5, 'side')} "
            f"x{_row_value(row, 6, 'contracts')} cost=${float(_row_value(row, 7, 'expected_cost')):.2f}"
        )

    if args.dry_run:
        logger.info("[DRY RUN] Would execute these trades")
        return

    client = KalshiClient()
    if not getattr(client, "is_authenticated", False):
        logger.warning("KalshiClient unavailable: Kalshi API credentials not configured for live trading")
        return

    risk_service = _build_risk_service(engine, client)
    risk_service.ensure_config()

    can_trade, reason = risk_service.check_circuit_breakers()
    if not can_trade:
        logger.warning(f"Circuit breaker triggered — not executing: {reason}")
        return

    queue_service = KalshiQueueService(engine)
    approved_rows = queue_service.fetch_approved_rows(trade_ids)
    if not approved_rows:
        logger.warning("No approved trades found in queue")
        return

    trades, expired_ids = split_executable_approved_rows(approved_rows)
    if expired_ids:
        queue_service.mark_expired_trade_ids(expired_ids)
        logger.warning(f"Expired {len(expired_ids)} trades from queue")

    if not trades:
        return

    execution_service = KalshiExecutionService(
        engine=engine,
        client=client,
        get_best_available_price=lambda ticker, side, target_cents: _get_best_available_price(
            client, ticker, side, target_cents,
        ),
        calculate_kelly_contracts=_calculate_kelly_contracts,
        send_trade_placed_alert=_send_trade_placed_alert,
        sweep_max_cents=_env_int("KALSHI_SWEEP_MAX_CENTS", 10),
        sweep_edge_retention=_env_float("KALSHI_SWEEP_EDGE_RETENTION", 0.50),
    )
    results = execution_service.execute_trades(trades)
    queue_service.mark_execution_results(trades, results)

    elapsed = time.time() - start
    placed = sum(1 for r in results if r.get("order_id"))
    logger.info(
        f"Executed {placed}/{len(trade_ids)} trades in {elapsed:.1f}s "
        f"(results={len(results)})"
    )

    executed_tickers: set = set()
    for r in results:
        if r.get("order_id"):
            executed_tickers.add(r["ticker"])
            try:
                from src.discord_bot.alerts import send_kalshi_trade_alert_sync

                row_data = next((row for row in rows if _row_value(row, 1, "ticker") == r["ticker"]), None)
                player_name = _row_value(row_data, 2, "player_name") if row_data else "Unknown"
                stat_type = _row_value(row_data, 3, "stat_type") if row_data else ""
                side = _row_value(row_data, 5, "side") if row_data else r.get("side", "")
                line = float(_row_value(row_data, 4, "line")) if row_data else 0
                contracts = _row_value(row_data, 6, "contracts") if row_data else r.get("contracts", 0)
                edge_val = (
                    float(_row_value(row_data, 8, "fee_adjusted_edge"))
                    if row_data and _row_value(row_data, 8, "fee_adjusted_edge") is not None else 0
                )

                channel = os.getenv("DISCORD_CHANNEL_KALSHI") or os.getenv("DISCORD_CHANNEL_PREDICTIONS")
                if channel:
                    send_kalshi_trade_alert_sync("placed", {
                        "player_name": player_name,
                        "stat_type": stat_type,
                        "side": side,
                        "line": line,
                        "fill_price": r.get("fill_price"),
                        "contracts": contracts,
                        "total_cost": r.get("total_cost", 0),
                        "fee_adjusted_edge": edge_val,
                        "balance_after": 0,
                    }, channel_id=channel)
            except Exception as e:
                logger.warning(f"Discord alert failed (non-fatal): {e}")

    approved_trades = [
        {
            "id": _row_value(row, 0, "id"),
            "ticker": _row_value(row, 1, "ticker"),
            "player_name": _row_value(row, 2, "player_name"),
            "stat_type": _row_value(row, 3, "stat_type"),
            "line": float(_row_value(row, 4, "line")) if _row_value(row, 4, "line") else 0,
            "side": _row_value(row, 5, "side"),
            "contracts": _row_value(row, 6, "contracts"),
            "expected_cost": float(_row_value(row, 7, "expected_cost")) if _row_value(row, 7, "expected_cost") else 0,
            "fee_adjusted_edge": (
                float(_row_value(row, 8, "fee_adjusted_edge"))
                if _row_value(row, 8, "fee_adjusted_edge") is not None else 0
            ),
        }
        for row in rows
    ]
    for trade in approved_trades:
        if trade["ticker"] not in executed_tickers:
            _send_trade_execution_failed_alert(
                trade,
                "Order returned no fill — check orderbook liquidity",
            )


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Job failed: {e}", exc_info=True)
        sys.exit(1)
