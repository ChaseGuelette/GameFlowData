#!/usr/bin/env python3
"""
Kalshi Stale Fills Job — runs every 5 minutes (9 AM - 11 PM ET).
Detects pending Kalshi orders whose game has already started and enqueues
them in kalshi_cancel_queue for human review.

Detection methods (any one triggers):
1. game_start_time <= now() (from kalshi_markets.close_time)
2. Ticker-parsed game time <= now() (fallback when game_start_time is NULL)
3. game_date < today (yesterday's leftovers)
"""

import logging
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from dotenv import load_dotenv

load_dotenv()

log_dir = Path(__file__).resolve().parents[2] / "logs"
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_dir / "kalshi_stale_fills.log"),
    ],
)
logger = logging.getLogger("kalshi_stale_fills_job")


ET = ZoneInfo("America/New_York")

# Regex to parse game datetime from Kalshi ticker second segment.
# Example: KXMLBHIT-26APR251415SEASTL-... → year=26, mon=APR, day=25, time=1415
_TICKER_DT_RE = re.compile(
    r"-(\d{2})([A-Z]{3})(\d{2})(\d{4})[A-Z]",
)
_MONTH_MAP = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}


def _parse_game_time_from_ticker(ticker: str) -> datetime | None:
    """Extract game start time (ET) from Kalshi ticker.

    Ticker format: KXMLBHIT-26APR251415SEASTL-...
    where 26=year(2026), APR=month, 25=day, 1415=14:15 ET.
    """
    m = _TICKER_DT_RE.search(ticker)
    if not m:
        return None
    try:
        year = 2000 + int(m.group(1))
        month = _MONTH_MAP.get(m.group(2))
        if month is None:
            return None
        day = int(m.group(3))
        hhmm = m.group(4)
        hour, minute = int(hhmm[:2]), int(hhmm[2:])
        return datetime(year, month, day, hour, minute, tzinfo=ET)
    except (ValueError, IndexError):
        return None


def main():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        logger.error("DATABASE_URL not set")
        return

    from sqlalchemy import text

    from src.db.client import get_engine

    engine = get_engine()

    # Fetch ALL pending orders (not just stale) — filter in Python
    with engine.connect() as conn:
        all_pending = conn.execute(text("""
            SELECT id, kalshi_order_id, game_date, ticker, sport, player_id,
                   player_name, stat_type, line, side, contracts, total_cost,
                   game_start_time
            FROM kalshi_live_orders
            WHERE status = 'pending'
              AND game_date >= CURRENT_DATE - INTERVAL '3 days'
        """)).fetchall()

    if not all_pending:
        logger.info("No pending orders found.")
        return

    now = datetime.now(ET)
    stale_rows = []

    for row in all_pending:
        game_start = row[12]  # game_start_time from DB
        ticker = row[3]
        game_date = row[2]

        # Method 1: DB game_start_time is set and in the past
        if game_start is not None:
            # Ensure timezone-aware comparison
            if game_start.tzinfo is None:
                game_start = game_start.replace(tzinfo=ET)
            if game_start <= now:
                stale_rows.append(row)
                continue

        # Method 2: Parse game time from ticker (fallback for NULL game_start_time)
        parsed_time = _parse_game_time_from_ticker(ticker)
        if parsed_time is not None and parsed_time <= now:
            logger.info(
                f"Detected stale via ticker parse: {row[6] or ticker} "
                f"(game started {parsed_time.strftime('%I:%M %p ET')})"
            )
            stale_rows.append(row)
            continue

        # Method 3: Yesterday's leftovers (game_date < today)
        if game_date is not None and game_date < now.date():
            stale_rows.append(row)
            continue

    if not stale_rows:
        logger.info(f"No stale orders among {len(all_pending)} pending orders.")
        return

    logger.info(f"Found {len(stale_rows)} stale pending order(s)")

    with engine.connect() as conn:
        already_queued = conn.execute(text("""
            SELECT kalshi_order_id
            FROM kalshi_cancel_queue
            WHERE status != 'rejected'
        """)).fetchall()
    queued_ids = {row[0] for row in already_queued}

    new_orders = [r for r in stale_rows if r[1] not in queued_ids]
    if not new_orders:
        logger.info("All stale orders already in cancel queue.")
        return

    for row in new_orders:
        try:
            with engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO kalshi_cancel_queue
                        (kalshi_order_id, game_date, ticker, sport, player_id,
                         player_name, stat_type, line, side, contracts,
                         expected_cost, game_start_time, status)
                    VALUES
                        (:kalshi_order_id, :game_date, :ticker, :sport, :player_id,
                         :player_name, :stat_type, :line, :side, :contracts,
                         :expected_cost, :game_start_time, 'pending_review')
                    ON CONFLICT (kalshi_order_id) DO NOTHING
                """), {
                    "kalshi_order_id": row[1],
                    "game_date": str(row[2]),
                    "ticker": row[3],
                    "sport": row[4],
                    "player_id": row[5],
                    "player_name": row[6],
                    "stat_type": row[7],
                    "line": float(row[8]) if row[8] is not None else None,
                    "side": row[9],
                    "contracts": row[10],
                    "expected_cost": float(row[11]) if row[11] is not None else None,
                    "game_start_time": row[12],
                })
        except Exception as e:
            logger.error(f"Failed to enqueue order {row[1]}: {e}")

    logger.info(f"Enqueued {len(new_orders)} stale orders for cancellation review.")

    try:
        from src.discord_bot.alerts import send_kalshi_trade_alert_sync

        channel = os.getenv("DISCORD_CHANNEL_KALSHI") or os.getenv("DISCORD_CHANNEL_PREDICTIONS")
        if channel:
            lines = []
            for row in new_orders:
                cost_str = f"${float(row[11]):.2f}" if row[11] is not None else "?"
                lines.append(
                    f"  {row[6] or row[3]} | {row[7] or ''} "
                    f"{row[9] or ''} | {row[10] or '?'} contracts | {cost_str}"
                )

            msg = "\n".join(lines)
            send_kalshi_trade_alert_sync("circuit_breaker", {
                "reason": f"{len(new_orders)} stale Kalshi order(s) need review — game has started.\n"
                          f"Approve cancellation on the dashboard (Bot Tracker -> Stale Orders).\n\n{msg}",
                "action": "Review stale orders",
                "balance": 0,
            }, channel_id=channel)
    except Exception as e:
        logger.warning(f"Discord alert failed (non-fatal): {e}")


if __name__ == "__main__":
    main()
