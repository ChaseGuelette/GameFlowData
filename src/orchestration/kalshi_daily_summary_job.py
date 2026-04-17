#!/usr/bin/env python3
"""
Kalshi Daily Summary Job
========================
Once-per-day job (8:00 AM ET) that:
1. Resolves any remaining pending Kalshi paper bets (catchup)
2. Queries yesterday's bankroll/P&L from kalshi_paper_trading_daily_log
3. Computes 14-day rolling analysis from kalshi_paper_bets
4. Sends P&L summary + analysis embeds to Discord #performance channel

Usage:
    python src/orchestration/kalshi_daily_summary_job.py
    python src/orchestration/kalshi_daily_summary_job.py --dry-run
"""

import argparse
import logging
import sys
import time
from datetime import date, timedelta
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from dotenv import load_dotenv

load_dotenv()

LOG_DIR = Path(__file__).resolve().parents[2] / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "kalshi_daily_summary.log"),
    ],
)
logger = logging.getLogger("KalshiDailySummaryJob")


def _resolve_pending(dry_run: bool = False) -> dict:
    """Resolve any remaining pending Kalshi bets."""
    logger.info(f"{'[DRY RUN] ' if dry_run else ''}STARTING: Resolving pending Kalshi bets")

    empty_result = {
        "dates_processed": 0, "dates_skipped": 0,
        "total_resolved": 0, "total_won": 0, "total_lost": 0,
        "total_cancelled": 0, "by_date": {},
    }

    if dry_run:
        logger.info("  Would call: KalshiPaperTrader().resolve_all_pending()")
        return empty_result

    start = time.time()
    try:
        from src.paper_trading.kalshi_paper_trader import KalshiPaperTrader

        trader = KalshiPaperTrader()
        result = trader.resolve_all_pending()

        elapsed = time.time() - start
        logger.info(
            f"COMPLETED: Kalshi resolution — {result['total_resolved']} bets "
            f"({result['total_won']}W {result['total_lost']}L) ({elapsed:.1f}s)"
        )
        return result

    except Exception as e:
        elapsed = time.time() - start
        logger.warning(f"FAILED: Kalshi resolution ({elapsed:.1f}s) — {e}")
        return empty_result


def _get_yesterday_daily_log() -> dict:
    """Fetch yesterday's bankroll/P&L/record from kalshi_paper_trading_daily_log.

    Returns a dict with keys: bankroll, daily_pnl, total_pnl, bets_won, bets_lost, total_bets.
    Falls back to current bankroll on failure.
    """
    yesterday = date.today() - timedelta(days=1)

    try:
        from sqlalchemy import text

        from src.db.client import get_engine

        engine = get_engine()

        # Get yesterday's row (sum across sports in case there are multiple rows)
        with engine.connect() as conn:
            row = conn.execute(
                text("""
                    SELECT
                        SUM(total_pnl)       AS daily_pnl,
                        MAX(cumulative_pnl)  AS total_pnl,
                        MAX(bankroll_after)  AS bankroll,
                        SUM(bets_won)        AS bets_won,
                        SUM(bets_lost)       AS bets_lost,
                        SUM(total_bets)      AS total_bets
                    FROM kalshi_paper_trading_daily_log
                    WHERE game_date = :game_date
                """),
                {"game_date": yesterday},
            ).fetchone()

        if row and row[2] is not None:  # bankroll_after not null → row exists
            log = {
                "daily_pnl": float(row[0] or 0),
                "total_pnl": float(row[1] or 0),
                "bankroll": float(row[2]),
                "bets_won": int(row[3] or 0),
                "bets_lost": int(row[4] or 0),
                "total_bets": int(row[5] or 0),
            }
            # Augment with overflow stats from kalshi_paper_bets
            log.update(_get_overflow_stats(engine, yesterday))
            return log

        # No entry for yesterday — use latest available (P&L only, no bet record)
        with engine.connect() as conn:
            row = conn.execute(
                text("""
                    SELECT cumulative_pnl, bankroll_after
                    FROM kalshi_paper_trading_daily_log
                    ORDER BY game_date DESC
                    LIMIT 1
                """),
            ).fetchone()

        if row:
            logger.info("No daily log entry for yesterday; using most recent entry")
            return {
                "daily_pnl": 0.0,
                "total_pnl": float(row[0] or 0),
                "bankroll": float(row[1]),
                "bets_won": 0,
                "bets_lost": 0,
                "total_bets": 0,
                "overflow_bets": 0,
                "overflow_pnl": 0.0,
            }

    except Exception as e:
        logger.warning(f"Failed to query kalshi_paper_trading_daily_log: {e}")

    # Fallback: try to get live bankroll from trader
    try:
        from src.paper_trading.kalshi_paper_trader import KalshiPaperTrader

        bankroll = KalshiPaperTrader().get_bankroll()
        return {"daily_pnl": 0.0, "total_pnl": 0.0, "bankroll": bankroll,
                "bets_won": 0, "bets_lost": 0, "total_bets": 0,
                "overflow_bets": 0, "overflow_pnl": 0.0}
    except Exception:
        pass

    return {"daily_pnl": 0.0, "total_pnl": 0.0, "bankroll": 100.0,
            "bets_won": 0, "bets_lost": 0, "total_bets": 0,
            "overflow_bets": 0, "overflow_pnl": 0.0}


def _get_overflow_stats(engine, game_date) -> dict:
    """Query overflow bet stats for a given game_date from kalshi_paper_bets."""
    try:
        from sqlalchemy import text

        with engine.connect() as conn:
            row = conn.execute(
                text("""
                    SELECT
                        COUNT(*) FILTER (WHERE status IN ('overflow_won', 'overflow_lost'))
                            AS overflow_bets,
                        COUNT(*) FILTER (WHERE status = 'overflow_won')
                            AS overflow_won,
                        COUNT(*) FILTER (WHERE status = 'overflow_lost')
                            AS overflow_lost,
                        COALESCE(SUM(pnl) FILTER (WHERE status IN ('overflow_won', 'overflow_lost')), 0)
                            AS overflow_pnl,
                        COALESCE(SUM(
                            CASE
                                WHEN status IN ('overflow_won', 'overflow_lost')
                                THEN contracts * (CASE WHEN side = 'no' THEN (100 - price::int) ELSE price::int END)::float / 100.0
                                ELSE 0
                            END
                        ), 0) AS overflow_cost
                    FROM kalshi_paper_bets
                    WHERE game_date = :game_date
                      AND status LIKE 'overflow%'
                """),
                {"game_date": game_date},
            ).fetchone()
        if row:
            return {
                "overflow_bets": int(row[0] or 0),
                "overflow_won":  int(row[1] or 0),
                "overflow_lost": int(row[2] or 0),
                "overflow_pnl":  float(row[3] or 0),
                "overflow_cost": float(row[4] or 0),
            }
    except Exception as e:
        logger.warning(f"Failed to query overflow stats: {e}")
    return {"overflow_bets": 0, "overflow_won": 0, "overflow_lost": 0, "overflow_pnl": 0.0, "overflow_cost": 0.0}


def _send_pnl_summary(resolution_result: dict, log_data: dict, dry_run: bool = False) -> None:
    """Send Kalshi P&L summary embed to Discord."""
    logger.info(f"{'[DRY RUN] ' if dry_run else ''}STARTING: Sending Kalshi P&L summary")

    if dry_run:
        logger.info(
            f"  Would send P&L summary: bankroll=${log_data['bankroll']:.2f}, "
            f"daily_pnl=${log_data['daily_pnl']:+.2f}, total_pnl=${log_data['total_pnl']:+.2f}, "
            f"overflow={log_data.get('overflow_bets', 0)} bets "
            f"(${log_data.get('overflow_pnl', 0.0):+.2f} hypothetical)"
        )
        return

    try:
        from src.discord_bot.alerts import send_kalshi_pnl_summary_sync

        sent = send_kalshi_pnl_summary_sync(
            resolution_result=resolution_result,
            bankroll=log_data["bankroll"],
            daily_pnl=log_data["daily_pnl"],
            total_pnl=log_data["total_pnl"],
            log_data=log_data,
        )
        if sent:
            logger.info("COMPLETED: Sent Kalshi P&L summary to Discord")
        else:
            logger.warning("COMPLETED: Kalshi P&L summary send returned False (check Discord config)")
    except Exception as e:
        logger.warning(f"FAILED: Kalshi P&L summary — {e}")


def _send_analysis(dry_run: bool = False) -> None:
    """Compute 14-day Kalshi analysis and send embed to Discord."""
    logger.info(f"{'[DRY RUN] ' if dry_run else ''}STARTING: Kalshi analysis (14d)")

    try:
        from src.paper_trading.kalshi_analysis import compute_kalshi_analysis

        metrics = compute_kalshi_analysis(lookback_days=14)

        if metrics is None:
            logger.info("COMPLETED: Kalshi analysis — insufficient data (< 5 resolved NO bets)")
            return

        logger.info(
            f"COMPLETED: Kalshi analysis — {metrics.n_bets} bets, "
            f"win={metrics.win_rate:.1%}, z={metrics.z_score:.1f}σ, "
            f"verdict={metrics.verdict}"
        )

        if dry_run:
            logger.info(
                f"  Would send analysis embed: {metrics.verdict}, "
                f"ROI={metrics.roi:+.1%}, n={metrics.n_bets}"
            )
            return

        from src.discord_bot.alerts import send_kalshi_analysis_alert_sync

        sent = send_kalshi_analysis_alert_sync(metrics)
        if sent:
            logger.info("COMPLETED: Sent Kalshi analysis embed to Discord")
        else:
            logger.warning("COMPLETED: Kalshi analysis send returned False (check Discord config)")

    except Exception as e:
        logger.warning(f"FAILED: Kalshi analysis — {e}")


def main():
    parser = argparse.ArgumentParser(description="Kalshi daily summary: P&L + analysis to Discord")
    parser.add_argument("--dry-run", action="store_true", help="Log what would be sent, skip Discord")
    args = parser.parse_args()

    dry_run = args.dry_run
    prefix = "[DRY RUN] " if dry_run else ""

    logger.info("=" * 60)
    logger.info(f"{prefix}Kalshi Daily Summary Job Starting")
    logger.info("=" * 60)

    # Step 1: Resolve pending bets (catchup)
    resolution_result = _resolve_pending(dry_run=dry_run)

    # Step 2: Get yesterday's bankroll / P&L data
    log_data = _get_yesterday_daily_log()
    logger.info(
        f"Daily log: bankroll=${log_data['bankroll']:.2f}, "
        f"daily_pnl=${log_data['daily_pnl']:+.2f}, "
        f"total_pnl=${log_data['total_pnl']:+.2f}, "
        f"record={log_data['bets_won']}W-{log_data['bets_lost']}L "
        f"({log_data['total_bets']} total bets)"
    )

    # Step 3: Send P&L summary embed
    _send_pnl_summary(resolution_result, log_data, dry_run=dry_run)

    # Step 4: Compute analysis + send embed
    _send_analysis(dry_run=dry_run)

    logger.info("=" * 60)
    logger.info(f"{prefix}Kalshi Daily Summary Job Complete")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
