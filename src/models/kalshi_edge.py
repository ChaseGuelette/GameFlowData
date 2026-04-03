"""
Kalshi Edge Calculator
======================
Computes model edges against Kalshi prediction market prices.

Uses stored MC samples (empirical CDF — always) to derive model probabilities,
then compares to Kalshi implied prices with fee adjustments.

Also cross-references sportsbook consensus lines for comparison.

Usage:
    python -m src.models.kalshi_edge --date 2026-03-31 --sport nba
    python -m src.models.kalshi_edge --mock --date 2026-03-31
"""

import argparse
import gzip
import logging
import sys
from datetime import date
from pathlib import Path

import numpy as np
from sqlalchemy import text

sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.db.client import get_engine
from src.models.black_litterman import BLConfig, BlackLittermanBlender
from src.scrapers.kalshi.kalshi_utils import (
    COMBINED_STATS,
    fee_adjusted_edge,
    kalshi_mid_to_prob,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("KalshiEdgeCalculator")


class KalshiEdgeCalculator:
    """Computes model edges for Kalshi markets using MC samples."""

    def __init__(self, engine=None):
        self.engine = engine or get_engine()

    def _load_latest_markets(self, target_date: date, sport: str) -> list[dict]:
        """Load latest Kalshi market snapshots for a date.

        Returns the most recent snapshot per ticker.
        """
        query = text("""
            SELECT DISTINCT ON (ticker)
                id, ticker, player_name, stat_type, line, player_id,
                yes_price, no_price, yes_bid, yes_ask, bid_ask_spread,
                volume, open_interest, close_time, market_status, snapshot_time
            FROM kalshi_markets
            WHERE sport = :sport
              AND snapshot_time::date = :target_date
              AND market_status = 'open'
            ORDER BY ticker, snapshot_time DESC
        """)

        with self.engine.connect() as conn:
            rows = conn.execute(query, {"sport": sport, "target_date": target_date}).fetchall()

        markets = []
        for row in rows:
            markets.append({
                "id": row[0],
                "ticker": row[1],
                "player_name": row[2],
                "stat_type": row[3],
                "line": float(row[4]),
                "player_id": row[5],
                "yes_price": row[6],
                "no_price": row[7],
                "yes_bid": row[8],
                "yes_ask": row[9],
                "bid_ask_spread": row[10],
                "volume": row[11],
                "open_interest": row[12],
                "close_time": row[13],
                "market_status": row[14],
                "snapshot_time": row[15],
            })

        logger.info(f"Loaded {len(markets)} open {sport.upper()} markets for {target_date}")
        return markets

    def _load_samples(self, target_date: date, sport: str) -> dict[tuple, np.ndarray]:
        """Load MC samples for a date from the appropriate samples table.

        Returns:
            dict[(player_id, game_id, stat) -> np.ndarray]
        """
        table = "daily_prediction_samples" if sport == "nba" else "mlb_daily_prediction_samples"
        query = text(f"""
            SELECT player_id, game_id, stat, n_samples, samples_gz
            FROM {table}
            WHERE prediction_date = :prediction_date
        """)

        samples_dict: dict[tuple, np.ndarray] = {}

        with self.engine.connect() as conn:
            rows = conn.execute(query, {"prediction_date": target_date}).fetchall()

        for row in rows:
            player_id, game_id, stat, n_samples, blob = row
            if isinstance(blob, memoryview):
                blob = bytes(blob)
            samples = np.frombuffer(gzip.decompress(blob), dtype=np.float64, count=n_samples)
            samples_dict[(int(player_id), str(game_id), stat)] = samples

        logger.info(f"Loaded {len(samples_dict)} sample arrays for {target_date}")
        return samples_dict

    def _find_sportsbook_consensus(self, player_id: int, stat_type: str, target_date: date, sport: str) -> float | None:
        """Find consensus sportsbook line for a player/stat from props table."""
        if sport == "nba":
            query = text("""
                SELECT prop_line
                FROM daily_predictions
                WHERE player_id = :pid
                  AND stat = :stat
                  AND prediction_date = :target_date
                  AND prop_line IS NOT NULL
                LIMIT 1
            """)
        else:
            query = text("""
                SELECT line
                FROM mlb_daily_predictions
                WHERE player_id = :pid
                  AND stat = :stat
                  AND prediction_date = :target_date
                  AND line IS NOT NULL
                LIMIT 1
            """)

        try:
            with self.engine.connect() as conn:
                row = conn.execute(query, {
                    "pid": player_id, "stat": stat_type, "target_date": target_date,
                }).fetchone()
            return float(row[0]) if row else None
        except Exception:
            return None

    def _find_sportsbook_odds(
        self, player_id: int, stat_type: str, target_date: date, sport: str
    ) -> tuple[float, float, float] | None:
        """Find sportsbook over/under odds for BL market prior.

        Returns (over_odds, under_odds, sportsbook_line) or None.
        """
        if sport == "nba":
            query = text("""
                SELECT over_odds, under_odds, prop_line
                FROM daily_predictions
                WHERE player_id = :pid
                  AND stat = :stat
                  AND prediction_date = :target_date
                  AND over_odds IS NOT NULL
                  AND under_odds IS NOT NULL
                LIMIT 1
            """)
        else:
            query = text("""
                SELECT over_odds, under_odds, line
                FROM mlb_daily_predictions
                WHERE player_id = :pid
                  AND stat = :stat
                  AND prediction_date = :target_date
                  AND over_odds IS NOT NULL
                  AND under_odds IS NOT NULL
                LIMIT 1
            """)

        try:
            with self.engine.connect() as conn:
                row = conn.execute(query, {
                    "pid": player_id, "stat": stat_type, "target_date": target_date,
                }).fetchone()
            if row:
                return (float(row[0]), float(row[1]), float(row[2]))
            return None
        except Exception:
            return None

    def compute_edges(self, target_date: date, sport: str = "nba") -> dict:
        """Compute edges for all Kalshi markets on a date.

        Steps:
            1. Load latest kalshi_markets for target_date
            2. Load MC samples
            3. For each matched market: empirical CDF, fee-adjusted edges
            4. Compare to sportsbook consensus
            5. Update kalshi_markets with edge columns

        Returns:
            Summary dict with counts.
        """
        stats = {"markets": 0, "matched": 0, "updated": 0, "no_samples": 0}

        markets = self._load_latest_markets(target_date, sport)
        stats["markets"] = len(markets)

        if not markets:
            logger.info("No open markets found")
            return stats

        samples_dict = self._load_samples(target_date, sport)

        if not samples_dict:
            logger.warning("No MC samples found — edges cannot be computed")
            return stats

        # Build player_id → [(game_id, stat, samples)] lookup
        player_samples: dict[int, list[tuple]] = {}
        for (pid, gid, stat), samples in samples_dict.items():
            player_samples.setdefault(pid, []).append((gid, stat, samples))

        # BL blender — same config as proven NBA Model Picks (tau=0.5, z_max=1.0)
        bl_config = BLConfig(tau=0.5, z_max=1.0)
        blender = BlackLittermanBlender(config=bl_config)

        updates = []

        for market in markets:
            pid = market.get("player_id")
            if not pid or pid not in player_samples:
                stats["no_samples"] += 1
                continue

            # Find matching samples for this player/stat
            market_stat = market["stat_type"]
            matched_samples = None

            if market_stat in COMBINED_STATS:
                # Combined stat (e.g., hits+runs+RBIs): sum component samples
                component_stats = COMBINED_STATS[market_stat]
                component_arrays = []
                for component in component_stats:
                    for gid, stat, samples in player_samples[pid]:
                        if stat == component:
                            component_arrays.append(samples)
                            break
                if len(component_arrays) == len(component_stats):
                    # All components found — sum element-wise
                    # Truncate to shortest array length if they differ
                    min_len = min(len(a) for a in component_arrays)
                    matched_samples = sum(a[:min_len] for a in component_arrays)
            else:
                for gid, stat, samples in player_samples[pid]:
                    if stat == market_stat:
                        matched_samples = samples
                        break

            if matched_samples is None:
                stats["no_samples"] += 1
                continue

            stats["matched"] += 1

            # Empirical CDF
            # Kalshi uses integer lines with "N+" semantics (YES wins if actual >= N)
            # Sportsbooks use half-integer lines (YES wins if actual > N)
            # For integer lines: use >= ; for half-integer lines: use >
            line = market["line"]
            if line == int(line):
                model_prob_over = float((matched_samples >= line).mean())
            else:
                model_prob_over = float((matched_samples > line).mean())

            # Kalshi implied from midpoint (removes spread)
            yes_bid = market.get("yes_bid", 0) or 0
            yes_ask = market.get("yes_ask", 0) or 0
            yes_price = market.get("yes_price", 0) or 0

            if yes_bid > 0 and yes_ask > 0:
                kalshi_implied = kalshi_mid_to_prob(yes_bid, yes_ask)
            else:
                kalshi_implied = yes_price / 100.0 if yes_price else 0.5

            # Raw edge
            raw_edge_val = model_prob_over - kalshi_implied

            # Fee-adjusted edges (YES side — over)
            maker_edge_yes = fee_adjusted_edge(model_prob_over, yes_price, is_yes=True, is_maker=True)
            taker_edge_yes = fee_adjusted_edge(model_prob_over, yes_price, is_yes=True, is_maker=False)

            # Also check NO side (under) — API-only advantage
            maker_edge_no = fee_adjusted_edge(model_prob_over, yes_price, is_yes=False, is_maker=True)
            taker_edge_no = fee_adjusted_edge(model_prob_over, yes_price, is_yes=False, is_maker=False)

            # Use whichever side has better edge
            if maker_edge_no > maker_edge_yes:
                best_maker_edge = maker_edge_no
                best_taker_edge = taker_edge_no
            else:
                best_maker_edge = maker_edge_yes
                best_taker_edge = taker_edge_yes

            # Sportsbook comparison
            sb_line = self._find_sportsbook_consensus(pid, market["stat_type"], target_date, sport)
            line_diff = (line - sb_line) if sb_line is not None else None

            # --- Black-Litterman blending ---
            # 1. Get market prior (sportsbook devigged, or Kalshi fallback)
            sb_odds = self._find_sportsbook_odds(pid, market_stat, target_date, sport)
            if sb_odds:
                over_odds, under_odds, _sb_line = sb_odds
                market_over, _market_under = blender.devig(over_odds, under_odds)
            else:
                market_over = kalshi_implied

            # 2. model_prob_over already computed with correct >= logic above

            # 3. Confidence from MC samples
            bl_confidence = blender.compute_confidence(matched_samples, line)

            # 4. Blend in log-odds space
            bl_prob_over = blender.blend(model_prob_over, market_over, bl_confidence)

            # 5. BL fee-adjusted edge (best side, taker)
            bl_taker_edge_yes = fee_adjusted_edge(bl_prob_over, yes_price, is_yes=True, is_maker=False)
            bl_taker_edge_no = fee_adjusted_edge(bl_prob_over, yes_price, is_yes=False, is_maker=False)
            bl_best_edge = max(bl_taker_edge_yes, bl_taker_edge_no)

            updates.append({
                "id": market["id"],
                "model_prob": model_prob_over,
                "kalshi_implied": kalshi_implied,
                "raw_edge": raw_edge_val,
                "maker_fee_adjusted_edge": best_maker_edge,
                "taker_fee_adjusted_edge": best_taker_edge,
                "sportsbook_consensus_line": sb_line,
                "line_vs_sportsbook": line_diff,
                "bl_model_prob": round(bl_prob_over, 6),
                "bl_edge": round(bl_best_edge, 6),
                "bl_confidence": round(bl_confidence, 6),
            })

        # Batch update
        if updates:
            update_stmt = text("""
                UPDATE kalshi_markets SET
                    model_prob = :model_prob,
                    kalshi_implied = :kalshi_implied,
                    raw_edge = :raw_edge,
                    maker_fee_adjusted_edge = :maker_fee_adjusted_edge,
                    taker_fee_adjusted_edge = :taker_fee_adjusted_edge,
                    sportsbook_consensus_line = :sportsbook_consensus_line,
                    line_vs_sportsbook = :line_vs_sportsbook,
                    bl_model_prob = :bl_model_prob,
                    bl_edge = :bl_edge,
                    bl_confidence = :bl_confidence
                WHERE id = :id
            """)

            with self.engine.begin() as conn:
                for u in updates:
                    conn.execute(update_stmt, u)

            stats["updated"] = len(updates)

        logger.info(
            f"Edge computation: {stats['markets']} markets, "
            f"{stats['matched']} matched, {stats['updated']} updated, "
            f"{stats['no_samples']} no samples"
        )
        return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute Kalshi edges from MC samples")
    parser.add_argument("--date", type=str, default=date.today().isoformat(), help="Target date (YYYY-MM-DD)")
    parser.add_argument("--sport", type=str, default="nba", choices=["nba", "mlb"])
    parser.add_argument("--mock", action="store_true", help="Run with mock market data first")
    return parser.parse_args()


def main():
    args = parse_args()
    target_date = date.fromisoformat(args.date)

    logger.info("=" * 60)
    logger.info("Kalshi Edge Calculator")
    logger.info(f"  Date: {target_date}")
    logger.info(f"  Sport: {args.sport.upper()}")
    logger.info("=" * 60)

    if args.mock:
        # Insert mock markets first, then compute edges
        from src.scrapers.kalshi.kalshi_market_scraper import scrape_and_store
        logger.info("Inserting mock markets...")
        scrape_and_store(sport=args.sport, mock=True)

    calc = KalshiEdgeCalculator()
    stats = calc.compute_edges(target_date, sport=args.sport)

    logger.info("=" * 60)
    logger.info("EDGE COMPUTATION COMPLETE")
    for k, v in stats.items():
        logger.info(f"  {k}: {v}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
