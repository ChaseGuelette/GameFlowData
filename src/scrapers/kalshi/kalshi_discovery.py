"""
Kalshi API Discovery Script
============================
One-time local script to enumerate ALL available Kalshi series and sample
markets from each. Run this locally to learn what series/market types
Kalshi offers beyond player props, then populate KALSHI_GAME_SERIES in
kalshi_utils.py with the results.

Usage:
    python -m src.scrapers.kalshi.kalshi_discovery
    python -m src.scrapers.kalshi.kalshi_discovery --output discovery_results.json
    python -m src.scrapers.kalshi.kalshi_discovery --status open --limit 5000
"""

import argparse
import json
import logging
import sys
import time
from collections import defaultdict
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[3]))

from src.scrapers.kalshi.kalshi_client import KalshiClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("KalshiDiscovery")

# ---------------------------------------------------------------------------
# Category detection heuristics for Kalshi series tickers
# ---------------------------------------------------------------------------

_SPORT_PREFIXES = {
    "nba": ["KXNBA"],
    "mlb": ["KXMLB"],
    "nfl": ["KXNFL"],
    "nhl": ["KXNHL"],
    "ncaab": ["KXNCAAB", "KXCFB"],
    "soccer": ["KXSOCCER", "KXMLS"],
}

_NON_SPORT_KEYWORDS = [
    "PRES",     # presidential
    "SENATE",   # senate
    "HOUSE",    # house of reps
    "FED",      # federal reserve
    "CPI",      # inflation
    "GDP",      # economics
    "BTC",      # bitcoin
    "ETH",      # ethereum
    "CRYPTO",   # crypto
    "TEMP",     # weather/temperature
    "HURR",     # hurricane
]

_GAME_KEYWORDS = ["NRFI", "YRFI", "SPREAD", "TOTAL", "ML", "MONEYLINE", "WINNER"]
_FUTURE_KEYWORDS = ["CHAMP", "PENNANT", "SERIES", "MVP", "TITLE", "WINNER", "CUP"]


def classify_series(series_ticker: str, sample_title: str = "") -> str:
    """Classify a Kalshi series ticker into a category.

    Returns one of: nba_prop, mlb_prop, nfl_prop, game_outcome, season_future,
    non_sports, or unknown.
    """
    upper = series_ticker.upper()
    title_upper = sample_title.upper()

    # Sport-specific prop series (contain stat keywords)
    for sport, prefixes in _SPORT_PREFIXES.items():
        for prefix in prefixes:
            if upper.startswith(prefix):
                # Check if it's a game outcome or future
                if any(kw in upper for kw in _GAME_KEYWORDS):
                    return f"{sport}_game"
                if any(kw in upper for kw in _FUTURE_KEYWORDS):
                    return f"{sport}_future"
                # Check title for prop indicators
                if any(kw in title_upper for kw in ["POINTS", "REBOUNDS", "ASSISTS",
                                                      "STRIKEOUTS", "HITS", "BASES"]):
                    return f"{sport}_prop"
                return f"{sport}_other"

    # Non-sports
    if any(kw in upper for kw in _NON_SPORT_KEYWORDS):
        return "non_sports"

    # Game outcomes (generic)
    if any(kw in upper for kw in _GAME_KEYWORDS):
        return "game_outcome"

    if any(kw in upper for kw in _FUTURE_KEYWORDS):
        return "season_future"

    return "unknown"


# ---------------------------------------------------------------------------
# Core discovery logic
# ---------------------------------------------------------------------------


def discover_all_series(
    client: KalshiClient,
    status: str = "open",
    max_markets: int = 10000,
    pause_between_pages: float = 0.2,
) -> dict[str, list[dict]]:
    """Enumerate ALL Kalshi series by paginating through all markets.

    Collects unique series_ticker values and samples 5 markets from each.

    Args:
        client: Authenticated KalshiClient.
        status: Market status to query ("open", "closed", "settled").
        max_markets: Stop after fetching this many total markets (safety cap).
        pause_between_pages: Sleep between pages to respect rate limits.

    Returns:
        Dict mapping series_ticker -> list of sample market dicts.
    """
    logger.info(f"Starting discovery (status={status}, max={max_markets})...")

    series_samples: dict[str, list[dict]] = defaultdict(list)
    total_fetched = 0
    cursor = None
    page_num = 0

    while total_fetched < max_markets:
        page_num += 1
        result = client.list_markets(
            status=status,
            limit=200,
            cursor=cursor,
        )
        if result is None:
            logger.error("API call returned None — stopping")
            break

        markets = result.get("markets", [])
        if not markets:
            logger.info("No more markets returned")
            break

        for mkt in markets:
            series = mkt.get("series_ticker") or ""
            if not series:
                continue
            # Keep up to 5 samples per series
            if len(series_samples[series]) < 5:
                series_samples[series].append({
                    "ticker": mkt.get("ticker", ""),
                    "title": mkt.get("title", ""),
                    "event_ticker": mkt.get("event_ticker", ""),
                    "volume": mkt.get("volume_fp") or mkt.get("volume") or 0,
                    "open_interest": mkt.get("open_interest_fp") or mkt.get("open_interest") or 0,
                    "yes_bid": mkt.get("yes_bid_dollars") or 0,
                    "yes_ask": mkt.get("yes_ask_dollars") or 0,
                    "status": mkt.get("status", ""),
                    "close_time": mkt.get("close_time") or "",
                })

        total_fetched += len(markets)
        cursor = result.get("cursor")

        if page_num % 5 == 0:
            logger.info(
                f"  Page {page_num}: fetched {total_fetched} total, "
                f"{len(series_samples)} unique series so far..."
            )

        if not cursor:
            logger.info("No next cursor — pagination complete")
            break

        time.sleep(pause_between_pages)

    logger.info(
        f"Discovery complete: {total_fetched} markets fetched, "
        f"{len(series_samples)} unique series found"
    )
    return dict(series_samples)


def build_report(series_samples: dict[str, list[dict]]) -> list[dict]:
    """Build a structured report from series samples.

    Returns list of dicts sorted by volume (desc), one row per series.
    """
    rows = []
    for series_ticker, samples in series_samples.items():
        if not samples:
            continue

        # Volume stats
        volumes = [s.get("volume") or 0 for s in samples]
        total_vol = sum(volumes)
        max_vol = max(volumes)

        # Sample title
        sample_title = samples[0].get("title", "")

        # Classify
        category = classify_series(series_ticker, sample_title)

        rows.append({
            "series_ticker": series_ticker,
            "category": category,
            "n_samples": len(samples),
            "total_vol_samples": total_vol,
            "max_vol_sample": max_vol,
            "sample_title": sample_title[:80],
            "sample_ticker": samples[0].get("ticker", ""),
            "samples": samples,
        })

    # Sort by total volume desc
    rows.sort(key=lambda r: r["total_vol_samples"], reverse=True)
    return rows


def print_report(report: list[dict]) -> None:
    """Print the discovery report as a formatted table."""
    print("\n" + "=" * 100)
    print("KALSHI DISCOVERY REPORT")
    print("=" * 100)
    print(f"{'Series Ticker':<22} {'Category':<18} {'#Smpl':>6} {'MaxVol':>10}  Sample Title")
    print("-" * 100)

    # Group by category
    categories: dict[str, list[dict]] = defaultdict(list)
    for row in report:
        categories[row["category"]].append(row)

    for cat in sorted(categories.keys()):
        cat_rows = categories[cat]
        print(f"\n  [{cat.upper()}] ({len(cat_rows)} series)")
        for row in cat_rows:
            print(
                f"    {row['series_ticker']:<20} {row['category']:<18} "
                f"{row['n_samples']:>4}  {row['max_vol_sample']:>8}  "
                f"{row['sample_title'][:60]}"
            )

    print("\n" + "=" * 100)
    print(f"TOTAL: {len(report)} unique series")

    # Summary by category
    print("\nSUMMARY BY CATEGORY:")
    for cat in sorted(categories.keys()):
        tickers = [r["series_ticker"] for r in categories[cat]]
        print(f"  {cat:<20} {len(tickers):>4} series: {', '.join(tickers[:8])}")
        if len(tickers) > 8:
            print(f"  {'':20}        ... and {len(tickers) - 8} more")

    # KALSHI_GAME_SERIES suggestion
    game_series = [r for r in report if any(
        kw in r["category"] for kw in ["game", "future"]
    )]
    if game_series:
        print("\n" + "=" * 100)
        print("SUGGESTED KALSHI_GAME_SERIES ENTRIES (add to kalshi_utils.py):")
        print("KALSHI_GAME_SERIES: dict[str, dict[str, str]] = {")
        for row in game_series[:20]:
            # Guess market_type from category
            cat = row["category"]
            if "game" in cat:
                mtype = "moneyline"
                if "NRFI" in row["series_ticker"]:
                    mtype = "nrfi"
                elif "TOTAL" in row["series_ticker"]:
                    mtype = "total"
                elif "SPREAD" in row["series_ticker"]:
                    mtype = "spread"
            else:
                mtype = "season_future"
            sport = cat.split("_")[0] if "_" in cat else "other"
            print(f"    \"{row['series_ticker']}\": {{\"market_type\": \"{mtype}\", \"sport\": \"{sport}\"}},")
        print("}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Discover all Kalshi series and sample markets from each",
    )
    parser.add_argument(
        "--status", default="open", choices=["open", "closed", "settled"],
        help="Market status to query (default: open)",
    )
    parser.add_argument(
        "--max-markets", type=int, default=10000,
        help="Max total markets to fetch (default: 10000)",
    )
    parser.add_argument(
        "--output", type=str, default=None,
        help="Optional: save full report to this JSON file",
    )
    parser.add_argument(
        "--pause", type=float, default=0.2,
        help="Pause between pagination requests in seconds (default: 0.2)",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    client = KalshiClient()
    if not client.is_authenticated:
        logger.error(
            "Kalshi credentials not configured. "
            "Set KALSHI_API_KEY and KALSHI_PRIVATE_KEY_PATH (or KALSHI_PRIVATE_KEY_B64)."
        )
        sys.exit(1)

    series_samples = discover_all_series(
        client,
        status=args.status,
        max_markets=args.max_markets,
        pause_between_pages=args.pause,
    )

    report = build_report(series_samples)
    print_report(report)

    if args.output:
        out_path = Path(args.output)
        with open(out_path, "w") as f:
            # Strip 'samples' key for compact output, keep summary
            compact = [{k: v for k, v in row.items() if k != "samples"} for row in report]
            json.dump({"series": compact, "raw": series_samples}, f, indent=2, default=str)
        logger.info(f"Saved full report to {out_path}")


if __name__ == "__main__":
    main()
