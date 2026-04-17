"""
Polymarket Market Pattern Sampler
===================================
One-time local script to query polymarket_markets for ~1000 diverse markets
across all categories and export a pattern catalog for analysis.

Run this locally after the Polymarket scraper has populated data to understand:
  - Question text patterns per (category, market_type)
  - Event slug structure per sport
  - Which fields are reliably populated vs NULL
  - Price distribution and liquidity ranges

Usage:
    python scripts/sample_polymarket_markets.py
    python scripts/sample_polymarket_markets.py --output patterns.json --limit 2000
    python scripts/sample_polymarket_markets.py --category sports --market-type moneyline
"""

import argparse
import json
import logging
import sys
from collections import defaultdict
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from sqlalchemy import text

from src.db.client import get_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("PolymarketSampler")


# ---------------------------------------------------------------------------
# Query
# ---------------------------------------------------------------------------


SAMPLE_QUERY = text("""
    WITH ranked AS (
        SELECT
            condition_id, question, event_slug, category, market_type,
            sport, team1, team2, yes_price, no_price, volume, liquidity,
            player_name, stat_type, line,
            -- Rank within each (category, market_type) bucket for diversity
            ROW_NUMBER() OVER (
                PARTITION BY category, market_type
                ORDER BY volume DESC NULLS LAST
            ) AS rn
        FROM polymarket_markets
        WHERE yes_price IS NOT NULL
          AND market_status = 'open'
          AND (:category IS NULL OR category = :category)
          AND (:market_type IS NULL OR market_type = :market_type)
    )
    SELECT *
    FROM ranked
    WHERE rn <= :per_bucket
    ORDER BY category, market_type, volume DESC NULLS LAST
    LIMIT :limit_rows
""")


def sample_markets(
    engine,
    limit: int = 1000,
    per_bucket: int = 150,
    category: str | None = None,
    market_type: str | None = None,
) -> list[dict]:
    """Query a diverse sample of Polymarket markets from the DB.

    Args:
        engine: SQLAlchemy engine.
        limit: Total row limit.
        per_bucket: Max rows per (category, market_type) bucket.
        category: Optional category filter.
        market_type: Optional market_type filter.

    Returns:
        List of row dicts.
    """
    with engine.connect() as conn:
        rows = conn.execute(SAMPLE_QUERY, {
            "limit_rows": limit,
            "per_bucket": per_bucket,
            "category": category,
            "market_type": market_type,
        }).fetchall()
    return [dict(row._mapping) for row in rows]


# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------


def analyze_patterns(markets: list[dict]) -> dict:
    """Build a pattern catalog from the sampled markets.

    Returns a nested dict with statistics per (category, market_type).
    """
    # Group by (category, market_type)
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for m in markets:
        key = (m.get("category") or "unknown", m.get("market_type") or "unknown")
        groups[key].append(m)

    catalog: dict[str, dict] = {}

    for (cat, mtype), rows in sorted(groups.items()):
        group_key = f"{cat} / {mtype}"

        # Field population rates
        fields = ["team1", "team2", "player_name", "stat_type", "line",
                  "yes_price", "no_price", "volume", "liquidity"]
        pop_rates = {}
        for f in fields:
            non_null = sum(1 for r in rows if r.get(f) is not None)
            pop_rates[f] = round(non_null / len(rows), 3) if rows else 0

        # Slug patterns
        slug_examples = sorted({
            (r.get("event_slug") or "")[:60] for r in rows if r.get("event_slug")
        })[:5]

        # Question text examples
        q_examples = [r.get("question", "")[:100] for r in rows[:5]]

        # Price stats
        prices = [r.get("yes_price") for r in rows if r.get("yes_price") is not None]
        price_stats = {}
        if prices:
            price_stats = {
                "min": round(min(prices), 1),
                "max": round(max(prices), 1),
                "mean": round(sum(prices) / len(prices), 1),
            }

        # Volume stats
        vols = [r.get("volume") or 0 for r in rows if r.get("volume") is not None]
        vol_stats = {}
        if vols:
            vol_stats = {
                "min": round(min(vols), 0),
                "max": round(max(vols), 0),
                "mean": round(sum(vols) / len(vols), 0),
            }

        # Sport distribution
        sport_dist: dict[str, int] = defaultdict(int)
        for r in rows:
            sport_dist[r.get("sport") or "null"] += 1

        catalog[group_key] = {
            "count": len(rows),
            "field_population": pop_rates,
            "slug_examples": slug_examples,
            "question_examples": q_examples,
            "price_stats": price_stats,
            "volume_stats": vol_stats,
            "sport_distribution": dict(sport_dist),
        }

    return catalog


# ---------------------------------------------------------------------------
# Printing
# ---------------------------------------------------------------------------


def print_catalog(catalog: dict, markets: list[dict]) -> None:
    """Print pattern catalog to stdout."""
    print("\n" + "=" * 100)
    print("POLYMARKET PATTERN CATALOG")
    print("=" * 100)
    print(f"Total markets sampled: {len(markets)}")
    print()

    for group_key, info in catalog.items():
        print(f"\n{'─' * 80}")
        print(f"  {group_key.upper()}  ({info['count']} markets)")
        print(f"{'─' * 80}")

        # Field population
        pop = info["field_population"]
        pop_str = " | ".join(f"{f}: {v:.0%}" for f, v in pop.items())
        print(f"  Fields:   {pop_str}")

        # Prices / volume
        if info["price_stats"]:
            ps = info["price_stats"]
            print(f"  YES price: min={ps['min']} max={ps['max']} mean={ps['mean']}")
        if info["volume_stats"]:
            vs = info["volume_stats"]
            print(f"  Volume:   min={vs['min']:,.0f} max={vs['max']:,.0f} mean={vs['mean']:,.0f}")

        # Sport distribution
        if info["sport_distribution"]:
            sp_str = " | ".join(f"{k}: {v}" for k, v in sorted(info["sport_distribution"].items()))
            print(f"  Sports:   {sp_str}")

        # Slug examples
        if info["slug_examples"]:
            print("  Slugs:")
            for s in info["slug_examples"][:3]:
                print(f"    - {s}")

        # Question examples
        if info["question_examples"]:
            print("  Questions:")
            for q in info["question_examples"][:3]:
                print(f"    - {q}")

    # NULL analysis
    print("\n" + "=" * 100)
    print("FIELD NULL ANALYSIS (all markets)")
    print("=" * 100)
    fields = ["team1", "team2", "player_name", "stat_type", "line", "yes_price", "volume"]
    total = len(markets)
    for f in fields:
        non_null = sum(1 for m in markets if m.get(f) is not None)
        print(f"  {f:<20}: {non_null:>5}/{total} populated ({non_null/total:.1%})")

    # Slug structure analysis
    print("\n" + "=" * 100)
    print("SLUG STRUCTURE PATTERNS")
    print("=" * 100)
    slug_patterns: dict[str, int] = defaultdict(int)
    for m in markets:
        slug = m.get("event_slug") or ""
        if not slug:
            slug_patterns["(empty)"] += 1
            continue
        parts = slug.split("-")
        # Pattern: first part structure
        if len(parts) >= 4:
            # Check if it matches sport-team1-team2-date pattern
            if parts[0] in ("mlb", "nba", "nfl", "nhl", "ncaab"):
                if len(parts[1]) <= 4 and len(parts[2]) <= 4:
                    slug_patterns[f"{parts[0]}-{{team1}}-{{team2}}-{{date}}"] += 1
                else:
                    slug_patterns[f"{parts[0]}-other"] += 1
            else:
                slug_patterns[f"other-{parts[0]}"] += 1
        elif len(parts) >= 2:
            slug_patterns[f"{parts[0]}-other"] += 1
        else:
            slug_patterns["single-part"] += 1

    for pattern, count in sorted(slug_patterns.items(), key=lambda x: -x[1])[:15]:
        print(f"  {pattern:<40}: {count:>5}")

    print("\n" + "=" * 100)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sample and analyze Polymarket markets from the DB",
    )
    parser.add_argument("--limit", type=int, default=1000, help="Total row limit (default: 1000)")
    parser.add_argument("--per-bucket", type=int, default=150,
                        help="Max rows per (category, market_type) bucket (default: 150)")
    parser.add_argument("--category", type=str, default=None,
                        help="Filter to specific category (optional)")
    parser.add_argument("--market-type", type=str, default=None,
                        help="Filter to specific market_type (optional)")
    parser.add_argument("--output", type=str, default=None,
                        help="Save catalog + samples to JSON file (optional)")
    return parser.parse_args()


def main():
    args = parse_args()

    logger.info("Connecting to DB...")
    engine = get_engine()

    logger.info(f"Sampling up to {args.limit} markets (≤{args.per_bucket}/bucket)...")
    markets = sample_markets(
        engine,
        limit=args.limit,
        per_bucket=args.per_bucket,
        category=args.category,
        market_type=args.market_type,
    )
    logger.info(f"Got {len(markets)} markets")

    if not markets:
        logger.warning("No markets found — make sure polymarket_markets table is populated")
        return

    catalog = analyze_patterns(markets)
    print_catalog(catalog, markets)

    if args.output:
        out_path = Path(args.output)
        with open(out_path, "w") as f:
            json.dump({
                "total": len(markets),
                "catalog": catalog,
                "samples": markets[:200],  # limit raw samples in output
            }, f, indent=2, default=str)
        logger.info(f"Saved catalog to {out_path}")


if __name__ == "__main__":
    main()
