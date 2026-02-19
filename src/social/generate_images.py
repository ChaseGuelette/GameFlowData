#!/usr/bin/env python
"""CLI tool to generate social media pick images from daily predictions.

Usage:
    # Daily slate (main use case)
    python src/social/generate_images.py --date 2026-02-18 --type picks

    # Results recap
    python src/social/generate_images.py --date 2026-02-17 --type results

    # Both picks + yesterday's results
    python src/social/generate_images.py --date 2026-02-18 --type both

    # Story format for IG stories / TikTok
    python src/social/generate_images.py --date 2026-02-18 --type picks --format story

    # Also generate individual pick cards
    python src/social/generate_images.py --date 2026-02-18 --type picks --individual

    # Dry run (no DB, no image output)
    python src/social/generate_images.py --date 2026-02-18 --type picks --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).resolve().parents[2] / "output" / "social"


def parse_date(date_str: str) -> date:
    """Parse YYYY-MM-DD date string."""
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate social media pick images from daily predictions"
    )
    parser.add_argument(
        "--date",
        required=True,
        type=parse_date,
        help="Primary date (YYYY-MM-DD). For 'both', picks come from this date.",
    )
    parser.add_argument(
        "--type",
        choices=["picks", "results", "both"],
        default="picks",
        help="What to generate: picks, results, or both (default: picks)",
    )
    parser.add_argument(
        "--format",
        choices=["square", "story"],
        default="square",
        help="Image format: square (1080x1080) or story (1080x1920)",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=5,
        help="Number of top picks to include (default: 5)",
    )
    parser.add_argument(
        "--min-edge",
        type=float,
        default=0.05,
        help="Minimum edge threshold (default: 0.05)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help=f"Output directory (default: {OUTPUT_DIR})",
    )
    parser.add_argument(
        "--individual",
        action="store_true",
        help="Also generate individual pick cards",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be generated without making DB calls or saving images",
    )
    args = parser.parse_args()

    out_dir = args.output_dir or OUTPUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    generate_picks = args.type in ("picks", "both")
    generate_results = args.type in ("results", "both")

    # For 'both', results come from yesterday
    picks_date = args.date
    results_date = args.date - timedelta(days=1) if args.type == "both" else args.date

    if args.dry_run:
        print("[DRY RUN] Would generate images:")
        if generate_picks:
            print(f"  - Slate card: top {args.top} picks for {picks_date} ({args.format})")
            if args.individual:
                print(f"  - Individual pick cards for top {args.top} picks")
        if generate_results:
            print(f"  - Results card for {results_date} ({args.format})")
        print(f"  Output dir: {out_dir}")
        return 0

    # Lazy imports so --dry-run works without DB connection
    from src.social.card_renderer import (
        HeadshotCache,
        PickCardRenderer,
        ResultsCardRenderer,
        SlateCardRenderer,
    )
    from src.social.data_provider import (
        get_daily_summary,
        get_performance_stats_sync,
        get_resolved_bets,
        get_top_picks_sync,
    )

    headshots = HeadshotCache()
    saved = []

    # --- Generate picks ---
    if generate_picks:
        logger.info(f"Fetching top {args.top} picks for {picks_date}...")
        picks = get_top_picks_sync(picks_date, n=args.top, min_edge=args.min_edge)
        if not picks:
            logger.warning(f"No picks found for {picks_date}")
        else:
            logger.info(f"Found {len(picks)} picks")

            # Slate card
            slate = SlateCardRenderer(headshots)
            slate_img = slate.render(picks, picks_date, fmt=args.format)
            slate_path = out_dir / f"slate_{picks_date}_{args.format}.png"
            slate_img.save(slate_path, "PNG")
            saved.append(slate_path)
            logger.info(f"Saved slate card: {slate_path}")

            # Individual cards
            if args.individual:
                pick_renderer = PickCardRenderer(headshots)
                for i, pick in enumerate(picks):
                    card_img = pick_renderer.render(pick, picks_date, fmt=args.format)
                    name_slug = pick.get("player_name", "unknown").replace(" ", "_").lower()
                    stat = pick.get("stat", "")
                    card_path = out_dir / f"pick_{picks_date}_{name_slug}_{stat}_{args.format}.png"
                    card_img.save(card_path, "PNG")
                    saved.append(card_path)
                    logger.info(f"Saved pick card: {card_path}")

    # --- Generate results ---
    if generate_results:
        logger.info(f"Fetching results for {results_date}...")
        bets = get_resolved_bets(results_date)
        summary = get_daily_summary(results_date)
        performance = get_performance_stats_sync(days=30)

        if not bets:
            logger.warning(f"No resolved bets for {results_date}")
        else:
            logger.info(f"Found {len(bets)} resolved bets")

        results_renderer = ResultsCardRenderer()
        results_img = results_renderer.render(
            bets, summary, results_date, performance, fmt=args.format
        )
        results_path = out_dir / f"results_{results_date}_{args.format}.png"
        results_img.save(results_path, "PNG")
        saved.append(results_path)
        logger.info(f"Saved results card: {results_path}")

    # Summary
    print(f"\nGenerated {len(saved)} image(s):")
    for p in saved:
        print(f"  {p}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
