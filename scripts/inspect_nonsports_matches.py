"""
Inspect non-sports matched pairs — shows actual Kalshi ↔ Poly question text
alongside prices and match scores so we can evaluate match quality.

Usage:
    python scripts/inspect_nonsports_matches.py [--series _CAT_ELECTIONS|_CAT_POLITICS|all]
                                                [--top N]
                                                [--sort margin|score]
"""
import argparse
import sys
from difflib import SequenceMatcher
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.arbitrage.market_matcher import MarketMatcher, _norm_q
from src.arbitrage.non_sports_extractor import extract_kalshi, extract_poly, match_score
from src.db.client import get_engine
from src.scrapers.kalshi.kalshi_utils import KALSHI_SERIES_POLY_CONFIG


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--series", default="all", help="Series to inspect (or 'all')")
    parser.add_argument("--top", type=int, default=30, help="How many pairs to show")
    parser.add_argument("--sort", default="margin", choices=["margin", "score"])
    parser.add_argument("--min-score", type=float, default=0.0)
    args = parser.parse_args()

    engine = get_engine()
    matcher = MarketMatcher(engine)

    kalshi_rows = matcher._load_kalshi_non_sports()
    print(f"Loaded {len(kalshi_rows)} Kalshi non-sports markets from DB")

    # Group by series_ticker
    series_groups: dict[str, list] = {}
    for row in kalshi_rows:
        ticker = row.get("ticker", "")
        series = row.get("series_ticker") or ticker.split("-")[0]
        series_groups.setdefault(series, []).append(row)

    results = []

    for series, k_rows in series_groups.items():
        if args.series != "all" and series != args.series:
            continue

        cfg = KALSHI_SERIES_POLY_CONFIG.get(series)
        if not cfg:
            continue

        min_kalshi_volume = cfg.get("min_kalshi_volume", 0)
        min_poly_liquidity = cfg.get("min_poly_liquidity", None)
        fallback_threshold = cfg.get("fallback_threshold", 0.80)

        if min_kalshi_volume > 0:
            k_rows = [r for r in k_rows if (r.get("volume") or 0) >= min_kalshi_volume]
        if not k_rows:
            continue

        poly_rows = matcher._load_poly_non_sports(
            categories=cfg["poly_categories"],
            min_liquidity=min_poly_liquidity if min_poly_liquidity else None,
        )

        poly_keywords = cfg["poly_keywords"]
        poly_filtered = [
            r for r in poly_rows
            if any(kw in (r.get("question") or "").lower() for kw in poly_keywords)
        ]

        print(f"\n[{series}] {len(k_rows)} Kalshi × {len(poly_filtered)} Poly")

        seen_poly: set[str] = set()
        k_data = [
            (r, _norm_q(r.get("market_title") or ""),
             extract_kalshi(series, r.get("ticker", ""), r.get("market_title") or ""))
            for r in k_rows
        ]

        for poly in poly_filtered:
            cid = poly.get("condition_id", "")
            if cid in seen_poly:
                continue
            poly_q_norm = _norm_q(poly.get("question") or "")
            if not poly_q_norm:
                continue

            p_fields = extract_poly(series, poly.get("question") or "")

            best_score, best_k, best_sm = 0.0, None, 0.0
            for k_row, k_n, k_fields in k_data:
                if not k_n:
                    continue
                if k_fields is not None and p_fields is not None:
                    score = match_score(k_fields, p_fields)
                else:
                    sm = SequenceMatcher(None, poly_q_norm, k_n).ratio()
                    if sm >= fallback_threshold:
                        from src.arbitrage.market_matcher import _extract_candidate
                        k_cand = _extract_candidate(k_row.get("market_title") or "")
                        p_cand = _extract_candidate(poly.get("question") or "")
                        if k_cand and p_cand:
                            name_sim = SequenceMatcher(None, k_cand.lower(), p_cand.lower()).ratio()
                            score = sm if name_sim >= 0.65 else 0.0
                        elif k_cand or p_cand:
                            score = 0.0
                        else:
                            score = sm
                    else:
                        score = 0.0
                    best_sm = max(best_sm, sm)
                if score > best_score:
                    best_score, best_k = score, k_row
                    if k_fields is None or p_fields is None:
                        best_sm = score

            if best_k is None or best_score == 0.0:
                continue

            seen_poly.add(cid)

            k_yes = float(best_k.get("yes_price") or 0)
            p_yes = float(poly.get("yes_price") or 0)
            net_margin = max(100 - k_yes - (100 - p_yes), 100 - (100 - k_yes) - p_yes) / 100

            results.append({
                "series": series,
                "k_ticker": best_k.get("ticker", ""),
                "k_title": best_k.get("market_title", "")[:80],
                "k_yes": k_yes,
                "k_vol": best_k.get("volume", 0),
                "p_question": poly.get("question", "")[:80],
                "p_yes": p_yes,
                "p_liq": poly.get("liquidity", 0),
                "score": best_score,
                "net_margin": net_margin,
            })

    # Sort
    if args.sort == "margin":
        results.sort(key=lambda x: x["net_margin"], reverse=True)
    else:
        results.sort(key=lambda x: x["score"], reverse=True)

    # Filter by min score
    if args.min_score > 0:
        results = [r for r in results if r["score"] >= args.min_score]

    print(f"\n{'='*100}")
    print(f"TOP {args.top} MATCHES (sorted by {args.sort})")
    print(f"{'='*100}")

    for i, r in enumerate(results[:args.top]):
        margin_str = f"+{r['net_margin']*100:.1f}c" if r['net_margin'] > 0 else f"{r['net_margin']*100:.1f}c"
        print(f"\n#{i+1} [{r['series']}] score={r['score']:.3f} | margin={margin_str} | "
              f"Kalshi YES={r['k_yes']:.0f}c vol={r['k_vol']:,} | Poly YES={r['p_yes']:.0f}c liq={r['p_liq']:,.0f}")
        print(f"  KALSHI: {r['k_title']}")
        print(f"  POLY:   {r['p_question']}")

    print(f"\nTotal matches: {len(results)}")
    print(f"Shown: top {min(args.top, len(results))}")

    # Score distribution
    if results:
        scores = [r["score"] for r in results]
        print("\nScore distribution:")
        for lo in [0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95]:
            hi = lo + 0.05
            count = sum(1 for s in scores if lo <= s < hi)
            print(f"  {lo:.2f}-{hi:.2f}: {count:3d} matches")


if __name__ == "__main__":
    main()
