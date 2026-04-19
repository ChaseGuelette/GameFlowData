> Part of [[Handoffs]]

# Session 035 Handoff

**Date**: April 18, 2026 at 10:32 PM

## Summary
Replaced the SequenceMatcher 0.55 fuzzy matching approach for non-sports Kalshi↔Polymarket arb detection with a new deterministic structured field extractor. The old approach produced 2,447 false positives (e.g., "$95k vs $90k" markets scoring 0.92 similarity); the new extractor parses (price, direction, period) from Kalshi ticker encoding and Polymarket question text before comparing. A dry-run validation on Railway is the final step before production deploy.

## What Was Done
- **NEW**: `src/arbitrage/non_sports_extractor.py` (~260 lines) — deterministic extraction of price, direction, and period from Kalshi ticker (e.g., `B95000` → $95,000) and Polymarket question text. Scores 1.0 when all three fields confirmed, 0.85 when period unknown, falls back to SequenceMatcher >= 0.80 when extraction fails on either side.
- **MODIFIED**: `src/arbitrage/market_matcher.py` — `match_non_sports_markets()` loop replaced to call extractor; internal constant renamed to match new interface.
- **MODIFIED**: `src/scrapers/kalshi/kalshi_utils.py` — `"eth"` keyword changed to `"ether"` to prevent false matches on common English words like "method", "health", and "whether".

## Decisions Made
- **Structured extraction over fuzzy similarity**: SequenceMatcher at 0.55 produced 2,447 matches because question wording varies wildly between platforms (e.g., both "$95k" and "$90k" price markets scoring 0.92). Deterministic field extraction avoids this class of false positive entirely.
- **PRICE_TOLERANCE = 0.5% relative**: Allows tiny float rounding differences between platforms without crossing price levels (e.g., $95,000 vs $95,001 should match; $95,000 vs $90,000 should not).
- **Fallback threshold raised to 0.80**: When extraction fails on either side, the fallback SequenceMatcher threshold is 0.80 (vs old 0.55), making the fallback path conservative rather than permissive.
- **Score tiers**: 1.0 = price + direction + period all confirmed; 0.85 = price + direction confirmed, period unknown. This lets downstream filtering distinguish high-confidence from partial matches.
- **"eth" → "ether" keyword**: Prevents KXETH/KXETHD markets from matching Polymarket questions containing common English words with "eth" as a substring.

## Blockers and Open Questions
None

## Recommended Next Steps
1. Run dry-run to validate match counts drop to expected range (~10-100): `python src/orchestration/arb_scan_job.py --include-non-sports --dry-run`
2. If 0 matches: add debug logging to count extraction success rate, lower PRICE_TOLERANCE, or check parser logic for edge cases.
3. If still high (1000+): add `logger.debug` fallback rate counter to see how often extraction is failing and falling back to SequenceMatcher.
4. Deploy to Railway once dry-run output looks clean (match count reasonable, confidence values only 0.85 or 1.0, no "Poly yes 0c" stale markets).

## Files to Read on Resume
- src/arbitrage/non_sports_extractor.py — new structured field extractor (new file, ~260 lines)
- src/arbitrage/market_matcher.py — `match_non_sports_markets()` function (~lines 472-590)
- [[handoff-035]] — this handoff
