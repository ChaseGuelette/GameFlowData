> Part of [[Handoffs]]

**Date**: April 21, 2026 at 12:35 PM

## Summary

Implemented the two-track non-sports arb system. Fuzzy text matches (SequenceMatcher) no longer flow directly into `arb_opportunities` — they're queued in a new `verified_market_links` table for human review. Only structured extraction matches (KXGDP/KXFED/KXCPI) and human-approved pairs (Track A) get processed into arbs. A new Queue tab on the arb-scanner dashboard shows pending fuzzy matches with approve/reject buttons.

## What Was Done

- **DB migration**: Created `verified_market_links` table (`bigserial` PK, unique on `(kalshi_ticker, poly_condition_id)`, status=pending/approved/rejected, RLS + authenticated_read policy)
- **`src/arbitrage/market_matcher.py`**:
  - Added `match_method`, `kalshi_title`, `poly_question` fields to `MatchedMarket` dataclass (backward-compatible defaults)
  - Tagged each match in the inner loop as `'structured'` (both sides extracted) or `'fuzzy'` (SequenceMatcher fallback); tracked `best_method` alongside `best_score`/`best_k`
  - Added `_build_verified_matches()` — loads approved links from DB, fetches live prices, returns `MatchedMarket` list with `match_method='verified'`
  - Added 3 supporting DB loaders: `_load_approved_links()`, `_load_kalshi_by_tickers()`, `_load_poly_by_condition_ids()`
  - `match_non_sports_markets()` now calls `_build_verified_matches()` at start and extends `matched` at the end
- **`src/arbitrage/arb_scanner.py`**:
  - Non-sports block splits output into Track A (`structured` + `verified`) and Track B (`fuzzy`)
  - Track A extends `all_matched` and flows to arb detection as before
  - Track B calls `_store_pending_links()` → writes to `verified_market_links` with `ON CONFLICT DO NOTHING`
- **`dashboard/src/app/api/arb/verify/route.ts`** (new): POST endpoint to approve/reject pending links using service-role client (linter moved supabase init inside handler)
- **`dashboard/src/types/arb-scanner.ts`**: Added `VerifiedMarketLink` type; updated `ArbTab` union to include `'queue'`
- **`dashboard/src/lib/hooks/useArbScanner.ts`**: Added `useMatchQueue()` hook — fetches pending links with confidence ≥ 0.70, ordered by confidence desc; optimistic removal on approve/reject
- **`dashboard/src/components/arb-scanner/MatchQueueTable.tsx`** (new): Table with Series / Kalshi Title / Poly Question / Confidence (color-coded) / Approve + Reject buttons; pending count badge; empty state
- **`dashboard/src/app/(protected)/arb-scanner/page.tsx`**: Added Queue tab with pending count badge; renders `MatchQueueTable`; TypeScript clean

## Decisions Made

- **Track A only = structured + verified** — `match_method == 'unknown'` defaults as Track B (fuzzy) to be safe. Any non-sports match that isn't clearly structured goes to the queue, not to arb_opportunities.
- **`ON CONFLICT DO NOTHING`** in `_store_pending_links()` — preserves existing pending/approved/rejected entries so previously-reviewed decisions are never overwritten by a new scan cycle.
- **Confidence ≥ 0.70 filter in `useMatchQueue()`** — hides very low-confidence fuzzy matches from the queue view; they're still stored in DB but not surfaced for review until threshold is reconsidered.
- **Verified matches always have `match_confidence=1.0`** — they were human-approved, so confidence is treated as perfect regardless of the original fuzzy score.

## Blockers and Open Questions

- Non-sports scan (every 30 min, 9 AM–11 PM ET) hasn't fired since deploy — `verified_market_links` is empty until next :00 or :30. Results visible within ≤30 min.
- After first scan: expect 0 Track A matches (no structured KXGDP/KXFED/KXCPI arbs right now) and some Track B fuzzy matches in the queue.
- The `--skip-paper` flag stays on the Railway non-sports job — even Track A matches won't generate paper bets until signal quality is confirmed and the flag is removed.

## Recommended Next Steps

1. **Watch first post-deploy Railway scan** (next :00 or :30 ET) — verify logs say "Track A: N, Track B: M queued" and that `verified_market_links` populates
2. **Review queue on dashboard** — use the new Queue tab to approve/reject fuzzy matches. Good GDP/CPI/FED matches are likely worth approving.
3. **Enable paper trading for Track A** — once 2-3 weeks of approved matches accumulate, remove `--skip-paper` from the non-sports scheduler job to let approved pairs generate paper bets
4. **Phase 9 expansion** — add `_CAT_FINANCE` / `_CAT_ENTERTAINMENT` / `_CAT_SCOTUS` to `KALSHI_SERIES_POLY_CONFIG`. These are Binary event categories that should match well.

## Files to Read on Resume

- [[non-sports-arb-market-structure]] — full audit of Kalshi non-sports universe, why crypto/range-bracket series don't match, current match ceiling
- `src/arbitrage/market_matcher.py` — two-track matching logic, `_build_verified_matches()`
- `src/arbitrage/arb_scanner.py` — `_store_pending_links()`, Track A/B split
- `dashboard/src/components/arb-scanner/MatchQueueTable.tsx` — queue UI component
