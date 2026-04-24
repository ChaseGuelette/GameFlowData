# Handoff 041 — Kalshi Sportsbook Line Alignment + Star-Hitter Filter

> Part of [[Handoffs]]

**Date**: April 24, 2026 at 2:10 PM

## Summary

Implemented sportsbook line alignment for Kalshi trade selection (preferring the Kalshi line that matches the sportsbook prediction), fixed a live trader SQL bug that made the old tiebreaker dead code, and added a star-hitter filter after data analysis showed star batters at line=1 NO bets were losing -$161 while non-stars were +$285. DB migration applied, dashboard updated with SB line display and alignment warnings.

## What Was Done

### Code Changes
- **`src/paper_trading/kalshi_live_trader.py`**: Fixed SQL SELECT (added `sportsbook_consensus_line`, `line_vs_sportsbook`), replaced 3% tiebreaker with 8% sportsbook-alignment dedup, added `sportsbook_consensus_line` to trade queue INSERT, added star-hitter filter (NO on line=1 batter_hits blocked when yes_price >= 72)
- **`src/paper_trading/kalshi_paper_trader.py`**: Same dedup logic replacement + star-hitter filter
- **`dashboard/src/types/bot-tracker.ts`**: Added `sportsbook_consensus_line` to `KalshiTradeQueueItem`
- **`dashboard/src/components/bot-tracker/TradeApprovalPanel.tsx`**: Line column now shows SB consensus below Kalshi line + yellow warning icon when lines don't match

### DB Migration
- `add_sportsbook_consensus_line_to_trade_queue`: Added `sportsbook_consensus_line DECIMAL(5,1)` to `kalshi_trade_queue`

### Data Analysis (not deployed — informational)
- Queried paper trader hits data: 153 resolved bets across lines 1/2/3
- Line 1: 35% win rate, +$124 PnL (+7.5% ROI) — profitable but volatile
- Line 2: 46% win rate, +$41 PnL (+25.7% ROI) — 3.4x better ROI, tiny sample (13)
- Star vs non-star split at line 1: Stars 28.8% win / -$161, Non-stars 39.4% win / +$285
- Worst offenders: Judge (1-6, -$36), Henderson (1-4, -$51), Ohtani (2-5, -$16), Alvarez (1-3, -$42)

## Decisions Made

1. **8% sportsbook fallback gap**: Replaced the old 3% proximity tiebreaker with a system that forces the SB-aligned Kalshi line unless a non-aligned line beats it by 8%+ edge. Analysis showed this won't actually change most bets today because the edge gap between line 1 and line 2 is typically 15-25%, but it's the right structural fix.

2. **Star-hitter filter via yes_price >= 72**: Chose market price as the proxy for "star" rather than a hardcoded player list or feature. Stars average 72+ yes_price on line 1 markets; non-stars average 70-. Configurable via `KALSHI_STAR_HITS_YES_PRICE` env var.

3. **Did NOT implement hitless-rate feature**: Discussed adding a trailing hitless-game-pct feature to the model, but concluded it's redundant if the lineup_position inference bug gets fixed (that's the real root cause — model sees lineup_position=0 at inference, underestimates PAs for 1-4 hitters).

4. **Did NOT hard-block all non-SB-aligned lines**: Line 1 bets are +$124 overall. The star filter addresses the losing subset without killing the profitable non-star bets.

## Blockers and Open Questions

- **All changes are local — not deployed.** Need to commit + push for Railway (Python) and Vercel (dashboard) to pick up changes.
- **lineup_position inference bug** is the deeper fix for the star-hitter miscalibration. It's 90% done (data exists in `mlb_game_lineups`, just not flowing to feature vector). Fixing this may make the yes_price filter unnecessary.
- **yes_price=72 threshold** is based on ~150 bets. Monitor as sample grows — may need tuning.

## Recommended Next Steps

1. **Commit and deploy** all changes from this session
2. **Fix lineup_position inference pipeline** — the data is already scraped 3x/day, just needs to flow into the feature vector. This is the real fix for star-hitter miscalibration.
3. **Monitor star filter effectiveness** over next 7 days — check if line 1 win rate improves with stars filtered out
4. **Consider lowering min_edge for SB-aligned lines** — line 2 bets at 10-12% edge have 25.7% ROI, but fail the 15% threshold. Could add a reduced threshold (e.g., 10%) specifically for matching lines.

## Files to Read on Resume

- [[Handoffs]] — this handoff (041)
- [[Execution-Plan]] — Phase 7 status
- `src/paper_trading/kalshi_live_trader.py` — live trader with all fixes
- `src/paper_trading/kalshi_paper_trader.py` — paper trader with matching fixes
- `MEMORY.md` — updated with sportsbook alignment + star filter notes
