> Part of [[Handoffs]]

**Date**: April 24, 2026 at 10:45 AM

## Summary

Short session focused on two bot-tracker improvements: fixing broken Kalshi contract links (both in the orders table and the approval panel) and adding a BetAnalysisModal so historical bets can be inspected for player game history and bet-time decision data. No backend changes — dashboard-only.

## What Was Done

- **Fixed `getKalshiUrl()` in `BotOrdersTable.tsx`**: Was hardcoding `kxnbagame`/`kxmlbgame` for all bets — player prop bets link to completely different Kalshi series. Now derives series from `ticker.split('-')[0].toLowerCase()` and builds `https://kalshi.com/markets/{series}/{ticker.toLowerCase()}`. Removed unused `sport` parameter.
- **Fixed `TradeApprovalPanel.tsx` link**: URL was uppercase. Added `.toLowerCase()` to both the series prefix and the full ticker.
- **Created `dashboard/src/components/bot-tracker/BetAnalysisModal.tsx`**: New modal component that shows:
  - Player header (avatar, sport badge, stat, side YES/NO, line)
  - Bet details grid (edge, model prob, Kalshi implied, result with status badge + P&L)
  - Last 5 Games: L5 chart + stats table for MLB batting (AB/H/TB/HR/RBI/R), MLB pitching (K), or NBA (MIN/PTS/REB/AST). HRR shows H+R+RBI sum column in amber.
- **Updated `BotOrdersTable.tsx`**: Added `analysisOrder` state, bar-chart icon button on each row (hidden when `player_id` is null), renders `BetAnalysisModal` when order selected.
- **TypeScript clean**: GLM verified with `npx tsc --noEmit` — no errors.

## Decisions Made

- **Read-only modal, no sportsbook lines or Kelly sizing**: Bet records don't store quantiles, game_id, or bookmaker lines at bet time. Rather than stub a half-broken full AnalysisModal, built a focused BetAnalysisModal that shows exactly what's useful: the data actually stored at bet time (model_prob, kalshi_implied, edge) plus always-available player history from the stats tables.
- **Fetch player history from current stats tables**: L5 shows the most recent 5 games, not necessarily the 5 games before the bet was placed. Accepted trade-off — retrospective historical context is useful even if slightly different from what the bot saw.

## Blockers and Open Questions

- None for this session's work.
- Ongoing: `batter_rbis` still disabled (confirmed broken — -$13k over 233 bets). Needs decision on whether to attempt retraining or leave off permanently.
- Ongoing: `NBA_TRADING_ENABLED=false` — paused after Apr 19 incident. Playoff v2 model deployed but trading held pending additional validation.
- Outstanding: lineup_position pipeline (90% done per memory) — will obsolete the star-hitter filter when complete.

## Recommended Next Steps

1. **Test the BetAnalysisModal in browser** — click the bar-chart icon on any historical bot bet and verify the L5 chart loads for MLB and NBA stats.
2. **Verify Kalshi URLs** — click the external link icon on an NBA and MLB prop bet; should navigate to the correct player prop series page (e.g., `kxnbapts/...` not `kxnbagame/...`).
3. **Re-enable NBA trading** — when confident in the playoff v2 model, set `NBA_TRADING_ENABLED=true` on Railway.
4. **Lineup position pipeline** — finish the 10% remaining work to feed lineup_position into MLB model at inference time; will obsolete the star-hitter filter.
5. **batter_rbis decision** — either retrain on 2026 data (0.5-line binary market) or formally remove from SUPPORTED_STATS.

## Files to Read on Resume

- [[Bot-Tracker]] — updated with session 49 notes on URL fix and BetAnalysisModal
- [[handoff-041]] — previous session context (sportsbook line alignment, star-hitter filter)
- [[Execution-Plan]] — current phase status
