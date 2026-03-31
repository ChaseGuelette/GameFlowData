# DFS Slip Builder

> Part of [[Product]]

## Overview
The DFS Slip Builder lets users select legs from the DFS Edge Finder, compute parlay Kelly-based sizing, place entries to track, and view P&L on the history page. Standard parlays only — all legs must win.

## Three Slip Types
| Type | Platform | Legs | Payout | Break-Even |
|------|----------|------|--------|------------|
| PP 2-Pick Power | PrizePicks | 2 | 3x | 57.7% |
| UD 3-Pick Standard | Underdog | 3 | 6x | 55.0% |
| UD 5-Pick Standard | Underdog | 5 | 20x | 54.9% |

No flex partial payouts in user tracking — standard parlays only.

## Architecture

### Frontend
- **Leg selection**: Click any row in DfsTable to toggle it into the slip (green border + checkmark)
- **SlipBuilderPanel**: Desktop fixed right panel (~320px), mobile fixed bottom bar
- **SlipLegCard**: Compact card per leg showing player, stat, line, direction, prob
- **Kelly calculation**: `calcParlayKelly()` in `dfs-kelly.ts` — product of leg probs, Kelly criterion, capped at 10% bankroll
- **Confirmation**: Two-click placement ("Place Entry" then "Confirm $X on UD 3-Pick?")

### State Management
- `useSlipBuilder` hook holds legs in local state (not persisted until placed)
- `useUserPreferences` provides bankroll + kelly fraction
- Validation: correct leg count, no duplicate players, valid probs

### Data Flow
1. User selects legs from DfsTable rows
2. Hook computes combined prob, Kelly stake, EV
3. On placement: insert to `user_dfs_entries` + `user_dfs_legs` via Supabase
4. Clear slip on success

### History
- Third tab "DFS Entries" on `/history` page
- `DfsEntrySummary`: KPIs (entries, win rate, P&L, ROI, per-type breakdown)
- `DfsEntryCard`: Expandable card with legs, actuals, metadata
- Pending entries can be deleted (cascade deletes legs)

### Backend Resolution
- `UserDfsResolver` in `src/paper_trading/user_dfs_resolver.py`
- Called from `daily_stats_job.py` after user bet resolution
- Standard parlay: any leg lost = entry lost, all won = entry won

## Database Tables
- `user_dfs_entries`: parlay-level (stake, combined_prob, kelly_fraction, payout_multiplier, status, pnl)
- `user_dfs_legs`: per-leg (player_id, stat, line, direction, model_prob, market_prob, actual_value)
- RLS: users can only see/insert their own entries, delete only pending

## Key Files
- `dashboard/src/types/dfs-entries.ts` — Types and slip type constants
- `dashboard/src/lib/dfs-kelly.ts` — Parlay Kelly calculation
- `dashboard/src/lib/hooks/useSlipBuilder.ts` — Slip builder hook
- `dashboard/src/components/dfs/SlipBuilderPanel.tsx` — Panel UI
- `dashboard/src/components/dfs/SlipLegCard.tsx` — Leg card
- `dashboard/src/components/history/DfsEntryCard.tsx` — History entry card
- `dashboard/src/components/history/DfsEntryList.tsx` — Entry list
- `dashboard/src/components/history/DfsEntrySummary.tsx` — Summary KPIs
- `src/paper_trading/user_dfs_resolver.py` — Backend resolver
- `migrations/user_dfs_entries.sql` — Database migration

#dfs #product #feature #slip-builder
