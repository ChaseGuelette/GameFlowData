# DFS Edge Finder

> Part of [[Product]]

## Overview
The DFS page (`/dfs`) compares DFS platform lines against devigged sportsbook consensus to find +EV entries. Works for all 6 stats including those the model doesn't predict (steals, blocks, threes).

## Three Modes
1. **Model Edge**: Uses MC simulation probabilities (PTS/REB/AST only)
2. **Market Edge**: Compares DFS lines vs devigged sportsbook consensus (all 6 stats)
3. **Combined**: Blends model and market signals

## Six Stats Supported
PTS, REB, AST, STL, BLK, 3PM

## Filters
- Platform filter (PrizePicks, Underdog, etc.)
- Slip type selector
- +EV toggle
- Stat filter buttons (dynamic from `STAT_LABELS`)

## Data Source
`get_dfs_lines` RPC queries `raw_player_props_combined` with `us_dfs` region filter. The RPC has `SET statement_timeout = '30s'` override because the query takes ~9-14s on the 67M+ row table.

## Key Design Decision
DFS market edge works WITHOUT the model — it compares DFS lines against devigged sportsbook consensus. This means it works for all 6 stats even though the model only predicts PTS/REB/AST.

## DFS Paper Trading
Automated via `dfs_paper_trader.py`:
- 4 entries/day (ud_3_standard, ud_5_standard, pp_5_flex, pp_6_flex)
- Selects top-N positive-edge legs by consensus probability
- Flex partial payouts (e.g., PP 5-flex: 5/5=10x, 4/5=2x, 3/5=0.4x)
- $500 starting bankroll, $10/entry

## User Slip Builder
See [[DFS-Slip-Builder]] for the user-facing slip builder that lets users select legs, compute parlay Kelly sizing, place entries, and track P&L on the history page.

#dfs #product #feature
