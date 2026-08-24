# Active schema contracts

This is the repository's only migrations directory.

It retains schema definitions that are still directly tied to active NBA/MLB runtime, model, CLV, injury, or DFS behavior:

- `mlb_clv_snapshot_table.sql`
- `mlb_clv_snapshot_linking.sql`
- `mlb_pitcher_min_ip_l5.sql`
- `mlb_player_season_advanced_history.sql`
- `rapidapi_injuries_constraints.sql`
- `user_dfs_entries.sql`

Older applied migration history was removed from the active tree during the 2026-08-24 reduction. It remains recoverable from the verified local source bundle at:

`C:\Users\Chase\Archives\GameFlowData\2026-08-24-pre-prune\source-pre-prune.bundle`

These files are schema contracts, not evidence that a target database still needs migration. Check live schema state before applying anything.
