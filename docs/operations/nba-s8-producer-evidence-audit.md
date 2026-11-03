# Slice 8 retained producer evidence audit

## Result and bounds

A file-only audit inspected three NBA producers; coordinator verified key source locations with targeted searches. No SQL, archive scan, source repair, network request or data write was performed. These findings do not rule out external scheduler logs, HTTP captures or backups.

## Props

`src/scrapers/player_prop_scraper.py` calls the historical endpoint and passes the requested historical timestamp into `snapshot_time` (`parse_and_store`, around line 207). It retains `scrape_progress.json` through `save_progress` (around lines 81–85), keyed by snapshot/event plus market-set identity. Empty/422 responses are also marked processed (around lines 450–451).

This ledger corroborates attempts/resume state but does not bind source row IDs, inserted counts or exact quote values to a successful retained response. It is not enough by itself to authenticate each row of a mixed historical/live table. `snapshot_time` is requested time, not actual backfill execution time.

## Game lines

`src/scrapers/game_lines_scraper.py` requests historical bulk odds and passes requested schedule time into `snapshot_time`. `_batch_insert` inserts rows directly with `execute_values` (around lines 125–128). The inspected producer retains no request/response manifest or raw-response digest; any durable stdout retention depends on external infrastructure.

## Injuries

`src/scrapers/rapidapi_injury_backfill.py` stores `report_time` but the unique conflict key excludes report-time and scrape-time (`store_records`, around lines 232–235). Equal date/team/player/status/reason rows collapse even across distinct report editions. `scraped_at` defaults to insertion time (around line 73).

`get_scraped_dates` selects any represented report date (around lines 212–214); the backfill may skip that date after partial insertion. An empty successful response leaves no durable completion marker. The inspected producer has no retained complete-edition receipt, expected count, raw-response hash or ordered edition identifier.

## Consequence

Strict historical consumers must not infer injury edition completeness or source authenticity from these timestamps/progress entries. Existing source fields can be acquired and classified, but unsupported populations remain blocked. External authenticated receipts remain a possible evidence route, not an established source. This is a data acceptance limit, not permission to weaken the approved policy or fabricate historical inputs.

8B consumer implementation proceeds parameterized against explicit authority; 8D/8E real acceptance requires actual source evidence and an exact manifested artifact. No executable policy, cohort split or baseline is frozen by this audit.
