# Slice 8 native source contract

## Working entry points

`FeatureStore(engine).get_features_for_date(day, historical_policy=policy)` now acquires real sources by default. Training and date-range entry points use the same reader/assembler. Caller evidence objects remain controlled-only; normal requests do not need or accept them.

For a sidecar instead of database JSON metadata, construct `PostgresHistoricalSources(engine, metadata_path=path)` and supply it as `FeatureStore(engine, historical_sources=reader)`. No new DB table or migration is required. Repaired data can alternatively expose a `historical_metadata` JSON object on each source row. Both mechanisms provide source facts only: feature values still come from queried tables.

A `BacktestHarness` with the same FeatureStore and `historical_policy` obtains native commence contexts/routes from that reader and reads raw executable quotes through it. No controlled flag or supplied quote tuple is required. CLI/lifecycle configuration attachment is separate work; default production paths are unchanged.

## Tables and selection

- Targets: `player_game_stats`, with `team_game_stats` for actual target opponent/home/away identity.
- Shifted basic/advanced rows: `player_average_game_stats` and `player_average_advanced_stats`, latest game date <= target date, target season only.
- Team/opponent rows: `team_average_game_stats`, same as-of/season restriction.
- Position: `player_position_history`, latest snapshot date strictly before target date.
- Positional defense: `team_allowed_by_position`, actual as-of position and latest date <= target; source season comes from its own game/team join to `team_game_stats`, not an invented column/default or the target season alone.
- Feature lines: `raw_game_lines_staging` and `raw_player_props_combined`, explicit configured bookmakers/markets. Exact selected source IDs and coherent counterpart records remain required.
- Injury rows: `rapidapi_injuries`, explicit edition row IDs, complete asserted per-team roster membership. OUT-player basic/advanced weights are acquired in bulk even if that player has no target-game box-score row.
- Executable quotes: raw `raw_player_props_combined` outcomes, retaining source/snapshot/clock metadata until the accepted quote adapter evaluates them.

Read batches default to 200 targets. Source transactions are read-only with a 30-second statement timeout. Replay retains its explicit game/per-game candidate caps. No live data operations were launched during code verification.

## Metadata storage and identity

Sidecar shape: an object with `records`, an array of objects containing `table`, `key` (array), and `metadata` (object). Duplicate keys or conflicting embedded/sidecar objects fail.

Keys:
- Target: `player_game_stats`, `[game_id, player_id]`.
- Basic/advanced/team: their table name, `[entity_id, row_game_id]`.
- Defense: `team_allowed_by_position`, `[opponent_id, row_game_id, position_group]`.
- Position: `player_position_history`, `[player_id, ISO_snapshot_date]`.
- Quotes/game lines: their table name, `[string_native_staging_id_or_id]`.
- Injury row: `rapidapi_injuries`, `[string_native_id]`.

Metadata may provide `source_record_id` and a string-to-string `attributes` object. Otherwise the reader uses the native single key or canonical JSON natural key as record identity. These are identifiers, not certification of authenticity. All clocks must have explicit timezone offsets; opaque identifiers are not trimmed/repaired.

## Required source facts

Target metadata:
- `season_type`: `regular_season`, `play_in` or `playoffs`.
- `game_context`: `commence_at`, `commence_provenance_ref`.
- `market_routes`: array of objects with `bookmaker`, `period`, `variant`, `provenance_evidence_ref`.
- `market_selections`: six `[feature_name, source_namespace, source_record_id]` selections for spread, total and the four existing prop-line features. Namespace is the actual raw table name.
- `injury_edition`: `source_namespace`, `source_record_id`, `team_ids`, `completeness_evidence_ref`, `row_ids`, `roster_membership` (array of `[team_id, [player_ids...]]`). Selected rows must exactly cover the asserted rosters; omitted rows never imply healthy.

Rolling metadata:
- `producer_ref`, `carryover_mode`, `contributing_seasons`, `contribution_game_ids`.
- `latest_contribution_at`, `source_observed_at`.
- `rolling_min_minutes`, `transition_policy_id`, `cold_start_policy_id`.
- Provenance attributes binding carryover authority: `carryover_evidence_ref`, or family-specific `carryover_evidence_ref:<family_id>` entries when a real row serves multiple families. Basic player schedule context additionally needs `schedule_evidence_ref`; defensive snapshots need `position_group`.
- The consumer binds the requested policy identity; it does not require historical producers to have known a future modeling split's content hash.

Position metadata: `observed_at`, plus optional identity/attributes.

Market metadata:
- `source_class`, `snapshot_key`, `side`, `period`, `variant`.
- `historical_requested_at` for authenticated historical-request interpretation, or `local_observed_at` for supported local-observation interpretation; never both conflicting authorities.
- Provenance attribute `provenance_evidence_ref` matching explicit policy authority.
- Prices/lines and market/bookmaker update clocks come from the actual raw row, not the sidecar. `snapshot_time` remains the actual source row clock. Home/away spread side is explicit, not guessed from a team name.

Injury metadata:
- `history_state`, `local_observed_at`, optional `position_group` (required for OUT contribution aggregation).
- Provenance attribute `edition_ref` matching the selected edition ID.
- Raw status/team/player/report display fields come from the actual injury row. Publication/report text is never converted into an observation timestamp.

## Verification and limitations

The normal API path ran through the real reader, typed adapters, assembler and actual direct/prefetch harness, with only DB reads and model predictions controlled at their external boundaries. It produced date/training/range parity and matching replay outputs, including an OUT player without a target-game box score. A missing source metadata record raised a specific blocked-data error rather than an empty success. Existing protected probability/selection math remained unchanged.

This establishes executable code architecture, not actual database coverage, SQL execution against the live database, authenticity certification, artifact/lifecycle acceptance or launch readiness. Outputs remain `decision_grade=False`; normal frames are labeled `historical_readiness=source_integrated`, distinct from `controlled_only`. Chase owns data repair/completeness. No scraping, synchronization, backfill, schema write, model training, commit/merge/push or deployment was performed.
