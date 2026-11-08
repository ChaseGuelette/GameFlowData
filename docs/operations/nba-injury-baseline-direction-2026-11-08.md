# NBA injury baseline direction — November 8, 2026

## Owner direction and scope

Latest owner decision: implement the explicit injury-free baseline and compatible incumbent hyperparameter file, then hand over the first full training command. Additional seasonal sampling is deferred and is not a launch gate. Implementation scope: `.hermes/plans/nba-first-no-injury-training.md`. Training remains owner-launched; the first run is exploratory, not leakage/promotion certification. The known game-line cutoff caveat remains documented, not silently resolved.

Chase proposes leaving historical injury features out of the first baseline while injury-history repair proceeds separately, and recovering/rescraping historical records where required information is absent. Late injury news should be treated as real decision-time uncertainty, not information retroactively available to earlier predictions.

This records the owner direction and the recommendations below; it does not claim feature exclusion is implemented or authorize an agent-launched scrape, training run, database write, deployment, or promotion. Existing fixed-parameter/split policy and parallel paper/backend ownership remain unchanged.

## Verified distinction

The saved November 7 acceptance receipt reports calibration `accepted_games=360` of 1,278: at least one consumer-selected injury observation per counted game, not complete team-report coverage or a game-ID linking rate.

Calibration selector accounting records 21,531 `unsupported_report_time` events and 1,057 `missing_or_invalid_observation` events. It does not record an `after_cutoff` rejection bucket. These are consumer processing counters, not disjoint unique-player/game counts; unknown clocks are rejected before cutoff comparison. They do not prove a prevalence of late scratches. All 1,278 calibration games have schedule-derived starts.

The current selector requires resolved player/team identities, matching payload game date and team context, retained source records, an interpretable publication instant, publication no later than the decision cutoff, exact flat/raw identity/status agreement, and a nonconflicting latest eligible team edition. Its supported request-day routing is game day or the previous day. The current ordinary training policy uses T-60. A stored injury-row game ID is not itself required by this selector's team/date matching.

The January 29, 2026 ingestion stored request date as `report_date`; current ingestion stores payload game date and retains the separate request date. Legacy dates are ambiguous, not categorically absent. The current game-ID linker excludes rows without retained observations rather than guessing their date meaning.

## Recommendations, not implemented changes

1. Prepare an explicit injury-feature-excluded baseline variant across fitting, calibration, evaluation, and inference for that artifact. Verify the full injury-derived family in the actual manifest; do not encode missing injuries as zero/healthy or silently use real injury feature values only at serving time. Keep valid games rather than restricting the baseline to accepted injury-report games.
2. Repair historical injury evidence independently. Existing November recovery already reacquired many RapidAPI records, so inspect representative missing/unsupported cases and validate provider clock/date semantics before another broad rescrape. A source must expose the earlier report edition and publication timing needed for the chosen cutoff; another final-status response alone does not solve chronology. Preserve raw captures and existing repair results. No new broad job is launched here.
3. For late news, preserve the original prediction and any already-placed wager. A contemporaneously known scratch may suppress new recommendations under a declared eligibility policy. Do not erase prior bets or affected teammates' results after learning the outcome. Settle any existing wager according to its applicable participation/void rules; keep model-error scoring distinct from wagering accounting and do not reopen settled structural-zero policy here.
4. If an updated prediction is used for a new decision, retain its later decision timestamp and corresponding information/quote state; it is not a rewrite of the original decision.

## Evidence and relevant lessons

- `logs/receipts/nba_training_input_acceptance_20261107/post_cached_replay/acceptance_results.json` — saved November 7 evidence, not a fresh database query.
- `src/models/features/nba/injury_context.py:282-327,363-452,524-612` — identity, publication parsing and public selector.
- `src/scrapers/nba_injury_db.py:59-111` — request date versus payload date and retained records.
- `src/processing/link_injury_data.py:469-491` — guarded unique date/team fixture linking.
- Git `48a41172:src/scrapers/rapidapi_injury_backfill.py:226-265` — legacy request-date storage.
- `docs/operations/nba-training-input-acceptance-2026-11-07.md` — current bounded acceptance and separate holdout gap.
- GBrain `lessons/raw-timestamps-do-not-guarantee-temporal-integrity`: verify actual consumer chronology; do not equate stored data with usable pre-decision information.
- GBrain `lessons/cheap-baseline-before-architecture`: establish a simple controlled baseline rather than blocking it on unrelated complexity.

## Next implementation gate

Inspect existing feature-exclusion controls and the complete injury-derived feature family; provide a bounded implementation/preflight scope if needed. No source changes or runnable launch command are asserted by this note.
