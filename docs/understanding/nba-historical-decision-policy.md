## Kick off immediately

Slice 8A is the read-only evidence and policy-freeze stage for trustworthy NBA historical training and replay. It does **not** change model behavior, repair data, run training, or authorize Slices 8B–8E.

Chase’s immediate job is to review the choices in **Owner decisions** after the bounded database evidence is available. Until each required choice has a real value or an explicit blocked state, historical runs are not decision-grade.

## Status labels used here

- **Current fact:** established by the reviewed source code or planning documents.
- **Owner decision:** a policy value Chase must explicitly approve.
- **Pending DB evidence:** a question that still requires bounded, read-only SQL-runner proof.

## What 8A is

**Current fact:** 8A prepares one versioned policy contract covering decision time, source clocks, quote pairing, injuries, cohorts, data splits, season carryover, and missing evidence.

**Current fact:** it is a documentation and read-only evidence lane. It must not mutate the database, copy or index the archive, rescrape, relink, train, replay, or change source behavior.

**Current fact:** Slice 7 review does not block 8A. It does block the later lifecycle integration and frozen launch handoff where Slice 7’s final contract is required.

**Pending DB evidence:** the November 2 audit found current combined props for only 1 of 19 selected diagnostic games, six selected games without `game_id_map` entries, and no local `raw_player_props_archive`. That is a historical-readiness block, not proof that the completed current-table transfer failed.

## Why timestamps are not interchangeable

**Current fact:** the same column name can represent different events in different ingestion paths:

- Historical game lines and player props store the **requested historical time**.
- Live game lines store a local UTC time created after the bulk response arrives, near parse start.
- Live player props store one local UTC time created before event-by-event fetching begins.
- Injuries retain a caller date, textual `report_time`, and first local insertion time.

A request time, provider snapshot time, market update time, parse time, invocation-start time, insertion time, decision time, and game commence time answer different questions. Substituting one for another can make information look available earlier than it was.

**Current fact:** for rows proven to come from the historical odds route, the requested time may support a conservative upper bound because the provider describes the returned snapshot as being at or before that request. It does not prove the exact snapshot, quote executability, row immutability, market-update ordering, or provenance of mixed-table rows.

**Current fact:** that historical interpretation must never be applied to the live player-prop invocation-start clock.

## Decision time versus commence

Decision time is the policy cutoff: what the model was allowed to know. Commence is the game’s authoritative start boundary. A quote must satisfy the approved availability rule at or before decision time and remain strictly before commence.

**Owner decision:** choose one timezone-aware decision-time rule:

- a game-relative T-minus rule; or
- explicit `America/New_York` slate times with defined handling for early games.

There is no approved implicit 18:30 rule and no approved season-wide fixed 23:30 UTC rule.

**Owner decision:** when decision time, commence, or their source evidence is missing, choose either blocked/unknown or a separately named diagnostic-only fallback.

**Current fact:** commence must not be invented from the game date.

**Pending DB evidence:** identify authoritative commence coverage and count the selected games that cannot be mapped safely.

## Quote provenance and pairing

A quote is usable only when its provenance tells us how its clocks should be interpreted. The policy must name each accepted source namespace and interpreter rather than treating every row in a combined table alike.

**Owner decision:** for each provenance class, approve:

- its availability clock and timezone;
- which cutoff evidence is required;
- whether historical requested time is an accepted conservative bound;
- whether market and bookmaker update clocks are mandatory;
- null-clock and stale-quote behavior;
- duplicate and conflict handling;
- the snapshot compatibility key;
- rejected or unknown-source behavior.

**Current fact:** Over and Under must form one compatible pair with the same player, game, market, book, line, source, and snapshot identity. Selecting the independently latest Over and latest Under can create a pair that never existed together.

**Current fact:** identical duplicate rows and conflicting same-identity prices are different cases. Conflicts must not be silently resolved without an approved rule.

**Pending DB evidence:** measure provenance classes, clock ordering, nulls, stale candidates, duplicate/conflict cases, and compatible paired-side coverage in a bounded indexed sample.

## Injury editions

An injury report cycle label is not automatically an as-of timestamp, and a later database ID is not automatically the next report edition.

**Current fact:** the current storage can retain distinct status/reason variants but does not establish a complete ordered edition history. First insertion time may reflect a later backfill and cannot prove earlier availability.

**Owner decision:** approve:

- the parseable `report_time` format and timezone;
- how report editions or versions are identified;
- the status eligibility cutoff;
- what an omitted player means, if evidence establishes it;
- how missing or ambiguous history behaves.

**Current fact:** decision-grade history may be blocked/unknown, or Chase may separately approve a clearly named diagnostic-only reduced-feature mode. Silent healthy/zero/latest fallback is not allowed.

**Pending DB evidence:** inspect retained cycle values, same-player status changes, edition retention, insertion-versus-report ordering, and omitted-player behavior.

## Cohort, split, and carryover choices

The cohort defines who and which games belong in the study. The split defines what is training, calibration, protected out-of-sample, and independent validation. Carryover defines what historical feature families may use across season boundaries.

**Owner decision:** freeze:

- included seasons and exact date bounds;
- regular-season, play-in, and playoff treatment;
- late-season or abnormal-rotation handling;
- traded-player and team-transition handling;
- season-boundary and cold-start behavior;
- target eligibility minimum minutes, currently five;
- prior-game rolling contribution threshold, currently eight;
- training window;
- calibration window with an exclusive cutoff;
- protected OOS window;
- independent validation window, if distinct.

No arbitrary last-X-games exclusion should be chosen.

**Owner decision:** for every required feature family, choose exactly one behavior:

- current-season only;
- prior-season carryover;
- carryover with explicit decay or indicator;
- neutral default with provenance;
- prediction-blocking when unavailable.

**Current fact:** historical and production policies may differ, but the difference must be explicit and versioned. Slice 8 must not silently change production inference.

**Pending DB evidence:** verify opening, middle, and end-of-season coverage; classify postseason separately; and prove required feature variation through assembled features rather than raw-row presence alone.

## Blocked versus diagnostic-only

**Blocked/unknown** means the evidence cannot support a decision-grade run. The pipeline should stop or classify the affected population as unavailable; it must not publish a success-shaped result.

**Diagnostic-only** means Chase has explicitly approved a named reduced-evidence mode for mechanics or coverage investigation. Its output cannot be presented as historical acceptance, profitability evidence, or a promotion candidate.

Missing quotes are measured source dropout, not evidence that the model found no edge. Fatal query, loader, or inference failures are execution failures, not empty slates. Legitimate no-game or no-target populations remain explicit successful no-ops.

## The sequence: 8A → 8B → 8C → 8D → 8E

1. **8A — Evidence and policy freeze:** collect bounded read-only evidence and approve the versioned policy. Unknowns remain unknown.
2. **8B — Feature and injury integration:** make training, single-date, and date-range loaders use the same approved per-game policy and produce matching ordered features, provenance, defaults, and readiness.
3. **8C — Quote-clean replay integration:** make direct and prefetched replay use the same temporal eligibility and coherent quote-pair selector, with matching rejection evidence.
4. **8D — Bounded real-data acceptance:** progress from static checks to one-day smoke, a three-to-seven-day replay, and finally the complete bounded lifecycle after required integrations are accepted.
5. **8E — Frozen baseline handoff:** freeze exact policy, artifact, windows, sources, config, and output identity. Agents prepare and dry-run; Chase launches the approved baseline.

A later stage does not begin merely because the prior plan exists. Its evidence must pass and its owner decision must be recorded. A profitable backtest cannot substitute for temporal integrity or source readiness.

## What Chase must decide

Before the versioned 8A policy can be frozen, Chase must approve:

1. The exact decision-time rule and early-game behavior.
2. Accepted quote provenance classes and clock interpretations.
3. Quote cutoff, staleness, null, duplicate, conflict, and pairing rules.
4. Injury time, edition, omission, and missing-history rules.
5. Included seasons, game types, transitions, and abnormal-period handling.
6. Training, calibration, protected OOS, and validation windows.
7. Target-minutes and rolling-contribution thresholds.
8. Per-feature-family season and carryover behavior.
9. Blocked versus explicitly named diagnostic-only behavior for missing commence, clocks, sources, injuries, and features.

These choices must be based on source semantics and bounded coverage evidence, not selected by whichever option produces the best retrospective ROI.
