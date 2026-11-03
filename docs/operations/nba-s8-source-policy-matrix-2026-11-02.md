## Kick off immediately

Slice 8A evidence collection is complete enough to present conservative source-policy proposals, but it does **not** make historical training or replay decision-grade. This page is an owner-decision matrix, not implementation authorization. It makes no production change and does not authorize Slices 8B–8E.

Related context: [historical decision policy](../understanding/nba-historical-decision-policy.md) and [Slice 8 readiness receipt](nba-s8-readiness-receipt-2026-11-02.md).

## Status meanings

- **SUPPORTED:** bounded evidence supports the stated conservative rule.
- **BLOCKED:** current evidence cannot support a decision-grade value or population.
- **UNKNOWN:** evidence does not establish the fact; absence must not be inferred.
- **PROPOSED:** conservative value awaiting Chase's approval.
- **FROZEN:** approved value. Nothing in this packet is frozen until Chase checks the corresponding approval item.

## Bounded evidence snapshot

- The diagnostic selection contains **19 games**: **18 regular-season** and **1 playoff** game across candidate seasons `22023`, `22024`, and `22025`; **6 play-in games** were classified separately.
- Current combined props cover **1/19** selected games. The represented game has **1,147,195** rows, **514,125** paired keys, **62,507** one-sided keys, **191** conflicting-price groups, and **4,578** exact-duplicate groups locally; the remote duplicate count timed out and remains **UNKNOWN**.
- Current game lines cover both spreads and totals for **11/19** games, with **1,492** selected-game rows, **251** structural totals pairs, and **251** structural spread pairs.
- **6/19** selected games lack `game_id_map` entries; only **11/19** have mappings with both team IDs.
- Archive contribution remains **UNKNOWN** in this receipt. Slice 8A intentionally performed no archive query or transfer, so current-table misses cannot be interpreted as permanent historical absence.
- In the inspected props sample, `bookmaker_last_update` was null for every row. Structural pairs do not establish decision-time eligibility.
- Retained injury evidence shows multiple local editions only in the selected **2026-04-12** and **2026-05-01** samples. Earlier selected dates are January 29 backfilled snapshots and do not establish decision-grade edition replay.
- No candidate decision time has eligible-retention evidence across competing times. No historical train/calibration/OOS cohort can be certified from current props coverage of **1/19** diagnostic games.

## Source and policy matrix

| # | Policy area | Evidence status | Proposed/frozen value | Rationale | Owner decision |
|---:|---|---|---|---|---|
| 1 | Decision time | **BLOCKED** | **Proposed comparison:** (A) game-relative **T-60 minutes**; versus (B) an **18:30 America/New_York** slate cutoff with an early-game cutoff of **T-60** whenever the game commences before 19:30 ET. Do not freeze either rule until bounded retention/timing evidence is reviewed. | T-60 is the existing relative-policy reference used elsewhere in the project; 18:30 ET reflects the old NBA operational intent without reusing the unsafe fixed 23:30 UTC implementation. Neither candidate is decision-grade yet. | Approve these two comparison candidates or replace either value before the comparison. |
| 2 | Historical-route provenance | **SUPPORTED only for authenticated historical-route rows; otherwise BLOCKED** | **Proposed:** requested historical time may be used only as a conservative upper bound for rows whose historical-route provenance is authenticated. Mixed/current-table rows without explicit quote provenance are blocked. | The provider route means returned snapshot at or before the request, but current tables do not explicitly prove which producer route created each row. Request time is not exact provider snapshot time or proof of executability. | Approve the conservative upper-bound interpretation and fail-closed treatment for unproven provenance. |
| 3 | Quote freshness clocks | **PARTIALLY SUPPORTED; null-clock cases BLOCKED** | **Proposed:** require `market_last_update <= decision_time` and `market_last_update < commence_time` when `market_last_update` is present. Treat `snapshot_time` as bucket/request evidence, never the sole freshness proof. Keep `bookmaker_last_update` optional because it was entirely null in the inspected props sample. Never use `inserted_at` as availability. | The clocks describe different events. Every inspected game-line insertion was at/after commence, demonstrating that insertion cannot establish observation availability. | Approve these clock roles and the fail-closed handling of missing required evidence. |
| 4 | Duplicate, conflict, and pairing policy | **SUPPORTED as a conservative fail-closed rule** | **Proposed:** deduplicate exact duplicates while preserving all source IDs; block conflicting prices sharing one quote identity; block one-sided player props. Pair totals Over/Under by game, book, market, line, and snapshot. Pair spread team outcomes by game, book, market, and snapshot, requiring opposite orientation. | Independently selecting latest sides can synthesize a market that never coexisted. Exact repeats are not the same as conflicting prices. | Approve the identity keys, ID preservation, and blocking behavior. |
| 5 | Commence time | **BLOCKED when missing or ambiguous** | **Proposed:** missing, conflicting, naive, or ambiguously sourced commence is decision-grade **BLOCKED**. Never derive commence from game date. | Eligibility requires a strict pre-commence boundary. A date does not identify a start instant or timezone. | Approve blocked behavior; separately name any diagnostic-only exception if desired. |
| 6 | Injury editions and clocks | **BLOCKED through the selected 2026-01-16 date; selected 2026-04-12/2026-05-01 history only partially supported** | **Proposed:** decision-grade injury edition replay is blocked for the selected dates through 2026-01-16. In the selected April/May evidence, `scraped_at` may establish only local availability at that time. `report_time` remains non-timezone display metadata. Omission remains **UNKNOWN**. Missing edition history is **BLOCKED** unless Chase explicitly approves a named diagnostic-only reduced-feature mode. | Storage does not prove a complete ordered edition history, vendor publication time, or omitted-player meaning. First local insertion cannot establish earlier availability. | Approve fail-closed decision-grade behavior and decide whether to permit a separately labeled diagnostic-only mode. |
| 7 | Historical cohort and split | **BLOCKED** | **Proposed:** freeze only the diagnostic sample classification: 18 regular-season games, 1 playoff game, and 6 separately classified play-in games. Do not freeze train, calibration, protected OOS, validation, season, or date windows. | Props coverage is only 1/19, so the present sample cannot certify a historical cohort or non-overlapping split. | Approve the diagnostic classification and leave every modeling window blocked pending adequate retained coverage. |
| 8 | Minutes thresholds | **SUPPORTED by the existing intended contract; awaiting owner freeze** | **Proposed:** retain target minimum eligibility at **5 minutes** and prior-game rolling contribution at **8 minutes**, unless changed in a separate reviewed decision. | These thresholds serve different purposes and must not be collapsed. Slice 8A found no evidence requiring a change. | Approve 5/8 unchanged or request a separate evidence review. |
| 9 | Feature-family carryover | **BLOCKED per family** | **Proposed:** no blanket carryover rule. Each required family remains blocked until assembled training/date/date-range evidence proves behavior and variation. Any league-prior carryover must be explicit, versioned, and provenance-bearing. | Raw-row presence does not prove the assembled path used a value correctly. Different families may need different season-boundary behavior. | Approve per-family blocking and require a later family-by-family matrix before decision-grade use. |
| 10 | Historical versus production policy | **SUPPORTED boundary** | **Proposed:** version every intentional historical/production difference. Slice 8 makes no production inference, scheduler, source, or deployment changes. | A historical adapter must not silently redefine live behavior. Separate identities preserve reviewability and rollback. | Approve the versioned separation and no-production-change boundary. |
| 11 | Failure, dropout, and no-op taxonomy | **SUPPORTED** | **Proposed:** fatal execution, query, loader, or inference failures fail the run and cannot publish complete results. A missing eligible quote is measured source dropout. A legitimate empty slate or no-target population is an explicit successful no-op. | These outcomes have different operational and analytical meanings; combining them creates success-shaped false evidence. | Approve this taxonomy for all decision-grade historical paths. |
| 12 | Policy configuration creation | **BLOCKED pending owner approval** | **Proposed:** do not create `configs/nba/historical_policy_v1.yaml` until Chase approves every required value or explicitly approves its blocked state. | A placeholder or implicit default would make an unresolved policy look executable and frozen. | Approve this creation gate. |

## Interpretation boundaries

- **No exact quote is declared eligible by this matrix.** Structural pairing counts prove only that compatible-looking sides exist in bounded samples.
- A requested historical time is not an exact snapshot identity, and it cannot be assigned to a row without authenticated producer-route provenance.
- `snapshot_time`, update clocks, insertion, decision, and commence remain distinct evidence.
- Blocked populations may be counted and diagnosed; they may not contribute to historical acceptance, ROI claims, promotion, or production decisions.
- Diagnostic-only behavior must have a separate name and policy identity and must never be reported as decision-grade.
- No retrospective ROI comparison may select source, timing, missingness, cohort, split, or carryover policy.

## Owner approval packet

Check only after reviewing the matrix. Any unchecked item remains unresolved.

- [ ] **Decision-time candidates:** I approve comparing **T-60 minutes** against **18:30 America/New_York with T-60 used for games commencing before 19:30 ET**. I understand that no exact decision time is frozen yet.
- [ ] **Historical provenance:** I approve requested historical time only as a conservative upper bound for authenticated historical-route rows; rows without proven provenance remain blocked.
- [ ] **Quote clocks:** I approve the `market_last_update` decision/commence checks, `snapshot_time` limitation, optional null `bookmaker_last_update`, and prohibition on `inserted_at` as availability.
- [ ] **Quote identity:** I approve exact-duplicate deduplication with preserved IDs, conflict blocking, one-sided-prop blocking, totals pairing by game/book/market/line/snapshot, and spread pairing by game/book/market/snapshot with opposite orientation.
- [ ] **Commence:** I approve decision-grade blocking for missing or ambiguous commence and prohibit derivation from game date. Diagnostic-only exception, if any: `____________________`.
- [ ] **Injuries:** I approve decision-grade blocking for the selected dates through 2026-01-16; selected April/May `scraped_at` as local-availability-only evidence; `report_time` as non-timezone display metadata; omission as unknown; and missing history as blocked. Named diagnostic-only mode, if approved: `____________________`.
- [ ] **Cohort/split:** I approve freezing only the 19-game diagnostic classification and leaving train/calibration/OOS/validation windows blocked pending adequate coverage.
- [ ] **Thresholds:** I approve target minimum **5 minutes** and rolling contribution minimum **8 minutes** unchanged.
- [ ] **Carryover:** I approve per-family blocking pending assembled-path evidence and require explicit versioned league-prior carryover.
- [ ] **Environment boundary:** I approve versioned historical/production differences and no production behavior changes in Slice 8.
- [ ] **Outcome taxonomy:** I approve fatal failure, missing-quote dropout, and legitimate-empty-slate no-op as distinct outcomes.
- [ ] **Configuration gate:** I approve creating no `configs/nba/historical_policy_v1.yaml` until every required value or explicit blocked state above is approved.

## Approval result

Slice 8A may be recorded as **policy packet reviewed** only when Chase dates and approves the packet. It remains **implementation blocked** wherever the matrix says BLOCKED, including decision time, unproven quote provenance, missing/ambiguous commence, pre-2026-01-17 injury replay, historical cohort/split, and per-family carryover.

Owner: Chase
Decision date: `YYYY-MM-DD`
Result: `APPROVE AS WRITTEN / APPROVE WITH MARKED CHANGES / RETURN FOR EVIDENCE`
