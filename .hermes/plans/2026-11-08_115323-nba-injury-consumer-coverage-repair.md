# NBA Injury Consumer Coverage Repair Plan

> **For Hermes:** After Chase approves a bounded implementation slice, use the GameFlow implementation-worker lane and independent diff/validation review. This document authorizes research/planning only, not implementation, migrations, recovery jobs, training, deployment, commits or pushes.

**Goal:** Work toward trustworthy pre-decision injury context for every game in the frozen training/calibration/test cohorts, fixing recoverable acquisition/parser/storage/consumer gaps without admitting late information or manufacturing healthy statuses.

**Architecture:** Reuse the official NBA PDF reader, existing injury storage/linkage, recovery launcher and public consumers. Discover real report editions, retain complete source/section manifests, ingest authentic statuses even when absent from the legacy API rows, and select eligible editions consistently. Keep the first injury-excluded baseline independent of this repair.

**Tech stack:** Existing Python environment, requests, pypdf, PostgreSQL via the isolated GameFlow SQL runner, current CLI/PowerShell recovery tools. No new provider purchase, framework or infrastructure is proposed for the first slice.

**Research date:** November 8, 2026. Primary public pages were retrieved directly; three official PDFs were downloaded and exercised through the current parser without database access. The database coverage figures below are saved November 7 receipts, not fresh live DB counts.

---

## 1. Executive finding

**Follow-up validation:** See [the actual RapidAPI/PDF comparison](../../docs/operations/nba-rapidapi-clock-validation-2026-11-08.md). A global `reportTime` publication map failed validation; matched older statuses and previous-day subsets show useful data can still be recovered without acquiring every edition. Task 5 now has bounded empirical evidence; a previous-day model variant remains a separate owner decision, not a change to the current baseline.

The coverage gap is not established to be mostly last-minute scratches. We reproduced two repairable failures:

1. **Incorrect report discovery:** the old generated `_05PM.pdf` URL failed with HTTP 403 on two dates, while published minute-stamped URLs for those same dates returned real PDFs.
2. **Parser failure:** the accessible November 4 PDF is rejected in full because `_canonical_player` does not accept `Udeh, Jr., Ernest`.

Source review also confirmed same-day-only discovery, recovery restricted to existing matching injury rows, cached errors that are never retried on ordinary resume, and missing report-completeness representation. Fixing only HTTP requests will not finish the job.

Recommendation: official reports are the primary historical recovery path; RapidAPI remains a potentially useful validated secondary path. Run a bounded end-to-end pilot before any broad historical re-acquisition.

## 2. What "100%" will mean

Freeze the eligible game IDs, season/date bounds, team IDs and cutoff policy before measuring. Preserve fit `22023/22024`, calibration `22025`, initial holdout `22026` through November 4 unless Chase changes that policy. The current ordinary policy is T-60; actual production decision timestamps must remain separate rather than silently treated as T-60.

Report these distinct metrics:

| Metric | Definition / target |
|---|---|
| Source discovery coverage | Frozen team-game contexts with an identified relevant source edition before cutoff. Target 100%. |
| Complete usable team-game coverage | Team-game contexts with an authentic submitted section, supported publication clock, complete parsing/persistence and resolved required identities. Target 100%. |
| Complete usable game coverage | Games for which both teams satisfy the preceding gate. Target 100%. |
| Injury feature availability | Actual player-game feature outputs accepted under the approved status/omission and weighting policy. Target 100%, with the exact feature-family denominator visible. |
| Eligible-record consumer agreement | Every independently verified eligible listed status reaches the real consumer with matching identity/status/edition; no post-cutoff item is admitted. Target 100% agreement and zero ineligible admissions. |
| Accounting coverage | Every frozen game has a usable result or a specific unresolved reason. Target 100%; this does NOT mean 100% usable evidence. |

Do not use the current `accepted_games` metric as the final definition: it means at least one accepted row, not complete reports for both teams. Do not shrink the denominator by silently dropping early games, absent reports or unknown players. Keep later/contradictory raw observations as rejected audit evidence; accepting every raw observation is not a valid goal.

If a team genuinely had not submitted before cutoff, or no recoverable historical edition exists, the usable-rate target remains unmet. Record the exact remaining gap; do not relabel it healthy or count an unknown value as recovered data. The separate no-injury baseline can still proceed.

## 3. Verified starting evidence

### Saved consumer receipt

`logs/receipts/nba_training_input_acceptance_20261107/post_cached_replay/acceptance_results.json:97-126`:

- Calibration: 1,278 eligible games, 360 with at least one accepted injury item, 3,325 accepted items, 711 team-games with listed accepted statuses.
- All calibration games have schedule-derived starts; missing start times are no longer the measured blocker for this cohort.
- Processing counters: `unsupported_report_time=21531`, `missing_or_invalid_observation=1057`; no recorded `after_cutoff` bucket. Unknown clocks fail before cutoff comparison, so this does not prove rejected records were early or late. Counters are not disjoint unique-row/game counts.
- The initial holdout has zero accepted games in the saved receipt. Its API capture/identity gap remains separate from calibration.

### Public-source canaries and real parser execution

| Date / source filename | Direct HTTP result | Current parser result |
|---|---|---|
| `2026-11-04_05PM.pdf` | 403, 263 bytes | Not a PDF; no parse attempted |
| `2026-11-04_05_45PM.pdf` | 200, 103,258 bytes | FAIL: `Unsupported official player name layout: 'Udeh, Jr., Ernest'` |
| `2026-05-05_05PM.pdf` | 403, 263 bytes | Not a PDF; no parse attempted |
| `2026-05-05_12_15PM.pdf` | 200, 80,989 bytes | PASS: one page, four listed statuses, publication `2026-05-05T16:15:00+00:00` |
| `2025-11-03_11AM.pdf` | 200, 73,249 bytes | PASS: five pages, 52 listed statuses, publication `2025-11-03T16:30:00+00:00` |

The valid reports are direct NBA documents[6][8][9].

The November 2025 filename says `11AM` but the printed header says 11:30 AM; a filename/report-cycle token must not be mapped mechanically to an exact on-the-hour publication time[9].

Downloaded PDF SHA-256 values:

- November 4: `e881432edd5f69bfe578d506b5a66e2a4ea7f9a9b6533e70dc8f82c220d4e3ef`
- May 5: `b55ddeb1211d7dc3ce3eb6ac948bbe98903b16a4f6e02d23934d446e1f9c7514`
- November 3, 2025: `a98b99c4518fc2a83ca517adbc14be556e696da015f54ce12b8584b20dd963e1`

These samples prove accessible evidence and specific local failures, not whole-season coverage or the exact date a filename convention changed. A 403 is not proof that the dated report is absent; distinguish a published URL access failure from a guessed object path.

### Current public documentation

- The NBA's live index lists ET-labelled publication links and currently exposes minute-stamped filenames. A request to the old 2025-26 season page redirected to the 2026-27 page; do not treat old season index URLs as immutable historical inventories.[10]
- The NBA describes prior-day/game-day submission rules and continually updated reports. That supports searching earlier editions and previous-day publications rather than only one evening file; it does not guarantee every team submitted every historical report on time.[10]
- The actual RapidAPI marketplace listing documents `date` as game date and `reportTime` as a cycle, advertising 11 AM/3 PM/5 PM ET updates. Its example uses a different hostname/route from our configured endpoint, and its documented labels do not settle our observed historical `08PM` semantics. This is useful provider documentation, not enough to enable a blanket clock map.[2]
- Sportradar documents dated Daily Injuries, but its documented injury fields use dates and a different status vocabulary; this page does not establish retrievable intraday as-of editions. Treat it as an escalation candidate requiring a sample/contract check, not a ready substitute or purchase recommendation.[3]

### Reproducibility artifacts

Research evidence directory: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-injury-plan-20261108/evidence/`.

- `pdf_parser_probe.json` retains the actual parser outcomes above.
- Original PDFs, extracted text, raw public documentation and `evidence.jsonl` are retained there.
- Citation ledger: sibling `ledger.json`.
- Scratch is temporary. Before implementation, re-download/reverify the cited public objects or promote approved evidence into the existing receipt directory. This plan embeds the decisive results/hashes so it does not depend solely on scratch survival.

## 4. Confirmed implementation limitations

| Area | Evidence | Consequence |
|---|---|---|
| Single-slot, same-day discovery | `scripts/recover_nba_injury_history.py:_candidate_urls:339-345`; default `05PM` | Misses minute-stamped filenames, prior-day reports and useful earlier editions. Even explicit URLs are filtered to game date. |
| Parser name/layout assumptions | `src/scrapers/nba_official_injury_reports.py:_canonical_player:189`; live November 4 failure | One real suffix/comma variation aborts the document. |
| Missing section states | `nba_official_injury_reports.py:255-308` | `NOT YET SUBMITTED` is not retained; no complete/submitted-empty/absent team-section contract exists. |
| Existing-row-only recovery | `recover_nba_injury_history.py:_plan_game:431-505` | Can attach evidence only to already-present matching statuses/reasons; authentic missing or different earlier statuses cannot be recovered as new evidence. Teams are discovered from existing rows rather than the target fixture. |
| No controlled error replay | `recover_nba_injury_history.py:_fetch_reports:373-380`; `scripts/replay_nba_cached_injuries.py` receipt skip logic | Parser/discovery improvements do not automatically revisit previously accounted failures. Ordinary resume correctly avoids retry loops; retain that protection. |
| Sparse rows masquerade as edition coverage | `src/models/features/nba/injury_context.py:586-615` | Latest accepted row time is not proof of a complete persisted report. |
| Capture metrics outside consumer data | `src/scrapers/nba_injury_db.py:102-121` | Expected raw-record/section counts are not available as an authoritative consumer manifest. |
| Strict replay uses another contract | `injury_context.py:751-812`; `historical_observation_adapters.py:evaluate_injury_observation`; `historical_sources.py:325-341` | Requires exact full-roster rows and local-observation evidence. Repairing normal training ingestion alone does not certify replay parity. |

## 5. Proposed sequence and approval gates

### Task 1 — Freeze the denominator and produce an actionable gap inventory

**Objective:** Identify exactly what can be fixed without another broad audit.

**Inspect/reuse:** the existing acceptance receipt/checker, `scripts/smoke_nba_training_inputs.py` if its concurrent owner has completed it, and current SQL-runner workflow. The smoke script and loader are actively modified by another lane: do not edit or assume their provisional API.

**Actions:**
1. Confirm ownership and snapshot scoped source revisions/diffs; preserve active injury/checkpoint files.
2. Have an isolated read-only SQL runner refresh only the frozen cohort, with indexed date/game windows and existing timeouts. No giant raw-props scan.
3. Produce one game/team/cutoff manifest plus record-level rejection detail. Distinguish discovery missing, HTTP failure, unsupported layout/name, unknown clock, no retained capture, unmapped identity, missing official row, partial section, source not submitted, conflicting evidence, and legitimately after-cutoff.
4. Record all contributing flags and one deterministic primary blocker per team-game. Avoid summing overlapping processing counters as unique missing games.
5. Select a bounded pilot of up to 12 games covering the reproduced dates, old/new filename conventions, prior-day/early-game, DST, changing status, missing legacy row and complete/partial team sections. Freeze IDs before repair.

**Gate:** Every pilot context has a known cutoff and a specific repair hypothesis. New inventory is read-only; no migration or source requests triggered implicitly.

### Task 2 — Fix discovery, parser support and versioned cache replay first

**Modify:** `src/scrapers/nba_official_injury_reports.py`, `scripts/recover_nba_injury_history.py`, and narrowly `scripts/replay_nba_cached_injuries.py`.

**Actions:**
1. Parse actual official index links for forward collection. For historical gaps, use indexed/archived official links and a bounded, versioned filename candidate policy validated on real files. Support both observed filename families without assuming a single unverified transition date.
2. Search game day and previous publication day; use the report row's game date/matchup for applicability. Scope limits apply to target games, not an accidental prohibition on their prior-day reports.
3. Choose publications at/before the configured cutoff and retain the latest usable eligible team sections. Prefer relevant candidates near the cutoff; deduplicate shared league-wide documents across games. Do not crawl every quarter-hour of every historical day by default.
4. Preserve the printed header as publication authority, `America/New_York` DST conversion, and independent official schedule cutoffs. Keep filename tokens as discovery hints only.
5. Handle the demonstrated `Udeh, Jr., Ernest` layout and other observed suffix/continuation variants while retaining raw text. Name formatting is not player-ID validation. Unknown identities remain quarantined.
6. Preserve `NOT YET SUBMITTED` and per-team parse completeness. A bad row may be isolated only with an explicit incomplete section; never skip it and certify the team as complete. Keep the first repair small rather than replacing the PDF stack.
7. Separate immutable raw-download cache from parsed-result cache. Reparse retained bytes after parser-version changes; retry only selected failed URLs/contexts with attempt caps and backoff. A new discovery version can introduce new URLs without deleting successful checkpoints.
8. Version discovery/parser/normalization policy in replay receipts so old accounted failures can be explicitly reopened. Ordinary resume must still skip recorded errors and advance rather than loop forever.

**Gate:** The current November 4 parser failure is reproduced before the edit and resolved afterward with source-row count/identity checks. May 5 and November 2025 samples remain correct; late editions remain excluded. No DB writes are needed for this gate.

### Task 3 — Ingest complete official evidence, not only annotate matching API rows

**Modify after schema review:** `scripts/recover_nba_injury_history.py`, `src/scrapers/nba_injury_db.py`, `src/processing/link_injury_data.py`; prepare an additive migration only if necessary.

**Proposed new migration path if needed:** `database/migrations/20261108_nba_injury_report_capture_manifests.sql` (not created by this plan).

**Minimal design:** Prefer reusing existing per-status injury rows/source-observation storage plus an additive immutable capture-manifest relation. Avoid a general-purpose new ingestion framework. The SQL runner must verify the actual schema/indexes before finalizing DDL.

**Capture/section contract:** source URL and hash; retrieved-at distinct from published-at; printed-header clock evidence; parser version; original row ordinal/raw fields; expected and persisted row identities; target game dates/matchups; submitted/not-submitted/absent/incomplete section states; identity-resolution evidence. A league report may legitimately contain multiple game dates: scope sections by game/team rather than rejecting the entire multi-date capture.

**Actions:**
1. Use canonical fixture teams as the target scope, including teams with no legacy injury rows.
2. Stage every authentic official listed status in that scope. Create a separate source-backed status row when the earlier status/reason differs from the legacy row, instead of overwriting later history or requiring equality to the wrong flattened status.
3. Resolve player/team identity with existing exact mappings/verified aliases and date-appropriate team context. Do not require an injured player to appear in a box score; do not guess ambiguous names.
4. Preserve RapidAPI observations and prior official evidence. Exact-match guards should remain for any legacy-row update; new authentic statuses need their own valid identity/provenance, not relaxed update guards.
5. Publish a capture/team section as consumer-ready only after declared records and ordinals are fully persisted and verified. Partial writes remain pending, not a successful edition.
6. Execute one approved bounded remote-first batch, independently read it back, then mirror the exact accepted rows and verify parity. Rerun must be a no-op. No blanket table sync, rewrite or deletion.

**Gate:** An official pre-cutoff status absent from the legacy table becomes a real consumer candidate without fabricating status, clocks or healthy roster members. Partial writes/interruption cannot expose a supposedly complete edition.

### Task 4 — Reconcile completeness and train/replay semantics explicitly

**Modify only after owner policy approval:** `src/models/features/nba/injury_context.py`, `src/models/features/nba/historical_observation_adapters.py`, `src/models/features/nba/historical_sources.py`; coordinate any loader/request wiring with its current owner.

**Policy decision required:** A complete official injury report is not necessarily a row for every roster member. The current strict replay contract demands those rows. Choose an explicit source interpreter for complete official sections rather than manufacturing `Healthy` records to satisfy the old interface.

**Proposed interpretation:** Distinguish explicit listed status, not listed in a verified complete submitted section, and unknown/incomplete evidence. "Not listed" is not a medical assertion of health. How that state maps to player status flags and zero reported-Out aggregates requires owner approval and identical train/replay/live behavior. Do not silently alter the existing strict interpreter or structural-zero/DNP policy.

**Actions:**
1. Select coherent eligible editions/sections from capture manifests, not the maximum timestamp of whichever individual rows survived matching. Do not mix arbitrary per-player snapshots into a falsely complete team edition.
2. Share the accepted identity/status/edition selection across ordinary training and replay; maintain versioned source-policy metadata and feature fingerprints.
3. Support source-publication evidence for retrospective public-information replay as a distinct provenance mode from actual contemporaneous local observation. Never backdate `retrieved_at` to pretend the system historically captured a newly recovered report.
4. Keep after-cutoff records excluded; when a late update exists, use the eligible earlier edition. Do not carry an old status into a later complete edition after the player disappears unless approved report semantics support it.
5. Check teammate contribution weights and position history as separate pregame inputs. Complete report recovery must not hide missing weights by assigning zero contribution.
6. Compare actual feature vectors and missingness across ordinary training and strict replay on the pilot. Do not claim parity solely from SQL/link counts.

**Gate:** Complete authentic report evidence is usable through both public consumer paths under the approved interpreter; partial/not-submitted evidence remains explicitly incomplete. No retrospective use of actual minutes, participation or final statuses as pregame features.

### Task 5 — Evaluate RapidAPI recovery only where it can close remaining gaps

**Reuse:** `src/scrapers/rapidapi_injury_backfill.py`, `src/scrapers/nba_injury_db.py`, retained request/response captures.

**Actions:**
1. Compare representative retained labels (including `08PM` and zero-padded labels) with exact official documents from the same request/game dates.
2. Resolve the discrepancy between the marketplace example route and our configured route, and ask the provider what each historical label identifies: publication time, file cycle, response refresh time, and whether older intraday editions are retrievable.
3. Validate request-day versus payload game-date semantics, winter/summer UTC conversion, and source version/effective dates. An on-the-hour mapping must not replace a half-hour printed publication time.
4. Enable a source-specific clock interpreter only after evidence supports it. Otherwise use RapidAPI for identity/status cross-checks, not authoritative pre-cutoff timing.
5. Reacquire only missing captures/editions that the source can actually return. No paid/broad API call without owner approval and a scoped request/credit estimate derived from the gap manifest.

**Gate:** A bounded end-to-end sample passes actual consumer checks; provider marketing alone does not certify historical chronology. Official ingestion need not wait for this secondary path.

### Task 6 — Owner-launched historical recovery, then full cohort acceptance

**Reuse:** `scripts/run_nba_injury_history_recovery.ps1`, the improved recovery/replay CLIs and existing receipt layout.

**Order:** Validate the blocked October 20–November 4 holdout window first as a small integration slice; then calibration gaps; then remaining fitting gaps. This is source repair, not tuning on holdout outcomes. Preserve already-successful evidence and the frozen split.

**Actions:**
1. Present unique-document request count, remaining game/team counts, cache reuse and mutation scope before launch. Do not estimate whole-job duration from a three-document sample.
2. Chase launches bounded batches. Prefer sequential writers and exact mirrors; do not duplicate another chat's job or resume from a new empty state directory.
3. Each batch records discovery, parse, identity, persistence, selected-edition and actual consumer results separately. Report newly recovered contexts and remaining reasons.
4. Reconcile every unresolved context after the first pass. Use another authentic source/archive only for named remaining gaps; perform provider due diligence before any purchase.
5. Run full frozen-cohort acceptance through the actual consumers and compare local/remote evidence IDs. Use an independent count/digest query for critical persisted totals.

**Gate:** Report the real achieved percentages and remaining game IDs by blocker. Only claim the 100% target when every declared denominator passes; an accounted-with-gaps exit or a completed crawler is not acceptance.

### Task 7 — Prevent new gaps with capture-based forward collection

**Inspect before modifying:** `src/orchestration/lines_job.py` existing injury scraper/linker chain and `src/orchestration/scheduler.py`. Scheduler is concurrently dirty and protected until ownership is coordinated.

**Actions:**
1. Collect published report editions once globally, retain raw bytes/response, publication and actual retrieval times, and the edition/team manifests.
2. Verify the official-source access path on the actual runtime host before enabling it; workstation public access does not certify a Railway canary. Do not move advanced stats scraping to Railway.
3. Run the same selector for the actual prediction timestamp. Record the chosen capture/edition with predictions; preserve old decisions when later news arrives.
4. Alert on missing expected reports, incomplete parse/identity, stale captures and no pre-cutoff usable section. Retry transient errors within bounded budgets without hiding persistent gaps.
5. Verify several real slates, including an early game and a status change, before making this collection path authoritative.

**Gate:** Actual scheduled execution, source capture, DB readback and consumer output are verified end-to-end. No live betting or injury-feature model promotion is implied.

## 6. Validation strategy

Honor the current no-new-unit-tests convention unless Chase explicitly changes it. Reuse existing tests and controlled probes against retained real documents; do not create a separate testing framework.

Existing discovered test targets:

- `tests/test_nba_injury_context.py`
- `tests/test_rapidapi_injury_backfill.py`
- Inspect `tests/test_injury_database.py` and `tests/test_injury_scraper_job.py` before running; isolate database/network boundaries rather than allowing live connections.

Suggested commands after approved edits, from repo root, using the existing environment:

- `.\venv\Scripts\python.exe -m pytest tests/test_nba_injury_context.py tests/test_rapidapi_injury_backfill.py -q`
- `.\venv\Scripts\python.exe -m ruff check src/scrapers/nba_official_injury_reports.py src/scrapers/nba_injury_db.py scripts/recover_nba_injury_history.py scripts/replay_nba_cached_injuries.py`
- Add only the actual modified consumer/source files to scoped lint/compile validation.

These test commands are proposed validation, not tests executed during this research. Existing tests alone are insufficient; the following real-path probes are mandatory:

1. Old and minute-stamped PDFs; suffix/comma names; page continuations; prior-day report for the next game; printed header/filename mismatch; DST conversion.
2. A report before cutoff versus one after cutoff; later Out must not overwrite earlier Questionable for the earlier decision.
3. Missing legacy status row; unknown player; conflicting source identity; partial section; `NOT YET SUBMITTED`; multiple legitimate game dates in one league report.
4. Full expected-versus-parsed/persisted row accounting; no silently dropped records or fabricated healthy roster rows.
5. Interruption between capture/rows/manifest, retry with limits, parser-version reparse from cached bytes, no-op rerun, and no repeated download for unchanged successful captures.
6. Ordinary training and strict replay select the same approved evidence and produce equivalent features; missing teammate weights remain visible.
7. Remote-first bounded write and exact local mirror only after approval, each independently read back. No fresh model fit hidden inside a loader smoke.

## 7. Relevant prior lessons/invariants

- `operations/hard-facts` and `operations/critical-invariants`: temporal integrity, SQL-runner isolation, scoped DB approval and Chase-launched long jobs remain binding.
- `lessons/raw-timestamps-do-not-guarantee-temporal-integrity`: verify the consumer path, not merely stored timestamps; conversely, do not rescrape blindly when an interpreter/discovery bug explains rejection.
- `lessons/cheap-baseline-before-architecture`: the explicitly injury-excluded fixed-parameter baseline can proceed independently. This repair is not a reason to reopen completed props/harness work.
- Keep original prediction/bet history. No actual participation/box-score outcome used to invent pregame injury status.
- Preserve empirical probabilities, no global conformal offsets, and no blind Q10 correction. This plan changes source evidence/selection, not model calibration.

## 8. Decisions and execution boundary

Recommended first implementation slice: Tasks 1-2 only—bounded gap manifest, real-URL discovery, demonstrated parser fix and version-aware controlled cache replay. This is the fastest evidence-backed improvement and can be verified without broad DB writes.

Separate approvals required:

- Full official-section/omission semantics and the new strict replay interpreter.
- Additive schema and authentic missing-status insert path.
- Bounded live write pilot, then broad owner-launched recovery.
- Paid provider use or a new scheduled collector/deployment.

Do not weaken checks merely to display 100%. Fix the source and consumer together, retain irrecoverable gaps honestly, and verify the final percentage against the frozen population.

## Sources

[2] https://rapidapi.com/nichustm/api/nba-injuries-reports
[3] https://developer.sportradar.com/basketball/reference/nba-daily-injuries
[6] https://ak-static.cms.nba.com/referee/injury/Injury-Report_2026-11-04_05_45PM.pdf
[8] https://ak-static.cms.nba.com/referee/injury/Injury-Report_2026-05-05_12_15PM.pdf
[9] https://ak-static.cms.nba.com/referee/injury/Injury-Report_2025-11-03_11AM.pdf
[10] https://official.nba.com/nba-injury-report-2026-27-season
