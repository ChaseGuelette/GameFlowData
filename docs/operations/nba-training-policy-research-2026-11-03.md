# NBA training-policy research and historical decision audit

Date: November 3, 2026. Status: research and recommendations, **not owner-approved policy or training authorization**.

## Executive findings

1. We are not designing from a blank slate. Earlier NBA models established separate regular/playoff artifacts, recent-season regular training, locked incumbent hyperparameters for subsequent retrains, player history that follows the player within a season, season-grouped rolling producers, and the eight-minute rolling contribution threshold. [I1–I5]
2. Some apparent policies were implementation defaults, not deliberate decisions: cross-season latest-row fallback, neutral/zero cold-start values, latest-in-day injuries, and latest historical prop lines without decision-time bounds. [I6]
3. The March 17/19/23 generation supplied `22025` to both training and calibration. The inspected historical trainer filtered the calibration frame only. The reported March backtests are therefore not established clean holdouts; their historical profitability cannot choose the next model policy. [I2–I4,I6]
4. The old playoff “2025 fully OOS” claim has a season/calendar error. `42024` is the 2024–25 playoff season, played in calendar 2025. Existing metadata names it as calibration, while an archived sweep command targets April 19–June 17, 2025. Treat the headline as reused-calibration/selection evidence, not independent certification. [I7][12][13]
5. The most defensible first experiment is a regular-season, current-season-feature, explicitly bounded baseline, followed by a separately scoped early-season prior/cold-start comparison. This is a recommendation for an interpretable control, not proof that resetting everything is optimal.
6. Stats recovery is complete for the audited recovery window. Source eligibility for historical quote/injury features and the actual training population still needs acceptance. Neither another blanket backfill nor reopening accepted Slice 8 coding follows from this report. [I8–I10]

## Scope and evidence quality

Inspected current regular/playoff JSON metadata and selected-feature manifests; historical Git versions of the trainer, feature store, rolling producer, development records and playoff notes; canonical GBrain decisions/handoffs; and public primary documentation/research. No model binary was deserialized. No SQL, paid API call, training, backfill, deployment, source/config change or publication was performed. Current live database completeness and actual old run-row membership were not measured.

Historical Git citations use `commit:path:line`; the files need not exist in the reduced checkout. They were read using Git, not restored. Older session documents are evidence of what was believed/decided, not automatic proof of the claim. This report takes precedence over their interpretation where source/configuration contradicts it. Unrelated concurrent changes in `docs/README.md` and `docs/ncaab/` are outside scope.

## 1. What we actually did across old iterations

| Iteration | Recovered settings / decision | Interpretation now |
|---|---|---|
| February 10: `run_20260210_095220` | Session 75 identifies its training seasons as `22023,22024`, calibration `22025` through January 1; the February 10 preparation command explicitly uses `--cal-end-date 2026-01-01` and tuning. | Intent was previous seasons for fitting and newer season for calibration. This is stronger chronological separation than the later March configuration, but no original immutable run manifest was recovered. “Through Jan 1” in prose must be interpreted against the exclusive CLI cutoff. [I1,I2] |
| February 18 retrain: `run_20260218` | Calibration guide records degraded performance; later session 75 attributes the retrain failure to retuning and chooses locked incumbent hyperparameters. | Documented failed iteration; exact original season/cutoff metadata was not recovered. Attribution to retuning is a historical diagnosis, not a fresh controlled causal finding. [I2,I11] |
| March 4/5 promotion and March 6 comparison | Session 65 says `run_20260304_214009` was promoted. Session 67's later comparison says production and archived production used the same February 10 XGBoost models, with sampling/copula differences. | Do not count every promotion or calibration change as a separately trained generation. The records conflict on lineage without the intermediate immutable artifacts. [I12] |
| March 17: `run_20260317_133145` | Added `22025` to `22023,22024`; calibration also `22025`, before March 1. Reused production hyperparameters. Compared March 1–16 and promoted the no-offset version. | A deliberate decision to incorporate current-season observations and retain hyperparameters. The supplied training population was not bounded by the calibration cutoff. Reported performance is not accepted independent OOS evidence. [I2,I6] |
| March 19: `run_20260319_110125` | Same season/calibration settings, `--cal-end-date 2026-03-01`, retrained minutes and PTS/REB/AST with `player_starter_prob` force-included. Rejected after March 2–18 comparison. | Explicit “do not promote” decision; the old comparison reported lower ROI despite better calibration/drawdown. Retain the incumbent decision, but do not turn that contaminated comparison into a universal scientific conclusion about starter features. [I3,I6] |
| March 23: `nba_run_20260323_212931` | Current regular `run_config.json` confirms `22023,22024,22025`, calibration `22025`, exclusive cutoff March 18. Locked hyperparameters. Raised basic rolling contribution minimum from 5 to 8 minutes and added Q50-versus-L5 under-bet sanity checks. | This is the retained regular metadata. The eight-minute rule is not the same as target eligibility. March 18–23 backtest is not proven outside the supplied training population. `22025` was incomplete in March, so “three full seasons” was inaccurate. [I4,I5,I6] |
| April 15 playoff iteration: `nba_run_20260415_152608` | GBrain/archived NBA model page and retained `.source` name this run. Separate playoff model and BL policy deployed; train `42019–42023`, calibration `42024`. | Clear decision to treat playoff modeling separately. Calendar-year OOS interpretation in the old handoff is wrong. [I7] |
| April 19/20 playoff replacement | Current playoff config timestamp is April 19, 15:33:28, with the same season settings. Archived command names `nba_run_20260419_153328`; April 20 commit updates the model binaries. `.source` still names April 15. | There was a later replacement not fully reflected in the lineage note. Do not attach April 15 results to current playoff bytes merely from `.source`. [I7] |

### What was truly settled versus merely inherited

Settled decisions: separate regular/playoff models; exclude regular-season `22019`/`22020`; preserve minutes × rate/copula/empirical probabilities; do not deploy global conformal offsets; lock incumbent hyperparameters for the March retrains; reject the March 19 starter-feature candidate; use eight-minute prior-game basic rolling contributions while retaining a distinct target cohort rule. [I1–I6,I11]

Not established as owner-approved, tested policies: offseason shrinkage weight; rookie prior; number of new-season observations needed; a special post-trade decay/reset rule; complete-edition injury omission semantics; a protected historical test untouched by prior selection. The November family proposal remains a proposal. [I8,I9]

The “COVID excluded” policy was not universal: the actual regular exclusion tuple was `22019,22020`, while playoff training explicitly included `42019,42020`. Preserve that distinction rather than claiming every old model excluded bubble/shortened seasons. [I6,I7]

## 2. Chronology findings that change interpretation

### March overlap is a code-backed finding

At `e053dfff`, `TrainingOrchestrator.run` loads all requested training seasons, then independently loads the calibration season and applies `game_date < cal_end_date` only to that second frame. Feature selection, fitting and copula estimation use the unbounded training frame. The historical feature store's training query filters season/minutes/position but has no training-date cutoff. [I6]

Consequences:
- The calibration population overlaps the supplied training population whenever `22025` is in both arguments.
- The calibration end date alone does not reserve later dates from model fitting.
- Exact March replay-row inclusion needs the original run population or source snapshot, which was not recovered. The old commands do not establish clean OOS status.
- This does not prove every future live prediction was invalid. It specifically invalidates treating those historical comparisons as independently protected evidence.

Time-series evaluation requires fitting information to precede evaluated observations. Selecting models/configurations on a finite evaluation sample can itself overfit even when base model fitting is separate.[7][8]

### Playoff year correction

The season-start year is not the calendar year of the postseason. The official NBA identifies game `0042400407` as June 22, 2025, and describes it as the conclusion of 2024–25.[12][13] The repository resolver also decodes the game ID's start year. Thus:

- `42023`: 2023–24 postseason, calendar 2024.
- `42024`: 2024–25 postseason, calendar 2025.
- `42025`: 2025–26 postseason, calendar 2026.

Therefore the handoff saying “train 42019–42023, calibrate 42024, 2025 fully OOS” cannot stand as written. The archived April 19 model sweep explicitly uses `2025-04-19` through `2025-06-17`, even naming its output `playoffs_sweep_2024`. Directory labels are not calendar truth. [I7]

Additionally, a winning BL configuration chosen on a season makes that season a selection set, not an untouched final test, even if it had been outside model fitting.[8]

## 3. Cohort and split recommendation

### Recommended cohort

Start with PTS/REB/AST regular-season targets only, preserve target/rolling thresholds 5/8, and keep playoff and play-in targets separate. Do not remove the last X games merely because results look worse. Report late-season/rest-heavy cohorts separately, and authorize any exclusion from pregame-observable evidence rather than retrospective losses. [I8,I9]

Keep `22023,22024` as the initial fitting seasons to obtain a clean comparison against `22025`; do not automatically reproduce March's inclusion of `22025` in both roles. A more recent final refit may include it, but that creates a new artifact and needs later validation. This is my proposed experimental design, not an owner decision.

The Odds API's player-prop archive begins May 3, 2023; therefore a complete 2022–23 regular season with this provider's historical prop features is not available from that archive. This makes 2023–24 a practical candidate start, but says nothing about our retained local coverage.[9]

### Concrete candidate windows

All intervals below are **start-inclusive/end-exclusive** and additionally filtered to the named regular-season population. July boundaries are bookkeeping boundaries, not claims about opening-night dates. These are preregistration candidates, conditional on accepted feature/source coverage, not instructions to launch.

| Role | Candidate window | Use |
|---|---|---|
| Fitting | `2023-07-01 <= date < 2025-07-01`, seasons `22023,22024` | Model parameters, feature selection and any internal chronological validation. Group folds by game/date, not independent player rows. |
| Calibration/diagnostic holdout | `2025-07-01 <= date < 2026-01-01`, season `22025` | Distribution/coverage diagnostics and only explicitly permitted fitted calibration. This is not authorization for global offsets. |
| Development/selection | `2026-01-01 <= date < 2026-03-01`, season `22025` | A small preregistered model/operating comparison. Source admissibility and split definitions are fixed before outcomes. |
| Locked retrospective evaluation | `2026-03-01 <= date < 2026-07-01`, season `22025` | Fixed-config comparison with complete dropout accounting. Do not call this historically untouched: March windows have already influenced old decisions. |
| New-season transfer diagnostic | `2026-07-01 <= date < 2026-11-03`, season `22026` | Includes actual eligible games through November 2; tests boundary/cold-start behavior. Short and not an independent promotion certificate. |
| Truly prospective validation | If the specification is frozen November 3: `2026-11-04 <= date < 2026-12-01` | Initial fixed-policy forward window. If freeze happens later, move the start to after the actual freeze before inspecting results. Extend if evidence/volume is insufficient; no automatic promotion at month-end. |

This avoids pretending a previously explored period is newly protected. If an actually untouched retrospective window can be demonstrated from experiment history, it can substitute after an explicit decision. No coverage assertion or expected sample count is attached to these dates.

For the first comparison, keep incumbent hyperparameters fixed. However, hyperparameters inherited from old tuning are part of historical development, not a fresh independent design. A later final refit can use more recent data after decisions are frozen; keep its identity separate from the benchmark artifact and evaluate it on later data. [I2–I4][7][8]

## 4. Season boundaries and cold starts

### Old behavior: producer resets, consumer fallback

Historical basic/advanced player producers group by `(player_id, season_id)` and team producers by `(team_id, season_id)`. They shift prior observations; the basic producer expressly leaves the first season game without prior values. The basic rolling window accepts partial history; it did not enforce a universal first-five/first-ten-game exclusion. [I5]

But the old consumer chooses the latest rolling row by entity/date without a season predicate. Missing values become zeros or fixed neutral values. Consequently the old model did **not** have one coherent offseason policy: available same-season rows reset, absent rows can fall through to prior-season snapshots, and nulls can become constants. [I6]

Current Slice 8 rejects carryover modes other than `current_season_only`; it also blocks required missing/default-dependent fields. Its accepted implementation does not already implement a Bayesian prior or decay policy. Selecting such a policy would be a new targeted scope, not a YAML-only switch. [I13]

### What research supports—and does not

Fearnhead and Taylor explicitly model NBA ability across years, shrinking toward a population mean and increasing uncertainty at season transitions; they assign new players a population prior. This supports investigating structured carryover, not silently copying last season's L5. It studies player impact, not our minutes/PTS/REB/AST model, so its fitted coefficients are not plug-in GameFlow defaults.[11]

Brown's baseball study finds empirical/hierarchical Bayes approaches improve on naive current-average extrapolation in its setting. It supports the statistical rationale for small-sample pooling, not a demonstrated NBA prop-betting advantage.[5]

### Recommended family policy

| Family | First baseline | Separately test later |
|---|---|---|
| Player rolling | Explicit current-season prior-game history; preserve 8-minute basic contribution contract; block missing required values. | Prior-season rate prior with effective sample size/age and explicit decay; separate stable rate information from volatile role/minutes. |
| Team context | Target team's current-season history; no accidental offseason row. | Prior-season team prior only with explicit roster/change semantics. |
| Opponent/position defense | Target opponent's current-season aggregates under approved valid-position contribution exclusions. | Separately validated opponent priors; do not transfer a traded player's former team's defense into new-team context. |
| Schedule | Reconstruct from actual schedule/appearances; do not treat stale prior-season `rest_days` as current. | Explicit capped/encoded offseason rest, if required and tested. |
| Game identity | Exact target game/team/opponent/home-away and as-of roster. | No statistical fallback for an unresolved identity. |
| Lines | Authenticated target-game quote at the policy cutoff. | No cross-game/season imputation masquerading as an observed quote. |
| Injuries | Complete accepted target-game/team edition as of cutoff. | Explicit missingness-aware/reduced-source model only as a separately approved variant. |

This control is intentionally narrower than an opening-night product. It may abstain on early-season/rookie rows. Report how much that excludes; do not call exclusion superior prediction. A rookie after enough valid within-season observations is not permanently excluded. An early-season model with priors needs separate train/serve-consistent validation, not a hidden fallback.

**Important implementation/policy distinction:** the current historical loader raises a blocking exception; it is not an automatic per-player abstention/export mechanism. A restricted cohort must be explicitly defined and exclusions accounted for rather than silently dropping exceptions. Also, `historical_loader.py:413-417` blocks nonpositive ratio denominators including `avg_fg3m_l5`. A genuine zero three-point average is observed data, not necessarily a cold start or failed source. This can block established non-shooters as well as rookies. Define structural zero versus missing history and zero-denominator ratio semantics before accepting that exclusion as intended policy. Any change is a narrow follow-up for explicit approval, not an automatic reopening of accepted Slice 8 code. [I13]

## 5. Trades and team transitions

The historical player producer groups on player and season, **not team**, so the player's within-season history follows him across a trade. Team/opponent queries use the target game's team identities. This establishes implemented semantics, not proof that an owner explicitly compared all trade policies. [I5,I6]

Recommendation:
- Retain individual pre-trade history within the same season for the control.
- Switch team pace, opponent defense, home/away and teammate injury context immediately to the target game's actual/as-of team.
- Never use today's roster to reconstruct an earlier trade date; preserve the September effective-dated roster direction. [I9]
- Treat minutes/usage change after a trade as a separate diagnostic stratum. “First few games with a new team” can be reported before choosing any special filter; do not derive an arbitrary exclusion from poor ROI.
- If the strata show meaningful errors, compare one targeted role-adaptation/prior variant on predeclared development data, with fixed source and cohort rules. Do not reset all player rates merely because the jersey changed.

Unknown: no recovered owner decision specifies a post-trade decay weight, minimum new-team appearances, or tested pre-/post-trade blend. Those would be new decisions.

## 6. Source eligibility and missingness

### Old model dependence is real

The retained regular selected-feature JSON contains game lines, prop-line features and injury features. The retained playoff JSON includes injury features but no `prop_line_*`, `line_spread` or `line_total` in the parsed selected-feature union. Thus quote-feature concerns do not affect those two artifacts identically; betting evaluation still requires eligible quotes for both. This was a JSON inventory, not a binary-load proof. [I14]

The historical training query took latest prop lines by snapshot without an as-of cutoff, grouped game lines with `MAX`, selected injury status by date/latest ID, and filled absent injury values with zero. These are concrete old source behaviors, not evidence of approved decision-time semantics. [I6]

### Separate archival reconstruction from actual operational replay

A historical-provider snapshot recovered today can legitimately describe what the provider recorded before a past decision. Its modern download/insert time is not the historical quote time. Conversely, it does not prove GameFlow actually fetched the quote then. Label these as different experiments.

The Odds API documents that a historical request returns the closest snapshot at or before the requested time. Save the returned snapshot timestamp, request parameters, payload and row mapping; use the returned timestamp rather than pretending the request is the exact observation time. Its documentation also warns historical errors can persist and market/book coverage begins only when added.[2]

Proposed quote eligibility:
- Known target game and authoritative timezone-aware commence.
- Authenticated historical route/payload or actual live capture.
- Snapshot/availability at or before the cutoff, with strict pre-commence checks and the approved strict-boundary semantics preserved.
- Explicit freshness limit; an update time before cutoff alone does not make a days-old quote fresh. The exact maximum age remains an owner/research decision, not a value invented here.
- Coexisting Over/Under sides for the same game, bookmaker, market, line and response/snapshot; preserve conflicts and rejection reasons.
- Do not independently maximize opposite sides or substitute today's quote/line.

Current producer ledgers record attempts, including empty/422 responses, not a binding of values to successful retained responses. The game-line producer likewise lacks a retained response receipt. External archives/logs may establish more, but none was newly audited here. An old diagnostic sample with props for 1/19 games is not a current whole-database coverage estimate. [I10]

### Injury evidence

NBA rules require injury reporting before games and continual updates; a date alone cannot identify the pre-decision state.[6] Official dated PDFs are a credible recovery candidate: two April 12 editions were already captured in the project, but full-window coverage, mapping and publication semantics remain unaccepted. [I10]

Do not require recovering every intraday edition merely for its own sake. For a decision at T-60, the necessary proof is an eligible complete edition for the game's teams, with approved timing/freshness and omission semantics. A complete sequence is required only where establishing “latest edition” or changes relies on that sequence. A consistent earlier-edition policy could be considered explicitly; never silently reconstruct it from latest-of-day records.

Treat the following separately:

| Observation | Meaning for features |
|---|---|
| Complete accepted report; rostered player omitted under accepted publisher rules | Eligible “not listed” status, not a medical claim of perfect health. |
| Team report not submitted / report missing / parsing failed | Unknown; never healthy by omission. |
| Player positively listed Out/Questionable/etc. in an eligible edition | Use the observed status with edition identity. |
| No RapidAPI row in a collapsed/incomplete archive | Unknown source coverage, not proof of availability. |
| Target-game DNP discovered after tip | Outcome/settlement information, not a pregame injury feature. |
| No eligible prop pair at cutoff | Quote dropout/no bet, not a fabricated neutral line or negative outcome. |

Under the currently approved strict policy, required unknown evidence blocks acceptance; there is no silent diagnostic fallback. A genuine reduced-feature model would require explicit approval, fitting without those inputs and its own identity—not zero-filling an old artifact. [I8,I10]

## 7. Proposed decision packet

Recommended now, pending Chase approval:
1. Regular-season control; separate play-in/playoff targets; no arbitrary last-X-games exclusion.
2. Candidate chronological windows above; explicit fit/calibration/development/test roles, no shared train/calibration population, no false untouched-OOS label.
3. Current-season-only baseline families and explicit early-season abstention; investigate shrinkage as a named next comparison, not implicit default.
4. Player history follows the player within season; target-team context changes at the trade; effective-dated roster authority.
5. Distinguish authenticated historical reconstruction from actual operational replay. Preserve strict source blocking, with explicit quote freshness and injury-edition semantics to approve.
6. Freeze fixed hyperparameters for the first policy comparison; no offsets or architecture expansion.

Before launch: approve the resource guard; accept the selected real source/cohort through the actual feature loader; prove the training route carries policy and date boundaries. Lifecycle historical mode is currently attach-only, so a historical trainer may need targeted wiring. These remain separate from completed Slice 8 coding. [I13]

## Relevant prior lessons/invariants

- `lessons/raw-timestamps-do-not-guarantee-temporal-integrity`: applied to the old latest-line queries and source clocks, not merely cited.
- `lessons/cheap-baseline-before-architecture`: first compare a coherent control, not a new hierarchical model suite.
- `lessons/feature-selector-is-not-an-ablation` and `lessons/correlated-feature-family-validation`: selected-feature inventory is dependence evidence, not proof a family helps. New prior/role families need controlled comparisons.
- `operations/critical-invariants`: empirical CDF; no global offsets; preserve Q10 and quantile order; SQL isolation; owner-launched long runs; local advanced scraping and Railway CDN-only.

## Internal evidence index

- **I1:** `e053dfff:docs/development_docs/2026-02-10_session21.md:187-190`; `decisions/model-architecture-decisions`.
- **I2:** `e053dfff:docs/development_docs/2026-03-17_session75.md:11-52,83-93`.
- **I3:** `e053dfff:docs/development_docs/2026-03-19_session78.md:9-57`.
- **I4:** `e053dfff:docs/development_docs/2026-03-23_session86.md:28-61,80-85`; `src/models/artifacts/production/run_config.json:1-12`.
- **I5:** `e053dfff:src/processing/populate_average_stats.py:209-245,504-530,687-712`.
- **I6:** `e053dfff:src/models/train_pipeline.py:73-123`; `e053dfff:src/models/feature_store.py:17,927-1034,1170-1264,1278-1344`.
- **I7:** `handoffs/handoff-030`; `76cf464f:brain/Models/NBA-Model.md`; `76cf464f:.thoughts.md:601-626,674-677,716`; `git show --stat 76cf464f`; `src/models/artifacts/production_playoffs/run_config.json:1-14`; `src/models/artifacts/production_playoffs/.source:1`; `src/models/features/nba/season.py:25-43`.
- **I8:** `handoffs/handoff-137`, `handoffs/handoff-138`; `docs/operations/nba-s8-completion-decisions-2026-11-02.md:13-31`; `docs/operations/nba-s8-family-policy-proposal-2026-11-02.md:1-37`.
- **I9:** `handoffs/handoff-127`; `docs/operations/nba-core-product-restart-plan.md`.
- **I10:** `docs/operations/nba-s8-producer-evidence-audit.md:5-27`; `docs/operations/nba-s8-source-policy-matrix-2026-11-02.md:15-50` (T-60/5/8 subsequently settled by I8); `docs/operations/nba-s8-source-recovery-2026-11-03.md:3-46`.
- **I11:** `models/calibration-guide`; `operations/critical-invariants`; `operations/hard-facts` (individual fact validation statuses preserved, not universally revalidated).
- **I12:** `e053dfff:docs/development_docs/2026-03-05_session65.md:40`; `e053dfff:docs/development_docs/2026-03-06_session67.md:38-56`.
- **I13:** `src/models/features/nba/historical_loader.py:175-195,395-417`; `docs/understanding/nba-s8-operator-workflow.md:42-54`; `docs/operations/nba-slice8-completion-status.md` latest integration heading supersedes earlier chronological status text.
- **I14:** `src/models/artifacts/production/selected_features.json`; `src/models/artifacts/production_playoffs/selected_features.json`. Parsed string-union inventory: regular 65 distinct feature names; playoff 66. Not a deserialized artifact proof.

Public sources below are mechanically rendered from the retrieval ledger. Public HTML/JSON/text was fetched without credentials; the CDN boxscore probe returned 403, so season/calendar corroboration uses the successfully fetched official game page and NBA season article instead. The initial extraction tool was unavailable; the reusable citation-skill fetcher recovered the cited pages. No failed probe is treated as source evidence.

Sources:
[2] https://the-odds-api.com/liveapi/guides/v4
[5] https://arxiv.org/abs/0803.3697
[6] https://official.nba.com/nba-injury-report-2026-27-season
[7] https://otexts.com/fpp3/tscv.html
[8] https://www.jmlr.org/papers/v11/cawley10a.html
[9] https://the-odds-api.com/historical-odds-data
[11] https://arxiv.org/html/1008.0705v1
[12] https://www.nba.com/game/0042400407
[13] https://www.nba.com/news/starting-5-june-22-2025
