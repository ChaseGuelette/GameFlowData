# Agent, GBrain, Documentation, and Knowledge-Workflow Audit

**Audit lane:** 09 — agent/knowledge workflow
**Audit date:** 2026-07-18
**Mode:** Read-only repository and remote-canonical GBrain audit; this report is the only file written
**Scope:** agent contracts, source-of-truth routing, remote canonical GameFlowBrain, handoffs, lessons/Hard Facts, audit/register workflow, selected relevant `.hermes` plans, SQL/implementation-worker boundaries, source qualification, graph/orphan hygiene, and validation/documentation claims
**Excluded:** DB/production access, Supabase MCP, GBrain writes, sync/embed/backfill/link/dream/cycle actions, skills/config/memory changes, plan/card/register creation, implementation, deployment, training, backtests, and broad source-code review

## Executive assessment

GameFlow has strong individual controls but a weakly reconciled control plane. SQL isolation, scoped worker review, remote-canonical GBrain routing, candidate-first debt adjudication, atomic lessons, source-qualified sync, and zero-orphan graph hygiene are each documented. The main risk is that those controls are spread across several documents and Hermes skills whose current-state claims, thresholds, destinations, and authority labels have drifted.

The highest-impact findings are:

1. `Execution-Plan.md` is still designated as the current roadmap even though its active phase and next-step text are months behind the latest handoff and contradict its own completed-phase table.
2. The migration checklist and brain index retain transition-era instructions that point to the old checked-in `brain/` tree and obsolete Claude/Haiku roles, while current contracts say the remote `/home/chase/GameFlowBrain` source is canonical.
3. The census program and lane reports are currently untracked local artifacts while the tracked register is supposed to receive only independently adjudicated findings; the evidence needed for that adjudication is therefore not yet portable or durable.
4. The planned register schema and the actual tracked register template disagree, so promotion can discard confidence, source-report provenance, validation state, priority, type, and effort.
5. Lesson retrieval is mandatory in the Hermes/repository agent contract but absent from the parallel Claude contract, and one active high-priority lesson is already beyond its own review date.
6. `operations/hard-facts` is mechanically privileged by the resume skill even though the page says every fact remains `needs-chase-validation` and contains transition-era local topology facts.
7. The implementation-worker threshold and provider handoff differ between the repository contract and the live Hermes skill.
8. Current GBrain content/graph integrity is good, but source freshness is not: read-only health reported full embeddings and zero orphans while doctor remained unhealthy because all four non-default sources were stale and no source had completed a full cycle.

This report separates repository/documentation debt from GBrain/Hermes operational debt. It does not promote any candidate into the register or authorize implementation.

## Method and evidence boundaries

### Local repository evidence

Read and reconciled:

- `AGENTS.md` and `CLAUDE.md`;
- the preloaded `gameflow-explore` guidance;
- `.hermes/audits/tech-debt/README.md` and report 00;
- the census program, GBrain weekly code-source refresh plan, remote kanban validation plan, comprehension program, selected god-class/stat-suite plans, and existing audit precedents;
- `docs/understanding/{README,agent-workflow,system-atlas,glossary,tech-debt-register}.md`;
- `ACTIONITEMS.md`;
- live Hermes skills `gameflow-resume`, `gameflow-wrap-up`, `gameflow-sql-runner`, and `gameflow-implementation-worker` from `C:/Users/Chase/AppData/Local/hermes/skills/gameflow/`.

A tracked-file check returned 62 tracked files across `.hermes/plans`, `.hermes/audits/tech-debt`, and `docs/understanding`; for the four primary census artifacts checked explicitly, only `docs/understanding/tech-debt-register.md` was tracked. The current working tree showed the audit directory and census plans as untracked rather than committed project history.

### Remote canonical GBrain evidence

All remote inspection was read-only over SSH to `chase@gameflow-agent:/home/chase/GameFlowBrain`. Selected canonical files were read with numbered lines, including:

- `schema.md`, `RESOLVER.md`, `Execution-Plan.md`, `BRAIN-INDEX.md`, and `.gbrain-source`;
- `Operations/{Critical-Invariants,Hard-Facts,GBrain-Operating-Standard,Hermes-GBrain-Migration-Checklist,Known-Issues,Operations}.md`;
- `Handoffs/Handoffs.md` and handoffs 106-110;
- `Lessons/Lessons.md` and all nine lesson pages.

At `2026-07-18 16:35:44 -0400`, source-qualified read-only CLI checks reported:

- 223 pages, 1,003 chunks, 1,003 embedded chunks, 286 links, and 194 timeline entries;
- zero orphans across 223 linkable pages;
- brain score 98/100 and 100% embedding coverage;
- doctor status `unhealthy`, score 50, because `sync_freshness` and `cycle_freshness` failed;
- source counts: `gameflow` 193 pages, three code sources totaling 30 pages, and `default` zero pages;
- remote GameFlowBrain Git clean on `master...origin/master`, latest commit `7fcb29a` dated 2026-07-16.

No sync, embedding, graph backfill, link extraction, dream/cycle, DB query, or production action was run.

## Current authority map observed

| Subject | Intended primary authority | Supporting evidence | Drift observed |
|---|---|---|---|
| Project knowledge | remote `gameflow` markdown source | `AGENTS.md:9-14`; resume skill `:66,121-130` | transition checklist and brain index still point to old checked-in `brain/` |
| Current roadmap | `execution-plan` | resume skill `:50-52`; `AGENTS.md:14` | roadmap effective/current content is stale and internally contradictory |
| Critical model rules | `operations/critical-invariants`, atomic lessons | `AGENTS.md:47-56`; resume skill `:94-110` | parallel `CLAUDE.md` lacks lesson retrieval enforcement |
| Structured facts | `operations/hard-facts` | resume skill `:104-110` | all facts remain validation-gated; some topology facts are stale |
| Session continuity | newest typed handoff plus hub | resume skill `:112-130` | current hub is coherent through handoff 110; no continuity break found |
| DB truth | delegated SQL runner | `AGENTS.md:39-41`; SQL skill `:17,36-44` | control is consistent; no missing-isolation finding |
| Large implementation | implementation-worker from precise spec | `AGENTS.md:58-65`; worker skill `:78-116` | threshold/provider launch contract differs |
| Debt candidates | lane reports, then independent adjudication, then register | audit README `:13-30`; census plan `:105-124` | reports/program are untracked and register schema is incompatible |
| Graph health | source-qualified graph/backfill plus zero orphans | wrap-up skill `:85-105,201-210` | current orphans are zero; source freshness/cycle health is not clean |

## Findings summary

| ID | Severity | Confidence | Ownership domain | Debt class |
|---|---|---|---|---|
| AKW-01 | High | High | GBrain project documentation | GBrain operational debt |
| AKW-02 | High | High | GBrain taxonomy + Hermes routing | GBrain/Hermes operational debt |
| AKW-03 | High | High | GameFlowData audit program | Repository/process debt |
| AKW-04 | High | High | GameFlowData debt register | Repository/documentation debt |
| AKW-05 | High | High | GameFlowData agent contracts + GBrain lessons | Split repo/GBrain debt |
| AKW-06 | High | High | GBrain facts/retrieval policy | GBrain operational debt |
| AKW-07 | Medium | High | Hermes implementation-worker skill + repo contract | Hermes operational debt |
| AKW-08 | High | High | Remote GBrain maintenance | GBrain/Hermes operational debt |
| AKW-09 | Medium | High | Legacy repo/GBrain trackers | Split repo/GBrain debt |
| AKW-10 | Medium | High | GameFlowData understanding docs | Repository/documentation debt |

---

## Detailed findings

### AKW-01 — The canonical current roadmap is stale and internally contradictory

**Severity:** High
**Confidence:** High
**Ownership domain:** GBrain project documentation
**Debt classification:** GBrain operational debt

**Exact evidence**

- `AGENTS.md:11-14` identifies GBrain as the first lookup and `execution-plan` as the current roadmap/build order.
- The resume skill routes current-roadmap questions to `execution-plan` as primary authority and MLB status to `models/mlb-model` plus `execution-plan` (`C:/Users/Chase/AppData/Local/hermes/skills/gameflow/gameflow-resume/SKILL.md:43-54`).
- Remote `/home/chase/GameFlowBrain/Execution-Plan.md:3-8` labels the page `status: active` with `effective_date: 2026-03-26`.
- `Execution-Plan.md:12-30` calls MLB inference/modeling the current phase, marks feature audit and training complete, and then says the “Next step” is to continue that same feature audit and model training.
- `Execution-Plan.md:86-97` still describes Phase 5 work as not started, including MLB backtesting, prediction storage, daily runner, and deployment preparation that later handoffs treat as established lanes.
- `Execution-Plan.md:208-223` still recommends remaining March-era feature expansion, dashboard, MLB, NCAAB, and cleanup tasks rather than the July audit/adjudication program.
- Latest remote `/home/chase/GameFlowBrain/Handoffs/handoff-110.md:31-52` instead records completion of remote kanban validation and recommends executing the tech-debt audit program before creating approved debt cards.

**Concrete failure mode**

A resume or planning agent following the documented authority map can present obsolete work as the current priority, duplicate already-completed MLB pipeline work, or recommend DB/deployment/model actions before the active audit/adjudication gate. Because this is the named canonical roadmap, the problem is not merely historical prose: correct routing leads to incorrect current-state output.

**Mitigation**

Reconcile `Execution-Plan.md` against the latest accepted handoff and current tracked artifacts. Keep historical phase completion as dated history, but add a single current phase, current gate, last-verified handoff, and explicit supersession rule. Do not use handoffs as a second timeless roadmap; use them only to update the canonical plan.

**Safe evidence step**

Read-only reconcile handoffs 106-110, the current local audit program, and targeted artifact/file existence. Produce a proposed phase-status diff for Chase review without running SQL, training, deployment, or GBrain sync.

**Done condition**

`execution-plan` has a current review date, names the active audit/adjudication gate, contains no self-contradictory “complete/in progress” state, distinguishes historical completed phases from current work, and agrees with the latest accepted handoff on the next decision.

---

### AKW-02 — Transition-era authority and agent-role pointers remain active inside canonical GBrain

**Severity:** High
**Confidence:** High
**Ownership domain:** GBrain taxonomy and Hermes routing
**Debt classification:** GBrain/Hermes operational debt

**Exact evidence**

- Current repository authority is explicit: remote canonical brain path and source are `/home/chase/GameFlowBrain` and `gameflow` (`AGENTS.md:9-14`).
- The resume skill also states that remote canonical markdown fallback is `/home/chase/GameFlowBrain`, not `GameFlowData/brain` (`gameflow-resume/SKILL.md:112-130`).
- The same resume skill still opens with “the checked-in markdown brain remains canonical during transition” (`gameflow-resume/SKILL.md:15-19`), leaving two authority statements in one live skill.
- Remote `/home/chase/GameFlowBrain/Operations/Hermes-GBrain-Migration-Checklist.md:1-5` remains `status: draft`.
- That checklist says “Existing `brain/` markdown remains canonical” and tells agents to treat `GameFlowData/brain/` as the source of truth during migration (`Hermes-GBrain-Migration-Checklist.md:16-23`).
- Remote `/home/chase/GameFlowBrain/BRAIN-INDEX.md:29-36` still routes status through the migration checklist and documents old Claude Opus/Haiku explorer and SQL-runner roles.
- In contrast, the checklist’s later execution log says Phase 4 rewrote active instructions around Hermes skills and preserved current SQL/file/worker boundaries (`Hermes-GBrain-Migration-Checklist.md:922-950`).

**Concrete failure mode**

A migration/status query can correctly route to a canonical page and still receive obsolete instructions to read or write the checked-in `brain/` tree or use retired role/model terminology. That can create divergent handoffs, stale local-brain writes, wrong delegation assumptions, and apparent GBrain readback failures when the agent inspects the wrong host/source.

**Mitigation**

Convert the migration checklist from an active draft/status authority into a completed historical migration record with a current-state banner. Replace old `brain/` authority text and role names with the remote `gameflow` route and current skill/profile roles. Make the resume skill’s opening authority sentence agree with its remote fallback rule.

**Safe evidence step**

Run a read-only pointer inventory over canonical hubs, migration/status pages, and active skills for `GameFlowData/brain`, “checked-in brain remains canonical,” Claude Haiku/Opus role routing, and local-Windows fallback language. Classify each occurrence as historical quotation or active instruction.

**Done condition**

No active authority/status page or live skill directs normal work to `GameFlowData/brain`; historical references are clearly labeled; one remote canonical path/source rule is used consistently; and current role names map to actual Hermes skills/profiles rather than retired Claude model roles.

---

### AKW-03 — Audit evidence is local and untracked while adjudication depends on it

**Severity:** High
**Confidence:** High
**Ownership domain:** GameFlowData audit program
**Debt classification:** Repository/process debt

**Exact evidence**

- Audit workers are required to write only their assigned report, findings remain candidates, and no plans/cards are created until independent review (`.hermes/audits/tech-debt/README.md:13-30`).
- The census program requires report 00 first, parallel domain reports, reconciliation, and only then reviewed register/card/plan promotion (`.hermes/plans/2026-07-14_210940-gameflow-tech-debt-census-and-agent-execution-program.md:105-124,178-218`).
- The repository’s tracked register calls itself the current source of truth and says candidates require validation before implementation (`docs/understanding/tech-debt-register.md:3-12`).
- A scoped `git ls-files` check across the four primary artifacts listed only `docs/understanding/tech-debt-register.md`; the census program, audit README, and report 00 were not tracked. The working-tree inventory showed `.hermes/audits/tech-debt/` and the census plans as untracked.
- Remote handoff 110 says the clean remote checkout is ready to execute the audit plan and then create approved cards (`/home/chase/GameFlowBrain/Handoffs/handoff-110.md:31-52`), but the local audit evidence is not yet in remote canonical or tracked repository history.

**Concrete failure mode**

Independent reviewers or remote workers can see the tracked register but not the evidence reports that determine whether entries should be accepted, rejected, merged, or superseded. A machine loss, cleanup, branch switch, or remote execution session can drop the only evidence trail. Promotion may then rely on summaries or handoff prose rather than exact findings, recreating duplicate plans/cards or accepting unreviewed claims.

**Mitigation**

Add an explicit artifact-durability checkpoint to the audit lifecycle: after each report passes its report-only validation, place the complete report set and census program under tracked review before adjudication. Keep “tracked for review” distinct from “accepted into register”; tracking a candidate report must not imply approval.

**Safe evidence step**

After all lanes finish, run only `git ls-files`, `git status --short`, and scoped diffs for `.hermes/audits/tech-debt/` and the census plan. Inventory missing/extra lane reports and confirm no implementation, register, card, or unrelated files are bundled.

**Done condition**

Every report used in adjudication and the governing census program is durably available from one reviewed repository revision or an explicitly documented canonical artifact store; remote reviewers can retrieve the exact same bytes; and register promotion cites that immutable report revision.

---

### AKW-04 — The live debt register cannot preserve the audit program’s required provenance and review state

**Severity:** High
**Confidence:** High
**Ownership domain:** GameFlowData debt register
**Debt classification:** Repository/documentation debt

**Exact evidence**

- The census plan requires each candidate to carry ID, type, severity, priority, impact, evidence, validation status, proposed action, suggested lane, effort, dependencies, confidence, and source reports (`.hermes/plans/2026-07-14_210940-gameflow-tech-debt-census-and-agent-execution-program.md:144-192`).
- The same plan requires cross-report deduplication, existing-plan reuse, validation classification, and explicit human approval before cards/plans (`:194-218`).
- The actual register template contains only area, category, severity, files, problem, impact, and proposed fix (`docs/understanding/tech-debt-register.md:33-54`).
- The register’s status vocabulary is implemented/planned/current, while audit findings are candidates pending independent review (`tech-debt-register.md:6-12`; `.hermes/audits/tech-debt/README.md:15-29`).
- The generic agent workflow sends evidence-backed migration or debt into the register or `.hermes/plans` without naming the intervening audit/adjudication state (`docs/understanding/agent-workflow.md:177-190`).

**Concrete failure mode**

Promotion into the current template can silently discard who found a debt item, which report/evidence revision supports it, whether it was independently reproduced, what competing tracker it supersedes, why its priority was chosen, and whether the implementation plan is existing or proposed. Later agents cannot distinguish an accepted finding from an old candidate or reconstruct why two similar entries were merged.

**Mitigation**

After adjudication—not during this audit—extend the register contract to preserve the census fields that affect routing and trust. Add explicit candidate/accepted/rejected/superseded states or keep candidates solely in immutable reports and store only accepted items in the register with report citations and adjudication date.

**Safe evidence step**

Take three representative findings from different lanes and perform a paper-only loss analysis: map every census field into the current template and list which values cannot be represented. Do not edit the register until Chase selects the lifecycle model.

**Done condition**

Every accepted register item records immutable source-report evidence, confidence, validation/reproduction state, ownership/lane, priority/type/effort/dependencies, adjudication status/date, and existing-plan/card links; rejected and merged candidates remain traceable without appearing as active work.

---

### AKW-05 — Lesson retrieval enforcement differs by agent entrypoint, and freshness metadata is not enforced

**Severity:** High
**Confidence:** High
**Ownership domain:** GameFlowData agent contracts and GBrain lesson corpus
**Debt classification:** Split repository/GBrain debt

**Exact evidence**

- `AGENTS.md:47-56` mandates a specific modeling/architecture retrieval sequence: Hard Facts and critical invariants, `list_pages(tag='lesson')`, keyword lesson search, canonical decisions/models, recent handoffs, and a “Relevant prior lessons/invariants” output section.
- The resume skill independently enforces the same order and says the success metric is applying/citing the lesson, not merely retrieving it (`gameflow-resume/SKILL.md:94-110`).
- `CLAUDE.md:1-81` mirrors most always-loaded safety, source, SQL, worker, and wrap-up rules but has no lesson-listing, lesson-keyword, or required prior-lessons output rule.
- Remote `/home/chase/GameFlowBrain/Lessons/Lessons.md:21-35` links nine atomic lessons and says pages past `review_after` must be treated as candidate context rather than blindly current truth.
- `/home/chase/GameFlowBrain/Lessons/Quote-Clean-CLV-Before-Feature-Work.md:3-10` remains `status: active`, was last reviewed 2026-05-14, and had `review_after: 2026-06-15`—already past at audit time.
- Some lesson evidence still includes unqualified “Session memory” rather than only durable canonical sources (`Lessons/Correlated-Feature-Family-Validation.md:34-39`; `Lessons/Feature-Selector-Is-Not-An-Ablation.md:34-39`).

**Concrete failure mode**

Hermes agents are required to retrieve prior lessons, while Claude Code can make the same model/promotion recommendation without that pass. Hermes can also retrieve an `active` lesson and apply it even though the hub’s own freshness contract says it is now candidate-only. This creates agent-dependent architecture recommendations and makes stale lesson status indistinguishable from reviewed policy.

**Mitigation**

Put the minimum mandatory lesson protocol in both always-loaded repo contracts or generate both from one policy fragment. Add a read-only freshness guard/lint that reports active lesson pages whose `review_after` has passed. Review or explicitly downgrade expired lessons; replace session-memory-only evidence with durable source pages where possible.

**Safe evidence step**

Run a no-write retrieval conformance test against the same model-change prompt through each agent entrypoint. Record routes, returned lesson slugs, freshness flags, and final citations. Separately list active lessons with expired `review_after` and inspect only their linked source pages.

**Done condition**

All supported GameFlow agent entrypoints enforce the same list-plus-keyword lesson retrieval and output requirement; expired lessons cannot be presented as current authority without a warning/review; all active lessons have current review metadata and durable source links; and an end-to-end fixture proves the relevant lesson changes or constrains the recommendation.

---

### AKW-06 — Mechanically privileged Hard Facts are entirely validation-gated and include stale topology

**Severity:** High
**Confidence:** High
**Ownership domain:** GBrain facts and retrieval policy
**Debt classification:** GBrain operational debt

**Exact evidence**

- The resume skill says Hard Facts are “mechanically privileged” and must be read before model/architecture recommendations (`gameflow-resume/SKILL.md:94-110`).
- Remote `/home/chase/GameFlowBrain/Operations/Hard-Facts.md:15-22` says the page is privileged but every fact remains `needs-chase-validation` until Chase confirms it.
- All listed facts GF-F001 through GF-F017 carry `status: needs-chase-validation` (`Hard-Facts.md:23-56`).
- GF-F016 and GF-F017 describe local WSL GBrain/MCP topology and an old MCP registration assumption (`Hard-Facts.md:53-56`).
- Current authority instead names the remote canonical brain (`AGENTS.md:9-14`), and latest remote handoff 110 validates the `gameflow-agent` workstation/kanban path (`/home/chase/GameFlowBrain/Handoffs/handoff-110.md:31-52`).
- The read-only source list showed `gameflow` bound to `/home/chase/GameFlowBrain`, three separate code sources, and an empty `default` source; this current topology is not represented in GF-F016/F017.

**Concrete failure mode**

The retrieval policy elevates facts whose own page says they are unconfirmed. An agent can turn a validation candidate into a definitive model or infrastructure claim, especially for source routing, role choice, DB behavior, or deployment assumptions. Volatile topology facts can outlive a workstation migration and direct troubleshooting toward the wrong host/service.

**Mitigation**

Separate stable reviewed invariants from pending extracted facts and volatile environment topology. Either validate individual facts with durable sources and dates or change retrieval so `needs-chase-validation` facts are returned with an explicit warning and cannot outrank confirmed canonical pages. Move live endpoint/source topology to an operational status page or generated readback, not a durable “hard fact.”

**Safe evidence step**

Read-only review each GF-F item against its cited source page and current remote source/health output. Produce a validation matrix for Chase; do not bulk-promote, modify GBrain, or infer validation from mere repetition across docs.

**Done condition**

No mechanically privileged fact is silently presented as confirmed while marked `needs-chase-validation`; confirmed facts have reviewer/date/source provenance; stale topology facts are retired or moved to a volatile status contract; and retrieval output preserves validation state.

---

### AKW-07 — The implementation-worker selection and provider handoff contract has two live versions

**Severity:** Medium
**Confidence:** High
**Ownership domain:** Hermes implementation-worker skill and repository agent contract
**Debt classification:** Hermes operational debt

**Exact evidence**

- `AGENTS.md:58-65` and `CLAUDE.md:52-59` select the worker lane when changes exceed roughly 20 lines, touch two or more implementation files, or already have a precise spec; both say Codex Spark first with OpenRouter Codex/OpenCode GLM fallback.
- The implementation-worker skill describes a provider-agnostic OpenCode worker in frontmatter, while its overview says Codex Spark is current first choice (`gameflow-implementation-worker/SKILL.md:1-17`).
- The skill’s actual use threshold is roughly 50 lines, two files, or an existing spec (`:50-57`).
- Its executable core workflow writes a `.Codex/glm_spec_*` and directly documents OpenCode/GLM attachment and server commands (`:78-116,125-135`), not a Codex Spark first-path command.
- Scoped diff review and focused validation are consistently required in both contracts (`AGENTS.md:64-65`; worker skill `:105-116,183-190`).

**Concrete failure mode**

A 25-49 line approved change can be delegated under the repo contract but edited directly under the live skill. A worker selected as “Codex Spark first” can receive only an OpenCode/GLM launch recipe, leading to ad hoc provider substitution, unnecessary main-context source reads, inconsistent approval expectations, or non-reproducible handoffs between agents.

**Mitigation**

Define one small-edit threshold and one provider-neutral worker handoff schema. Keep provider launchers as explicit ordered adapters (Codex Spark, OpenRouter Codex, OpenCode GLM, direct fallback) rather than embedding one provider in the core procedure/spec filename. Preserve the existing approval, exact-file scope, diff-review, and validation gates.

**Safe evidence step**

Table-test representative scopes (one 30-line file, two 10-line files, a precise 15-line spec, a 100-line multi-file change) against AGENTS, CLAUDE, and the skill. Record whether each route selects direct edit or which worker; no worker needs to be launched.

**Done condition**

The repo contracts and live skill produce the same lane/provider order for each fixture; the core spec is provider-neutral; every launcher consumes the same exact files/invariants/validation contract; and scoped diff plus real validation remain mandatory before success claims.

---

### AKW-08 — Graph integrity is clean, but source freshness and cycle health are not represented as current operational state

**Severity:** High
**Confidence:** High
**Ownership domain:** Remote GBrain maintenance
**Debt classification:** GBrain/Hermes operational debt

**Exact evidence**

- The operating standard requires one source-qualified weekly code refresh, exact source-scope checks, retrieval probes, doctor, and visible logs; it records the job as installed and smoke-tested successfully (`/home/chase/GameFlowBrain/Operations/GBrain-Operating-Standard.md:84-104`).
- The same standard says each new/changed high-value page requires exact readback and fallback documentation (`GBrain-Operating-Standard.md:43-52`).
- The local weekly refresh plan remains a proposal and correctly distinguishes code `reindex-code` from markdown sync (`.hermes/plans/gbrain-weekly-code-source-refresh.md:3-14,143-181`).
- Read-only current health showed 223/1,003 pages/chunks with 100% embeddings, zero orphans, and brain score 98/100.
- The same doctor run returned `status: unhealthy`, score 50: `gameflow-code-mlb-pilot` and `gameflow-code-mlb-backtests` were last synced three days earlier, `gameflow-code-modeling-core` 27 days earlier, and markdown `gameflow` 55 hours earlier. `cycle_freshness` failed because none of the four non-default sources had completed a full cycle.
- Remote Git itself was clean at commit `7fcb29a`, so the failure is not an uncommitted-markdown suspicion.

**Concrete failure mode**

Agents can see healthy embeddings/orphans and assume retrieval is current even while source content is outside the doctor freshness SLA. Code lookup can return old symbols or omit recent work; markdown can lag local audit state; and prose saying the weekly job was installed/tested can be mistaken for evidence that it is currently succeeding. Conversely, treating the entire brain as broken would also be wrong because content, embedding, routing, and orphan integrity are currently strong.

**Mitigation**

Expose current source-qualified freshness separately from durable operating-policy prose. Define per-source freshness/cycle expectations and whether a never-run full cycle is required or an accepted exemption. Verify timer execution/logging and exact source-scoped retrieval in a separately approved maintenance task; do not auto-run broad refresh/cycles during routine audits.

**Safe evidence step**

Read-only inspect the remote timer/service status and the last bounded log for `gbrain-code-source-refresh`, then compare each source’s `last_sync_at` to its configured SLA and run one source-scoped retrieval fixture per source. Do not invoke sync, reindex, dream, or cycle in the evidence pass.

**Done condition**

Doctor freshness is healthy or each intentional exception is documented with owner/SLA; the timer has recent successful execution evidence; source-qualified retrieval fixtures return current known symbols/pages without cross-source contamination; cycle requirements are explicit; and zero-orphan/full-embedding status remains intact.

---

### AKW-09 — Legacy trackers remain plausible authorities and one canonical pointer is broken

**Severity:** Medium
**Confidence:** High
**Ownership domain:** Legacy GameFlowData trackers and GBrain Known Issues
**Debt classification:** Split repository/GBrain debt

**Exact evidence**

- Root `ACTIONITEMS.md:1-26` opens as “GameFlowData — Roadmap,” has no archival/superseded banner, and presents March 25 session tasks as remaining action items.
- The file is 3,154 lines of session summaries and action lists; report 00 classifies it as historical, not a current source of truth (`.hermes/audits/tech-debt/00-existing-inventory-reconciliation.md:256-268`).
- The tracked debt register calls itself the current source of truth (`docs/understanding/tech-debt-register.md:3-12`).
- Remote `/home/chase/GameFlowBrain/Operations/Known-Issues.md:49-56` says final verification comes from current repository state but points to `.hermes/plans/tech-debt-issues.md`, a path that does not exist in the scoped plans inventory.
- `Known-Issues.md:20-36` retains migration-era issue status such as the old Claude task-output and OpenCode wrapper issues without an explicit supersession/current-review date.
- Report 00 already identifies ACTIONITEMS, Known Issues, the register, migration plans, and other ledgers as overlapping inventories rather than independent authority (`00-existing-inventory-reconciliation.md:12-31,256-295`).

**Concrete failure mode**

A filename- or keyword-driven agent can treat the root “Roadmap” as current, follow March action items, or attempt to read a nonexistent tech-debt plan from a canonical operations page. Duplicate items may be reopened or planned again because the historical tracker lacks a clear forward pointer and the broken GBrain pointer cannot route to the candidate reports/register.

**Mitigation**

Preserve historical content but add explicit supersession metadata/banner and one forward pointer. Correct or retire the nonexistent plan pointer after the audit lifecycle’s canonical destinations are approved. Keep GBrain Known Issues as an operations routing page, not a second detailed debt register.

**Safe evidence step**

Run a read-only link/path validation across named repo/GBrain trackers and classify every pointer as current, historical, missing, or proposed. Compare top open items against the register and candidate reports without creating plans/cards or changing statuses.

**Done condition**

Every legacy tracker is visibly archival at the top, links to the approved current register/audit process, and cannot be mistaken for a current roadmap; every active GBrain pointer resolves; Known Issues routes rather than duplicates detailed accepted debt; and duplicate items retain explicit supersession links.

---

### AKW-10 — Secondary workflow docs claim source-routing completion without the exact source/fallback contract

**Severity:** Medium
**Confidence:** High
**Ownership domain:** GameFlowData understanding documentation
**Debt classification:** Repository/documentation debt

**Exact evidence**

- The GBrain code-source refresh plan requires output to name the refreshed source IDs, exact command family, source-scoped verification, direct-read fallback, and next gate (`.hermes/plans/gbrain-weekly-code-source-refresh.md:127-139`).
- That plan’s progress log claims `docs/understanding/agent-workflow.md` was updated to explain source routing and direct-read fallback (`gbrain-weekly-code-source-refresh.md:298-306`).
- The actual workflow doc says only that code sources are source-routed and direct page readback can fail despite successful source-scoped retrieval (`docs/understanding/agent-workflow.md:68-73`). It does not name `gameflow`, the remote canonical host/path, `source_id='__all__'` fallback, or the rule against local Windows discovery when MCP is remote.
- The actual exact route exists elsewhere: `AGENTS.md:9-14` and the resume skill’s remote rule/fallback sequence (`gameflow-resume/SKILL.md:58-76,112-130`).
- `docs/understanding/README.md:43-51` tells readers to use GBrain first but likewise omits the remote/source-qualified route.

**Concrete failure mode**

A human or agent using the standalone understanding docs can know that “source routing matters” but still search a stale local checkout, use the empty `default` source, interpret direct `page_not_found` as server failure, or omit the exact source/fallback from a validation claim. The plan’s completion statement overstates what the durable user-facing workflow doc actually records.

**Mitigation**

Add a concise source-routing contract to the understanding layer: canonical source ID, canonical host/path, route selection based on active MCP config, remote-aware direct-read fallback, and requirement to label fallback evidence. Link to the live skill for command detail rather than copying its large troubleshooting body.

**Safe evidence step**

Give a reviewer only `docs/understanding/README.md` and `agent-workflow.md` and ask them to identify the canonical source ID/path and direct-read fallback. Record whether the answer is deterministic; then compare it to AGENTS and the resume skill.

**Done condition**

The standalone docs deterministically identify the remote `gameflow` source and safe direct-read fallback, contain no local-authority ambiguity, and the weekly-plan completion claim is backed by the exact documented contract rather than a general caveat.

## Cross-cutting lifecycle observations

### What is already strong

- **SQL isolation:** The repository and both major agent contracts prohibit direct main-context Supabase access (`AGENTS.md:39-41`; `CLAUDE.md:39-41`). The SQL skill repeats main-context isolation, SELECT-only default, file-only exploration, and independent count verification before destructive DB-adjacent work (`gameflow-sql-runner/SKILL.md:15-18,34-44`).
- **Implementation review boundary:** Despite selection/provider drift, exact-file specs, user approval, scoped diffs, invariant review, and focused validation are clearly required (`gameflow-implementation-worker/SKILL.md:78-116,183-190`).
- **Handoff continuity:** `Handoffs/Handoffs.md:81-98` indexes handoffs 106-110 in sequence, and handoff 110’s content agrees with the remote Git tip. No missing latest-handoff pointer was found.
- **Lesson graph shape:** Nine atomic lessons exist, all are linked from the Lessons hub (`Lessons/Lessons.md:21-31`), and current orphan count is zero.
- **Graph/orphan hygiene:** Current zero-orphan and full-embedding results reject a broad “GBrain graph is broken” claim. The actionable issue is freshness/cycle state, not orphan repair.
- **Source separation:** Current source inventory has an empty `default`, 193-page `gameflow`, and three distinct code sources; doctor reports no cross-source slug drift and source routing healthy. The problem is stale active pointers and freshness, not missing source separation.

### Audit-to-register lifecycle risk

The designed sequence is sound: inventory/reconcile → candidate reports → independent review → deduplicate/merge → accepted register entries → approved plans/cards. The operational gap is durability and schema compatibility, not an absence of process. AKW-03 and AKW-04 should therefore be resolved before mass promotion; otherwise the audit can produce high-quality evidence that the register cannot preserve and remote reviewers cannot retrieve.

### Ownership split

**GameFlowData repository debt** includes untracked audit artifacts, incompatible register schema, stale understanding docs, the unmarked ACTIONITEMS roadmap, and asymmetric AGENTS/CLAUDE lesson controls.

**GBrain/Hermes operational debt** includes the stale canonical execution plan, transition-era migration/index pointers, unvalidated privileged Hard Facts, lesson freshness, worker-skill contract drift, and stale source/cycle health.

Items spanning both domains should not be “fixed everywhere” independently. Choose the authority first, then make secondary docs/skills point to it and add conformance checks.

## Rejected suspicions

These hypotheses were investigated and are not supported strongly enough to register as findings:

1. **“SQL isolation is missing.” — Rejected.** The rule is explicit and consistent in AGENTS, CLAUDE, the SQL skill, and the remote migration/operating docs. No DB or Supabase MCP was called in this audit.
2. **“The latest handoff is missing or the handoff hub is broken.” — Rejected.** The hub is sequential through 110, remote Git is clean at the matching handoff commit, and resume has a deterministic typed-handoff fallback procedure.
3. **“GBrain graph/orphan hygiene is currently broken.” — Rejected.** Read-only health reported zero orphans, 100% embeddings, and brain score 98/100. Freshness/cycle failures are real but are a different control.
4. **“The lesson corpus is absent or unlinked.” — Rejected.** Nine atomic lesson pages exist and the hub links all nine. Freshness and entrypoint enforcement remain findings.
5. **“Implementation workers can self-certify without review.” — Rejected.** The worker skill requires user approval, exact scope, scoped diff review, and focused validation. The identified debt is inconsistent lane/provider selection.
6. **“All source qualification is missing.” — Rejected.** AGENTS and the resume/wrap-up skills contain strong exact remote/source routing. The gap is stale canonical transition text and incomplete secondary understanding docs.
7. **“The remote canonical GameFlowBrain has uncommitted markdown.” — Rejected.** Remote `git status --short --branch` was clean on `master...origin/master`.
8. **“Candidate reports have already created duplicate plans/cards.” — Not observed.** The audit README and census plan prohibit this, and this lane created only its assigned report. The risk is future promotion without durable evidence/schema, not an observed card write.

## Coverage gaps and unverified claims

- No DB, production, deployment, Railway, Supabase, Vercel, Stripe, trading, scraper, training, backtest, or live dashboard state was inspected.
- No GBrain write/maintenance action was run. Current doctor/freshness findings are read-only observations; timer/service configuration and journals were not inspected.
- No MCP call was used. Remote canonical markdown and source-qualified CLI readback were used because SSH was explicitly authorized and produced line-addressable evidence.
- The full GBrain corpus, all 62 tracked plan/understanding files, all historical handoffs, all Hermes profiles, and all remote kanban/systemd configuration were not exhaustively audited. Selection was bounded to the named authority, workflow, migration, audit, and recent continuity documents.
- Lesson retrieval was audited as policy/corpus, not by running every model-recommendation eval through every provider. That is the safe evidence step for AKW-05.
- The current `doctor` cycle-freshness requirement was not adjudicated as mandatory versus acceptable exception. This report records the failed check but does not prescribe running dream/autopilot.
- Existing report findings and handoff validation claims were not independently reproduced with tests, DB checks, or production calls. This lane audited whether the knowledge workflow preserves and qualifies those claims.
- The absence of a file from `git ls-files` proves it is not tracked in the current checkout; it does not prove Chase has not preserved it elsewhere. AKW-03’s done condition allows an explicitly documented canonical artifact store.
- `CLAUDE.md` lesson-policy absence was established from the complete 81-line file; behavior of an external Claude session with additional private/global instructions was not tested.

## Recommended adjudication order

This is sequencing advice for review only, not authorization to edit or create cards:

1. **AKW-03 and AKW-04:** make the audit evidence durable and ensure the register can preserve adjudication provenance before promoting any lane findings.
2. **AKW-01 and AKW-02:** repair current roadmap and authority routing so future resume/planning work starts from the right source and gate.
3. **AKW-05 and AKW-06:** reconcile lesson/fact validation semantics before the next model architecture or promotion recommendation.
4. **AKW-08:** inspect current remote maintenance evidence and agree on source freshness/cycle SLAs without automatically running broad maintenance.
5. **AKW-07, AKW-09, and AKW-10:** unify worker routing and retire/bound stale secondary pointers after primary authorities are settled.

## Report validation checklist

- [x] Only `.hermes/audits/tech-debt/09-agent-knowledge-workflow.md` was written by this lane.
- [x] Findings remain candidates pending independent review.
- [x] Every finding includes exact evidence, failure mode, confidence, mitigation, ownership domain, safe evidence step, and done condition.
- [x] Repository debt is distinguished from GBrain/Hermes operational debt.
- [x] Rejected suspicions and coverage gaps are recorded.
- [x] No DB/production/GBrain mutation or plan/card/register/skill/config/memory change was performed.
