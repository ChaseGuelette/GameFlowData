# NBA training and replay preflight

## Kick off immediately

Preflight is a safety boundary before expensive execution, not a shortcut around historical-readiness checks.

- This chat: preserve the reviewed [slice 1](../operations/nba-cli-preflight-slice1-2026-11-01.md) in its isolated worktree; no integration yet.
- Other chat: repair sync independently. No additional chat is needed for this coding slice.
- You: read this mental model and the [eight-slice roadmap](nba-training-evaluation-slices.md). Slice 2 (artifact identity/completion) is next for scope approval; no new unit tests are proposed.

Status: implemented in `repair/nba-cli-preflight`, not integrated into main. Independent specification and quality reviews passed. Owner-approved fixture reconciliation is complete; all 117 existing scoped tests pass.

## Try the safe training plan

This command prints the plan from the isolated implementation; it does not train or connect to the local database.

PowerShell, one line:

```powershell
Set-Location 'C:\Users\Chase\Projects\GameFlowData-worktrees\nba-cli-preflight'; & 'C:\Users\Chase\Projects\GameFlowData\venv\Scripts\python.exe' src/models/train_pipeline.py --dry-run --local
```

`--preflight` is an alias for `--dry-run`. The JSON says `execution: false` and `data_readiness: not_checked`. For calibration-only, the existing model directory is the actual output target and is validated for observable writability; the unused artifacts root is not treated as its output.

Replay planning also checks required artifact filenames. Use an exact existing artifact with the requested supported stats, not a fabricated run name. It does not deserialize the files, so a preflight pass is still not an inference pass.

## Current evidence and remaining gate

The safety boundary works in controlled CLI checks and the existing scoped suite is green.

- Coordinator: 20 CLI/safety checks passed; no planned output directory created. This includes an intentionally simulated permission failure, not an OS ACL certification.
- Existing scoped suite: 117 passed after Chase approved adapting three existing replay fixtures to provide required artifact filenames. No new test cases or weakened assertions were added; the pipeline remains mocked in those orchestration cases.
- A private validation bypass discovered during review was removed rather than retained to make fixtures pass.
- No real-data/model-quality check, integration or training was performed. See the scoped record for the exact acceptance state.

## What problem does this solve?

A typo should fail before the trainer prepares an engine or creates an incomplete run directory.

Today the NBA training CLI enters an orchestrator that obtains a database engine and creates an artifact directory. Replay also prepares output and runtime components before it has a robust artifact/preflight boundary. That makes it harder to answer the simple question: “Is this command configured correctly?” without starting work.

Slice 1 separates those jobs:

    Parse command → validate ordinary settings/files → print plan or execute

A dry-run stops at the plan. A normal run continues into the existing training or replay path.

## The pieces, in plain English

These are separate responsibilities so checking an instruction does not accidentally execute it.

- Parser: translates command-line text into settings. It does not train or connect to a database.
- Preflight: checks things knowable from those settings and ordinary local files: date order, numeric ranges, mode conflicts, supported stats and expected artifact files.
- Plan: says what would run, which paths/settings it would use and which checks have not been performed.
- Executor: constructs runtime objects and runs the existing model/replay work. Only a non-dry-run reaches it.

The implementation may keep these boundaries as functions rather than introduce a framework. The important separation is behavior, not the number of new files.

## What passing preflight means

A structurally sensible command is not necessarily a scientifically valid experiment.

A pass can establish:

- arguments are coherent;
- declared ordinary config files are readable and structurally acceptable;
- required artifact filenames exist;
- an output path is not obviously invalid;
- no runtime job has been launched by planning.

It does not establish:

- the local sync is complete or fresh;
- database tables or historical features are ready;
- model files deserialize or produce valid predictions;
- train/calibration/OOS splits are approved;
- quotes and injuries obey a valid historical observation policy;
- replay is quote-clean;
- the model is profitable or approved for customers.

File presence and model loading are different checks. Empty disposable artifact files can exercise the former but must never be reported as successful inference.

## Why preserve old defaults for now?

Changing season or clock defaults would mix a mechanical CLI repair with an unapproved historical experiment decision.

This slice makes existing settings visible and validates their shape. S2 still owns the evidence-backed decision-time/cohort policy. Later slices will add the approved shared contracts; preflight must say unknown rather than guess them now.

## How this relates to MLB

We are adopting MLB's explicit workflow boundaries, not its baseball model.

The canonical MLB lifecycle has a resolved configuration, command plan, artifact identity, stage gates and resume state. Safe standalone NBA entrypoints are a prerequisite for building equivalent orchestration. NBA continues to use minutes × rate; a YAML wrapper alone would not repair feature parity or quote selection.

## How we verify without creating unit tests

We run the existing suites and exercise the actual entrypoints, while making accidental execution observable.

Existing tests guard compatibility. Real child-process help/dry-run checks prove the operator interface runs. Disposable controlled safety probes install tripwires for runtime work and check that planning does not create output/run directories. Lint/compile are additional checks, not runtime evidence.

No new unit tests are part of this slice. Controlled probes are temporary validation artifacts, not renamed test suites or substitutes for later real-data acceptance.

## What is deliberately unchanged?

This slice should be easy to review because it does not change the experiment itself.

No data SQL, feature values, model math, calibration behavior, staking, training, sync, S1 integration or deployment is included. The later artifact, empirical-probability, flat-staking and quote-clean repairs remain separate coding slices.

## Read next

Use the review record for actual results and the backlog for later work.

- [Slice 1 scope and evidence](../operations/nba-cli-preflight-slice1-2026-11-01.md)
- [NBA coding readiness plan](../operations/nba-coding-readiness-plan-2026-11-01.md)
- [NBA rebuild mental model](nba-rebuild.md)
