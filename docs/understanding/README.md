# GameFlow Understanding

This directory is Chase's human-facing system understanding layer. It explains how GameFlowData, Hermes Agent, GBrain, and the surrounding production systems fit together.

Use these docs when the project starts to feel like “it works lol” instead of something explainable.

## Pages

- `system-atlas.md` — the high-level map of systems and responsibilities.
- `gameflow-data-flow.md` — how data moves from external providers through DB, models, recommendations, trading, and dashboard surfaces.
- `railway-scheduler.md` — how the always-on Railway APScheduler worker launches jobs, applies gates, records history, and maps current schedules.
- `agent-workflow.md` — how Hermes, GBrain, handoffs, skills, workers, and wrap-up should operate together.
- `glossary.md` — short operational definitions for recurring terms.
- `tech-debt-register.md` — evidence-backed debt and migration candidates.

## Rules

1. Optimize for Chase's explanation ability, not agent cleverness.
2. Prefer small durable explanations over generated walls of text.
3. Mark uncertain statements as `needs-validation` instead of pretending they are true.
4. Do not use these docs as a replacement for live DB/log/code checks.
5. Promote only durable, reviewed facts to GameFlowBrain canonical pages.
6. Add tech-debt items only with evidence and a safe first step.

## How to update this directory

During normal work, use the `gameflow-learning-mode` Hermes skill. When a non-trivial subsystem is touched, capture:

- what subsystem this is;
- why it exists;
- upstream/downstream dependencies;
- relevant invariants;
- what Chase should remember;
- any evidence-backed debt candidate.

If the explanation is durable, update one of these docs. If it is just session context, leave it in chat or the handoff.
