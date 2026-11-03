# Slice 8 family policy proposal — awaiting Chase

## Confirmed versus proposed

Confirmed: T-60, strict blocking, no diagnostic fallback, target/rolling 5/8. These values are recorded in [the completion decision record](nba-s8-completion-decisions-2026-11-02.md).

The following is a conservative proposal, not owner-approved policy or verified data readiness. It is derived from actual current loader behavior and avoids adding artifact features, defaults or decay parameters. It is not selected from ROI.

## Proposed family behavior

The existing canonical family IDs in `src/models/features/nba/contracts.py` are the authority; reuse all seven.

| Family | Proposed historical behavior | Missing/boundary behavior |
| --- | --- | --- |
| `player_rolling` | Current NBA season only; only producer rows proven shifted/as-of safe and based on the approved 8-minute prior-game contribution rule | Block cold-start or unproven lineage; do not substitute zero or last season |
| `team_context` | Current-season history for the target game's team identity | Block missing/ambiguous history; no offseason carryover |
| `opponent_context` | Current-season history for the actual target opponent and as-of position mapping | Block missing/ambiguous rows; no offseason carryover |
| `schedule_context` | Reconstruct from authoritative prior appearances in the target season, keeping schedule appearances separate from 8-minute rolling-value contribution | Block unknown schedule history; no invented offseason rest cap |
| `game_context` | Authoritative target-game team/opponent/home/away identity | Block unresolved identity; never substitute current roster/team |
| `line_context` | Same target game, explicitly authenticated source and coherent quote identity strictly before T-60/commence under the approved clock rules | Block missing/unproven evidence; no prior-game or date-level MAX/latest fallback |
| `injury_context` | Only authenticated, complete as-of editions with known omission semantics before T-60 | Block missing/ambiguous history; absence is not proven healthy |

Current training/date/date-range rolling SQL may select across season boundaries. The proposal deliberately replaces that implicit behavior only in explicit historical mode; legacy production remains unchanged.

## Transition/cohort proposals

- Regular season only for the first accepted baseline; do not mix play-in/playoff populations without a new explicit decision.
- No arbitrary last-X-games exclusion.
- A traded player's individual history may include prior teams within the same season if identity and producer lineage are provable; team/opponent features must use the target game's teams. This specific transition choice still needs Chase's approval.
- Cold-start rows block until required current-season families are supported; do not choose a fixed skip count without evidence.
- Target actual-minutes eligibility is five; prior-game rolling-value contribution is eight. A pregame replay's existing predicted/rolling-minutes filter is a distinct setting, not retrospectively replaced with the target actual-minutes criterion.

## Windows are not yet proposed as executable

The prior exact 19-game diagnostic sample cannot justify train/calibration/protected-OOS windows. Current metadata confirms props lack an explicit ingestion-route column; external authenticated receipts remain an open evidence route. The current artifact attachment and a strict-source-complete window are not yet identified. Therefore no exact modeling split is frozen or invented here.

Source metadata and ownership evidence must precede a credible exact-window recommendation. A diagnostic sample date is not a train/calibration/OOS split. This packet can approve family/cohort semantics independently, but cannot clear 8A/8D/8E source or launch gates.

## Implementation seams discovered

Existing requests and facade do not carry policy. Date-range per-date delegation and training per-season delegation would discard it unless explicitly forwarded. Shared historical mode must replace legacy injury and line assembly before returning ordered features; post-hoc tagging of already-defaulted frames is insufficient. No new generic schema is needed.

Next implementation scope: `requests.py`, `feature_store.py`, `training_loader.py`, `date_batch_loader.py`, `date_range_loader.py`, plus exact shared line/injury owners after tracing. Preserve feature names/order, empirical probability owners and default production behavior.
