# MLB Railway shutdown — 2026-11-08

Owner requested all MLB jobs on Railway be turned off.

## Verified production state

- GameFlowData production deployment `d7e3b328-46bd-4549-a41d-0f441d410475` is SUCCESS and the sole active running deployment. Original and superseded deployments are REMOVED.
- `MLB_JOBS_ENABLED=false`, `MLB_DENSE_CLV_ENABLED=false`, and `MLB_TRADING_ENABLED=false` independently read back.
- All 23 `mlb_` scheduler registrations are removed before scheduler start. Shared user paper-bet settlement skips its MLB branch while preserving NBA.
- Native container verification at 20:14 UTC executed actual deployed scheduler registration with `BlockingScheduler.start` replaced by read-only inspection: zero MLB registrations and 14 non-MLB registrations under production flags. No jobs were launched by this probe. `/proc` inspection found no running MLB job scripts.
- Railway logs showed NBA inference recovery executing. Original scheduler startup logs were sparse; registration evidence comes from the safe native probe, not claimed original startup logs.
- Deployed file hashes matched the reviewed isolated upload. Local mocked/AST tests covered enabled/disabled registration and settlement, including dry-run behavior, without DB writes.

## Publication boundary

Deployment was an isolated archive of prior production commit `963b426df006c41bf7431d96c719a2eacbde60da` plus changes to `src/orchestration/scheduler.py` and `src/orchestration/resolve_user_paper_bets.py`. Unrelated dirty work was not uploaded. App source changes remain uncommitted/unpushed; owner did not authorize Git publication. The next Git deployment must retain both master-gate changes or old source can ignore the stored false flag and resume MLB jobs.

## Documentation gap

Canonical remote GBrain update/sync/graph verification was delegated but failed with provider HTTP 400. No successful canonical publication or orphan-count verification is claimed. This local receipt preserves the result for subsequent approved wrap-up.
