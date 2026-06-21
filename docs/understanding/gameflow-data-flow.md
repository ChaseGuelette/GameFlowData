# GameFlow Data Flow

This page explains the main path from external data to recommendations/trading/dashboard output. It is a mental model, not a substitute for live code, DB, or production-log checks.

## High-level flow

```text
External providers
  -> raw/import tables in Supabase
  -> linkers and processing jobs
  -> feature stores / derived tables
  -> model artifacts and inference jobs
  -> predictions, MC samples, edges, recommendations
  -> paper/live trading, alerts, dashboard, analysis surfaces
```

## 1. External providers

Examples:
- NBA CDN for NBA game results and box scores.
- MLB Stats API for MLB schedules, stats, players, and probable pitchers.
- The Odds API for props/game lines.
- RapidAPI for NBA injuries.
- Kalshi and Polymarket for market/trading/arbitrage lanes.
- Local-only advanced-stat sources where datacenter blocking applies.

Key idea:
- Provider constraints are architecture constraints. If a source is blocked or expensive in production, the pipeline must route around that instead of pretending the scraper is a normal pure function.

## 2. Raw/import tables

Purpose:
- Preserve external observations with enough timestamp/context to support linking, backtests, and as-of queries.

Common risk:
- Raw tables can be large, append-heavy, and time-sensitive. Queries without date/snapshot predicates can be slow or semantically wrong.

Important invariant:
- `raw_player_props_combined` is large. Do not run unsafe index work or broad unbounded queries against it.

## 3. Linkers and processing jobs

Purpose:
- Convert provider-specific identifiers/names/events into internal player/game/team references.
- Create derived stats/features from raw game logs and market snapshots.

Examples:
- NBA/MLB linker jobs.
- Team/player ID backfills.
- Injury linking.
- Rolling average and opponent-context processing.

Common risk:
- A linker issue can look like missing model output, stale recommendations, or dashboard bugs.

## 4. Feature stores and derived context

Purpose:
- Provide model-ready inputs with temporal integrity.

Important invariants:
- Feature generation must use data known before the target event.
- Precomputed rolling averages should use shifted history, not same-game leakage.
- Feature availability/defaults need monitoring because all-zero or stale features can silently degrade predictions.

## 5. Model artifacts and inference

Purpose:
- Load trained model artifacts and produce predictive distributions or binary probabilities.

Output examples:
- Quantiles.
- Monte Carlo samples.
- Over/under probabilities.
- Edge metrics.
- Recommendation flags.

Important invariants:
- Probabilities from Monte Carlo samples use empirical CDF: `(samples > line).mean()`.
- Do not replace this with Gaussian CDF.
- Do not deploy global conformal recalibration offsets.
- Q10 miscalibration is a known edge; do not blindly correct it.

## 6. Market blending, recommendations, and gates

Purpose:
- Compare model probabilities with market lines/prices and apply filters, thresholds, Black-Litterman blending, sanity checks, and stat-specific gates.

Common risk:
- Model quality, line selection, market timing, and recommendation gates can each change the final pick set. Debugging must identify which layer changed.

## 7. Paper/live trading and alerts

Purpose:
- Convert recommendations into simulated or real trading actions, queue entries, alerts, and settlement/reconciliation updates.

Important boundaries:
- Live-money behavior must fail safe.
- Sport gates should default off when unset.
- Renewal/repricing/queue behavior can be economically equivalent to making new decisions and should respect gates.

## 8. Dashboard and analysis surfaces

Purpose:
- Show picks, model context, history, bot/trade tracker, analysis modal, account/subscription state, and supporting explanations.

Common risk:
- Dashboard bugs can be data-contract bugs, RLS/auth bugs, stale derived-data bugs, or frontend mapping bugs. Do not assume UI symptoms are frontend-only.

## NBA-specific notes

- Railway daily stats must use CDN-only mode.
- Advanced stats scraping is local-only; do not move stats.nba.com scraping to Railway.
- Full NBA lines/injury jobs can be gated by `NBA_FULL_LINES_ENABLED=false`.
- Props-only NBA refresh may still run while full lines are gated.

## MLB-specific notes

- MLB modeling has multiple stat lanes and artifact families.
- Batter_hits and pitcher_strikeouts have had different maturity levels in tooling; recent work has been moving toward shared stat-profile runners, feature controls, and artifact helpers.
- CLV/ranker/book gates matter before staking or promotion decisions.

## Debugging heuristic

When something is wrong, classify the failure layer before fixing:

1. Provider/API availability or schema changed?
2. Raw ingestion wrote expected rows?
3. Linker mapped rows to internal IDs?
4. Derived features populated and fresh?
5. Model artifact loaded correctly?
6. Inference produced sensible distributions/probabilities?
7. Recommendation gates selected or filtered as expected?
8. Paper/live/dashboard consumed the right rows and contracts?

Only after the layer is identified should implementation begin.
