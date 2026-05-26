# Fix plan 03 — Kalshi queries are timing out internally

## Verdict

The most suspicious Kalshi query shape is non-sargable date filtering on the large `kalshi_markets` table. It appears in both arb matching and live selection paths. Rewrite these to timestamp ranges before adding indexes.

## Evidence

From prior bounded Railway audit:

- Recent production logs included `QueryCanceled` / statement timeout symptoms inside Kalshi refresh / edge / paper-trading flow.
- The wrapper job can still be marked success while internal edge/paper-trading steps report non-fatal errors.

From DB audit:

- `kalshi_markets` is large: estimated ~26.7M rows.
- Broad grouped scans were intentionally skipped under a 15s timeout/cost guard.
- Smaller Kalshi queue/paper/live tables are not the timeout source.

Railway limitation during this doc pass:

- Railway MCP auth returned unauthorized, so fresh logs could not be pulled. Run `railway login` before final production certification.

## Code findings

Non-sargable query pattern found:

```sql
(snapshot_time AT TIME ZONE 'America/New_York')::date = :target_date
```

Files/locations:

- `src/arbitrage/market_matcher.py`
  - `_load_kalshi_props`: lines 342-356.
  - `_load_kalshi_game_markets`: lines 708-722.
  - corresponding Polymarket queries use the same ET-date cast pattern.
- `src/trading/kalshi/selection_loader.py`
  - `_load_market_rows`: lines 174-191.
  - `_lookup_game_start_times`: lines 222-233.
- `src/paper_trading/kalshi_paper_trader.py`
  - Similar `SELECT DISTINCT ON (ticker)` latest market read paths were found around line 201.
- `src/models/kalshi_edge.py`
  - `compute_edges()` calls `_load_latest_markets(target_date, sport)`; inspect/patch the loader similarly if it uses ET-date cast.

Why this matters:

- Casting `snapshot_time` for every row prevents normal use of indexes on raw `snapshot_time` or `(sport, snapshot_time)`.
- `DISTINCT ON (ticker) ... ORDER BY ticker, snapshot_time DESC` across a broad cast-filtered day can be expensive on tens of millions of rows.


## Implementation status

Implemented 2026-05-26:

- Added shared `src/utils/time_windows.py` with half-open ET-day-to-UTC bounds.
- Rewrote production Kalshi market loaders away from non-sargable ET date casts and onto `snapshot_time >= :start_utc AND snapshot_time < :end_utc`.
- Updated the relevant Kalshi/arbitrage/paper-trading paths, including `src/trading/kalshi/selection_loader.py`, `src/models/kalshi_edge.py`, `src/paper_trading/kalshi_paper_trader.py`, and `src/arbitrage/market_matcher.py`.
- Added regression coverage for DST-safe time-window calculation and for preventing production `kalshi_markets` queries from reintroducing `snapshot_time AT TIME ZONE 'America/New_York'` date casts.
- No indexes/DDL were added.

Validation run:

- `./venv/Scripts/python.exe -m ruff check src/utils/time_windows.py src/trading/kalshi/selection_loader.py src/models/kalshi_edge.py src/paper_trading/kalshi_paper_trader.py src/arbitrage/market_matcher.py tests/test_time_windows.py tests/test_kalshi_sargable_queries.py` — passed.
- `./venv/Scripts/python.exe -m py_compile src/utils/time_windows.py src/trading/kalshi/selection_loader.py src/models/kalshi_edge.py src/paper_trading/kalshi_paper_trader.py src/arbitrage/market_matcher.py tests/test_time_windows.py tests/test_kalshi_sargable_queries.py` — passed.
- `./venv/Scripts/python.exe -m pytest tests/test_time_windows.py tests/test_kalshi_sargable_queries.py -q` — 5 passed, 1 warning.

Remaining operational follow-up:

- Production certification still requires Railway auth/log review and a runtime dry run to confirm no new `QueryCanceled` / statement-timeout symptoms.
- If production is still slow after the rewrite, propose specific concurrent index DDL separately before running any DB changes.

## Fix proposal

### Phase A — centralize date-window calculation

Add helper, e.g. `src/utils/time_windows.py` or inside Kalshi modules:

```python
from datetime import date, datetime, time
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")

def et_day_utc_bounds(target_date: date) -> tuple[datetime, datetime]:
    start_et = datetime.combine(target_date, time.min, tzinfo=ET)
    end_et = datetime.combine(target_date, time.max, tzinfo=ET)
    # Prefer exclusive end at next midnight, not time.max, in implementation.
```

Use exclusive range:

```sql
snapshot_time >= :start_utc
AND snapshot_time < :end_utc
```

### Phase B — patch all production Kalshi loaders

Replace each ET-date cast with UTC bounds.

Before:

```sql
WHERE sport = :sport
  AND (snapshot_time AT TIME ZONE 'America/New_York')::date = :target_date
  AND market_status = 'open'
ORDER BY ticker, snapshot_time DESC
```

After:

```sql
WHERE sport = :sport
  AND snapshot_time >= :start_utc
  AND snapshot_time < :end_utc
  AND market_status = 'open'
ORDER BY ticker, snapshot_time DESC
```

Patch at least:

- `src/trading/kalshi/selection_loader.py`
- `src/models/kalshi_edge.py`
- `src/paper_trading/kalshi_paper_trader.py`
- `src/arbitrage/market_matcher.py`

### Phase C — add cheap regression tests

Tests should assert generated SQL/path behavior indirectly where possible:

- helper returns correct UTC bounds for normal and DST-transition dates;
- selection loader accepts a target date and passes `start_utc/end_utc` params;
- no remaining production `kalshi_markets` query uses `(snapshot_time AT TIME ZONE ... )::date`.

### Phase D — only then consider index work

Do not add indexes first. After query rewrite, run `EXPLAIN (ANALYZE, BUFFERS)` only on a bounded staging/local-safe path or ask Chase before production analysis.

If still slow, propose specific concurrent index DDL and risk before running anything. Candidate shape may be:

```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_kalshi_markets_sport_snapshot_ticker_open ON kalshi_markets (sport, snapshot_time DESC, ticker) WHERE market_status = 'open';
```

But do not execute this without explicit approval.

## Verification

1. Static check:

```powershell
.\venv\Scripts\python.exe -m pytest tests -k kalshi
```

2. Targeted grep/search expectation:

- No production-path `kalshi_markets` query should contain `(snapshot_time AT TIME ZONE 'America/New_York')::date`.

3. Runtime dry run after Railway auth is restored:

```powershell
.\venv\Scripts\python.exe src\orchestration\kalshi_refresh_job.py --sport mlb --dry-run --skip-discord
```

4. Production certification:

- restore Railway auth;
- check 2-4 hours of logs for `QueryCanceled`, `statement timeout`, and “Edges: matched/updated” counts;
- verify `kalshi_markets` model_prob/bl_edge rows for the target date using sargable UTC-window queries.

## Non-goals

- Do not fix NBA-specific model failures here.
- Do not add DDL/indexes before proving the sargable query rewrite is insufficient.
- Do not rely on wrapper job success when nested edge/paper/live steps can fail non-fatally.
