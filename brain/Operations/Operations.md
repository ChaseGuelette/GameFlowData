# Operations

> Part of [[BRAIN-INDEX]]

Daily runbooks, critical invariants, incident response procedures, and maintenance tasks. These rules keep the system running reliably.

## Key Files
- [[Critical-Invariants]] - Rules that must NEVER be violated
- [[Daily-Runbook]] - What to check every day
- [[Incident-Response]] - What to do when things break
- [[Maintenance-Tasks]] - Periodic maintenance and cleanup
- [[Known-Issues]] - Active bugs and technical debt
- [[Claude-Commands]] - Slash commands registered for Claude Code sessions
- [[Project-Root-Files]] - Root-level project documents (HANDOFF, ARCHITECTURE, etc.)
- [[Kalshi-Live-Trading-Startup]] - Live trading launch playbook: pre-flight checklist, $300 bankroll config, circuit breakers, scaling plan

## Incident Log

### Apr 27, 2026 — Kalshi Resolution Pipeline: 4 Compounding Bugs in `reconcile_fills()`

**Scope:** `src/paper_trading/kalshi_live_trader.py` — `reconcile_fills()` method

**Root Cause:** Four bugs compounded to silently destroy fill records and prevent P&L tracking:
1. WHERE clause only fetched `status='pending'` orders — missed `status='filled' AND fill_price IS NULL` orders (21 stuck bets)
2. When Kalshi API returns no fill history for old/settled markets, the code skipped derivation entirely — should use `total_cost / fill_count`
3. Pending orders already confirmed-filled in DB (fill_price + fill_count set) were not being promoted to `filled` status
4. **Most damaging**: cancellation logic checked only Kalshi API (which returns nothing for settled markets) and cancelled 32 real filled orders (299 contracts, $109.82 cost)

**Impact:**
- 32 real bets with fill data were cancelled (status=`cancelled`) — obliterating their P&L contribution
- 21 bets stuck with `status='filled', fill_price=NULL` — unresolvable without fill_price
- 22 pending orders with fill data were never promoted

**DB Remediation Applied:**
- Backfilled fill_price for 21 stuck orders: `100 - ROUND(total_cost / fill_count * 100)`
- Promoted 22 pending orders with fill data to `filled`
- Restored 32 cancelled orders to `filled` (un-cancelled)
- Ran `--resolve-only` → resolved 21 orders (9W/12L)

**Code Fix:** All 4 bugs fixed in `reconcile_fills()` (local only — needs Railway deploy).

**Invariant Added:** NEVER cancel a `kalshi_live_orders` row if `fill_price IS NOT NULL AND fill_count > 0` — it is a real bet regardless of what the API returns.

**Remaining Issue:** DB fill_prices are "expected values" from orderbook snapshot at placement time, not actual execution prices. Actual prices differ by 1-10 cents. Ground truth is the Kalshi CSV export (`~/Downloads/Kalshi-Transactions-2026.csv`). Future: add `--csv` flag to `scripts/kalshi_bet_category_analysis.py`.
