> Part of [[Handoffs]]

**Date**: April 16, 2026

## Summary

Implemented Phase 9.3+9.4: Cross-Platform Market Matcher (Discovery-First). Built the full infrastructure to match game-level markets (moneyline, NRFI, totals) and non-sports markets (politics, crypto, economics) between Kalshi and Polymarket, plus a discovery script to enumerate all Kalshi series.

## What Was Done

### New Files
- **`src/scrapers/kalshi/kalshi_discovery.py`**: One-time local script to enumerate ALL Kalshi series by paginating through all markets, classify them into categories (nba_prop, mlb_game, non_sports, etc.), and print a KALSHI_GAME_SERIES dict to add to kalshi_utils.py. Run with `python -m src.scrapers.kalshi.kalshi_discovery`.
- **`scripts/sample_polymarket_markets.py`**: Pattern sampling script that queries polymarket_markets for ~1000 diverse markets across all categories, shows field population rates, slug patterns, question examples, and price/volume distributions.
- **`src/arbitrage/team_normalizer.py`**: Team name normalization module with full NBA (30) and MLB (30) team dictionaries. Key functions: `normalize_team(raw, sport)`, `extract_teams_from_slug("mlb-tor-mil-2026-04-16")`, `extract_teams_from_question(question, sport)`.

### DB Migration Applied
- `kalshi_markets`: Dropped NOT NULL on `player_name` and `stat_type`; added `market_type TEXT DEFAULT 'player_prop'`, `team1 TEXT`, `team2 TEXT`; created index on `(market_type, sport, snapshot_time)`.
- `arb_opportunities`: Added `team1 TEXT`, `team2 TEXT`, `description TEXT`.
- Backfill query ran: populated `team1`/`team2` from `event_slug` for existing game-level polymarket_markets rows.

### Modified Files
- **`src/scrapers/kalshi/kalshi_utils.py`**: Added `KALSHI_GAME_SERIES: dict[str, dict[str, str]] = {}` (empty stub, populated after running discovery script).
- **`src/scrapers/kalshi/kalshi_market_scraper.py`**: Added `parse_game_market_kalshi()` function; updated `store_markets()` to include `market_type`/`team1`/`team2`; extended `scrape_and_store()` to iterate `KALSHI_GAME_SERIES` alongside `KALSHI_PROP_SERIES`.
- **`src/scrapers/polymarket/polymarket_utils.py`**: Fixed `detect_market_type()` to classify "Spread: Team (-N)" as `spread` not `moneyline`; added "Team vs. Team" matching to `parse_game_market()`; added "Spread: Team (-N)" regex to `parse_game_market()`.
- **`src/scrapers/polymarket/polymarket_market_scraper.py`**: Updated `_parse_event_markets()` to populate `team1`/`team2` from `extract_teams_from_slug()` first, falling back to `parse_game_market()`.
- **`src/arbitrage/market_matcher.py`**: Extended `MatchedMarket` dataclass with `market_type`, `team1`, `team2`, `game_date`, `description`; added `match_game_markets()` method (frozenset team key matching); added `match_non_sports_markets()` method (SequenceMatcher >= 0.80); added 4 new DB loader methods.
- **`src/arbitrage/arb_scanner.py`**: Added `include_game=True` and `include_non_sports=False` params to `scan()`; passes both to matcher; populates `team1`/`team2`/`description` in arb opportunity `extra`; updated `_store_opportunities()` INSERT with new columns.
- **`src/discord_bot/alerts.py`**: Updated `_build_arb_alert_embed()` to format game markets as "BOS vs MIA [MONEYLINE]" and non-sports as "[POLITICS] question text".
- **`src/orchestration/arb_scan_job.py`**: Added `include_game` and `include_non_sports` params to `run()`; added `--no-game` and `--include-non-sports` CLI flags.
- **`src/orchestration/scheduler.py`**: `run_arb_scan_all_categories` now passes `--include-non-sports`.

## Decisions Made

- **KALSHI_GAME_SERIES left empty** — must run `kalshi_discovery.py` locally first to learn actual series tickers, then populate. No guessing.
- **Non-sports off by default** — `include_non_sports=False` default to avoid noise; only the hourly all-categories job enables it.
- **Team matching uses frozenset** — handles home/away ordering differences between Kalshi and Polymarket.
- **game market matching is try/fail (non-fatal)** — won't break existing prop scanning if game series not yet configured.

## Blockers and Open Questions

- **KALSHI_GAME_SERIES is empty** — needs real discovery output. Run `python -m src.scrapers.kalshi.kalshi_discovery --output discovery.json` locally and populate `KALSHI_GAME_SERIES` in `kalshi_utils.py`.
- **Non-sports: Kalshi non-sport query** — the `_load_kalshi_non_sports()` query filters `sport IS NULL OR sport = ''`. Need to verify Kalshi stores non-sports markets with null sport after discovery populates the series.

## Test Results

694 tests pass, 1 skipped, 0 failures.

## Recommended Next Steps

1. **Run discovery**: `python -m src.scrapers.kalshi.kalshi_discovery --output discovery.json` locally with Kalshi credentials. Review the report and populate `KALSHI_GAME_SERIES` in `kalshi_utils.py`.
2. **Run pattern sampler**: `python scripts/sample_polymarket_markets.py` to understand Poly question patterns. Verify slug parsing works.
3. **Dry-run scan**: `python -m src.orchestration.arb_scan_job --sport mlb --dry-run` — confirm game-level matches appear (once KALSHI_GAME_SERIES is populated).
4. **Non-sports dry-run**: `python -m src.orchestration.arb_scan_job --mode all --include-non-sports --dry-run`
5. **Deploy to Railway** after KALSHI_GAME_SERIES is populated.

## Files to Read on Resume

- [[Handoffs]] — this handoff-009 + prior handoff-008 for context
- `src/scrapers/kalshi/kalshi_utils.py` — KALSHI_GAME_SERIES stub, fill it in
- `src/scrapers/kalshi/kalshi_discovery.py` — run this first
- `src/arbitrage/team_normalizer.py` — understand the team lookup tables
- `src/arbitrage/market_matcher.py` — new match_game_markets() and match_non_sports_markets()
