# Linker System

> Part of [[Pipeline]]

The linker bridges two worlds that don't share identifiers: official stats (NBA/MLB APIs) and sportsbook data (Odds API).

## NBA Linker (`src/processing/nba_linker_local.py`)

### Matching Pipeline
1. Manual overrides from `data/linker_data/player_mappings.csv`
2. Exact normalized name match (vectorized via `.map()`)
3. Fuzzy cache lookup for remaining unmatched

### Persistent Fuzzy Cache
File-based cache at `linker_data/_fuzzy_cache.json`. Stores `{normalized_name: player_id_or_null}` mappings. 95%+ cache hits on typical runs. Auto-invalidates when player count changes.

### Modes
- `download` — Pull full tables to local CSV (one-time)
- `process` — Match IDs locally using CSVs
- `upload` — Push linked results back to DB
- `incremental` — Lightweight daily mode (only unlinked records)

## MLB Linker (`src/processing/mlb/mlb_linker_local.py`)
Local CSV-based with checkpoint/resume. 96.8% coverage (21.97M/22.71M rows). 5 processing sub-stages. Retry/backoff survives laptop sleep.

## NCAAB Linker (`src/processing/ncaab/ncaab_linker.py`)
Game-level only (no player matching). Team name normalization + fuzzy matching (threshold 0.72).

#linker #pipeline #data
