# Phase 2 Spec: Handedness-Stratified Park Factors

## Goal
Extend MLB park factor infrastructure to support L/R split factors. The DB already has
hits_factor_l/r, hr_factor_l/r, runs_factor_l/r columns added. Now update Python code
to seed L/R data and use it at training + inference time.

## Files to Modify

### 1. `src/scrapers/mlb/mlb_reference.py`

**Current PARK_FACTORS dict** (line 36):
```python
PARK_FACTORS = {
    # venue_id: (venue_name, runs, hr, hits, so)
    15: ("Chase Field", 1.06, 1.10, 1.03, 0.98),
    ...
}
```

**Change**: Extend to a 10-tuple including L/R factors:
```python
PARK_FACTORS = {
    # venue_id: (venue_name, runs, hr, hits, so, runs_l, runs_r, hr_l, hr_r, hits_l, hits_r)
    # Values from FanGraphs split park factors (https://www.fangraphs.com/guts.aspx?type=pfh)
    # Estimated from known park characteristics where exact data unavailable.
    # L = left-handed batter factor, R = right-handed batter factor
    15:   ("Chase Field",             1.06, 1.10, 1.03, 0.98,  1.06, 1.06, 1.12, 1.08, 1.04, 1.02),  # ARI - symmetric
    2:    ("Fenway Park",             1.04, 0.95, 1.06, 0.97,  1.06, 1.02, 1.10, 0.82, 1.09, 1.03),  # BOS - Green Monster favors LHB hits; shorter RF for RHB avg
    4705: ("American Family Field",  1.02, 1.05, 1.01, 0.99,  1.02, 1.02, 1.08, 1.02, 1.01, 1.01),  # MIL - LHB HR slightly better
    17:   ("Wrigley Field",          1.05, 1.10, 1.03, 0.97,  1.05, 1.05, 1.12, 1.08, 1.04, 1.02),  # CHC - wind-dependent, roughly symmetric
    4:    ("Guaranteed Rate Field",  1.01, 1.03, 1.00, 1.00,  1.01, 1.01, 1.04, 1.02, 1.00, 1.00),  # CWS - symmetric
    27:   ("Great American Ball Park",1.08,1.15, 1.04, 0.97,  1.08, 1.08, 1.18, 1.12, 1.05, 1.03),  # CIN - short fences both sides
    5:    ("Progressive Field",      0.98, 0.95, 0.98, 1.01,  0.98, 0.98, 0.96, 0.94, 0.98, 0.98),  # CLE - symmetric pitcher's park
    19:   ("Coors Field",            1.20, 1.15, 1.15, 0.85,  1.20, 1.20, 1.15, 1.15, 1.15, 1.15),  # COL - altitude benefits all equally
    2394: ("Comerica Park",          0.97, 0.92, 0.98, 1.02,  0.97, 0.97, 0.90, 0.94, 0.97, 0.99),  # DET - deep CF/LF, moderate RHB penalty
    2392: ("Minute Maid Park",       1.03, 1.05, 1.02, 0.99,  1.03, 1.03, 1.08, 1.02, 1.03, 1.01),  # HOU - Crawford Boxes favor LHB HR
    7:    ("Kauffman Stadium",       0.98, 0.95, 0.99, 1.01,  0.98, 0.98, 0.94, 0.96, 0.98, 0.99),  # KC - symmetric pitcher's park
    1:    ("Angel Stadium",          0.97, 0.98, 0.98, 1.01,  0.97, 0.97, 0.97, 0.99, 0.97, 0.99),  # LAA - symmetric
    22:   ("Dodger Stadium",         0.95, 0.92, 0.97, 1.02,  0.95, 0.95, 0.90, 0.94, 0.96, 0.98),  # LAD - pitcher-friendly, symmetric
    4169: ("loanDepot park",         0.96, 0.90, 0.97, 1.02,  0.96, 0.96, 0.88, 0.92, 0.96, 0.98),  # MIA - pitcher's park, symmetric
    3289: ("Citi Field",             0.96, 0.93, 0.97, 1.02,  0.96, 0.96, 0.91, 0.95, 0.96, 0.98),  # NYM - pitcher-friendly, symmetric
    3313: ("Yankee Stadium",         1.05, 1.12, 1.02, 0.98,  1.06, 1.04, 1.22, 1.04, 1.04, 1.00),  # NYY - short RF porch LHB HR premium
    10:   ("Oakland Coliseum",       0.93, 0.88, 0.95, 1.03,  0.93, 0.93, 0.87, 0.89, 0.94, 0.96),  # OAK - pitcher's park, symmetric
    2681: ("Citizens Bank Park",     1.04, 1.08, 1.02, 0.99,  1.04, 1.04, 1.10, 1.06, 1.03, 1.01),  # PHI - slightly LHB-favorable RF porch
    31:   ("PNC Park",               0.96, 0.93, 0.97, 1.02,  0.96, 0.96, 0.92, 0.94, 0.97, 0.97),  # PIT - pitcher-friendly, symmetric
    2680: ("Petco Park",             0.94, 0.90, 0.96, 1.03,  0.94, 0.94, 0.89, 0.91, 0.95, 0.97),  # SD - pitcher's park, symmetric
    2395: ("Oracle Park",            0.92, 0.85, 0.95, 1.04,  0.92, 0.92, 0.83, 0.87, 0.94, 0.96),  # SF - extreme pitcher's park, symmetric
    680:  ("T-Mobile Park",          0.95, 0.92, 0.97, 1.02,  0.95, 0.95, 0.91, 0.93, 0.96, 0.98),  # SEA - pitcher-friendly, symmetric
    2889: ("Busch Stadium",          0.97, 0.95, 0.98, 1.01,  0.97, 0.97, 0.94, 0.96, 0.97, 0.99),  # STL - symmetric
    12:   ("Tropicana Field",        0.96, 0.93, 0.97, 1.02,  0.96, 0.96, 0.92, 0.94, 0.96, 0.98),  # TB - dome, symmetric
    13:   ("Globe Life Field",       1.01, 1.03, 1.00, 1.00,  1.01, 1.01, 1.04, 1.02, 1.00, 1.00),  # TEX - symmetric
    14:   ("Rogers Centre",          1.03, 1.05, 1.02, 0.99,  1.03, 1.03, 1.07, 1.03, 1.02, 1.02),  # TOR - turf, symmetric
    3309: ("Nationals Park",         1.00, 1.00, 1.00, 1.00,  1.00, 1.00, 1.00, 1.00, 1.00, 1.00),  # WSH - neutral
    2862: ("Target Field",           1.00, 1.02, 1.00, 1.00,  1.00, 1.00, 1.03, 1.01, 1.00, 1.00),  # MIN - symmetric
    5325: ("Truist Park",            1.01, 1.03, 1.00, 1.00,  1.01, 1.01, 1.04, 1.02, 1.00, 1.00),  # ATL - symmetric
}
```

**Update `seed_park_factors()` function** to also update the new L/R columns:
```python
def seed_park_factors(engine, seasons: list[int] | None = None):
    if seasons is None:
        seasons = [2022, 2023, 2024, 2025]

    logger.info(f"Seeding park factors for seasons {seasons}...")
    inserted = 0

    with engine.begin() as conn:
        for venue_id, vals in PARK_FACTORS.items():
            venue_name, runs, hr, hits, so = vals[:5]
            runs_l, runs_r, hr_l, hr_r, hits_l, hits_r = vals[5:] if len(vals) > 5 else (runs, runs, hr, hr, hits, hits)
            for season in seasons:
                conn.execute(
                    text("""
                        INSERT INTO mlb_park_factors
                            (venue_id, season, venue_name, runs_factor, hr_factor, hits_factor, so_factor,
                             runs_factor_l, runs_factor_r, hr_factor_l, hr_factor_r, hits_factor_l, hits_factor_r)
                        VALUES
                            (:vid, :season, :name, :runs, :hr, :hits, :so,
                             :runs_l, :runs_r, :hr_l, :hr_r, :hits_l, :hits_r)
                        ON CONFLICT (venue_id, season) DO UPDATE SET
                            venue_name = EXCLUDED.venue_name,
                            runs_factor = EXCLUDED.runs_factor,
                            hr_factor = EXCLUDED.hr_factor,
                            hits_factor = EXCLUDED.hits_factor,
                            so_factor = EXCLUDED.so_factor,
                            runs_factor_l = EXCLUDED.runs_factor_l,
                            runs_factor_r = EXCLUDED.runs_factor_r,
                            hr_factor_l = EXCLUDED.hr_factor_l,
                            hr_factor_r = EXCLUDED.hr_factor_r,
                            hits_factor_l = EXCLUDED.hits_factor_l,
                            hits_factor_r = EXCLUDED.hits_factor_r
                    """),
                    {
                        "vid": venue_id, "season": season, "name": venue_name,
                        "runs": runs, "hr": hr, "hits": hits, "so": so,
                        "runs_l": runs_l, "runs_r": runs_r,
                        "hr_l": hr_l, "hr_r": hr_r,
                        "hits_l": hits_l, "hits_r": hits_r,
                    },
                )
                inserted += 1

    logger.info(f"Inserted/updated {inserted} park factor records.")
    return inserted
```

Also add a `--reseed-park-factors` flag to `main()`:
```python
parser.add_argument("--reseed-park-factors", action="store_true", help="Reseed park factors only")
```
And in the if/else block, treat `--reseed-park-factors` same as `--parks-only`.

---

### 2. `src/models/mlb/mlb_batter_feature_store.py`

**A. Training SQL (two locations — `_load_single_season_training` and `get_features_for_date`)**

In BOTH SQL queries, replace the park factors SELECT block (current):
```sql
-- Park factors
COALESCE(pf.hits_factor, 1.0) AS park_hits_factor,
COALESCE(pf.hr_factor, 1.0) AS park_hr_factor,
COALESCE(pf.runs_factor, 1.0) AS park_runs_factor,
```

With a CASE expression that picks L/R based on batter handedness. Both SQL queries
already join `mlb_players` — in `_load_single_season_training` there's no explicit players join
but `mlb_player_game_stats_batting` has player_id, and in `get_features_for_date` there's
`LEFT JOIN mlb_players p ON p.player_id = bgs.player_id`.

For `_load_single_season_training`, add a join on `mlb_players`:
```sql
LEFT JOIN mlb_players bp ON bp.player_id = bgs.player_id
```

Then replace the park factor SELECT in BOTH queries:
```sql
-- Park factors (handedness-stratified)
CASE
    WHEN bp.bats = 'L' OR (bp.bats = 'S' AND opp_p.throws = 'R') THEN COALESCE(pf.hits_factor_l, pf.hits_factor, 1.0)
    WHEN bp.bats = 'R' OR (bp.bats = 'S' AND opp_p.throws = 'L') THEN COALESCE(pf.hits_factor_r, pf.hits_factor, 1.0)
    ELSE COALESCE(pf.hits_factor, 1.0)
END AS park_hits_factor,
CASE
    WHEN bp.bats = 'L' OR (bp.bats = 'S' AND opp_p.throws = 'R') THEN COALESCE(pf.hr_factor_l, pf.hr_factor, 1.0)
    WHEN bp.bats = 'R' OR (bp.bats = 'S' AND opp_p.throws = 'L') THEN COALESCE(pf.hr_factor_r, pf.hr_factor, 1.0)
    ELSE COALESCE(pf.hr_factor, 1.0)
END AS park_hr_factor,
CASE
    WHEN bp.bats = 'L' OR (bp.bats = 'S' AND opp_p.throws = 'R') THEN COALESCE(pf.runs_factor_l, pf.runs_factor, 1.0)
    WHEN bp.bats = 'R' OR (bp.bats = 'S' AND opp_p.throws = 'L') THEN COALESCE(pf.runs_factor_r, pf.runs_factor, 1.0)
    ELSE COALESCE(pf.runs_factor, 1.0)
END AS park_runs_factor,
```

For `opp_p.throws` in the CASE expression: `_load_single_season_training` does NOT have an
opposing pitcher join directly in the main query (it's enriched later via `enrich_with_matchup_features`).
For training SQL, it's acceptable to use `bp.bats` alone (no switch-hitter resolution at training time —
the L/R factor difference is small and the model will learn to generalize).

So for `_load_single_season_training`, use this simpler version (no opp_p join needed):
```sql
CASE
    WHEN bp.bats = 'L' THEN COALESCE(pf.hits_factor_l, pf.hits_factor, 1.0)
    WHEN bp.bats = 'R' THEN COALESCE(pf.hits_factor_r, pf.hits_factor, 1.0)
    ELSE COALESCE(pf.hits_factor, 1.0)
END AS park_hits_factor,
CASE
    WHEN bp.bats = 'L' THEN COALESCE(pf.hr_factor_l, pf.hr_factor, 1.0)
    WHEN bp.bats = 'R' THEN COALESCE(pf.hr_factor_r, pf.hr_factor, 1.0)
    ELSE COALESCE(pf.hr_factor, 1.0)
END AS park_hr_factor,
CASE
    WHEN bp.bats = 'L' THEN COALESCE(pf.runs_factor_l, pf.runs_factor, 1.0)
    WHEN bp.bats = 'R' THEN COALESCE(pf.runs_factor_r, pf.runs_factor, 1.0)
    ELSE COALESCE(pf.runs_factor, 1.0)
END AS park_runs_factor,
```

Add this join to `_load_single_season_training` (after the `LEFT JOIN mlb_park_factors pf` line):
```sql
LEFT JOIN mlb_players bp ON bp.player_id = bgs.player_id
```

For `get_features_for_date`, it already has `LEFT JOIN mlb_players p ON p.player_id = bgs.player_id`
where the alias is `p` not `bp`. Use `p.bats` instead of `bp.bats` in that query's CASE expressions.

**B. `_get_park_factors()` inference method** (lines 875-890)

Change signature and implementation:
```python
def _get_park_factors(self, venue_id: int, season: int, bats: str | None = None, opp_throws: str | None = None) -> dict:
    """Fetch park factors for venue, optionally stratified by batter handedness.

    bats: 'L', 'R', or 'S' (switch hitter)
    opp_throws: 'L' or 'R' (used to resolve switch hitters)
    """
    # Determine effective hand
    if bats == 'L' or (bats == 'S' and opp_throws == 'R'):
        hand = 'L'
    elif bats == 'R' or (bats == 'S' and opp_throws == 'L'):
        hand = 'R'
    else:
        hand = None  # unknown, use aggregate

    query = text("""
        SELECT hits_factor, hr_factor, runs_factor,
               hits_factor_l, hr_factor_l, runs_factor_l,
               hits_factor_r, hr_factor_r, runs_factor_r
        FROM mlb_park_factors
        WHERE venue_id = :venue_id AND season = :season
    """)
    with self.engine.connect() as conn:
        row = conn.execute(query, {"venue_id": venue_id, "season": season}).fetchone()
    if row is None:
        return {"hits_factor": 1.0, "hr_factor": 1.0, "runs_factor": 1.0}

    if hand == 'L':
        return {
            "hits_factor": float(row.hits_factor_l or row.hits_factor or 1.0),
            "hr_factor": float(row.hr_factor_l or row.hr_factor or 1.0),
            "runs_factor": float(row.runs_factor_l or row.runs_factor or 1.0),
        }
    elif hand == 'R':
        return {
            "hits_factor": float(row.hits_factor_r or row.hits_factor or 1.0),
            "hr_factor": float(row.hr_factor_r or row.hr_factor or 1.0),
            "runs_factor": float(row.runs_factor_r or row.runs_factor or 1.0),
        }
    else:
        return {
            "hits_factor": float(row.hits_factor or 1.0),
            "hr_factor": float(row.hr_factor or 1.0),
            "runs_factor": float(row.runs_factor or 1.0),
        }
```

**C. Update call site** in `get_player_game_features()` (lines 478-482)

Current:
```python
# 4. Park factors
park = self._get_park_factors(venue_id, season)
features["park_hits_factor"] = park["hits_factor"]
features["park_hr_factor"] = park["hr_factor"]
features["park_runs_factor"] = park["runs_factor"]
```

Replace with (fetch bats/throws first from the handedness query we already have in `_get_platoon_features`):
```python
# 4. Park factors (handedness-stratified)
# Fetch batter handedness for park factor resolution
bats, opp_throws = self._get_batter_handedness(player_id, opp_pitcher_id)
park = self._get_park_factors(venue_id, season, bats=bats, opp_throws=opp_throws)
features["park_hits_factor"] = park["hits_factor"]
features["park_hr_factor"] = park["hr_factor"]
features["park_runs_factor"] = park["runs_factor"]
```

Add a new private helper `_get_batter_handedness()`:
```python
def _get_batter_handedness(self, player_id: int, opp_pitcher_id: int | None) -> tuple[str | None, str | None]:
    """Fetch batter bats and opposing pitcher throws for park factor resolution."""
    if opp_pitcher_id:
        query = text("""
            SELECT bp.bats, opp.throws
            FROM mlb_players bp, mlb_players opp
            WHERE bp.player_id = :batter_id AND opp.player_id = :pitcher_id
        """)
        with self.engine.connect() as conn:
            row = conn.execute(query, {"batter_id": player_id, "pitcher_id": opp_pitcher_id}).fetchone()
        if row:
            return row.bats, row.throws
    else:
        query = text("SELECT bats FROM mlb_players WHERE player_id = :player_id")
        with self.engine.connect() as conn:
            row = conn.execute(query, {"player_id": player_id}).fetchone()
        if row:
            return row.bats, None
    return None, None
```

## Summary of Changes
- `src/scrapers/mlb/mlb_reference.py`: PARK_FACTORS extended to 10-tuple, seed_park_factors updated, --reseed-park-factors flag added
- `src/models/mlb/mlb_batter_feature_store.py`: Training SQL CASE expressions, `_get_park_factors()` new signature, `_get_batter_handedness()` helper, call site updated
