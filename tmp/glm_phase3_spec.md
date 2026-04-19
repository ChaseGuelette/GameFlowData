# Phase 3 Spec: Weather Integration

## Goal
Add weather features (air density, wind component, precipitation) to MLB batter and pitcher K models.
The `mlb_game_weather` table is already created in the DB.

## New File: `src/scrapers/mlb/mlb_weather_scraper.py`

Create this file from scratch:

```python
"""
MLB Weather Scraper
====================
Fetches weather data for MLB games from OpenWeatherMap API.

Usage:
    python -m src.scrapers.mlb.mlb_weather_scraper --backfill --start 2022-04-01 --end 2025-12-31
    python -m src.scrapers.mlb.mlb_weather_scraper --today
"""

import argparse
import logging
import math
import sys
import time
from datetime import datetime, date
from pathlib import Path
from typing import Optional

import requests
from sqlalchemy import text

sys.path.append(str(Path(__file__).resolve().parents[3]))

from src.db.client import get_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("MLBWeatherScraper")

# OpenWeatherMap API endpoint
OWM_BASE = "https://api.openweathermap.org/data/2.5"
OWM_ONECALL = "https://api.openweathermap.org/data/3.0/onecall"

# Venue coordinates (lat, lng) for all 30 MLB parks
PARK_LAT_LNG: dict[int, tuple[float, float]] = {
    15:   (33.4453, -112.0667),  # Chase Field (ARI)
    2:    (42.3467, -71.0972),   # Fenway Park (BOS)
    4705: (43.0280, -88.0970),   # American Family Field (MIL)
    17:   (41.9484, -87.6553),   # Wrigley Field (CHC)
    4:    (41.8300, -87.6340),   # Guaranteed Rate Field (CWS)
    27:   (39.0974, -84.5069),   # Great American Ball Park (CIN)
    5:    (41.4962, -81.6852),   # Progressive Field (CLE)
    19:   (39.7559, -104.9942),  # Coors Field (COL)
    2394: (42.3390, -83.0485),   # Comerica Park (DET)
    2392: (29.7572, -95.3553),   # Minute Maid Park (HOU)
    7:    (39.0517, -94.4803),   # Kauffman Stadium (KC)
    1:    (33.8003, -117.8827),  # Angel Stadium (LAA)
    22:   (34.0739, -118.2400),  # Dodger Stadium (LAD)
    4169: (25.7781, -80.2197),   # loanDepot park (MIA)
    3289: (40.7571, -73.8458),   # Citi Field (NYM)
    3313: (40.8296, -73.9262),   # Yankee Stadium (NYY)
    10:   (37.7516, -122.2005),  # Oakland Coliseum (OAK)
    2681: (39.9061, -75.1665),   # Citizens Bank Park (PHI)
    31:   (40.4469, -80.0057),   # PNC Park (PIT)
    2680: (32.7076, -117.1570),  # Petco Park (SD)
    2395: (37.7786, -122.3893),  # Oracle Park (SF)
    680:  (47.5914, -122.3324),  # T-Mobile Park (SEA)
    2889: (38.6226, -90.1928),   # Busch Stadium (STL)
    12:   (27.7683, -82.6534),   # Tropicana Field (TB)
    13:   (32.7473, -97.0832),   # Globe Life Field (TEX)
    14:   (43.6414, -79.3894),   # Rogers Centre (TOR)
    3309: (38.8730, -77.0074),   # Nationals Park (WSH)
    2862: (44.9817, -93.2776),   # Target Field (MIN)
    5325: (33.8908, -84.4678),   # Truist Park (ATL)
    # Baltimore - Camden Yards (venue_id may vary)
    2:    (39.2838, -76.6218),   # Oriole Park at Camden Yards (BAL) -- FIXME: check actual venue_id
}

# Compass bearing from home plate toward center field (degrees, 0=N, 90=E, 180=S, 270=W)
PARK_OF_BEARING: dict[int, int] = {
    15:   0,    # Chase Field (ARI) - NNE
    2:    35,   # Fenway Park (BOS) - NNE toward CF
    4705: 355,  # American Family Field (MIL) - N
    17:   45,   # Wrigley Field (CHC) - NE
    4:    130,  # Guaranteed Rate Field (CWS) - SE
    27:   330,  # Great American Ball Park (CIN) - NNW
    5:    335,  # Progressive Field (CLE) - NNW
    19:   350,  # Coors Field (COL) - N
    2394: 220,  # Comerica Park (DET) - SW
    2392: 80,   # Minute Maid Park (HOU) - E (partially open)
    7:    10,   # Kauffman Stadium (KC) - N
    1:    180,  # Angel Stadium (LAA) - S
    22:   0,    # Dodger Stadium (LAD) - N
    4169: 55,   # loanDepot park (MIA) - NE (retractable dome)
    3289: 175,  # Citi Field (NYM) - S
    3313: 330,  # Yankee Stadium (NYY) - NNW
    10:   270,  # Oakland Coliseum (OAK) - W
    2681: 20,   # Citizens Bank Park (PHI) - NNE
    31:   330,  # PNC Park (PIT) - NNW
    2680: 315,  # Petco Park (SD) - NW
    2395: 5,    # Oracle Park (SF) - N (marine wind from bay)
    680:  10,   # T-Mobile Park (SEA) - N
    2889: 15,   # Busch Stadium (STL) - N
    12:   180,  # Tropicana Field (TB) - dome
    13:   45,   # Globe Life Field (TEX) - NE (retractable)
    14:   90,   # Rogers Centre (TOR) - E (dome)
    3309: 345,  # Nationals Park (WSH) - NNW
    2862: 355,  # Target Field (MIN) - N
    5325: 25,   # Truist Park (ATL) - NNE
}

# Dome/retractable roof stadiums — weather irrelevant
DOME_VENUE_IDS: set[int] = {
    12,    # Tropicana Field (TB) - fixed dome
    4169,  # loanDepot park (MIA) - retractable (usually closed)
    14,    # Rogers Centre (TOR) - retractable
    13,    # Globe Life Field (TEX) - retractable
    2392,  # Minute Maid Park (HOU) - retractable
    4705,  # American Family Field (MIL) - retractable
    680,   # T-Mobile Park (SEA) - retractable
}

# Standard conditions for air density normalization
# At 68°F (20°C), 50% RH, sea level (1013.25 hPa)
STANDARD_DENSITY = 1.200  # kg/m³ approximately


def compute_air_density(temp_f: float, humidity_pct: float, pressure_hpa: float) -> float:
    """Compute air density index normalized to 1.0 at standard conditions (68°F, 50% RH, sea level).

    Lower = thinner air = more carry = more HRs.
    Physics: density ∝ pressure / (temp * (1 + 0.378 * Pv/P))
    where Pv = partial pressure of water vapor.
    """
    temp_c = (temp_f - 32) * 5 / 9
    temp_k = temp_c + 273.15

    # Saturation vapor pressure (Magnus formula)
    pv_sat = 6.1078 * math.exp(17.27 * temp_c / (temp_c + 237.3))  # hPa
    pv = (humidity_pct / 100.0) * pv_sat  # actual vapor pressure

    # Air density index (normalized)
    density_ratio = pressure_hpa / (temp_k * (1 + 0.378 * pv / pressure_hpa))

    # Standard conditions: 293.15K, 50% RH at 20°C → pv_sat≈23.4, pv≈11.7
    temp_std = 293.15
    pv_std = 11.7
    p_std = 1013.25
    density_std = p_std / (temp_std * (1 + 0.378 * pv_std / p_std))

    return density_ratio / density_std


def compute_wind_out_mph(wind_speed_mph: float, wind_dir_deg: int, venue_id: int) -> float:
    """Compute tailwind component toward outfield.

    Positive = wind blowing toward OF (HR-favorable).
    Negative = wind blowing toward home plate (HR-suppressing).
    """
    of_bearing = PARK_OF_BEARING.get(venue_id, 0)
    # Wind direction: meteorological convention (direction wind is coming FROM)
    # We want the component moving TOWARD the OF bearing
    angle_diff = math.radians(wind_dir_deg - of_bearing)
    # wind blowing FROM direction X means wind is MOVING toward X+180
    # tailwind_component = -cos(wind_dir - of_bearing) * speed (from = opposite of to)
    wind_out = -math.cos(angle_diff) * wind_speed_mph
    return round(wind_out, 2)


def fetch_weather_for_game(
    game_pk: int,
    game_dt: datetime,
    venue_id: int,
    api_key: str,
) -> Optional[dict]:
    """Fetch weather for a single game from OpenWeatherMap historical API."""
    lat, lng = PARK_LAT_LNG.get(venue_id, (0, 0))
    if lat == 0:
        logger.warning("No lat/lng for venue_id=%d, skipping", venue_id)
        return None

    is_dome = venue_id in DOME_VENUE_IDS

    if is_dome:
        # Dome stadiums: return neutral weather
        return {
            "game_pk": game_pk,
            "venue_id": venue_id,
            "game_date": game_dt.date(),
            "temp_f": 72.0,
            "humidity_pct": 50.0,
            "pressure_hpa": 1013.25,
            "air_density_idx": 1.0,
            "wind_speed_mph": 0.0,
            "wind_dir_deg": 0,
            "wind_out_mph": 0.0,
            "has_precip": False,
            "is_dome": True,
            "condition": "dome",
        }

    unix_ts = int(game_dt.timestamp())
    url = f"{OWM_ONECALL}/timemachine"
    params = {
        "lat": lat,
        "lon": lng,
        "dt": unix_ts,
        "appid": api_key,
        "units": "imperial",
    }

    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning("OWM request failed for game_pk=%d: %s", game_pk, e)
        return None

    # Handle both v2.5 and v3.0 response formats
    hourly = data.get("data") or data.get("hourly", [])
    if not hourly:
        current = data.get("current", {})
    else:
        current = hourly[0]

    temp_f = current.get("temp", 70)
    humidity = current.get("humidity", 50)
    pressure = current.get("pressure", 1013.25)
    wind_speed = current.get("wind_speed", 0)
    wind_deg = current.get("wind_deg", 0)
    weather_list = current.get("weather", [])
    condition = weather_list[0].get("description", "") if weather_list else ""
    has_precip = any(w.get("main", "").lower() in ("rain", "snow", "thunderstorm", "drizzle")
                     for w in weather_list)

    # Convert wind speed from mph to mph (already imperial)
    wind_out = compute_wind_out_mph(wind_speed, wind_deg, venue_id)
    air_density = compute_air_density(temp_f, humidity, pressure)

    return {
        "game_pk": game_pk,
        "venue_id": venue_id,
        "game_date": game_dt.date(),
        "temp_f": round(float(temp_f), 1),
        "humidity_pct": round(float(humidity), 1),
        "pressure_hpa": round(float(pressure), 2),
        "air_density_idx": round(air_density, 4),
        "wind_speed_mph": round(float(wind_speed), 1),
        "wind_dir_deg": int(wind_deg),
        "wind_out_mph": wind_out,
        "has_precip": has_precip,
        "is_dome": False,
        "condition": condition,
    }


def upsert_weather(engine, records: list[dict]) -> int:
    """Insert/update weather records."""
    if not records:
        return 0
    inserted = 0
    with engine.begin() as conn:
        for r in records:
            conn.execute(text("""
                INSERT INTO mlb_game_weather
                    (game_pk, game_date, venue_id, temp_f, humidity_pct, pressure_hpa,
                     air_density_idx, wind_speed_mph, wind_dir_deg, wind_out_mph,
                     has_precip, is_dome, condition)
                VALUES
                    (:game_pk, :game_date, :venue_id, :temp_f, :humidity_pct, :pressure_hpa,
                     :air_density_idx, :wind_speed_mph, :wind_dir_deg, :wind_out_mph,
                     :has_precip, :is_dome, :condition)
                ON CONFLICT (game_pk) DO UPDATE SET
                    temp_f = EXCLUDED.temp_f,
                    humidity_pct = EXCLUDED.humidity_pct,
                    pressure_hpa = EXCLUDED.pressure_hpa,
                    air_density_idx = EXCLUDED.air_density_idx,
                    wind_speed_mph = EXCLUDED.wind_speed_mph,
                    wind_dir_deg = EXCLUDED.wind_dir_deg,
                    wind_out_mph = EXCLUDED.wind_out_mph,
                    has_precip = EXCLUDED.has_precip,
                    is_dome = EXCLUDED.is_dome,
                    condition = EXCLUDED.condition,
                    fetched_at = NOW()
            """), r)
            inserted += 1
    return inserted


def backfill_weather(engine, api_key: str, start_date: str, end_date: str, delay_s: float = 1.0):
    """Backfill weather for all completed games between start_date and end_date."""
    # Find games missing weather
    query = text("""
        SELECT gs.game_id AS game_pk,
               gs.game_date,
               gs.venue_id,
               gs.game_datetime
        FROM mlb_game_schedule gs
        LEFT JOIN mlb_game_weather gw ON gw.game_pk = gs.game_id
        WHERE gs.game_date BETWEEN :start AND :end
          AND gs.game_type = 'R'
          AND gs.status IN ('Final', 'Completed Early')
          AND gw.game_pk IS NULL
        ORDER BY gs.game_date
    """)
    with engine.connect() as conn:
        rows = conn.execute(query, {"start": start_date, "end": end_date}).fetchall()

    logger.info("Found %d games needing weather backfill", len(rows))

    batch = []
    for i, row in enumerate(rows):
        game_dt = row.game_datetime or datetime.combine(row.game_date, datetime.min.time().replace(hour=19))
        if not isinstance(game_dt, datetime):
            game_dt = datetime(game_dt.year, game_dt.month, game_dt.day, 19, 0, 0)

        record = fetch_weather_for_game(row.game_pk, game_dt, row.venue_id, api_key)
        if record:
            batch.append(record)

        if len(batch) >= 50:
            upsert_weather(engine, batch)
            logger.info("  Saved batch of %d (processed %d/%d)", len(batch), i + 1, len(rows))
            batch = []

        time.sleep(delay_s)

    if batch:
        upsert_weather(engine, batch)

    logger.info("Backfill complete.")


def fetch_today_forecast(engine, api_key: str):
    """Fetch forecast weather for today's scheduled games."""
    today = date.today().isoformat()
    query = text("""
        SELECT gs.game_id AS game_pk,
               gs.game_date,
               gs.venue_id,
               gs.game_datetime
        FROM mlb_game_schedule gs
        WHERE gs.game_date = :today
          AND gs.game_type = 'R'
        ORDER BY gs.game_datetime
    """)
    with engine.connect() as conn:
        rows = conn.execute(query, {"today": today}).fetchall()

    logger.info("Fetching forecast for %d games on %s", len(rows), today)
    records = []
    for row in rows:
        game_dt = row.game_datetime or datetime(date.today().year, date.today().month, date.today().day, 19, 0, 0)
        record = fetch_weather_for_game(row.game_pk, game_dt, row.venue_id, api_key)
        if record:
            records.append(record)
        time.sleep(0.5)

    saved = upsert_weather(engine, records)
    logger.info("Saved weather for %d games", saved)
    return saved


def get_api_key(engine) -> str:
    """Get OpenWeatherMap API key from DB env or environment."""
    import os
    key = os.environ.get("OPENWEATHERMAP_API_KEY", "")
    if not key:
        raise RuntimeError("OPENWEATHERMAP_API_KEY not set in environment")
    return key


def main():
    parser = argparse.ArgumentParser(description="MLB Weather Scraper")
    parser.add_argument("--backfill", action="store_true", help="Backfill historical weather")
    parser.add_argument("--today", action="store_true", help="Fetch today's forecast")
    parser.add_argument("--start", default="2022-04-01", help="Backfill start date (YYYY-MM-DD)")
    parser.add_argument("--end", default=date.today().isoformat(), help="Backfill end date")
    args = parser.parse_args()

    engine = get_engine()
    api_key = get_api_key(engine)

    if args.backfill:
        backfill_weather(engine, api_key, args.start, args.end)
    elif args.today:
        fetch_today_forecast(engine, api_key)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
```

---

## Modify: `src/models/mlb/mlb_batter_feature_store.py`

### A. Add weather features to BATTER_BASE_FEATURES

Find the BATTER_BASE_FEATURES list (around line 50-90). Add at the end:
```python
"air_density_idx",
"wind_out_mph",
"has_precip",
```

### B. Training SQL — both `_load_single_season_training` and `get_features_for_date`

Add the LEFT JOIN after the park factors join:
```sql
-- Game weather
LEFT JOIN mlb_game_weather gw ON gw.game_pk = bgs.game_id
```

Add to SELECT block (after park_runs_factor):
```sql
-- Weather features
COALESCE(gw.air_density_idx, 1.0) AS air_density_idx,
COALESCE(gw.wind_out_mph, 0.0) AS wind_out_mph,
COALESCE(gw.has_precip::int, 0) AS has_precip,
```

### C. Inference (`get_player_game_features`)

Add step after park factors (step 4), before game context (step 5):
```python
# 4b. Weather features
weather = self._get_game_weather(game_id)
features.update(weather)
```

Add private helper:
```python
def _get_game_weather(self, game_id: int) -> dict:
    """Fetch weather features for a game."""
    query = text("""
        SELECT air_density_idx, wind_out_mph, has_precip
        FROM mlb_game_weather
        WHERE game_pk = :game_id
    """)
    with self.engine.connect() as conn:
        row = conn.execute(query, {"game_id": game_id}).fetchone()
    if row is None:
        return {"air_density_idx": 1.0, "wind_out_mph": 0.0, "has_precip": 0}
    return {
        "air_density_idx": float(row.air_density_idx or 1.0),
        "wind_out_mph": float(row.wind_out_mph or 0.0),
        "has_precip": int(bool(row.has_precip)),
    }
```

Also add weather features to the batch query `get_features_for_date()` — same JOIN and SELECT as training.

---

## Modify: `src/models/mlb/mlb_feature_store.py`

### A. Add to PITCHER_K_FEATURES list

Find PITCHER_K_FEATURES (line 29). Add before the closing `]`:
```python
# Weather
"air_density_idx",
"wind_out_mph",
```

### B. Training SQL in pitcher feature store

Add LEFT JOIN on mlb_game_weather (using pitcher's game_id):
```sql
LEFT JOIN mlb_game_weather gw ON gw.game_pk = gs.game_id
```

Add to SELECT:
```sql
COALESCE(gw.air_density_idx, 1.0) AS air_density_idx,
COALESCE(gw.wind_out_mph, 0.0) AS wind_out_mph,
```

### C. Inference method in MLBFeatureStore.get_player_game_features()

Add weather fetch (reuse same pattern as batter store):
```python
# Weather
query = text("SELECT air_density_idx, wind_out_mph FROM mlb_game_weather WHERE game_pk = :gid")
with self.engine.connect() as conn:
    wrow = conn.execute(query, {"gid": game_id}).fetchone()
features["air_density_idx"] = float(wrow.air_density_idx or 1.0) if wrow else 1.0
features["wind_out_mph"] = float(wrow.wind_out_mph or 0.0) if wrow else 0.0
```

---

## Modify: `src/orchestration/scheduler.py`

### A. Add job runner function

After the `run_mlb_roster_scraper` function, add:
```python
def run_mlb_weather_forecast():
    """Fetch weather forecast for today's MLB games (~2h before first pitch)."""
    run_job("mlb_weather_scraper_job.py", extra_args="--today", silent_on_success=True)
```

### B. Create `src/orchestration/mlb_weather_scraper_job.py`

```python
"""Wrapper job for MLB weather forecast scraping."""
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[2]))
from src.scrapers.mlb.mlb_weather_scraper import fetch_today_forecast, get_api_key
from src.db.client import get_engine

engine = get_engine()
api_key = get_api_key(engine)
fetch_today_forecast(engine, api_key)
```

### C. Add scheduler entry

In the MLB section of scheduler (around line 608, near `run_mlb_inference`), add the weather job:
Find where the APScheduler jobs are registered for MLB (look for `scheduler.add_job` calls).
Add:
```python
# 10:40 AM ET - MLB weather forecast (after daily stats, before 10:45 AM props scrape)
scheduler.add_job(
    run_mlb_weather_forecast,
    CronTrigger(hour=10, minute=40, timezone=ET),
    id="mlb_weather_forecast",
    name="MLB Weather Forecast (10:40 AM ET)",
)
```

Insert this block after the `mlb_daily_stats_retry` job block (around line 784) and before `mlb_lines_props_1045am`.

---

## Implementation Notes

1. Do NOT add `air_density_idx`, `wind_out_mph`, `has_precip` to BATTER_BASE_FEATURES if that would break pitcher K feature sets. Instead add them only to relevant specific feature lists if needed. Check whether BATTER_BASE_FEATURES is reused for pitcher features — if not, add to BATTER_BASE_FEATURES freely.

2. In the pitcher feature store (`mlb_feature_store.py`), check how `get_player_game_features` is structured — it likely has its own inference method pattern. Follow the same pattern as existing features.

3. The `has_precip` column is BOOLEAN in DB but should be cast to int (0/1) for the model features since XGBoost doesn't handle Python bool.

4. NULL handling: any game without weather data should get neutral defaults: air_density_idx=1.0, wind_out_mph=0.0, has_precip=0.

5. Do NOT break existing tests or inference paths — all weather features have safe fallback defaults.
