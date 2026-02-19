"""Quick test of feature store for a specific date."""
import traceback
from datetime import date

from src.db.client import get_engine
from src.models.feature_store import FeatureStore

engine = get_engine()
fs = FeatureStore(engine)

target_date = date(2026, 2, 9)
print(f"Testing feature store for {target_date}")

try:
    df = fs.get_features_for_date(target_date)
    print(f"Got {len(df)} rows")
    if len(df) > 0:
        print(df[['player_id', 'game_id', 'player_avg_min_l5']].head(10))
    else:
        print("No features returned!")
except Exception as e:
    print(f"Error: {e}")
    traceback.print_exc()
