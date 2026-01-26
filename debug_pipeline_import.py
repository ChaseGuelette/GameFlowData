import os
import sys

root_dir = os.getcwd()
sys.path.insert(0, root_dir)

try:
    print("Importing src.processing.feature_selection...")
    print("Success importing feature_selection")

    print("Importing src.models.quantile_trainer...")
    print("Success importing quantile_trainer")

except Exception as e:
    print(f"Caught exception: {type(e).__name__}: {e}")
