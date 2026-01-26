import os
import sys

# Mimic what pytest usually does
root_dir = os.getcwd()
sys.path.insert(0, root_dir)
# Often 'src' layout implies src is in path or we import as src.models...
# The pytest output showed "from src.models.train_pipeline import ..." so src is likely a package in root.
# But sometimes src is added to path. Let's stick to root first.

print("sys.path:", sys.path)

try:
    print("Attempting to import src.models.quantile_trainer")
    print("Success!")
except Exception as e:
    print(f"Caught exception: {type(e).__name__}: {e}")
    if "sklearn" in sys.modules:
        m = sys.modules["sklearn"]
        print(f"sklearn module: {m}")
        print(f"sklearn file: {getattr(m, '__file__', 'unknown')}")
        print(f"sklearn path: {getattr(m, '__path__', 'unknown')}")
