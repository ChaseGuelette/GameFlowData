# Per-Stat Configuration Documentation

**Module:** `src/config/stat_config.py`

## Overview

The stat config module enables different betting parameters for each stat type (pts, reb, ast). Backtesting revealed that stats perform differently:
- REB: +7.9% ROI (strongest)
- AST: +3.2% ROI (marginal)
- PTS: Variable performance

Per-stat configuration allows:
- Tighter edge thresholds on weaker stats
- Looser thresholds on stronger stats
- Different BL tau values per stat
- Disabling BL entirely for specific stats

## Classes

### `StatConfig`

Configuration for a single stat type.

```python
@dataclass
class StatConfig:
    stat: str                          # Stat identifier ("pts", "reb", "ast")
    enabled: bool = True               # Whether betting is enabled for this stat
    edge_threshold: float | None = None  # Minimum edge. None = use global.
    bl_tau: float | None = None        # Black-Litterman tau. None = no BL.
```

### `StatConfigSet`

Container for per-stat configurations with global fallbacks.

```python
@dataclass
class StatConfigSet:
    global_edge_threshold: float = 0.05
    global_bl_tau: float | None = None
    configs: dict[str, StatConfig] = field(default_factory=dict)
```

**Methods:**
- `get_edge_threshold(stat: str) -> float` — Returns per-stat value or global fallback
- `get_bl_tau(stat: str) -> float | None` — Returns per-stat value or global fallback
- `is_stat_enabled(stat: str) -> bool` — Checks if stat is enabled (default: True)
- `get_enabled_stats() -> list[str]` — Returns list of enabled stats
- `from_cli_args(edge_values, tau_values, stats) -> StatConfigSet` — Factory for CLI parsing
- `to_dict() -> dict` — Serialization for logging/debugging

## CLI Format

### Edge Thresholds

```bash
# Global (backward compatible)
--edge-threshold 0.05

# Per-stat values
--edge-threshold pts=0.10 reb=0.07 ast=0.15

# Mixed: global default + per-stat overrides
--edge-threshold 0.05 pts=0.10
```

### BL Tau Values

```bash
# Global BL tau
--bl-tau 0.10

# Per-stat BL tau
--bl-tau pts=0.05 reb=0.10

# Disable BL for specific stats using "none"
--bl-tau pts=0.05 reb=0.10 ast=none
```

## Helper Function

### `parse_stat_param(values: list[str])`

Parses CLI values into global and per-stat components.

```python
>>> parse_stat_param(["0.05"])
(0.05, {})

>>> parse_stat_param(["pts=0.10", "reb=0.07"])
(None, {"pts": 0.10, "reb": 0.07})

>>> parse_stat_param(["0.05", "pts=0.10"])
(0.05, {"pts": 0.10})

>>> parse_stat_param(["pts=none"])
(None, {"pts": None})
```

## Precedence Logic

1. **Per-stat value** (if configured) — highest priority
2. **Global value** (if set) — fallback
3. **Default value** (0.05 for edge) — final fallback

## Usage Examples

### In Backtesting

```python
from src.config.stat_config import StatConfigSet

# Parse CLI args
stat_config = StatConfigSet.from_cli_args(
    edge_values=["0.05", "pts=0.10"],
    tau_values=["pts=0.05", "ast=none"],
    stats=["pts", "reb", "ast"],
)

# Create harness with per-stat config
harness = BacktestHarness(
    model_dir=Path("src/models/artifacts/run_20260205_165808"),
    edge_threshold=stat_config.global_edge_threshold,
    stat_config=stat_config,
)
```

### In Paper Trading

```python
from src.config.stat_config import StatConfigSet

# Parse CLI args
stat_config = StatConfigSet.from_cli_args(
    edge_values=["pts=0.10", "reb=0.07", "ast=0.15"],
    tau_values=None,
    stats=["pts", "reb", "ast"],
)

# Create trader with per-stat config
trader = PaperTrader(
    edge_threshold=stat_config.global_edge_threshold,
    stat_config=stat_config,
)
```

### Getting Per-Stat Values

```python
config = StatConfigSet(global_edge_threshold=0.05)
config.configs["pts"] = StatConfig(stat="pts", edge_threshold=0.10)

# Returns 0.10 (per-stat override)
config.get_edge_threshold("pts")

# Returns 0.05 (global fallback)
config.get_edge_threshold("reb")

# Returns 0.05 (global fallback)
config.get_edge_threshold("unknown")
```

## Integration Points

### Files Using StatConfigSet

| File | Usage |
|------|-------|
| `bet_simulator.py` | Per-stat edge thresholds in `should_bet()` |
| `backtest_harness.py` | Per-stat BL blenders in `_calculate_edges()` |
| `run_backtest.py` | CLI parsing and harness construction |
| `run_sweep.py` | Sweep configuration integration |
| `paper_trader.py` | Per-stat edge thresholds in `select_bets()` |
| `place_bets.py` | CLI parsing and trader construction |

## Test Coverage

30 tests in `tests/test_stat_config.py`:
- `TestParseStatParam` — CLI argument parsing
- `TestStatConfig` — Dataclass behavior
- `TestStatConfigSet` — Container and getters
- `TestStatConfigSetFromCliArgs` — Factory method
- `TestStatConfigSetToDict` — Serialization
- `TestStatConfigSetRepr` — String representation
