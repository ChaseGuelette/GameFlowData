"""MLB-specific stat configuration for model types, edge thresholds, and BL configs.

Each stat has an optimal Black-Litterman config from backtest sweeps:
- tau: global scaling of model influence (higher = more aggressive)
- z_max: z-score at which confidence saturates to 1.0
- max_weight: hard cap on blending weight
- edge_threshold: minimum edge% required to place a bet
"""

from src.models.black_litterman import BLConfig

MLB_STATS: dict[str, dict] = {
    # Quantile regression stats (semi-continuous)
    "pitcher_strikeouts": {"model_type": "quantile", "edge_threshold": 0.08, "allowed_directions": ["under"]},
    "pitcher_outs": {"model_type": "quantile", "edge_threshold": 0.08},
    # Binomial stats (hits in at-bats — underdispersed)
    "batter_hits": {"model_type": "binomial", "edge_threshold": 0.08},
    # NegBin stats (discrete counts — overdispersed)
    "batter_rbis": {"model_type": "negbin", "edge_threshold": 0.08, "allowed_directions": ["over"]},
    # Combined batter offensive contribution: hits + runs scored + RBIs
    # Sportsbook market: batter_hits_runs_rbis (DK/FD/etc). Backtest: 270 bets, +19.76% ROI, Sharpe 2.32
    "batter_hrr": {"model_type": "negbin", "edge_threshold": 0.15},
}

# Per-stat optimal BL configs from backtest sweeps.
# Used by daily runner and backtest sweep for per-stat blending.
STAT_BL_CONFIGS: dict[str, BLConfig] = {
    "pitcher_strikeouts": BLConfig(tau=0.9, z_max=0.25, max_weight=0.80),
    "batter_hits": BLConfig(tau=0.75, z_max=1.0, max_weight=0.80),
    "batter_rbis": BLConfig(tau=0.9, z_max=0.5, max_weight=0.65),
    "batter_hrr": BLConfig(tau=0.9, z_max=0.25, max_weight=0.65),
}

# Fallback BL config for stats without a sweep-optimized config
DEFAULT_BL_CONFIG = BLConfig(tau=0.5, z_max=1.0, max_weight=0.50)
