"""MLB-specific stat configuration for model types, edge thresholds, and BL configs.

Each stat has an optimal Black-Litterman config from backtest sweeps:
- tau: global scaling of model influence (higher = more aggressive)
- z_max: z-score at which confidence saturates to 1.0
- max_weight: hard cap on blending weight
- edge_threshold: minimum edge% required to place a bet

Configs updated Apr 20 2026 after weather + L/R park factor feature sprint.
Early season (Apr-Jun) backtests on 2025 data. Direction restrictions removed —
model now finds edges in both directions for all stats.
"""

from src.models.black_litterman import BLConfig

MLB_STATS: dict[str, dict] = {
    # Quantile regression stats (semi-continuous)
    # Pitcher K: both directions. Backtest #417: 174 bets, 63.2% win, +24.0% ROI, Sharpe 2.78
    "pitcher_strikeouts": {"model_type": "quantile", "edge_threshold": 0.12},
    "pitcher_outs": {"model_type": "quantile", "edge_threshold": 0.08},
    # Binomial stats (hits in at-bats — underdispersed)
    # Hits: both directions. Backtest #475: 361 bets, 63.2% win, +28.1% ROI, Sharpe 2.20
    "batter_hits": {"model_type": "binomial", "edge_threshold": 0.10},
    # NegBin stats (discrete counts — overdispersed)
    # RBIs: both directions, no BL. Backtest #5: 241 bets, 64.7% win, +9.5% ROI, Sharpe 1.36
    "batter_rbis": {"model_type": "negbin", "edge_threshold": 0.12},
    # Combined batter offensive contribution: hits + runs scored + RBIs
    # No HRR prop lines in sportsbook data — Kalshi KXMLBHRR only
    "batter_hrr": {"model_type": "negbin", "edge_threshold": 0.15},
}

# Per-stat optimal BL configs from backtest sweeps.
# Used by daily runner and backtest sweep for per-stat blending.
# None = skip BL, use raw model probability (empirical CDF from MC samples).
STAT_BL_CONFIGS: dict[str, BLConfig | None] = {
    "pitcher_strikeouts": BLConfig(tau=0.75, z_max=0.25, max_weight=0.80),
    "batter_hits": BLConfig(tau=0.9, z_max=0.25, max_weight=0.50),
    "batter_rbis": None,  # No BL — raw model outperforms blended in early season
    "batter_hrr": BLConfig(tau=0.9, z_max=0.25, max_weight=0.65),
}

# Fallback BL config for stats without a sweep-optimized config
DEFAULT_BL_CONFIG = BLConfig(tau=0.5, z_max=1.0, max_weight=0.50)
