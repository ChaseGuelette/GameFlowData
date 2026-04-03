"""MLB-specific stat configuration for model types and edge thresholds."""

MLB_STATS: dict[str, dict] = {
    # Quantile regression stats (semi-continuous)
    "pitcher_strikeouts": {"model_type": "quantile", "edge_threshold": 0.08},
    "pitcher_outs": {"model_type": "quantile", "edge_threshold": 0.08},
    # Binomial stats (hits in at-bats — underdispersed)
    "batter_hits": {"model_type": "binomial", "edge_threshold": 0.08},
    # NegBin stats (discrete counts — overdispersed)
    "batter_total_bases": {"model_type": "negbin", "edge_threshold": 0.10},
    "batter_rbis": {"model_type": "negbin", "edge_threshold": 0.10},
    "batter_runs_scored": {"model_type": "negbin", "edge_threshold": 0.10},
}
