"""
Centralized combo market definitions and stat-to-market mappings.

Combo markets (PRA, PR, PA, RA) are derived by summing individual stat
Monte Carlo samples — no new model training needed.
"""

COMBO_DEFINITIONS = {
    "pra": {"components": ["pts", "reb", "ast"], "market_key": "player_points_rebounds_assists"},
    "pr": {"components": ["pts", "reb"], "market_key": "player_points_rebounds"},
    "pa": {"components": ["pts", "ast"], "market_key": "player_points_assists"},
    "ra": {"components": ["reb", "ast"], "market_key": "player_rebounds_assists"},
}

COMBO_STATS = set(COMBO_DEFINITIONS.keys())

# Base stat-to-market mapping (individual stats only)
BASE_STAT_TO_MARKET = {
    "pts": "player_points",
    "reb": "player_rebounds",
    "ast": "player_assists",
}

# Full mapping including combos
STAT_TO_MARKET = {
    **BASE_STAT_TO_MARKET,
    **{k: v["market_key"] for k, v in COMBO_DEFINITIONS.items()},
}

MARKET_TO_STAT = {v: k for k, v in STAT_TO_MARKET.items()}
