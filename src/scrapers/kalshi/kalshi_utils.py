"""
Kalshi Utility Functions
========================
Fee calculation, probability conversion, and stat/series mappings
for the Kalshi prediction markets integration.

Kalshi pricing: YES/NO contracts priced in cents (1-99).
  - 65c YES = 65% implied probability
  - Fees based on P * (1 - P) formula (max at 50/50)
"""

import math

# ---------------------------------------------------------------------------
# Stat Mapping: Kalshi ticker stat keys → our internal stat keys
# ---------------------------------------------------------------------------

KALSHI_STAT_MAP: dict[str, str] = {
    # NBA
    "PTS": "pts",
    "REB": "reb",
    "AST": "ast",
    "STL": "stl",
    "BLK": "blk",
    "3PM": "3pm",
    "PRA": "pra",
    "PR": "pr",
    "PA": "pa",
    "RA": "ra",
    # MLB
    "K": "pitcher_strikeouts",
    "HITS": "batter_hits",
    "TB": "batter_total_bases",
    "HR": "batter_home_runs",
    "RBI": "batter_rbis",
    "RUNS": "batter_runs_scored",
    "HRR": "batter_hits_runs_rbis",
}

# Combined stats that require summing multiple MC sample arrays
COMBINED_STATS: dict[str, list[str]] = {
    "batter_hits_runs_rbis": ["batter_hits", "batter_runs_scored", "batter_rbis"],
}

# Reverse map: our stat keys → Kalshi ticker stat keys
INTERNAL_TO_KALSHI_STAT: dict[str, str] = {v: k for k, v in KALSHI_STAT_MAP.items()}

# ---------------------------------------------------------------------------
# Series Mapping: sport → per-stat series tickers
# ---------------------------------------------------------------------------

# Old single-series map (championship futures only — NOT player props)
KALSHI_SERIES_MAP: dict[str, str] = {
    "nba": "KXNBA",
    "mlb": "KXMLB",
}

# Actual player prop series: each stat has its own series ticker
KALSHI_PROP_SERIES: dict[str, dict[str, str]] = {
    "nba": {
        "KXNBAPTS": "pts",
        "KXNBAREB": "reb",
        "KXNBAAST": "ast",
        "KXNBA3PT": "3pm",
        "KXNBABLK": "blk",
        "KXNBASTL": "stl",
    },
    "mlb": {
        "KXMLBTB": "batter_total_bases",
        "KXMLBHR": "batter_home_runs",
        "KXMLBHIT": "batter_hits",
        "KXMLBKS": "pitcher_strikeouts",
        "KXMLBHRR": "batter_hits_runs_rbis",  # combined: hits + runs + RBIs
    },
}

# Reverse: internal stat → series ticker
INTERNAL_STAT_TO_SERIES: dict[str, dict[str, str]] = {}
for _sport, _series_map in KALSHI_PROP_SERIES.items():
    INTERNAL_STAT_TO_SERIES[_sport] = {stat: series for series, stat in _series_map.items()}

# ---------------------------------------------------------------------------
# Probability Conversion
# ---------------------------------------------------------------------------


def kalshi_price_to_prob(yes_price_cents: int | float) -> float:
    """Convert Kalshi YES price (cents) to implied probability.

    Args:
        yes_price_cents: YES price in cents (1-99).

    Returns:
        Implied probability (0.01 to 0.99).
    """
    return yes_price_cents / 100.0


def kalshi_mid_to_prob(yes_bid: int | float, yes_ask: int | float) -> float:
    """Convert bid/ask midpoint to implied probability (removes spread).

    Accepts EITHER cents (0-100 range) or dollars (0-1 range) — auto-detects.

    Args:
        yes_bid: Best YES bid price.
        yes_ask: Best YES ask price.

    Returns:
        Midpoint implied probability (0-1).
    """
    mid = (yes_bid + yes_ask) / 2.0
    # Auto-detect: if values are > 1, they're in cents; divide by 100
    if mid > 1.0:
        return mid / 100.0
    return mid


# ---------------------------------------------------------------------------
# Fee Calculation
# ---------------------------------------------------------------------------


def kalshi_taker_fee(price_cents: int | float) -> float:
    """Calculate taker fee for a Kalshi contract.

    Formula: ceil(0.07 * P * (1-P) * 100) / 100
    Max fee at P=0.50 → $0.0175 per contract.

    Args:
        price_cents: Contract price in cents.

    Returns:
        Fee in dollars per contract.
    """
    p = price_cents / 100.0
    return math.ceil(0.07 * p * (1 - p) * 100) / 100


def kalshi_maker_fee(price_cents: int | float) -> float:
    """Calculate maker fee for a Kalshi contract.

    Formula: ceil(0.0175 * P * (1-P) * 100) / 100
    Significantly cheaper than taker (75% discount).

    Args:
        price_cents: Contract price in cents.

    Returns:
        Fee in dollars per contract.
    """
    p = price_cents / 100.0
    return math.ceil(0.0175 * p * (1 - p) * 100) / 100


def fee_adjusted_edge(
    model_prob: float,
    kalshi_price_cents: int | float,
    is_yes: bool = True,
    is_maker: bool = True,
) -> float:
    """Calculate fee-adjusted edge for a Kalshi contract.

    Edge = model_prob - kalshi_implied - fee (for YES)
    Edge = (1 - model_prob) - (1 - kalshi_implied) - fee (for NO)
         = kalshi_implied - model_prob - fee (simplified for NO)

    Args:
        model_prob: Our model's probability of the OVER outcome.
        kalshi_price_cents: YES contract price in cents.
        is_yes: True for YES (over) contract, False for NO (under).
        is_maker: True for maker fee, False for taker fee.

    Returns:
        Fee-adjusted edge as a decimal (e.g., 0.05 = 5%).
    """
    kalshi_implied = kalshi_price_cents / 100.0
    fee_fn = kalshi_maker_fee if is_maker else kalshi_taker_fee

    if is_yes:
        # Buying YES: pay yes_price, profit if over
        contract_price = kalshi_price_cents
        fee = fee_fn(contract_price)
        raw = model_prob - kalshi_implied
    else:
        # Buying NO: pay (100 - yes_price), profit if under
        contract_price = 100 - kalshi_price_cents
        fee = fee_fn(contract_price)
        raw = (1 - model_prob) - (1 - kalshi_implied)

    return raw - fee
