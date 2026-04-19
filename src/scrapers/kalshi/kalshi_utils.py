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
    "RBI": "batter_rbis",
    "HRR": "batter_hrr",  # Confirmed: KXMLBHRR series is live (8 events as of Apr 2026)
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
        "KXMLBHIT": "batter_hits",
        "KXMLBKS": "pitcher_strikeouts",
        "KXMLBHRR": "batter_hrr",  # Confirmed live: 8 events as of Apr 2026
    },
}

# Reverse: internal stat → series ticker
INTERNAL_STAT_TO_SERIES: dict[str, dict[str, str]] = {}
for _sport, _series_map in KALSHI_PROP_SERIES.items():
    INTERNAL_STAT_TO_SERIES[_sport] = {stat: series for series, stat in _series_map.items()}

# ---------------------------------------------------------------------------
# Game-Level Series: sport/outcome markets (moneyline, NRFI, totals, futures)
# Confirmed via /events API enumeration on 2026-04-16.
# Format: series_ticker -> {"market_type": ..., "sport": ...}
# ---------------------------------------------------------------------------

KALSHI_GAME_SERIES: dict[str, dict[str, str]] = {
    # -----------------------------------------------------------------------
    # MLB — Game outcomes
    # -----------------------------------------------------------------------
    "KXMLBGAME":     {"market_type": "moneyline",     "sport": "mlb"},  # Game winner (45 events)
    "KXMLBRFI":      {"market_type": "yrfi",           "sport": "mlb"},  # First inning run YES/NO (45 events)
    "KXMLBTOTAL":    {"market_type": "total",          "sport": "mlb"},  # Game total runs (11 events)
    "KXMLBSPREAD":   {"market_type": "spread",         "sport": "mlb"},  # Run line (11 events)
    "KXMLBTEAMTOTAL": {"market_type": "team_total",   "sport": "mlb"},  # Team total runs (11 events)
    "KXMLBF5":       {"market_type": "f5_moneyline",   "sport": "mlb"},  # First 5 innings winner (11 events)
    "KXMLBF5SPREAD": {"market_type": "f5_spread",      "sport": "mlb"},  # First 5 innings spread (11 events)
    "KXMLBF5TOTAL":  {"market_type": "f5_total",       "sport": "mlb"},  # First 5 innings total (11 events)
    # -----------------------------------------------------------------------
    # MLB — Season futures
    # -----------------------------------------------------------------------
    "KXMLB":         {"market_type": "season_future",  "sport": "mlb"},  # World Series champion
    "KXMLBAL":       {"market_type": "season_future",  "sport": "mlb"},  # AL champion
    "KXMLBNL":       {"market_type": "season_future",  "sport": "mlb"},  # NL champion
    "KXMLBALEAST":   {"market_type": "division_future", "sport": "mlb"},
    "KXMLBALCENT":   {"market_type": "division_future", "sport": "mlb"},
    "KXMLBALWEST":   {"market_type": "division_future", "sport": "mlb"},
    "KXMLBNLEAST":   {"market_type": "division_future", "sport": "mlb"},
    "KXMLBNLCENT":   {"market_type": "division_future", "sport": "mlb"},
    "KXMLBNLWEST":   {"market_type": "division_future", "sport": "mlb"},
    # -----------------------------------------------------------------------
    # NBA — Game outcomes (active during playoffs)
    # -----------------------------------------------------------------------
    "KXNBAGAME":     {"market_type": "moneyline",      "sport": "nba"},  # Game winner (26 events)
    "KXNBASPREAD":   {"market_type": "spread",          "sport": "nba"},  # Game spread (2 events)
    "KXNBATOTAL":    {"market_type": "total",           "sport": "nba"},  # Game total (2 events)
    "KXNBATEAMTOTAL": {"market_type": "team_total",    "sport": "nba"},  # Team totals (2 events)
    "KXNBA1HWINNER": {"market_type": "h1_moneyline",   "sport": "nba"},  # First half winner (2 events)
    "KXNBA1HSPREAD": {"market_type": "h1_spread",      "sport": "nba"},  # First half spread (2 events)
    "KXNBA1HTOTAL":  {"market_type": "h1_total",       "sport": "nba"},  # First half total (2 events)
    "KXNBA2HWINNER": {"market_type": "h2_moneyline",   "sport": "nba"},  # Second half winner (2 events)
    "KXNBA2HSPREAD": {"market_type": "h2_spread",      "sport": "nba"},  # Second half spread (2 events)
    "KXNBA2HTOTAL":  {"market_type": "h2_total",       "sport": "nba"},  # Second half total (2 events)
    # -----------------------------------------------------------------------
    # NBA — Playoff series markets
    # -----------------------------------------------------------------------
    "KXNBASERIES":      {"market_type": "series_winner",   "sport": "nba"},  # Series winner (6 events)
    "KXNBASERIESGAMES": {"market_type": "series_games",    "sport": "nba"},  # Series length (6 events)
    "KXNBASERIESSCORE": {"market_type": "series_score",    "sport": "nba"},  # Exact series score (6 events)
    "KXNBASERIESSPREAD": {"market_type": "series_spread",  "sport": "nba"},  # Series spread (6 events)
    # -----------------------------------------------------------------------
    # NBA — Season futures
    # -----------------------------------------------------------------------
    "KXNBA":         {"market_type": "season_future",  "sport": "nba"},  # NBA champion
    "KXNBAEAST":     {"market_type": "conference_future", "sport": "nba"},
    "KXNBAWEST":     {"market_type": "conference_future", "sport": "nba"},
    # -----------------------------------------------------------------------
    # NHL — Playoff series markets
    # -----------------------------------------------------------------------
    "KXNHLSERIES":      {"market_type": "series_winner",   "sport": "nhl"},  # Series winner (6 events)
    "KXNHLSERIESGAMES": {"market_type": "series_games",    "sport": "nhl"},
    "KXNHLSERIESSCORE": {"market_type": "series_score",    "sport": "nhl"},
    "KXNHLSERIESSPREAD": {"market_type": "series_spread",  "sport": "nhl"},
    "KXNHL":            {"market_type": "season_future",   "sport": "nhl"},  # Stanley Cup champion
    "KXNHLEAST":        {"market_type": "conference_future", "sport": "nhl"},
    "KXNHLWEST":        {"market_type": "conference_future", "sport": "nhl"},
}

# ---------------------------------------------------------------------------
# Non-Sports Series: economics, crypto, politics
# Stored with sport=NULL in kalshi_markets for non-sports arb matching.
# Format: series_ticker -> {"category": ..., "description": ...}
# ---------------------------------------------------------------------------

KALSHI_NON_SPORTS_SERIES: dict[str, dict[str, str]] = {
    # -----------------------------------------------------------------------
    # Economics / Macro
    # -----------------------------------------------------------------------
    "KXGDP":          {"category": "economics", "description": "US Real GDP growth"},
    "KXFED":          {"category": "economics", "description": "Federal funds rate / FOMC decisions"},
    "KXCPI":          {"category": "economics", "description": "Consumer Price Index (inflation)"},
    # -----------------------------------------------------------------------
    # Crypto / Digital Assets
    # -----------------------------------------------------------------------
    "KXBTC":          {"category": "crypto", "description": "Bitcoin price range"},
    "KXBTCD":         {"category": "crypto", "description": "Bitcoin daily close price"},
    "KXETH":          {"category": "crypto", "description": "Ethereum price range"},
    "KXETHD":         {"category": "crypto", "description": "Ethereum daily close price"},
    "KXDOGE":         {"category": "crypto", "description": "Dogecoin price range"},
    "KXXRP":          {"category": "crypto", "description": "XRP/Ripple price range"},
}

# Per-series Polymarket matching config for non-sports arb.
# poly_categories: Polymarket category values to query
# poly_keywords: series-specific keyword pre-filter (applied after category filter)
KALSHI_SERIES_POLY_CONFIG: dict[str, dict] = {
    "KXGDP": {
        "poly_categories": ["economics", "politics"],
        "poly_keywords": ["gdp", "gross domestic", "q1", "q2", "q3", "q4", "quarterly", "growth"],
    },
    "KXFED": {
        "poly_categories": ["economics", "politics"],
        "poly_keywords": ["fomc", "federal reserve", "fed", "rate cut", "rate hike", "bps", "basis point"],
    },
    "KXCPI": {
        "poly_categories": ["economics", "politics"],
        "poly_keywords": ["cpi", "inflation", "consumer price", "core"],
    },
    "KXBTC": {
        "poly_categories": ["crypto"],
        "poly_keywords": ["bitcoin", "btc"],
    },
    "KXBTCD": {
        "poly_categories": ["crypto"],
        "poly_keywords": ["bitcoin", "btc", "close", "daily"],
    },
    "KXETH": {
        "poly_categories": ["crypto"],
        "poly_keywords": ["ethereum", "ether"],
    },
    "KXETHD": {
        "poly_categories": ["crypto"],
        "poly_keywords": ["ethereum", "ether", "close", "daily"],
    },
    "KXDOGE": {
        "poly_categories": ["crypto"],
        "poly_keywords": ["dogecoin", "doge"],
    },
    "KXXRP": {
        "poly_categories": ["crypto"],
        "poly_keywords": ["ripple", "xrp"],
    },
}

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
