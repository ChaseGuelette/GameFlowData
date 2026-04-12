"""
Polymarket Utility Functions
============================
Market type detection, stat mapping, and question parsing for
the Polymarket prediction markets integration.

Polymarket pricing: YES contracts priced in cents (1-99).
  - 65c YES = 65% implied probability
  - Currently 0 trading fees
"""

import re

# ---------------------------------------------------------------------------
# Sport Tag IDs (discovered via /sports endpoint)
# Hardcoded after initial discovery run to avoid repeated API calls.
# Update these if Polymarket changes their tag IDs.
# ---------------------------------------------------------------------------

POLYMARKET_SPORT_TAGS: dict[str, int] = {
    "nba": 100029,
    "mlb": 100026,
    "nfl": 100028,
    "nhl": 100027,
    "ncaab": 100030,
    "soccer": 100031,
}

# ---------------------------------------------------------------------------
# Stat Mapping: Polymarket question patterns → internal stat keys
# ---------------------------------------------------------------------------

POLY_STAT_MAP: dict[str, str] = {
    # NBA
    "points": "pts",
    "point": "pts",
    "rebounds": "reb",
    "rebound": "reb",
    "assists": "ast",
    "assist": "ast",
    "steals": "stl",
    "steal": "stl",
    "blocks": "blk",
    "block": "blk",
    "threes": "3pm",
    "three-pointers": "3pm",
    "three pointers": "3pm",
    "3-pointers": "3pm",
    "3 pointers": "3pm",
    "three point": "3pm",
    # MLB
    "strikeouts": "pitcher_strikeouts",
    "strikeout": "pitcher_strikeouts",
    "hits": "batter_hits",
    "rbis": "batter_rbis",
    "rbi": "batter_rbis",
    "home runs": "batter_home_runs",
    "home run": "batter_home_runs",
    "total bases": "batter_total_bases",
    "walks": "batter_walks",
}


def normalize_poly_stat(raw_stat: str) -> str | None:
    """Map Polymarket stat description to internal stat key.

    Args:
        raw_stat: Raw stat text from Polymarket question.

    Returns:
        Internal stat key (e.g., "pts"), or None if unrecognized.
    """
    lowered = raw_stat.lower().strip()
    for pattern, internal in POLY_STAT_MAP.items():
        if pattern in lowered:
            return internal
    return None


# ---------------------------------------------------------------------------
# Market Type Detection
# ---------------------------------------------------------------------------

# Player prop patterns — "Will {player} record {N}+ {stat}?"
_PLAYER_PROP_PATTERNS = [
    re.compile(r"will\s+(.+?)\s+(?:record|score|get|have|hit|notch|grab|dish out|dish|tally|collect|make|drain|sink)\s+(\d+)\+?\s+(.+?)\??$", re.IGNORECASE),
    re.compile(r"will\s+(.+?)\s+(?:record|score|get|have|hit|notch)\s+(?:at least|more than)?\s*(\d+)\s+(.+?)\??$", re.IGNORECASE),
    re.compile(r"(.+?):\s+(\d+)\+\s+(.+?)$", re.IGNORECASE),  # "Player Name: 25+ points"
]

# Moneyline patterns — "Will the {team} beat/defeat the {team}?"
_MONEYLINE_PATTERNS = [
    re.compile(r"will\s+(?:the\s+)?(.+?)\s+(?:beat|defeat|win against|win over)\s+(?:the\s+)?(.+?)\??$", re.IGNORECASE),
    re.compile(r"will\s+(?:the\s+)?(.+?)\s+win\s+(?:the\s+)?(?:game|match)(?:\s+against\s+(?:the\s+)?(.+?))?\??$", re.IGNORECASE),
]

# Spread patterns — "Will the {team} win by {N}+ points?"
_SPREAD_PATTERNS = [
    re.compile(r"will\s+(?:the\s+)?(.+?)\s+win\s+by\s+(\d+)\+?\s+(?:points?|runs?|goals?)\??$", re.IGNORECASE),
    re.compile(r"will\s+(?:the\s+)?(.+?)\s+cover\s+(?:the\s+)?(?:spread)?\??$", re.IGNORECASE),
]

# Total/over-under patterns — "Will the total {stat} be over {N}?"
_TOTAL_PATTERNS = [
    re.compile(r"will\s+(?:the\s+)?total\s+(.+?)\s+(?:be\s+)?(?:over|exceed)\s+(\d+(?:\.\d+)?)\??$", re.IGNORECASE),
    re.compile(r"will\s+(?:there\s+be\s+)?(?:over|more than)\s+(\d+(?:\.\d+)?)\s+(.+?)\??$", re.IGNORECASE),
]


def detect_market_type(question: str, description: str = "") -> str:
    """Classify a Polymarket market by type.

    Args:
        question: Market question text.
        description: Optional market description for context.

    Returns:
        Market type: 'player_prop', 'moneyline', 'spread', or 'total'.
        Falls back to 'moneyline' if unclassified.
    """
    text = (question + " " + description).strip()

    # Check player prop first (most specific)
    for pat in _PLAYER_PROP_PATTERNS:
        if pat.search(text):
            return "player_prop"

    # Check total before moneyline (to avoid "total wins" matching moneyline)
    for pat in _TOTAL_PATTERNS:
        if pat.search(text):
            return "total"

    # Check spread
    for pat in _SPREAD_PATTERNS:
        if pat.search(text):
            return "spread"

    # Moneyline as fallback
    for pat in _MONEYLINE_PATTERNS:
        if pat.search(text):
            return "moneyline"

    # Extra heuristics
    text_lower = text.lower()
    if any(kw in text_lower for kw in ["record", "score", "assists", "rebounds", "strikeout", "home run"]):
        return "player_prop"
    if "total" in text_lower and any(c.isdigit() for c in text):
        return "total"

    return "moneyline"


# ---------------------------------------------------------------------------
# Question Parsers
# ---------------------------------------------------------------------------

# Specific prop patterns (in priority order)
_PROP_PARSE_PATTERNS = [
    # "Player Name: 25+ points" style
    re.compile(r"^(.+?):\s*(\d+\.?\d*)\+?\s+(.+?)$", re.IGNORECASE),
    # "Will Player Name record/score/etc. 25+ points?" style
    re.compile(
        r"will\s+(.+?)\s+(?:record|score|get|have|hit|notch|grab|dish out|dish|tally|collect|make|drain|sink|total)\s+(\d+\.?\d*)\+?\s+(.+?)\??$",
        re.IGNORECASE,
    ),
    # "Will Player Name have at least 25 points?"
    re.compile(
        r"will\s+(.+?)\s+(?:have|get|score|record)\s+at\s+least\s+(\d+\.?\d*)\s+(.+?)\??$",
        re.IGNORECASE,
    ),
    # "Will Player Name have more than 25 points?"
    re.compile(
        r"will\s+(.+?)\s+(?:have|get|score|record)\s+(?:more\s+than|over)\s+(\d+\.?\d*)\s+(.+?)\??$",
        re.IGNORECASE,
    ),
]


def parse_player_prop(question: str) -> tuple[str, str | None, float | None] | None:
    """Extract (player_name, stat_type, line) from a player prop question.

    Args:
        question: Market question text.

    Returns:
        Tuple of (player_name, stat_type, line) or None if parsing fails.
        stat_type is our internal key (e.g., "pts"), may be None if unrecognized.
        line may be None if not parseable.
    """
    for pat in _PROP_PARSE_PATTERNS:
        m = pat.search(question.strip())
        if m:
            groups = m.groups()
            player_name = groups[0].strip()
            try:
                line = float(groups[1])
            except (ValueError, IndexError):
                line = None
            raw_stat = groups[2].strip() if len(groups) > 2 else ""
            stat_type = normalize_poly_stat(raw_stat)
            return player_name, stat_type, line

    return None


def parse_game_market(question: str) -> dict | None:
    """Extract team/game info from a game-level market question.

    Args:
        question: Market question text.

    Returns:
        Dict with keys: team1, team2 (may be None), line (may be None), market_type.
        Or None if parsing fails.
    """
    q = question.strip()

    # Moneyline: "Will the {team} beat the {team}?"
    m = re.search(
        r"will\s+(?:the\s+)?(.+?)\s+(?:beat|defeat|win\s+(?:against|over))\s+(?:the\s+)?(.+?)\??$",
        q, re.IGNORECASE,
    )
    if m:
        return {
            "team1": m.group(1).strip(),
            "team2": m.group(2).strip(),
            "line": None,
            "market_type": "moneyline",
        }

    # Spread: "Will the {team} win by {N}+ points?"
    m = re.search(
        r"will\s+(?:the\s+)?(.+?)\s+win\s+by\s+(\d+\.?\d*)\+?\s+(?:points?|runs?|goals?)\??$",
        q, re.IGNORECASE,
    )
    if m:
        return {
            "team1": m.group(1).strip(),
            "team2": None,
            "line": float(m.group(2)),
            "market_type": "spread",
        }

    # Total: "Will the total {stat} be over {N}?"
    m = re.search(
        r"will\s+(?:the\s+)?total\s+.+?\s+be\s+(?:over|under)\s+(\d+\.?\d*)\??$",
        q, re.IGNORECASE,
    )
    if m:
        return {
            "team1": None,
            "team2": None,
            "line": float(m.group(1)),
            "market_type": "total",
        }

    return None


# ---------------------------------------------------------------------------
# Fee Calculation
# ---------------------------------------------------------------------------


def polymarket_fee(_price_cents: float = 0.0) -> float:
    """Return Polymarket fee per contract.

    Currently 0 (Polymarket charges 0 fees as of 2025-2026).
    Placeholder for future fee changes.

    Args:
        price_cents: Contract price in cents (unused).

    Returns:
        Fee in dollars per contract (currently 0.0).
    """
    return 0.0
