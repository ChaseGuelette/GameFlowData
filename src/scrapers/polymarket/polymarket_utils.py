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

# Inverted lookup: tag_id -> sport key (for category detection)
_SPORT_TAG_IDS: dict[int, str] = {v: k for k, v in POLYMARKET_SPORT_TAGS.items()}

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

# NRFI/YRFI patterns
_NRFI_PATTERN = re.compile(
    r"\bnrfi\b|\byrfi\b"
    r"|no\s+run\s+(?:scored\s+)?(?:in\s+)?(?:the\s+)?first\s+inning"
    r"|yes\s+run\s+(?:scored\s+)?(?:in\s+)?(?:the\s+)?first\s+inning"
    r"|(?:will\s+there\s+be\s+(?:a\s+)?run[s]?\s+(?:scored\s+)?in\s+the\s+first)",
    re.IGNORECASE,
)

# Season-long futures patterns
_SEASON_FUTURE_PATTERNS = [
    re.compile(r"world\s+series\s+(?:champion|winner)", re.IGNORECASE),
    re.compile(r"division\s+winner", re.IGNORECASE),
    re.compile(r"\b(?:al|nl)\s+(?:champion|pennant|mvp|cy\s+young)", re.IGNORECASE),
    re.compile(r"(?:regular[\s-]season\s+)?win\s+total", re.IGNORECASE),
    re.compile(r"(?:rookie|manager)\s+of\s+the\s+year", re.IGNORECASE),
    re.compile(r"(?:hank\s+aaron|batting\s+champion|era\s+title|strikeout\s+(?:title|leader))", re.IGNORECASE),
    re.compile(r"(?:mvp|cy\s+young|silver\s+slugger|gold\s+glove)\s+(?:award|winner)", re.IGNORECASE),
]

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


def detect_market_type(question: str, description: str = "", is_sports: bool = True) -> str:  # noqa: C901
    """Classify a Polymarket market by type.

    Args:
        question: Market question text.
        description: Optional market description for context.
        is_sports: False for non-sports markets (returns 'binary' immediately).

    Returns:
        Market type: 'nrfi', 'season_future', 'player_prop', 'moneyline', 'spread',
        'total', or 'binary' (non-sports).
        Falls back to 'moneyline' if sports market type is unclassified.
    """
    if not is_sports:
        return "binary"

    text = (question + " " + description).strip()
    text_lower = text.lower()

    # NRFI/YRFI (check before player props and moneyline)
    if _NRFI_PATTERN.search(text):
        return "nrfi"

    # Season-long futures (check before moneyline to catch "AL Champion" etc.)
    for pat in _SEASON_FUTURE_PATTERNS:
        if pat.search(text):
            return "season_future"

    # Spread: explicit "Spread:" prefix used by some platforms
    if text_lower.startswith("spread:") or re.search(r"\bspread[:\s]", text, re.IGNORECASE):
        return "spread"

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

    # Moneyline: "{Team} vs. {Team}" or "{Team} vs {Team}"
    m = re.search(
        r"^(.+?)\s+vs\.?\s+(.+?)(?:\s*[\(\[].*[\)\]])?\s*\??$",
        q, re.IGNORECASE,
    )
    if m:
        t1 = m.group(1).strip()
        t2 = m.group(2).strip()
        # Avoid matching player prop questions like "Player vs Player"
        # by requiring at least 3 chars and no digit+word prop indicators
        if len(t1) >= 3 and len(t2) >= 3 and not re.search(r"\d\+", q):
            return {
                "team1": t1,
                "team2": t2,
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

    # Spread: "Spread: {Team} (-N)" style
    m = re.search(
        r"spread:\s*(.+?)\s*\([+-]?(\d+\.?\d*)\)",
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
# Category Detection
# ---------------------------------------------------------------------------

# Non-sports keyword clusters for slug/title classification
_CATEGORY_KEYWORDS: list[tuple[str, list[str]]] = [
    ("politics", ["election", "president", "senate", "congress", "democrat", "republican",
                  "governor", "ballot", "vote", "primary", "supreme-court", "white-house"]),
    ("crypto", ["bitcoin", "ethereum", "crypto", "btc", "eth", "defi", "token",
                "blockchain", "coinbase", "solana", "xrp", "binance"]),
    ("economics", ["gdp", "inflation", "fed-", "interest-rate", "unemployment", "cpi",
                   "recession", "federal-reserve", "tariff", "jobs-report"]),
    ("culture", ["oscar", "grammy", "emmy", "award", "movie", "film", "music",
                 "celebrity", "box-office", "entertainment", "streaming", "netflix"]),
    ("weather", ["hurricane", "earthquake", "storm", "temperature", "weather", "climate",
                 "tornado", "wildfire"]),
]


def detect_category(event: dict) -> tuple[str, str | None]:
    """Detect (category, sport) from a Polymarket event dict.

    Checks event tags first (most reliable), then falls back to slug/title keywords.

    Args:
        event: Raw event dict from the Gamma API.

    Returns:
        Tuple of (category, sport_or_None).
        E.g. ('sports', 'mlb'), ('politics', None), ('other', None).
    """
    tags = event.get("tags") or []
    slug = (event.get("slug") or "").lower()
    title = (event.get("title") or "").lower()
    text = slug + " " + title

    # Check event tags for known sport IDs (most reliable signal)
    for tag in tags:
        tag_id = tag.get("id") or tag.get("tagId")
        if tag_id is not None:
            try:
                sport = _SPORT_TAG_IDS.get(int(tag_id))
                if sport:
                    return "sports", sport
            except (ValueError, TypeError):
                pass

    # Infer sport from slug/title keywords
    for sport_key in POLYMARKET_SPORT_TAGS:
        if sport_key in slug or sport_key in title:
            return "sports", sport_key
    # Broader sports slug patterns
    if any(k in text for k in ["-game-", "playoff", "championship", "pennant", "world-series",
                                "super-bowl", "stanley-cup", "march-madness"]):
        return "sports", None

    # Non-sports categories
    for category, keywords in _CATEGORY_KEYWORDS:
        if any(k in text for k in keywords):
            return category, None

    return "other", None


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
