"""
Team Name Normalizer
====================
Maps any team reference (full name, city, nickname, abbreviation, slug segment)
to a canonical abbreviation. Used by the cross-platform market matcher to
canonicalize team names from Kalshi tickers and Polymarket event slugs.

NBA: 30 teams  |  MLB: 30 teams

Usage:
    from src.arbitrage.team_normalizer import normalize_team, extract_teams_from_slug

    normalize_team("Los Angeles Lakers", sport="nba")  # -> "LAL"
    normalize_team("Dodgers", sport="mlb")             # -> "LAD"
    extract_teams_from_slug("mlb-tor-mil-2026-04-16")  # -> ("mlb", "TOR", "MIL", "2026-04-16")
"""

import re
from dataclasses import dataclass, field


@dataclass
class TeamInfo:
    canonical_abbr: str   # "LAD", "BOS", "NYY"
    full_name: str        # "Los Angeles Dodgers"
    city: str             # "Los Angeles"
    nickname: str         # "Dodgers"
    sport: str            # "mlb", "nba"
    aliases: list[str] = field(default_factory=list)  # Additional name variants


# ---------------------------------------------------------------------------
# NBA Teams
# ---------------------------------------------------------------------------

_NBA_TEAM_LIST: list[tuple[str, str, str, str, list[str]]] = [
    # (abbr, full_name, city, nickname, aliases)
    ("ATL", "Atlanta Hawks",             "Atlanta",       "Hawks",       ["atl"]),
    ("BOS", "Boston Celtics",            "Boston",        "Celtics",     ["bos"]),
    ("BKN", "Brooklyn Nets",             "Brooklyn",      "Nets",        ["bkn", "bk nets", "new york nets"]),
    ("CHA", "Charlotte Hornets",         "Charlotte",     "Hornets",     ["cha", "charlotte"]),
    ("CHI", "Chicago Bulls",             "Chicago",       "Bulls",       ["chi"]),
    ("CLE", "Cleveland Cavaliers",       "Cleveland",     "Cavaliers",   ["cle", "cavs"]),
    ("DAL", "Dallas Mavericks",          "Dallas",        "Mavericks",   ["dal", "mavs"]),
    ("DEN", "Denver Nuggets",            "Denver",        "Nuggets",     ["den"]),
    ("DET", "Detroit Pistons",           "Detroit",       "Pistons",     ["det"]),
    ("GSW", "Golden State Warriors",     "Golden State",  "Warriors",    ["gsw", "golden state", "gs warriors"]),
    ("HOU", "Houston Rockets",           "Houston",       "Rockets",     ["hou"]),
    ("IND", "Indiana Pacers",            "Indiana",       "Pacers",      ["ind"]),
    ("LAC", "Los Angeles Clippers",      "Los Angeles",   "Clippers",    ["lac", "la clippers", "clippers"]),
    ("LAL", "Los Angeles Lakers",        "Los Angeles",   "Lakers",      ["lal", "la lakers", "lakers"]),
    ("MEM", "Memphis Grizzlies",         "Memphis",       "Grizzlies",   ["mem"]),
    ("MIA", "Miami Heat",                "Miami",         "Heat",        ["mia"]),
    ("MIL", "Milwaukee Bucks",           "Milwaukee",     "Bucks",       ["mil"]),
    ("MIN", "Minnesota Timberwolves",    "Minnesota",     "Timberwolves",["min", "timberwolves", "twolves"]),
    ("NOP", "New Orleans Pelicans",      "New Orleans",   "Pelicans",    ["nop", "nola", "new orleans"]),
    ("NYK", "New York Knicks",           "New York",      "Knicks",      ["nyk", "ny knicks"]),
    ("OKC", "Oklahoma City Thunder",     "Oklahoma City", "Thunder",     ["okc"]),
    ("ORL", "Orlando Magic",             "Orlando",       "Magic",       ["orl"]),
    ("PHI", "Philadelphia 76ers",        "Philadelphia",  "76ers",       ["phi", "sixers", "philly"]),
    ("PHX", "Phoenix Suns",              "Phoenix",       "Suns",        ["phx", "phx suns", "phoenix"]),
    ("POR", "Portland Trail Blazers",    "Portland",      "Trail Blazers",["por", "blazers", "trail blazers"]),
    ("SAC", "Sacramento Kings",          "Sacramento",    "Kings",       ["sac"]),
    ("SAS", "San Antonio Spurs",         "San Antonio",   "Spurs",       ["sas", "sa spurs"]),
    ("TOR", "Toronto Raptors",           "Toronto",       "Raptors",     ["tor"]),
    ("UTA", "Utah Jazz",                 "Utah",          "Jazz",        ["uta"]),
    ("WAS", "Washington Wizards",        "Washington",    "Wizards",     ["was", "wsh", "dc"]),
]

NBA_TEAMS: dict[str, TeamInfo] = {}
for _abbr, _full, _city, _nick, _aliases in _NBA_TEAM_LIST:
    NBA_TEAMS[_abbr] = TeamInfo(
        canonical_abbr=_abbr,
        full_name=_full,
        city=_city,
        nickname=_nick,
        sport="nba",
        aliases=_aliases,
    )


# ---------------------------------------------------------------------------
# MLB Teams
# ---------------------------------------------------------------------------

_MLB_TEAM_LIST: list[tuple[str, str, str, str, list[str]]] = [
    ("ARI", "Arizona Diamondbacks",      "Arizona",       "Diamondbacks", ["ari", "az", "d-backs", "dbacks"]),
    ("ATL", "Atlanta Braves",            "Atlanta",       "Braves",       ["atl"]),
    ("BAL", "Baltimore Orioles",         "Baltimore",     "Orioles",      ["bal", "balt"]),
    ("BOS", "Boston Red Sox",            "Boston",        "Red Sox",      ["bos", "redsox"]),
    ("CHC", "Chicago Cubs",              "Chicago",       "Cubs",         ["chc", "cubs"]),
    ("CWS", "Chicago White Sox",         "Chicago",       "White Sox",    ["cws", "chw", "white sox"]),
    ("CIN", "Cincinnati Reds",           "Cincinnati",    "Reds",         ["cin"]),
    ("CLE", "Cleveland Guardians",       "Cleveland",     "Guardians",    ["cle", "clev"]),
    ("COL", "Colorado Rockies",          "Colorado",      "Rockies",      ["col"]),
    ("DET", "Detroit Tigers",            "Detroit",       "Tigers",       ["det"]),
    ("HOU", "Houston Astros",            "Houston",       "Astros",       ["hou"]),
    ("KC",  "Kansas City Royals",        "Kansas City",   "Royals",       ["kc", "kcr"]),
    ("LAA", "Los Angeles Angels",        "Los Angeles",   "Angels",       ["laa", "ana", "la angels"]),
    ("LAD", "Los Angeles Dodgers",       "Los Angeles",   "Dodgers",      ["lad", "la dodgers", "dodgers"]),
    ("MIA", "Miami Marlins",             "Miami",         "Marlins",      ["mia"]),
    ("MIL", "Milwaukee Brewers",         "Milwaukee",     "Brewers",      ["mil"]),
    ("MIN", "Minnesota Twins",           "Minnesota",     "Twins",        ["min"]),
    ("NYM", "New York Mets",             "New York",      "Mets",         ["nym", "ny mets"]),
    ("NYY", "New York Yankees",          "New York",      "Yankees",      ["nyy", "ny yankees", "yankees"]),
    ("OAK", "Oakland Athletics",         "Oakland",       "Athletics",    ["oak", "ath", "a's"]),
    ("PHI", "Philadelphia Phillies",     "Philadelphia",  "Phillies",     ["phi", "philly"]),
    ("PIT", "Pittsburgh Pirates",        "Pittsburgh",    "Pirates",      ["pit"]),
    ("SD",  "San Diego Padres",          "San Diego",     "Padres",       ["sd", "sdp"]),
    ("SF",  "San Francisco Giants",      "San Francisco", "Giants",       ["sf", "sfg", "san francisco"]),
    ("SEA", "Seattle Mariners",          "Seattle",       "Mariners",     ["sea"]),
    ("STL", "St. Louis Cardinals",       "St. Louis",     "Cardinals",    ["stl", "st louis", "st. louis"]),
    ("TB",  "Tampa Bay Rays",            "Tampa Bay",     "Rays",         ["tb", "tbr"]),
    ("TEX", "Texas Rangers",             "Texas",         "Rangers",      ["tex"]),
    ("TOR", "Toronto Blue Jays",         "Toronto",       "Blue Jays",    ["tor", "bluejays"]),
    ("WSH", "Washington Nationals",      "Washington",    "Nationals",    ["wsh", "was", "nats", "dc"]),
]

MLB_TEAMS: dict[str, TeamInfo] = {}
for _abbr, _full, _city, _nick, _aliases in _MLB_TEAM_LIST:
    MLB_TEAMS[_abbr] = TeamInfo(
        canonical_abbr=_abbr,
        full_name=_full,
        city=_city,
        nickname=_nick,
        sport="mlb",
        aliases=_aliases,
    )


# ---------------------------------------------------------------------------
# Internal lookup tables (built once at import time)
# ---------------------------------------------------------------------------

def _build_lookup(teams: dict[str, TeamInfo]) -> dict[str, str]:
    """Build raw_string -> canonical_abbr lookup (lowercase keys)."""
    lookup: dict[str, str] = {}
    for abbr, info in teams.items():
        # Direct abbreviation
        lookup[abbr.lower()] = abbr
        # Full name
        lookup[info.full_name.lower()] = abbr
        # City
        lookup[info.city.lower()] = abbr
        # Nickname
        lookup[info.nickname.lower()] = abbr
        # Aliases
        for alias in info.aliases:
            lookup[alias.lower()] = abbr
    return lookup


_NBA_LOOKUP: dict[str, str] = _build_lookup(NBA_TEAMS)
_MLB_LOOKUP: dict[str, str] = _build_lookup(MLB_TEAMS)

# Combined sport -> lookup
_SPORT_LOOKUP: dict[str, dict[str, str]] = {
    "nba": _NBA_LOOKUP,
    "mlb": _MLB_LOOKUP,
}

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def normalize_team(raw: str, sport: str | None = None) -> str | None:
    """Map any team reference to a canonical abbreviation.

    Args:
        raw: Raw team name/abbreviation/nickname from any source.
        sport: 'nba' or 'mlb'. If None, tries both lookups.

    Returns:
        Canonical abbreviation (e.g., "LAD"), or None if not found.
    """
    if not raw:
        return None

    key = raw.strip().lower()

    if sport in _SPORT_LOOKUP:
        return _SPORT_LOOKUP[sport].get(key)

    # Try both lookups
    for lookup in (_NBA_LOOKUP, _MLB_LOOKUP):
        result = lookup.get(key)
        if result:
            return result

    return None


# Slug pattern: sport-team1-team2-YYYY-MM-DD or sport-team1-team2-YYYYMMDD
_SLUG_PATTERN = re.compile(
    r"^(mlb|nba|nfl|nhl|ncaab)-([a-z]{1,4})-([a-z]{1,4})-(\d{4}-?\d{2}-?\d{2})",
    re.IGNORECASE,
)

# Date variants: 2026-04-16 -> 2026-04-16 (normalized)
_DATE_DASH = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")
_DATE_NODASH = re.compile(r"^(\d{4})(\d{2})(\d{2})$")


def _normalize_date(raw_date: str) -> str:
    """Normalize date string to YYYY-MM-DD."""
    m = _DATE_DASH.match(raw_date)
    if m:
        return raw_date
    m = _DATE_NODASH.match(raw_date)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return raw_date


def extract_teams_from_slug(
    event_slug: str,
) -> tuple[str, str, str, str] | None:
    """Extract sport, team1, team2, date from a Polymarket event slug.

    Expected slug format: "mlb-tor-mil-2026-04-16"
    Also handles slugs with extra suffix: "mlb-tor-mil-2026-04-16-game-1"

    Args:
        event_slug: Polymarket event slug string.

    Returns:
        Tuple of (sport, canonical_team1, canonical_team2, date_str), or None.
        team1/team2 are canonical abbreviations (e.g., "TOR", "MIL").
    """
    if not event_slug:
        return None

    m = _SLUG_PATTERN.match(event_slug.lower().strip())
    if not m:
        return None

    sport = m.group(1).lower()
    raw_t1 = m.group(2).lower()
    raw_t2 = m.group(3).lower()
    raw_date = m.group(4)

    # Normalize date
    date_str = _normalize_date(raw_date.replace("-", "").zfill(8) if len(raw_date) == 8 else raw_date)
    if len(raw_date) == 8 and not _DATE_DASH.match(raw_date):
        date_str = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}"
    else:
        date_str = raw_date

    # Resolve team abbreviations
    lookup = _SPORT_LOOKUP.get(sport, {})
    t1 = lookup.get(raw_t1) or raw_t1.upper()
    t2 = lookup.get(raw_t2) or raw_t2.upper()

    return (sport, t1, t2, date_str)


# Common team name patterns in questions (order matters: longer phrases first)
_QUESTION_TEAM_PATTERNS: dict[str, list[re.Pattern]] = {
    "nba": [
        re.compile(
            r"will\s+(?:the\s+)?(.+?)\s+(?:beat|defeat|win\s+(?:against|over))\s+(?:the\s+)?(.+?)\??$",
            re.IGNORECASE,
        ),
        re.compile(
            r"(.+?)\s+vs\.?\s+(.+?)\??$",
            re.IGNORECASE,
        ),
    ],
    "mlb": [
        re.compile(
            r"will\s+(?:the\s+)?(.+?)\s+(?:beat|defeat|win\s+(?:against|over))\s+(?:the\s+)?(.+?)\??$",
            re.IGNORECASE,
        ),
        re.compile(
            r"(.+?)\s+vs\.?\s+(.+?)\??$",
            re.IGNORECASE,
        ),
        # NRFI format: "No run scored in the 1st inning: NYY vs BOS"
        re.compile(
            r"(?:nrfi|yrfi|first\s+inning)[^:]*:\s*(.+?)\s+vs\.?\s+(.+?)(?:\?|$)",
            re.IGNORECASE,
        ),
    ],
}


def extract_teams_from_question(
    question: str,
    sport: str,
) -> tuple[str, str] | None:
    """Extract (team1, team2) canonical abbreviations from a game question.

    Args:
        question: Market question text.
        sport: 'nba' or 'mlb'.

    Returns:
        Tuple of (canonical_team1, canonical_team2), or None if not found.
    """
    patterns = _QUESTION_TEAM_PATTERNS.get(sport, [])
    lookup = _SPORT_LOOKUP.get(sport, {})

    for pat in patterns:
        m = pat.search(question.strip())
        if not m:
            continue

        raw1 = m.group(1).strip()
        raw2 = m.group(2).strip()

        # Strip "the " prefix
        raw1 = re.sub(r"^the\s+", "", raw1, flags=re.IGNORECASE)
        raw2 = re.sub(r"^the\s+", "", raw2, flags=re.IGNORECASE)

        t1 = lookup.get(raw1.lower()) or raw1
        t2 = lookup.get(raw2.lower()) or raw2

        # If we still have long strings, try partial matching
        if len(t1) > 4:
            t1 = _partial_match(raw1, lookup) or t1
        if len(t2) > 4:
            t2 = _partial_match(raw2, lookup) or t2

        return (t1.upper(), t2.upper())

    return None


def _partial_match(raw: str, lookup: dict[str, str]) -> str | None:
    """Try to match by checking if any lookup key is contained in raw or vice versa."""
    raw_lower = raw.lower()
    # Prefer longer matching keys (more specific)
    best = None
    best_len = 0
    for key, abbr in lookup.items():
        if len(key) >= 3 and (key in raw_lower or raw_lower in key):
            if len(key) > best_len:
                best = abbr
                best_len = len(key)
    return best


def get_team_info(abbr: str, sport: str) -> TeamInfo | None:
    """Get TeamInfo for a canonical abbreviation.

    Args:
        abbr: Canonical team abbreviation.
        sport: 'nba' or 'mlb'.

    Returns:
        TeamInfo or None.
    """
    teams = NBA_TEAMS if sport == "nba" else MLB_TEAMS
    return teams.get(abbr.upper())
