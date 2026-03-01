"""
MLB Processing Configuration
=============================
Shared constants for mlb_linker and mlb_populate_averages scripts.
"""

# ============================================================================
# ROLLING WINDOW DEFINITIONS
# ============================================================================

BATTING_WINDOWS = {"l5": 5, "l10": 10, "l20": 20, "szn": None}
PITCHING_WINDOWS = {"l3": 3, "l5": 5, "szn": None}

# ============================================================================
# STAT LISTS
# ============================================================================

# 12 batting stats for rolling averages
BATTING_STATS = [
    "pa", "ab", "r", "h", "doubles", "triples",
    "hr", "rbi", "bb", "so", "sb", "tb",
]

# 7 batting stats for L5 standard deviation
BATTING_STD_STATS = ["h", "hr", "tb", "rbi", "r", "so", "sb"]

# 8 pitching stats for rolling averages
PITCHING_STATS = [
    "ip", "h_allowed", "r_allowed", "er",
    "bb", "so", "hr_allowed", "pitches_thrown",
]

# 2 pitching stats for std dev
PITCHING_STD_STATS = ["so", "er"]

# ============================================================================
# BATCH SIZES
# ============================================================================

BATCH_SIZE = 100  # rows per DB insert batch (keeps params under PG 32k limit)
LINKER_BATCH_SIZE = 50_000  # rows per linker processing batch
FUZZY_THRESHOLD = 0.70  # minimum SequenceMatcher ratio for auto-match

# ============================================================================
# TEAM ALIASES — Odds API full names → MLB abbreviations
# ============================================================================

MLB_TEAM_ALIASES = {
    # Standard Odds API names
    "Arizona Diamondbacks": "ARI",
    "Atlanta Braves": "ATL",
    "Baltimore Orioles": "BAL",
    "Boston Red Sox": "BOS",
    "Chicago Cubs": "CHC",
    "Chicago White Sox": "CWS",
    "Cincinnati Reds": "CIN",
    "Cleveland Guardians": "CLE",
    "Colorado Rockies": "COL",
    "Detroit Tigers": "DET",
    "Houston Astros": "HOU",
    "Kansas City Royals": "KC",
    "Los Angeles Angels": "LAA",
    "Los Angeles Dodgers": "LAD",
    "Miami Marlins": "MIA",
    "Milwaukee Brewers": "MIL",
    "Minnesota Twins": "MIN",
    "New York Mets": "NYM",
    "New York Yankees": "NYY",
    "Oakland Athletics": "OAK",
    "Philadelphia Phillies": "PHI",
    "Pittsburgh Pirates": "PIT",
    "San Diego Padres": "SD",
    "San Francisco Giants": "SF",
    "Seattle Mariners": "SEA",
    "St. Louis Cardinals": "STL",
    "Tampa Bay Rays": "TB",
    "Texas Rangers": "TEX",
    "Toronto Blue Jays": "TOR",
    "Washington Nationals": "WSH",
    # Common variants
    "Cleveland Indians": "CLE",
    "LA Angels": "LAA",
    "LA Dodgers": "LAD",
    "Athletics": "OAK",
    "D-backs": "ARI",
    "Diamondbacks": "ARI",
    # Abbreviation pass-through
    "ARI": "ARI", "ATL": "ATL", "BAL": "BAL", "BOS": "BOS",
    "CHC": "CHC", "CWS": "CWS", "CIN": "CIN", "CLE": "CLE",
    "COL": "COL", "DET": "DET", "HOU": "HOU", "KC": "KC",
    "LAA": "LAA", "LAD": "LAD", "MIA": "MIA", "MIL": "MIL",
    "MIN": "MIN", "NYM": "NYM", "NYY": "NYY", "OAK": "OAK",
    "PHI": "PHI", "PIT": "PIT", "SD": "SD", "SF": "SF",
    "SEA": "SEA", "STL": "STL", "TB": "TB", "TEX": "TEX",
    "TOR": "TOR", "WSH": "WSH",
}
