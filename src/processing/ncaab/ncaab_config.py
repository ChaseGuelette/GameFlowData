"""
NCAAB Processing Configuration
================================
Shared constants for ncaab_linker, ncaab_populate_averages,
and ncaab_barttorvik_linker scripts.
"""

# ============================================================================
# ROLLING WINDOW DEFINITIONS
# ============================================================================

ROLLING_WINDOWS = {"l5": 5, "l10": 10, "l20": 20, "szn": None}

# ============================================================================
# STAT LISTS
# ============================================================================

# 16 team box score stats for rolling averages
TEAM_BOX_STATS = [
    "pts", "poss_est", "fga", "fta",
    "oreb", "dreb", "ast", "tov",
    "stl", "blk", "fg3m", "fg3a",
    "efg_pct", "tov_pct", "orb_pct", "ft_rate",
]

# Defensive Four Factors (opponent stats from box scores)
TEAM_OPP_STATS = [
    "opp_efg_pct", "opp_tov_pct", "opp_orb_pct", "opp_ft_rate",
]

# Stats for L5 standard deviation
STD_STATS = ["pts", "poss_est"]

# ============================================================================
# BATCH SIZES
# ============================================================================

BATCH_SIZE = 100
LINKER_BATCH_SIZE = 25_000
FUZZY_THRESHOLD = 0.72

# ============================================================================
# BARTTORVIK TEAM NAME -> ESPN TEAM NAME MAPPING
# ============================================================================
# Barttorvik uses slightly different team names than ESPN/CBBpy.
# This dict maps Barttorvik names to the canonical ESPN names stored
# in ncaab_teams.team_name. Build iteratively: scrape first, review
# unmatched names, add entries here.

BARTTORVIK_TO_ESPN = {
    "UConn": "Connecticut",
    "N.C. State": "NC State",
    "St. John's (NY)": "St. John's",
    "VCU": "Virginia Commonwealth",
    "UNLV": "Nevada-Las Vegas",
    "Pitt": "Pittsburgh",
    "Miami (FL)": "Miami",
    "Ole Miss": "Mississippi",
    "Southern Miss": "Southern Mississippi",
    "Middle Tennessee": "Middle Tennessee State",
    "UTSA": "UT San Antonio",
    "UTEP": "Texas-El Paso",
    "UAB": "Alabama-Birmingham",
    "UMass": "Massachusetts",
    "UIC": "Illinois-Chicago",
    "UMKC": "Missouri-Kansas City",
    "UMBC": "Maryland-Baltimore County",
    "FIU": "Florida International",
    "FAU": "Florida Atlantic",
    "FDU": "Fairleigh Dickinson",
    "LMU (CA)": "Loyola Marymount",
    "Loyola (IL)": "Loyola Chicago",
    "SIU Edwardsville": "SIUE",
    "Long Island": "LIU",
    "Cal State Fullerton": "CS Fullerton",
    "Cal State Bakersfield": "CS Bakersfield",
    "Cal State Northridge": "CS Northridge",
    # Add more as discovered during initial scrape
}

# ============================================================================
# ODDS API TEAM NAME NORMALIZATION
# ============================================================================
# The Odds API uses full "City Mascot" names for NCAAB teams.
# This dict will be populated iteratively after the first live scrape
# reveals the exact team name format used.
# Key = Odds API name, Value = ESPN team_name from ncaab_teams

ODDS_API_TEAM_ALIASES: dict[str, str] = {
    # Populated iteratively after first scrape
    # Example patterns:
    # "Duke Blue Devils": "Duke",
    # "Kansas Jayhawks": "Kansas",
    # "Connecticut Huskies": "UConn",  -> mapped via BARTTORVIK_TO_ESPN
}
