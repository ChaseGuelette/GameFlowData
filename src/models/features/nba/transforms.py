"""Pure NBA feature transforms and default policies.

These helpers are intentionally DB-free so Lane 03 can move transformation
behavior out of the production FeatureStore without changing feature semantics.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def _value(row: Mapping[str, Any], key: str, default: float = 0.0) -> Any:
    value = row.get(key)
    return default if value is None else value


def safe_ratio(numerator: float | int | None, denominator: float | int | None, *, default: float = 1.0) -> float:
    """Legacy safe division: missing numerator becomes 0, bad denominator returns default."""
    den = 0 if denominator is None else denominator
    if den <= 0:
        return default
    num = 0 if numerator is None else numerator
    return float(num) / float(den)


def starter_probability(games_started_l5: float | int | None) -> float:
    """Convert last-5 starts into the legacy capped starter probability."""
    starts = 0 if games_started_l5 is None else games_started_l5
    return min(float(starts) / 5.0, 1.0)


def rest_schedule_features(stored_rest_days: float | int | None, games_last_7d: float | int | None) -> dict[str, float | int]:
    """Return legacy rest/schedule features and defaults."""
    rest = min(stored_rest_days, 7) if stored_rest_days is not None else 3
    return {
        "rest_days": rest,
        "is_back_to_back": 1 if rest == 1 else 0,
        "games_in_last_7_days": games_last_7d or 2,
    }


def default_player_rolling_features() -> dict[str, float | int]:
    """Fallback returned when no player rolling row exists."""
    return {
        "player_avg_min_l5": 0,
        "player_avg_min_l15": 0,
        "player_avg_pts_l5": 0,
        "player_avg_pts_l15": 0,
        "player_avg_reb_l5": 0,
        "player_avg_ast_l5": 0,
        "player_avg_fg3m_l5": 0,
        "player_avg_fg3a_l5": 0,
        "player_avg_usg_pct_l5": 0.20,
        "player_avg_ts_pct_l15": 0.56,
        "player_avg_reb_pct_l5": 0.10,
        "player_avg_ast_pct_l5": 0.12,
        "player_avg_min_l3": 0,
        "player_avg_pts_l3": 0,
        "player_avg_reb_l3": 0,
        "player_avg_ast_l3": 0,
        "player_avg_fg3m_l3": 0,
        "player_avg_min_szn": 0,
        "player_pts_l3_l15_ratio": 1.0,
        "player_reb_l3_l15_ratio": 1.0,
        "player_ast_l3_l15_ratio": 1.0,
        "player_fg3m_l3_l15_ratio": 1.0,
        "player_min_l3_l5_ratio": 1.0,
        "player_min_l3_l15_ratio": 1.0,
        "player_min_std_l5": 0,
        "player_std_pts_l5": 0,
        "player_std_reb_l5": 0,
        "player_std_ast_l5": 0,
        "player_std_fg3m_l5": 0,
        "player_min_floor_l5": 0,
        "player_games_started_l5": 0,
        "player_starter_prob": 0,
        "rest_days": 3,
        "is_back_to_back": 0,
        "games_in_last_7_days": 2,
    }


def build_player_rolling_features(row: Mapping[str, Any]) -> dict[str, float | int]:
    """Map a player rolling DB row into legacy feature names and derived values."""
    stats: dict[str, float | int] = {}
    for key in [
        "avg_min_l5",
        "avg_min_l15",
        "avg_pts_l5",
        "avg_pts_l15",
        "avg_reb_l5",
        "avg_ast_l5",
        "avg_fg3m_l5",
        "avg_fg3a_l5",
        "avg_usg_pct_l5",
        "avg_ts_pct_l15",
        "avg_reb_pct_l5",
        "avg_ast_pct_l5",
        "avg_min_l3",
        "avg_pts_l3",
        "avg_reb_l3",
        "avg_ast_l3",
        "avg_fg3m_l3",
        "avg_min_szn",
    ]:
        stats[f"player_{key}"] = _value(row, key, 0)

    stats["player_min_std_l5"] = _value(row, "std_min_l5", 0)
    stats["player_std_pts_l5"] = _value(row, "std_pts_l5", 0)
    stats["player_std_reb_l5"] = _value(row, "std_reb_l5", 0)
    stats["player_std_ast_l5"] = _value(row, "std_ast_l5", 0)
    stats["player_std_fg3m_l5"] = _value(row, "std_fg3m_l5", 0)

    stats["player_min_floor_l5"] = _value(row, "min_floor_l5", 0)
    stats["player_games_started_l5"] = _value(row, "games_started_l5", 0)
    stats["player_starter_prob"] = starter_probability(stats["player_games_started_l5"])

    # NOTE (ISS-017): *_l3_l15_ratio names are artifact-compatible historical
    # names. Only PTS uses L15; REB/AST/THREES use L5 denominators.
    stats["player_pts_l3_l15_ratio"] = safe_ratio(row.get("avg_pts_l3"), row.get("avg_pts_l15"))
    stats["player_reb_l3_l15_ratio"] = safe_ratio(row.get("avg_reb_l3"), row.get("avg_reb_l5"))
    stats["player_ast_l3_l15_ratio"] = safe_ratio(row.get("avg_ast_l3"), row.get("avg_ast_l5"))
    stats["player_fg3m_l3_l15_ratio"] = safe_ratio(row.get("avg_fg3m_l3"), row.get("avg_fg3m_l5"))
    stats["player_min_l3_l5_ratio"] = safe_ratio(row.get("avg_min_l3"), row.get("avg_min_l5"))
    stats["player_min_l3_l15_ratio"] = safe_ratio(row.get("avg_min_l3"), row.get("avg_min_l15"))

    stats.update(rest_schedule_features(row.get("stored_rest_days"), row.get("games_last_7d")))
    return stats


__all__ = [
    "build_player_rolling_features",
    "default_player_rolling_features",
    "rest_schedule_features",
    "safe_ratio",
    "starter_probability",
]
