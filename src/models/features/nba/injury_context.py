"""NBA injury-context source helpers.

This module owns injury-context defaults, SQL query builders, and row mapping for
NBA FeatureStore compatibility paths. It preserves the current production
semantics while moving injury source ownership out of the god-class facade.
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, time
from math import isfinite
from types import SimpleNamespace
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.sql.elements import TextClause

from src.models.features.nba.historical_observation_adapters import (
    InjuryHistoryState,
    SourceProvenance,
    evaluate_injury_observation,
)
from src.models.features.nba.transforms import starter_probability

if TYPE_CHECKING:
    from src.models.features.nba.historical_loader import RollingSnapshot


def default_injury_context() -> dict[str, float | int]:
    """Default injury feature values when injury source rows are unavailable."""
    return {
        "team_out_count": 0,
        "team_out_min_sum": 0,
        "team_out_pts_sum": 0,
        "team_out_reb_sum": 0,
        "team_out_ast_sum": 0,
        "team_out_usg_sum": 0,
        "opp_out_count": 0,
        "opp_out_min_sum": 0,
        "player_is_questionable": 0,
        "player_is_probable": 0,
        "team_out_same_pos_count": 0,
        "team_out_same_pos_min_sum": 0,
        "team_out_same_pos_usg_sum": 0,
        "team_out_same_pos_starter_sum": 0,
    }


def status_flags(status: str | None) -> dict[str, int]:
    """Map injury status text into FeatureStore questionable/probable flags."""
    return {
        "player_is_questionable": 1 if status == "Questionable" else 0,
        "player_is_probable": 1 if status == "Probable" else 0,
    }


def _as_float(value) -> float:
    return float(value or 0)


def build_injury_context(
    *,
    team_game_result=None,
    team_advanced_result=None,
    opponent_result=None,
    player_status_result=None,
    same_position_result=None,
) -> dict[str, float | int]:
    """Build FeatureStore injury-context features from fetched query rows."""
    context = default_injury_context()

    if team_game_result:
        context["team_out_count"] = team_game_result.out_count or 0
        context["team_out_min_sum"] = _as_float(team_game_result.out_min_sum)
        context["team_out_pts_sum"] = _as_float(team_game_result.out_pts_sum)
        context["team_out_reb_sum"] = _as_float(team_game_result.out_reb_sum)
        context["team_out_ast_sum"] = _as_float(team_game_result.out_ast_sum)

    if team_advanced_result:
        context["team_out_usg_sum"] = _as_float(team_advanced_result.out_usg_sum)

    if opponent_result:
        context["opp_out_count"] = opponent_result.out_count or 0
        context["opp_out_min_sum"] = _as_float(opponent_result.out_min_sum)

    if player_status_result:
        context.update(status_flags(player_status_result.status))

    if same_position_result:
        context["team_out_same_pos_count"] = same_position_result.out_count or 0
        context["team_out_same_pos_min_sum"] = _as_float(same_position_result.out_min_sum)
        context["team_out_same_pos_usg_sum"] = _as_float(same_position_result.out_usg_sum)
        context["team_out_same_pos_starter_sum"] = _as_float(same_position_result.out_starter_sum)

    return context


def team_injury_game_query() -> TextClause:
    """Build team OUT-player game-stat injury aggregate query."""
    return text("""
        SELECT
            COUNT(*) as out_count,
            COALESCE(SUM(inj_sub.avg_min_l5), 0) as out_min_sum,
            COALESCE(SUM(inj_sub.avg_pts_l5), 0) as out_pts_sum,
            COALESCE(SUM(inj_sub.avg_reb_l5), 0) as out_reb_sum,
            COALESCE(SUM(inj_sub.avg_ast_l5), 0) as out_ast_sum
        FROM (
            SELECT DISTINCT ON (ri.player_id)
                pags_inj.avg_min_l5, pags_inj.avg_pts_l5,
                pags_inj.avg_reb_l5, pags_inj.avg_ast_l5
            FROM rapidapi_injuries ri
            LEFT JOIN player_average_game_stats pags_inj
                ON pags_inj.player_id = ri.player_id
                AND pags_inj.game_date < :game_date
            WHERE ri.nba_team_id = :team_id
              AND ri.report_date = :game_date
              AND ri.status = 'Out'
              AND ri.player_id IS NOT NULL
            ORDER BY ri.player_id, pags_inj.game_date DESC
        ) inj_sub
    """)


def team_injury_advanced_query() -> TextClause:
    """Build team OUT-player advanced-stat injury aggregate query."""
    return text("""
        SELECT
            COALESCE(SUM(inj_sub.avg_usg_pct_l5), 0) as out_usg_sum
        FROM (
            SELECT DISTINCT ON (ri.player_id)
                paas_inj.avg_usg_pct_l5
            FROM rapidapi_injuries ri
            LEFT JOIN player_average_advanced_stats paas_inj
                ON paas_inj.player_id = ri.player_id
                AND paas_inj.game_date < :game_date
            WHERE ri.nba_team_id = :team_id
              AND ri.report_date = :game_date
              AND ri.status = 'Out'
              AND ri.player_id IS NOT NULL
            ORDER BY ri.player_id, paas_inj.game_date DESC
        ) inj_sub
    """)


def opponent_injury_query() -> TextClause:
    """Build opponent OUT-player injury aggregate query."""
    return text("""
        SELECT
            COUNT(*) as out_count,
            COALESCE(SUM(inj_sub.avg_min_l5), 0) as out_min_sum
        FROM (
            SELECT DISTINCT ON (ri.player_id)
                pags_inj.avg_min_l5
            FROM rapidapi_injuries ri
            LEFT JOIN player_average_game_stats pags_inj
                ON pags_inj.player_id = ri.player_id
                AND pags_inj.game_date < :game_date
            WHERE ri.nba_team_id = :opponent_id
              AND ri.report_date = :game_date
              AND ri.status = 'Out'
              AND ri.player_id IS NOT NULL
            ORDER BY ri.player_id, pags_inj.game_date DESC
        ) inj_sub
    """)


def player_injury_status_query() -> TextClause:
    """Build latest player injury-status query for one player/date."""
    return text("""
        SELECT status
        FROM rapidapi_injuries
        WHERE player_id = :player_id
          AND report_date = :game_date
        ORDER BY id DESC LIMIT 1
    """)


def same_position_injury_query() -> TextClause:
    """Build same-position teammate OUT-player opportunity aggregate query."""
    return text("""
        SELECT
            COUNT(*) as out_count,
            COALESCE(SUM(inj_sub.avg_min_l5), 0) as out_min_sum,
            COALESCE(SUM(inj_sub.avg_usg_pct_l5), 0) as out_usg_sum,
            COALESCE(SUM(LEAST(inj_sub.games_started_l5 / 5.0, 1.0)), 0) as out_starter_sum
        FROM (
            SELECT DISTINCT ON (ri.player_id)
                pags_inj.avg_min_l5,
                pags_inj.games_started_l5,
                paas_inj.avg_usg_pct_l5
            FROM rapidapi_injuries ri
            LEFT JOIN player_average_game_stats pags_inj
                ON pags_inj.player_id = ri.player_id
                AND pags_inj.game_date < :game_date
            LEFT JOIN player_average_advanced_stats paas_inj
                ON paas_inj.player_id = ri.player_id
                AND paas_inj.game_date < :game_date
            LEFT JOIN LATERAL (
                SELECT position_group
                FROM player_position_history
                WHERE player_id = ri.player_id
                  AND snapshot_date < :game_date
                ORDER BY snapshot_date DESC LIMIT 1
            ) pph ON TRUE
            WHERE ri.nba_team_id = :team_id
              AND ri.report_date = :game_date
              AND ri.status = 'Out'
              AND ri.player_id IS NOT NULL
              AND ri.player_id != :player_id
              AND pph.position_group = :position_group
            ORDER BY ri.player_id, pags_inj.game_date DESC
        ) inj_sub
    """)


def bulk_injury_features_query() -> TextClause:
    """Build bulk OUT-player injury feature source query for training enrichment."""
    return text("""
        SELECT
            ri.nba_team_id,
            ri.report_date,
            ri.player_id as inj_player_id,
            pags_l.avg_min_l5,
            pags_l.avg_pts_l5,
            pags_l.avg_reb_l5,
            pags_l.avg_ast_l5,
            pags_l.games_started_l5,
            paas_l.avg_usg_pct_l5,
            pph.position_group as inj_position_group
        FROM (
            SELECT DISTINCT ON (nba_team_id, report_date, player_id)
                nba_team_id, report_date, player_id
            FROM rapidapi_injuries
            WHERE status = 'Out'
              AND player_id IS NOT NULL
              AND report_date = ANY(:dates)
            ORDER BY nba_team_id, report_date, player_id, id DESC
        ) ri
        LEFT JOIN LATERAL (
            SELECT avg_min_l5, avg_pts_l5, avg_reb_l5, avg_ast_l5, games_started_l5
            FROM player_average_game_stats
            WHERE player_id = ri.player_id
              AND game_date < ri.report_date
            ORDER BY game_date DESC LIMIT 1
        ) pags_l ON TRUE
        LEFT JOIN LATERAL (
            SELECT avg_usg_pct_l5
            FROM player_average_advanced_stats
            WHERE player_id = ri.player_id
              AND game_date < ri.report_date
            ORDER BY game_date DESC LIMIT 1
        ) paas_l ON TRUE
        LEFT JOIN LATERAL (
            SELECT position_group
            FROM player_position_history
            WHERE player_id = ri.player_id
              AND snapshot_date < ri.report_date
            ORDER BY snapshot_date DESC LIMIT 1
        ) pph ON TRUE
    """)


def player_injury_status_bulk_query() -> TextClause:
    """Build bulk latest player injury-status query for training enrichment."""
    return text("""
        SELECT DISTINCT ON (player_id, report_date)
            player_id, report_date, status as inj_status
        FROM rapidapi_injuries
        WHERE player_id IS NOT NULL
          AND report_date = ANY(:dates)
        ORDER BY player_id, report_date, id DESC
    """)


def training_injury_raw_rows_query() -> TextClause:
    """Build bounded raw injury rows for chronology-safe training enrichment."""
    return text("""
        SELECT
            ri.id as injury_row_id,
            ri.nba_team_id,
            ri.report_date,
            ri.player_id as inj_player_id,
            ri.player as inj_player_name,
            ri.team as inj_team_name,
            ri.status as inj_status,
            ri.reason as inj_reason,
            ri.source_observations,
            pags_l.avg_min_l5,
            pags_l.avg_pts_l5,
            pags_l.avg_reb_l5,
            pags_l.avg_ast_l5,
            pags_l.games_started_l5,
            paas_l.avg_usg_pct_l5,
            pph.position_group as inj_position_group
        FROM rapidapi_injuries ri
        LEFT JOIN LATERAL (
            SELECT avg_min_l5, avg_pts_l5, avg_reb_l5, avg_ast_l5, games_started_l5
            FROM player_average_game_stats
            WHERE player_id = ri.player_id
              AND game_date < ri.report_date
            ORDER BY game_date DESC LIMIT 1
        ) pags_l ON TRUE
        LEFT JOIN LATERAL (
            SELECT avg_usg_pct_l5
            FROM player_average_advanced_stats
            WHERE player_id = ri.player_id
              AND game_date < ri.report_date
            ORDER BY game_date DESC LIMIT 1
        ) paas_l ON TRUE
        LEFT JOIN LATERAL (
            SELECT position_group
            FROM player_position_history
            WHERE player_id = ri.player_id
              AND snapshot_date < ri.report_date
            ORDER BY snapshot_date DESC LIMIT 1
        ) pph ON TRUE
        WHERE ri.player_id IS NOT NULL
          AND ri.nba_team_id IS NOT NULL
          AND ri.report_date = ANY(:dates)
        ORDER BY ri.report_date, ri.nba_team_id, ri.player_id, ri.id
    """)


TEAM_INJURY_AGG_COLUMNS = [
    "nba_team_id",
    "report_date",
    "out_count",
    "out_min_sum",
    "out_pts_sum",
    "out_reb_sum",
    "out_ast_sum",
    "out_usg_sum",
]


def empty_team_injury_aggregation() -> pd.DataFrame:
    """Return the current empty bulk team-injury aggregation schema."""
    return pd.DataFrame(columns=TEAM_INJURY_AGG_COLUMNS)


def aggregate_team_injury_features(inj_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate bulk per-injured-player rows by NBA team and report date."""
    if inj_df.empty:
        return empty_team_injury_aggregation()

    return inj_df.groupby(["nba_team_id", "report_date"]).agg(
        out_count=("inj_player_id", "nunique"),
        out_min_sum=("avg_min_l5", lambda x: np.nansum(x)),
        out_pts_sum=("avg_pts_l5", lambda x: np.nansum(x)),
        out_reb_sum=("avg_reb_l5", lambda x: np.nansum(x)),
        out_ast_sum=("avg_ast_l5", lambda x: np.nansum(x)),
        out_usg_sum=("avg_usg_pct_l5", lambda x: np.nansum(x)),
    ).reset_index()


def _provider_observations(
    row: Mapping,
    supported_report_times: Mapping[str, time],
) -> tuple[list[dict], Counter]:
    """Parse explicitly supported provider clocks; retrieval time is provenance only."""
    counts: Counter = Counter()
    observations = row.get("source_observations")
    if isinstance(observations, str):
        try:
            observations = json.loads(observations)
        except (TypeError, ValueError):
            observations = None
    if not isinstance(observations, dict):
        counts["missing_or_invalid_observation"] += 1
        return [], counts

    parsed: list[dict] = []
    for capture_id, observation in observations.items():
        if not isinstance(observation, dict):
            counts["missing_or_invalid_observation"] += 1
            continue
        requested_raw = observation.get("requested_date")
        try:
            requested_date = date.fromisoformat(requested_raw)
        except (TypeError, ValueError):
            counts["missing_or_invalid_observation"] += 1
            continue
        records = observation.get("records")
        if not isinstance(records, list):
            counts["unsupported_payload"] += 1
            continue
        for record in records:
            raw = record.get("raw") if isinstance(record, dict) else None
            if not isinstance(raw, dict):
                counts["unsupported_payload"] += 1
                continue
            label = raw.get("reportTime")
            mapped_time = supported_report_times.get(label) if isinstance(label, str) else None
            if not isinstance(mapped_time, time) or mapped_time.tzinfo is None:
                counts["unsupported_report_time"] += 1
                continue
            provider_at = datetime.combine(requested_date, mapped_time)
            if provider_at.utcoffset() is None:
                counts["unsupported_report_time"] += 1
                continue
            raw_date = raw.get("date")
            try:
                payload_date = date.fromisoformat(raw_date)
            except (TypeError, ValueError):
                counts["unsupported_payload"] += 1
                continue
            parsed.append({
                "provider_at": pd.Timestamp(provider_at).tz_convert("UTC"),
                "requested_date": requested_date,
                "payload_date": payload_date,
                "capture_id": str(capture_id),
                "capture_sha256": str(observation.get("capture_sha256") or ""),
                "record_ordinal": record.get("ordinal"),
                "retrieved_at_utc": observation.get("retrieved_at_utc"),
                "raw_status": raw.get("status"),
                "raw_report_time": label,
                "raw_player": raw.get("player"),
                "raw_team": raw.get("team"),
                "raw_reason": raw.get("reason"),
            })
    return parsed, counts


def _normalise_game_contexts(game_contexts) -> pd.DataFrame:
    if isinstance(game_contexts, list | tuple | set) and all(
        not isinstance(value, Mapping) for value in game_contexts
    ):
        # Compatibility for the old private FeatureStore wrappers: dates alone
        # have no game/team/cutoff authority, so they can only produce unknowns.
        values = list(game_contexts)
        if not values:
            return pd.DataFrame(columns=[
                "game_id", "game_date", "nba_team_id", "injury_cutoff_utc"
            ])
        return pd.DataFrame({
            "game_id": [pd.NA] * len(values),
            "game_date": pd.to_datetime(values, errors="coerce").date,
            "nba_team_id": [pd.NA] * len(values),
            "injury_cutoff_utc": [pd.NaT] * len(values),
        })
    contexts = pd.DataFrame(game_contexts).copy()
    required = {"game_id", "game_date", "nba_team_id", "injury_cutoff_utc"}
    if contexts.empty:
        return pd.DataFrame(columns=sorted(required))
    missing = required.difference(contexts.columns)
    if missing:
        raise ValueError(f"Missing injury game context columns: {sorted(missing)}")
    contexts["game_date"] = pd.to_datetime(contexts["game_date"], errors="coerce").dt.date
    contexts["injury_cutoff_utc"] = pd.to_datetime(
        contexts["injury_cutoff_utc"], errors="coerce", utc=True
    )
    return contexts.drop_duplicates(["game_id", "nba_team_id"])


TRAINING_SELECTED_INJURY_COLUMNS = [
    "injury_row_id", "nba_team_id", "report_date", "inj_player_id",
    "inj_player_name", "inj_team_name", "inj_status", "inj_reason",
    "source_observations", "avg_min_l5", "avg_pts_l5", "avg_reb_l5",
    "avg_ast_l5", "games_started_l5", "avg_usg_pct_l5",
    "inj_position_group", "game_id", "game_date", "injury_cutoff_utc",
    "provider_at", "requested_date", "payload_date", "capture_id",
    "capture_sha256", "record_ordinal", "retrieved_at_utc", "raw_status",
    "raw_report_time", "raw_player", "raw_team", "raw_reason",
]


def empty_training_injury_rows() -> pd.DataFrame:
    """Return the explicit schema for chronology-selected training rows."""
    return pd.DataFrame(columns=TRAINING_SELECTED_INJURY_COLUMNS)


def empty_training_team_injury_aggregation() -> pd.DataFrame:
    """Return unknown ordinary-training aggregates with a stable merge schema."""
    return pd.DataFrame(columns=["game_id", *TEAM_INJURY_AGG_COLUMNS])


def _same_source_value(left, right) -> bool:
    """Compare retained flat identity fields to their nested raw record."""
    left_missing = left is None or (not isinstance(left, (list, dict)) and pd.isna(left))
    right_missing = right is None or (not isinstance(right, (list, dict)) and pd.isna(right))
    if left_missing or right_missing:
        return left_missing and right_missing
    return left == right


def _source_identity_value(value):
    if value is None or (not isinstance(value, (list, dict)) and pd.isna(value)):
        return None
    return value


def _select_training_injury_rows(
    raw_rows: pd.DataFrame,
    game_contexts,
    supported_report_times: Mapping[str, time] | None,
) -> tuple[pd.DataFrame, dict[str, int]]:
    """Select explicit statuses from each team's latest eligible report edition."""
    contexts = _normalise_game_contexts(game_contexts)
    counts: Counter = Counter()
    if contexts.empty or raw_rows.empty:
        counts["no_accepted_snapshot"] += len(contexts)
        return empty_training_injury_rows(), dict(counts)
    supported = supported_report_times or {}
    if not isinstance(supported, Mapping):
        raise ValueError("supported_report_times must be an explicit mapping")
    candidates: list[dict] = []
    rows_by_date: dict[date, list[dict]] = {}
    for row in raw_rows.to_dict("records"):
        report_date = pd.to_datetime(row.get("report_date"), errors="coerce")
        if pd.isna(report_date):
            counts["unsupported_payload"] += 1
            continue
        rows_by_date.setdefault(report_date.date(), []).append(row)

    for context in contexts.to_dict("records"):
        cutoff = context.get("injury_cutoff_utc")
        if pd.isna(cutoff):
            counts["missing_cutoff"] += 1
            continue
        team_rows = [
            row for row in rows_by_date.get(context["game_date"], [])
            if row.get("nba_team_id") == context["nba_team_id"]
        ]
        for row in team_rows:
            observations, rejected = _provider_observations(row, supported)
            counts.update(rejected)
            for observation in observations:
                # The retained raw payload must identify this exact game date. Adjacent-day
                # requests are accepted, but older/future request routing is not guessed.
                day_gap = (context["game_date"] - observation["requested_date"]).days
                if observation["payload_date"] != context["game_date"] or day_gap not in (0, 1):
                    counts["unsupported_payload"] += 1
                    continue
                if observation["provider_at"] > cutoff:
                    counts["after_cutoff"] += 1
                    continue
                if not all((
                    _same_source_value(row.get("inj_status"), observation["raw_status"]),
                    _same_source_value(row.get("inj_reason"), observation["raw_reason"]),
                    _same_source_value(row.get("inj_player_name"), observation["raw_player"]),
                    _same_source_value(row.get("inj_team_name"), observation["raw_team"]),
                )):
                    counts["identity_mismatch"] += 1
                    continue
                candidate = dict(row)
                candidate.update(context)
                candidate.update(observation)
                candidates.append(candidate)

    if not candidates:
        counts["no_accepted_snapshot"] += len(contexts)
        return empty_training_injury_rows(), dict(counts)

    candidate_df = pd.DataFrame(candidates)
    selected: list[pd.DataFrame] = []
    for _, team_group in candidate_df.groupby(
        ["game_id", "nba_team_id"], dropna=False, sort=True
    ):
        latest_at = team_group["provider_at"].max()
        latest_team = team_group[team_group["provider_at"] == latest_at]
        counts["latest_team_editions"] += 1
        for _, player_group in latest_team.groupby("inj_player_id", dropna=False, sort=True):
            signatures = set(zip(
                [_source_identity_value(value) for value in player_group["inj_status"]],
                [_source_identity_value(value) for value in player_group["inj_reason"]],
                [_source_identity_value(value) for value in player_group["inj_player_name"]],
                [_source_identity_value(value) for value in player_group["inj_team_name"]],
            ))
            if len(signatures) != 1:
                counts["conflicting_latest_status"] += 1
                continue
            selected.append(player_group.iloc[[-1]])
    if not selected:
        counts["no_accepted_snapshot"] += len(contexts)
        return empty_training_injury_rows(), dict(counts)
    result = pd.concat(selected, ignore_index=True).reindex(
        columns=TRAINING_SELECTED_INJURY_COLUMNS
    )
    counts["accepted_items"] += len(result)
    counts["accepted_games"] += result["game_id"].nunique()
    accepted_team_games = result[["game_id", "nba_team_id"]].drop_duplicates()
    counts["accepted_status_team_games"] += len(accepted_team_games)
    counts["no_accepted_snapshot"] += max(len(contexts) - len(accepted_team_games), 0)
    return result, dict(counts)


def load_injury_features_bulk(engine, game_dates: list) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Pre-aggregate injury features for all (team, date) pairs in one pass."""
    with engine.connect() as conn:
        inj_df = pd.read_sql(
            bulk_injury_features_query(),
            conn,
            params={"dates": list(set(game_dates))},
        )

    return aggregate_team_injury_features(inj_df), inj_df


def load_player_injury_status_bulk(engine, game_dates: list) -> pd.DataFrame:
    """Load latest player injury statuses for all (player, date) pairs."""
    with engine.connect() as conn:
        return pd.read_sql(
            player_injury_status_bulk_query(),
            conn,
            params={"dates": list(set(game_dates))},
        )


def load_training_injury_rows(
    engine,
    game_contexts,
    supported_report_times: Mapping[str, time] | None = None,
) -> tuple[pd.DataFrame, dict[str, int]]:
    """Load explicit latest-edition statuses; aggregate completeness stays unknown."""
    contexts = _normalise_game_contexts(game_contexts)
    dates = sorted({value for value in contexts["game_date"] if value is not None})
    with engine.connect() as conn:
        raw = pd.read_sql(
            training_injury_raw_rows_query(),
            conn,
            params={"dates": dates},
        )
    return _select_training_injury_rows(raw, contexts, supported_report_times)


def get_injury_context(
    conn,
    player_id,
    team_id,
    opponent_id,
    game_date,
    player_position_group: str | None = None,
) -> dict[str, float | int]:
    """Fetch injury context for one player/game feature row."""
    team_game_result = conn.execute(
        team_injury_game_query(),
        {"team_id": team_id, "game_date": game_date},
    ).fetchone()
    team_advanced_result = conn.execute(
        team_injury_advanced_query(),
        {"team_id": team_id, "game_date": game_date},
    ).fetchone()
    opponent_result = conn.execute(
        opponent_injury_query(),
        {"opponent_id": opponent_id, "game_date": game_date},
    ).fetchone()
    player_status_result = conn.execute(
        player_injury_status_query(),
        {"player_id": player_id, "game_date": game_date},
    ).fetchone()

    same_position_result = None
    if player_position_group:
        same_position_result = conn.execute(
            same_position_injury_query(),
            {
                "team_id": team_id,
                "game_date": game_date,
                "player_id": player_id,
                "position_group": player_position_group,
            },
        ).fetchone()

    return build_injury_context(
        team_game_result=team_game_result,
        team_advanced_result=team_advanced_result,
        opponent_result=opponent_result,
        player_status_result=player_status_result,
        same_position_result=same_position_result,
    )


@dataclass(frozen=True, slots=True)
class HistoricalInjuryRow:
    provenance: SourceProvenance
    player_id: int
    team_id: int
    status: str
    history_state: InjuryHistoryState
    local_observed_at: datetime | None
    report_cycle_text: str | None
    position_group: str | None = None
    rolling: RollingSnapshot | None = None
    advanced: RollingSnapshot | None = None

    def __post_init__(self):
        if not isinstance(self.provenance, SourceProvenance) or not isinstance(self.history_state, InjuryHistoryState):
            raise ValueError('injury provenance/history state must be explicit immutable records')


@dataclass(frozen=True, slots=True)
class HistoricalInjuryEdition:
    """Trusted complete team rosters for ONE observed edition, not latest-ID.

    Each row's provenance attributes must reference this edition record ID.
    ``completeness_evidence_ref`` binds the caller's explicit policy authority.
    Missing target/roster records never mean healthy.
    """

    provenance: SourceProvenance
    game_id: str
    team_ids: tuple[int, ...]
    completeness_evidence_ref: str
    rows: tuple[HistoricalInjuryRow, ...]
    roster_membership: tuple[tuple[int, tuple[int, ...]], ...] = ()

    def __post_init__(self):
        if not isinstance(self.provenance, SourceProvenance) or not isinstance(self.team_ids, tuple) or not isinstance(self.rows, tuple):
            raise ValueError('injury edition requires immutable source records')
        if any(not isinstance(row, HistoricalInjuryRow) for row in self.rows):
            raise ValueError('injury rows must be HistoricalInjuryRow')
        if not isinstance(self.roster_membership, tuple) or any(
            not isinstance(item, tuple) or len(item) != 2 or not isinstance(item[1], tuple)
            for item in self.roster_membership
        ):
            raise ValueError('injury roster membership must be deeply immutable')


def historical_injury_features(*, context, player_id, team_id, opponent_id, position_group,
                               edition, injury_policy, weight_values):
    """Aggregate only a complete, eligible observed edition; no omission inference."""
    if injury_policy['missing_history'] != 'block' or injury_policy['omitted_player'] != 'block':
        raise ValueError('unsupported injury missing/omission semantics')
    if (not isinstance(edition, HistoricalInjuryEdition) or edition.game_id != context.game_id
            or set(edition.team_ids) != {team_id, opponent_id}
            or edition.completeness_evidence_ref != injury_policy['completeness_evidence_ref']
            or injury_policy['interpreter_id'] != 'nba-injury-local-observed-at/v1'):
        raise ValueError('missing/incompatible complete injury edition authority')
    roster = dict(edition.roster_membership)
    if len(roster) != len(edition.roster_membership) or set(roster) != {team_id, opponent_id}:
        raise ValueError('exact per-team injury roster membership required')
    expected = {(team, member) for team, members in roster.items() for member in members}
    if (any(not members or len(set(members)) != len(members) for members in roster.values())
            or len({member for _, member in expected}) != len(expected)
            or (team_id, player_id) not in expected
            or expected != {(row.team_id, row.player_id) for row in edition.rows}):
        raise ValueError('injury edition rows do not exactly cover the asserted team rosters')
    ledger, records, target, seen = [], [], None, set()
    observed_times = set()
    for row in edition.rows:
        observation = evaluate_injury_observation(context=context, provenance=row.provenance,
            history_state=row.history_state, local_observed_at=row.local_observed_at,
            report_cycle_text=row.report_cycle_text)
        ledger.append((row.provenance, observation))
        if (not observation.accepted or row.team_id not in edition.team_ids or row.player_id in seen
                or dict(row.provenance.attributes).get('edition_ref') != edition.provenance.source_record_id
                or row.status not in {'Out', 'Doubtful', 'Questionable', 'Probable', 'Available', 'Healthy'}):
            raise HistoricalInjuryBlockedError('ambiguous/ineligible injury row or edition membership', ledger)
        seen.add(row.player_id)
        observed_times.add(row.local_observed_at)
        if row.player_id == player_id:
            if row.team_id != team_id:
                raise ValueError('target injury team identity mismatch')
            target = row
        if row.status == 'Out':
            weights = weight_values(row)
            required = {'avg_min_l5', 'avg_pts_l5', 'avg_reb_l5', 'avg_ast_l5', 'avg_usg_pct_l5', 'games_started_l5'}
            if not required <= weights.keys() or not row.position_group:
                raise ValueError('OUT player missing raw contribution/position evidence')
            if (any(not isfinite(weights[key]) or weights[key] < 0 for key in required)
                    or weights['avg_min_l5'] <= 0 or weights['games_started_l5'] > 5):
                raise ValueError('OUT player has invalid raw contribution values')
            records.append({**weights, 'nba_team_id': row.team_id, 'report_date': context.game_date,
                            'inj_player_id': row.player_id, 'inj_position_group': row.position_group})
    if target is None or len(observed_times) != 1 or any(not any(r.team_id == team for r in edition.rows) for team in edition.team_ids):
        raise ValueError('missing player/roster or mixed injury editions; omission is not health')
    frame = pd.DataFrame(records)
    aggregated = aggregate_team_injury_features(frame) if records else empty_team_injury_aggregation()
    def team_result(team):
        selected = aggregated[aggregated['nba_team_id'] == team]
        return SimpleNamespace(**selected.iloc[0].to_dict()) if not selected.empty else SimpleNamespace(
            out_count=0, out_min_sum=0, out_pts_sum=0, out_reb_sum=0, out_ast_sum=0, out_usg_sum=0)
    same = [r for r in records if r['nba_team_id'] == team_id and r['inj_player_id'] != player_id
            and r['inj_position_group'] == position_group]
    features = build_injury_context(team_game_result=team_result(team_id), team_advanced_result=team_result(team_id),
        opponent_result=team_result(opponent_id), player_status_result=SimpleNamespace(status=target.status),
        same_position_result=SimpleNamespace(out_count=len(same), out_min_sum=sum(r['avg_min_l5'] for r in same),
            out_usg_sum=sum(r['avg_usg_pct_l5'] for r in same),
            out_starter_sum=sum(starter_probability(r['games_started_l5']) for r in same)))
    return features, (edition.provenance, tuple(ledger))


class HistoricalInjuryBlockedError(ValueError):
    def __init__(self, reason, ledger):
        self.ledger = tuple(ledger)
        super().__init__(reason)
