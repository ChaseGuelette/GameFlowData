"""NBA betting line source helpers.

This module owns the SQL/query defaults for NBA game lines and player prop-line
features used by the FeatureStore compatibility facade. These feature lines are
model-input centering features and intentionally remain separate from downstream
edge-calculation line selection.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.sql.elements import TextClause

from src.models.features.nba.historical_observation_adapters import (
    HistoricalSourceClass,
    SourceProvenance,
    evaluate_market_observation,
)
from src.models.features.nba.quote_selection import QuoteInput, select_quote_pairs


def default_game_lines() -> dict[str, float | int]:
    """Default game-line feature values when no quote row is available."""
    return {"line_spread_raw": 0, "line_total": 0}


def default_player_prop_lines() -> dict[str, float | int]:
    """Default player prop-line feature values when no quote row is available."""
    return {
        "prop_line_pts": 0,
        "prop_line_reb": 0,
        "prop_line_ast": 0,
        "prop_line_threes": 0,
    }


def game_lines_query() -> TextClause:
    """Build the current as-of/pre-game NBA game-line feature query."""
    return text("""
        SELECT
            MAX(CASE WHEN market_key = 'spreads' THEN line END) as spread,
            MAX(CASE WHEN market_key = 'totals' THEN line END) as total
        FROM raw_game_lines_staging
        WHERE nba_game_id = :game_id
          AND bookmaker IN ('pinnacle', 'draftkings')
          AND (
              :as_of_date IS NULL
              OR COALESCE(snapshot_time, inserted_at)::date <= :as_of_date
          )
          AND (commence_time IS NULL OR COALESCE(snapshot_time, inserted_at) < commence_time)
    """)


def player_prop_lines_query() -> TextClause:
    """Read frozen T-60 features; unavailable early decisions retain zero defaults."""
    return text("""
        SELECT
            MAX(CASE WHEN sub.market_key = 'player_points' THEN sub.line END) as prop_line_pts,
            MAX(CASE WHEN sub.market_key = 'player_rebounds' THEN sub.line END) as prop_line_reb,
            MAX(CASE WHEN sub.market_key = 'player_assists' THEN sub.line END) as prop_line_ast,
            MAX(CASE WHEN sub.market_key = 'player_threes' THEN sub.line END) as prop_line_threes
        FROM (
            SELECT market_key, line
            FROM nba_player_prop_feature_lines
            WHERE player_id = :player_id
              AND game_id = :game_id
              AND selection_policy_version = 't60_v1'
              AND (
                  :as_of_date IS NULL
                  OR game_date <= :as_of_date
              )
              AND source_snapshot_time <= decision_time
              AND decision_time = commence_time - INTERVAL '60 minutes'
              AND decision_time <= COALESCE(CAST(:as_of_time AS timestamptz), CURRENT_TIMESTAMP)
        ) sub
    """)


def row_to_game_lines(row) -> dict[str, float | int]:
    """Map a raw SQL row into FeatureStore game-line feature keys."""
    if row is None:
        return default_game_lines()
    return {
        "line_spread_raw": row.spread if row.spread else 0,
        "line_total": row.total if row.total else 0,
    }


def row_to_player_prop_lines(row) -> dict[str, float | int]:
    """Map a raw SQL row into FeatureStore prop-line feature keys."""
    if row is None:
        return default_player_prop_lines()
    return {
        "prop_line_pts": row.prop_line_pts or 0,
        "prop_line_reb": row.prop_line_reb or 0,
        "prop_line_ast": row.prop_line_ast or 0,
        "prop_line_threes": row.prop_line_threes or 0,
    }


def get_game_lines(conn, game_id, as_of_date=None) -> dict[str, float | int]:
    """Fetch NBA spread/total feature lines using only as-of, pregame quotes."""
    result = conn.execute(
        game_lines_query(),
        {"game_id": game_id, "as_of_date": as_of_date},
    ).fetchone()
    return row_to_game_lines(result)


def get_player_prop_lines(conn, player_id, game_id, as_of_date=None, *, as_of_time=None) -> dict[str, float | int]:
    """Fetch T-60 features without revealing a later decision to an earlier caller."""
    result = conn.execute(
        player_prop_lines_query(),
        {"player_id": player_id, "game_id": game_id, "as_of_date": as_of_date, "as_of_time": as_of_time},
    ).fetchone()
    return row_to_player_prop_lines(result)


@dataclass(frozen=True, slots=True)
class HistoricalMarketRow:
    """Trusted raw source record, NOT an interpretation inferred from DB clocks.

    ``snapshot_key`` attests source-specific edition compatibility. ``home`` is
    only accepted for home-team spread feature observations, never executable OU.
    """

    provenance: SourceProvenance
    source_class: HistoricalSourceClass
    game_id: str
    player_id: str
    market: str
    bookmaker: str
    snapshot_key: str
    side: str
    line: float
    odds: float
    period: str
    variant: str
    historical_requested_at: datetime | None = None
    market_updated_at: datetime | None = None
    bookmaker_updated_at: datetime | None = None
    local_observed_at: datetime | None = None

    def __post_init__(self):
        if not isinstance(self.provenance, SourceProvenance) or not isinstance(self.source_class, HistoricalSourceClass):
            raise ValueError('market source authority must be an explicit immutable source record/class')
        if any(not isinstance(value, str) or not value or value != value.strip() for value in (
            self.game_id, self.player_id, self.market, self.bookmaker, self.snapshot_key, self.side, self.period, self.variant
        )):
            raise ValueError('market identifiers must be preserved exact strings')


@dataclass(frozen=True, slots=True)
class HistoricalMarketRoute:
    """Explicit controlled route authority; never a source authentication receipt."""

    bookmaker: str
    period: str
    variant: str
    provenance_evidence_ref: str

    def __post_init__(self):
        if any(not isinstance(value, str) or not value or value != value.strip() for value in (
            self.bookmaker, self.period, self.variant, self.provenance_evidence_ref
        )):
            raise ValueError('market route requires exact immutable identifiers')


class HistoricalMarketBlockedError(ValueError):
    def __init__(self, reason, ledger):
        self.ledger = tuple(ledger)
        super().__init__(reason)


def historical_market_features(*, context, player_id, is_home, rows, selections, source_policy, routes=()):
    """Consume raw observations; caller selects exact source refs, not a hidden rank.

    Returns (features, per-occurrence ledger). All rows, including ineligible
    counterparts, reach the pure selector before selection. No zero fallback.
    """
    from math import isfinite

    features = {}
    ledger = []
    quotes = []
    observations = {}
    def block(reason):
        raise HistoricalMarketBlockedError(reason, ledger)
    if not isinstance(routes, tuple) or not routes or any(not isinstance(route, HistoricalMarketRoute) for route in routes):
        block('missing explicit market route authority')
    allowed_routes = {(route.bookmaker, route.period, route.variant) for route in routes
                      if route.provenance_evidence_ref == source_policy['provenance_evidence_ref']}
    for row in rows:
        if row.game_id != context.game_id or row.player_id not in {str(player_id), "game"}:
            block("market record identity mismatch")
        observation = evaluate_market_observation(
            context=context, source_class=row.source_class, provenance=row.provenance,
            historical_requested_at=row.historical_requested_at, market_updated_at=row.market_updated_at,
            bookmaker_updated_at=row.bookmaker_updated_at, local_observed_at=row.local_observed_at,
            require_book_update=source_policy["require_bookmaker_update"],
        )
        allowed = observation.interpreter_id in source_policy["interpreters"]
        availability = None if observation.interpretation is None else observation.interpretation.availability_at
        fresh = observation.accepted and availability is not None and (
            context.decision_at - availability
        ).total_seconds() <= source_policy["max_quote_age_minutes"] * 60
        allowed = allowed and fresh and (
            not source_policy["require_market_update"] or row.market_updated_at is not None
        )
        allowed = allowed and dict(row.provenance.attributes).get('provenance_evidence_ref') == source_policy['provenance_evidence_ref']
        allowed = allowed and (row.bookmaker, row.period, row.variant) in allowed_routes
        if observation.accepted and observation.interpretation is not None:
            allowed = allowed and all(
                evidence.observed_at < context.decision_at
                and (context.decision_at - evidence.observed_at).total_seconds() <= source_policy['max_quote_age_minutes'] * 60
                for evidence in observation.interpretation.cutoff_evidence
            )
        ref = (row.provenance.source_namespace, row.provenance.source_record_id)
        if ref in observations and observations[ref][0] != row:
            block("reused market source identity has contradictory content")
        observations[ref] = (row, observation, allowed)
        if row.market == "spreads":
            ledger.append((ref, observation, "feature_only_spread"))
        else:
            # Additional policy rejection must not bypass conflict detection.
            quotes.append(QuoteInput(
                row.game_id, row.player_id, row.market, row.period, row.variant, row.bookmaker,
                row.line, row.side, row.provenance.source_namespace, row.snapshot_key,
                row.provenance.source_record_id, "american", row.odds, observation,
            ))
    result = select_quote_pairs(tuple(quotes))
    ledger.extend(result.ledger)
    expected = {"line_spread", "line_total", *default_player_prop_lines()}
    if not isinstance(selections, tuple) or len(selections) != len(expected) or {s[0] for s in selections} != expected:
        block("exact feature-line source selection required for all six features")
    markets = {"line_spread": "spreads", "line_total": "totals", "prop_line_pts": "player_points",
               "prop_line_reb": "player_rebounds", "prop_line_ast": "player_assists", "prop_line_threes": "player_threes"}
    for feature, namespace, record_id in selections:
        ref = (namespace, record_id)
        if ref not in observations:
            block(f"missing selected market record: {feature}:{ref}")
        row, observation, allowed = observations[ref]
        if not observation.accepted or not allowed or not isfinite(row.line) or row.market != markets[feature]:
            block(f"selected market observation ineligible: {feature}:{ref}")
        expected_player = "game" if feature.startswith("line_") else str(player_id)
        if row.player_id != expected_player:
            block("selected market entity mismatch")
        if feature == "line_spread":
            conflicts = [other for other in rows if (
                other.market, other.bookmaker, other.snapshot_key, other.side
            ) == (row.market, row.bookmaker, row.snapshot_key, row.side) and other != row]
            if row.side != "home" or conflicts:
                block("home spread orientation or snapshot conflict unresolved")
            counterparts = {other for other in rows if (
                other.game_id, other.player_id, other.market, other.bookmaker, other.snapshot_key,
                other.period, other.variant, other.provenance.source_namespace, other.side
            ) == (row.game_id, 'game', row.market, row.bookmaker, row.snapshot_key,
                  row.period, row.variant, row.provenance.source_namespace, 'away')}
            if len(counterparts) != 1:
                block('spread requires one coherent opposite team outcome')
            counterpart = next(iter(counterparts))
            counterpart_ref = (counterpart.provenance.source_namespace, counterpart.provenance.source_record_id)
            if not observations[counterpart_ref][2] or counterpart.line != -row.line:
                block('spread counterpart line/source/clock mismatch')
            # Explicit home-directed raw spread; no inference from team names.
            features[feature] = row.line if is_home else -row.line
        else:
            pairs = [pair for pair in result.eligible_pairs if any(
                (source.source_namespace, source.source_record_id) == ref
                for side in (pair.over, pair.under) for source in side.source_refs
            )]
            if len(pairs) != 1:
                block(f"selected market has no unique eligible OU pair: {feature}:{ref}")
            pair = pairs[0]
            if not all(observations[(source.source_namespace, source.source_record_id)][2]
                       for side in (pair.over, pair.under) for source in side.source_refs):
                block("selected counterpart violates freshness/source policy")
            features[feature] = float(pair.line)
    return features, tuple(ledger)
