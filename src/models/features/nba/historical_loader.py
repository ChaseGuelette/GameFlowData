"""Shared historical NBA assembly for native readers and explicit controlled inputs."""
from __future__ import annotations

from dataclasses import dataclass, fields, is_dataclass
from datetime import date, datetime, timedelta
from math import isfinite
from types import SimpleNamespace

import pandas as pd
from sqlalchemy import bindparam, text

from src.models.features.nba.contracts import NBA_FEATURE_FAMILIES, NBA_FEATURE_LISTS
from src.models.features.nba.historical_observation_adapters import SourceProvenance
from src.models.features.nba.historical_policy import (
    HistoricalGameContext,
    HistoricalPolicy,
    HistoricalPolicyContractError,
    build_game_context,
    policy_payload,
)
from src.models.features.nba.injury_context import HistoricalInjuryEdition, historical_injury_features
from src.models.features.nba.line_sources import HistoricalMarketRoute, HistoricalMarketRow, historical_market_features
from src.models.features.nba.opponent_sources import OPPONENT_POSITIONAL_DEFAULTS, row_to_opponent_positional_stats
from src.models.features.nba.requests import DateFeatureRequest, DateRangeFeatureRequest, TrainingFeatureRequest
from src.models.features.nba.team_sources import TEAM_ROLLING_DEFAULTS, row_to_team_rolling_stats
from src.models.features.nba.transforms import build_player_rolling_features

REQUIRED_FAMILIES = tuple(sorted(NBA_FEATURE_FAMILIES))
ORDERED_FEATURES = tuple(dict.fromkeys(feature for group in NBA_FEATURE_LISTS.values() for feature in group))
IDENTIFIERS = ('game_id', 'player_id', 'game_date', 'season_id', 'team_id', 'opponent_id', 'position_group')
TARGETS = ('actual_minutes', 'actual_pts', 'actual_reb', 'actual_ast', 'actual_threes')


@dataclass(frozen=True, slots=True)
class HistoricalBlock:
    policy_content_id: str
    game_id: str | None
    player_id: int | None
    source: str
    family: str
    reason: str
    source_refs: tuple[SourceProvenance, ...] = ()


class HistoricalFeatureBlockedError(RuntimeError):
    """No partial feature population is published. Read blocks/accounting instead."""

    def __init__(self, blocks, *, population_count=0, excluded=(), ledger=()):
        self.blocks = tuple(blocks)
        self.population_count = population_count
        self.excluded = tuple(excluded)
        self.ledger = tuple(ledger)
        super().__init__('historical features blocked: ' + '; '.join(
            f'{b.source}/{b.family}/{b.game_id}/{b.player_id}: {b.reason}' for b in self.blocks
        ))


@dataclass(frozen=True, slots=True)
class RollingSnapshot:
    """Raw derived row plus producer attestation, specific to this policy/row.

    Same-game row_date is permitted ONLY because latest_contribution_at and
    contribution_game_ids attest the pre-game shift. Values are not feature rows.
    """

    provenance: SourceProvenance
    entity_id: int
    row_game_id: str
    row_date: date
    season_id: str
    policy_content_id: str
    producer_ref: str
    carryover_mode: str
    contributing_seasons: tuple[str, ...]
    contribution_game_ids: tuple[str, ...]
    latest_contribution_at: datetime
    source_observed_at: datetime
    rolling_min_minutes: float
    transition_policy_id: str
    cold_start_policy_id: str
    values: tuple[tuple[str, float], ...]

    def __post_init__(self):
        if not isinstance(self.provenance, SourceProvenance):
            raise HistoricalPolicyContractError('snapshot provenance must be SourceProvenance')
        if not all(isinstance(value, tuple) for value in (
            self.values, self.contributing_seasons, self.contribution_game_ids
        )) or any(not isinstance(item, tuple) or len(item) != 2 or not isinstance(item[0], str)
                  or isinstance(item[1], bool) or not isinstance(item[1], int | float) for item in self.values):
            raise HistoricalPolicyContractError('raw snapshot values and lineage must be immutable scalars')
        if len(dict(self.values)) != len(self.values):
            raise HistoricalPolicyContractError('duplicate snapshot fields')
        if any(not isinstance(value, str) or not value or value != value.strip() for value in (
            self.row_game_id, self.season_id, self.policy_content_id, self.producer_ref,
            self.carryover_mode, self.transition_policy_id, self.cold_start_policy_id,
            *self.contributing_seasons, *self.contribution_game_ids
        )):
            raise HistoricalPolicyContractError('snapshot identifiers must be immutable exact strings')


def snapshot_values(snapshot, *, context, policy, cohort, carryover, family, source, entity_id):
    """Verify producer lineage BEFORE calling a legacy defaulting transform."""
    if not isinstance(snapshot, RollingSnapshot):
        raise ValueError(f'missing raw {source} snapshot/producer lineage')
    if (snapshot.provenance.source_namespace != source or snapshot.entity_id != entity_id
            or snapshot.policy_content_id != policy.content_id or not snapshot.producer_ref
            or snapshot.row_date > context.game_date or snapshot.season_id not in cohort['seasons']
            or snapshot.carryover_mode != carryover[family]['mode']
            or snapshot.rolling_min_minutes != cohort['rolling_min_minutes']
            or snapshot.transition_policy_id != cohort['transition_policy_id']
            or snapshot.cold_start_policy_id != cohort['cold_start_policy_id']):
        raise ValueError(f'{source} snapshot identity/policy/producer semantics mismatch')
    if (snapshot.source_observed_at.tzinfo is None or snapshot.source_observed_at.utcoffset() is None
            or snapshot.source_observed_at > context.decision_at
            or dict(snapshot.provenance.attributes).get('carryover_evidence_ref:' + family,
                dict(snapshot.provenance.attributes).get('carryover_evidence_ref')) != carryover[family]['evidence_ref']):
        raise ValueError(f'{source} observed availability/carryover authority absent or ineligible')
    if snapshot.row_date == context.game_date and snapshot.row_game_id != context.game_id:
        raise ValueError(f'{source} same-day shifted row belongs to a different game')
    if (not snapshot.contribution_game_ids or not snapshot.contributing_seasons
            or context.game_id in snapshot.contribution_game_ids
            or snapshot.latest_contribution_at.tzinfo is None
            or snapshot.latest_contribution_at.utcoffset() is None
            or snapshot.latest_contribution_at >= context.decision_at):
        raise ValueError(f'{source} prior-game contribution lineage absent or after decision')
    if snapshot.carryover_mode == 'current_season_only':
        if snapshot.contributing_seasons != (snapshot.season_id,):
            raise ValueError(f'{source} cross-season contribution forbidden')
    elif any(season not in cohort['seasons'] or season > snapshot.season_id for season in snapshot.contributing_seasons):
        raise ValueError(f'{source} unapproved contributing season')
    values = dict(snapshot.values)
    if not values or any(not isfinite(value) for value in values.values()):
        raise ValueError(f'{source} missing/nonfinite raw values; defaults forbidden')
    return values


@dataclass(frozen=True, slots=True)
class HistoricalPlayerEvidence:
    context: HistoricalGameContext
    player_id: int
    team_id: int
    opponent_id: int
    season_id: str
    season_type: str
    is_home: bool
    position_group: str
    position_provenance: SourceProvenance
    position_observed_at: datetime
    player: RollingSnapshot | None
    advanced: RollingSnapshot | None
    team: RollingSnapshot | None
    opponent: RollingSnapshot | None
    defense: RollingSnapshot | None
    markets: tuple[HistoricalMarketRow, ...]
    market_selections: tuple[tuple[str, str, str], ...]
    injury: HistoricalInjuryEdition | None
    market_routes: tuple[HistoricalMarketRoute, ...] = ()

    def __post_init__(self):
        if not isinstance(self.markets, tuple) or not isinstance(self.market_selections, tuple):
            raise HistoricalPolicyContractError('market evidence must be immutable tuples')
        if any(not isinstance(row, HistoricalMarketRow) for row in self.markets) or any(
            not isinstance(item, tuple) or len(item) != 3 or not all(isinstance(s, str) for s in item)
            for item in self.market_selections
        ):
            raise HistoricalPolicyContractError('invalid market evidence records')


class HistoricalFeatureLoader:
    def __init__(self, feature_store):
        self.feature_store = feature_store
        self.engine = feature_store.engine
        self.config = feature_store.config

    def load(self, request):
        policy = request.historical_policy
        if not isinstance(policy, HistoricalPolicy):
            raise HistoricalPolicyContractError('historical evidence requires explicit HistoricalPolicy')
        policy.require_execution_ready(required_families=REQUIRED_FAMILIES)
        if type(request.controlled_historical_evidence) is not bool:
            raise HistoricalPolicyContractError('controlled_historical_evidence must be an explicit boolean')
        if not request.controlled_historical_evidence and request.historical_evidence:
            raise HistoricalFeatureBlockedError((HistoricalBlock(
                policy.content_id, None, None, 'historical_sources', 'all',
                'caller evidence requires controlled mode; normal execution reads native sources',
            ),))
        authorities = policy_payload(policy)['authorities']
        cohort = authorities['cohort_split']['value']
        budget = getattr(self.feature_store, 'acquisition_budget', None)
        if not request.controlled_historical_evidence and budget is None:
            raise ValueError('Native historical execution requires explicit acquisition limits')
        carryover = {entry['family_id']: entry['value'] for entry in authorities['carryover']}
        unsupported = [HistoricalBlock(policy.content_id, None, None, 'policy', family,
                        f'unsupported carryover mode: {setting["mode"]}')
                       for family, setting in carryover.items()
                       if setting['mode'] != 'current_season_only']
        if unsupported:
            raise HistoricalFeatureBlockedError(unsupported)
        if not isinstance(request.historical_evidence, tuple):
            raise HistoricalPolicyContractError('historical_evidence must be an immutable tuple')
        evidence = {}
        contexts = {}
        source_contents = {}

        def register_sources(value):
            if is_dataclass(value) and not isinstance(value, type):
                provenance = getattr(value, 'provenance', None)
                if isinstance(provenance, SourceProvenance):
                    ref = (provenance.source_namespace, provenance.source_record_id)
                    if isinstance(value, HistoricalInjuryEdition):
                        # One publisher edition can have different per-game roster views.
                        # Raw injury row identities below remain globally conflict-checked.
                        ref = (*ref, value.game_id)
                    content = repr(value)
                    if ref in source_contents and source_contents[ref] != content:
                        raise HistoricalPolicyContractError('conflicting content reused for one source record')
                    source_contents[ref] = content
                for field in fields(value):
                    if field.name != 'provenance':
                        register_sources(getattr(value, field.name))
            elif isinstance(value, tuple):
                for item in value:
                    register_sources(item)

        query = '''SELECT pgs.game_id, pgs.player_id, pgs.game_date::date AS game_date,
                   pgs.season_id, pgs.team_id, tgs.opponent_id,
                   CASE WHEN pgs.matchup LIKE '%vs.%' THEN 1 ELSE 0 END AS is_home,
                   pgs.min AS actual_minutes, pgs.pts AS actual_pts, pgs.reb AS actual_reb,
                   pgs.ast AS actual_ast, pgs.fg3m AS actual_threes
                   FROM player_game_stats pgs
                   LEFT JOIN team_game_stats tgs ON pgs.game_id=tgs.game_id AND pgs.team_id=tgs.team_id
                   WHERE '''
        if isinstance(request, TrainingFeatureRequest):
            if not request.seasons or len(set(request.seasons)) != len(request.seasons) or any(
                season not in cohort['seasons'] for season in request.seasons
            ):
                raise HistoricalPolicyContractError('training seasons must be explicit unique cohort members')
            params: dict = {'seasons': request.seasons}
            predicate = 'pgs.season_id IN :seasons'
            if request.historical_role == 'training':
                predicate += ' AND pgs.game_date >= :start AND pgs.game_date <= :end'
                params.update(start=date.fromisoformat(cohort['train_start']), end=date.fromisoformat(cohort['train_end']))
            elif request.historical_role == 'calibration':
                predicate += ' AND pgs.game_date >= :start AND pgs.game_date < :end'
                params.update(start=date.fromisoformat(cohort['calibration_start']),
                              end=date.fromisoformat(cohort['calibration_end_exclusive']))
            elif not request.controlled_historical_evidence:
                raise ValueError('Native training requires explicit training/calibration role')
        else:
            start = request.game_date if isinstance(request, DateFeatureRequest) else request.start_date
            end = request.game_date if isinstance(request, DateFeatureRequest) else request.end_date
            if not isinstance(request, DateFeatureRequest | DateRangeFeatureRequest) or start > end:
                raise HistoricalPolicyContractError('invalid historical date request')
            if budget is not None:
                budget.limits.check_span(start, end)
            predicate = 'pgs.season_id IN :seasons AND pgs.game_date >= :start AND pgs.game_date <= :end'
            params = {'start': start, 'end': end, 'seasons': cohort['seasons']}
        sql = query + predicate + ' ORDER BY pgs.game_date, pgs.game_id, pgs.player_id'
        if budget is not None:
            sql += ' LIMIT :population_limit'
            params['population_limit'] = budget.fetch_limit
        clause = text(sql).bindparams(bindparam('seasons', expanding=True))
        # Fatal acquisition errors propagate unchanged; there is no legacy retry.
        with self.engine.connect() as conn:
            if budget is None:  # Explicit controlled fixtures only.
                population = pd.read_sql(clause, conn, params=params)
            else:
                transaction = conn.begin()
                try:
                    conn.execute(text("SELECT set_config('statement_timeout', :timeout, true)"),
                                 {'timeout': str(budget.limits.population_timeout_ms)})
                    population = pd.read_sql(clause, conn, params=params)
                finally:
                    transaction.rollback()
        if budget is not None:
            budget.consume(population)
        input_evidence = request.historical_evidence
        if not request.controlled_historical_evidence:
            from src.models.features.nba.historical_sources import PostgresHistoricalSources

            sources = self.feature_store.historical_sources
            if sources is None:
                sources = PostgresHistoricalSources(self.engine)
                self.feature_store.historical_sources = sources
            try:
                population, input_evidence = sources.acquire(population, policy)
            except (ValueError, KeyError, TypeError) as exc:
                reason = f'missing source metadata field: {exc.args[0]}' if isinstance(exc, KeyError) else str(exc)
                raise HistoricalFeatureBlockedError((HistoricalBlock(
                    policy.content_id, None, None, 'native_historical_sources', 'all', reason,
                ),), population_count=len(population)) from None
        for item in input_evidence:
            if not isinstance(item, HistoricalPlayerEvidence):
                raise HistoricalPolicyContractError('invalid historical player evidence')
            key = (item.context.game_id, item.player_id)
            if key in evidence:
                raise HistoricalPolicyContractError('duplicate/ambiguous player-game authority')
            if item.context.game_id in contexts and contexts[item.context.game_id] != item.context:
                raise HistoricalPolicyContractError('multiple authoritative contexts for one game')
            contexts[item.context.game_id] = item.context
            register_sources(item)
            evidence[key] = item
        blocks, excluded, output, ledger = [], [], [], []
        population_dates, used_evidence = {}, set()
        seen = set()
        for row in population.to_dict('records'):
            key = (row['game_id'], row['player_id'])
            if key in seen:
                raise HistoricalPolicyContractError('duplicate target population identity')
            seen.add(key)
            day = row['game_date']
            if isinstance(day, datetime):
                day = day.date()
            row['game_date'] = day
            population_dates[key] = day
            windows = [(cohort['train_start'], cohort['train_end']),
                       (cohort['calibration_start'], (date.fromisoformat(cohort['calibration_end_exclusive']) - timedelta(days=1)).isoformat()),
                       (cohort['oos_start'], cohort['oos_end'])]
            if cohort['independent_validation']:
                windows.append((cohort['independent_validation']['start'], cohort['independent_validation']['end']))
            if row['season_id'] not in cohort['seasons'] or not any(a <= day.isoformat() <= b for a, b in windows):
                excluded.append((*key, 'outside_explicit_cohort_or_target_minutes'))
                continue
            try:
                valid_minutes = (not isinstance(row['actual_minutes'], bool)
                                 and isfinite(row['actual_minutes'])
                                 and row['actual_minutes'] >= 0)
            except (TypeError, ValueError):
                valid_minutes = False
            if valid_minutes and row['actual_minutes'] < cohort['target_min_minutes']:
                excluded.append((*key, 'outside_explicit_cohort_or_target_minutes'))
                continue
            if row.get('season_type') is not None and row['season_type'] not in cohort['season_types']:
                excluded.append((*key, 'excluded_season_type'))
                continue
            item = evidence.get(key)
            if item is None:
                blocks.append(HistoricalBlock(policy.content_id, *key, 'historical_sources', 'game_context',
                                               'missing authoritative commence and player source evidence'))
                continue
            used_evidence.add(key)
            if item.season_type not in cohort['season_types']:
                excluded.append((*key, 'excluded_season_type'))
                continue
            try:
                valid_targets = all(not isinstance(row[target], bool) and isfinite(row[target]) and row[target] >= 0
                                    for target in TARGETS)
            except (TypeError, ValueError):
                valid_targets = False
            if not valid_targets:
                blocks.append(HistoricalBlock(policy.content_id, *key, 'player_game_stats', 'cohort', 'missing/nonfinite/invalid targets'))
                continue
            try:
                values, row_ledger = self.assemble(row, item, policy, cohort, carryover, authorities)
                output.append(values)
                ledger.append((key, row_ledger))
            except HistoricalFeatureBlockedError as exc:
                blocks.extend(exc.blocks)
                ledger.append((key, exc.ledger))
        if blocks:
            raise HistoricalFeatureBlockedError(blocks, population_count=len(population), excluded=excluded, ledger=ledger)
        frame = pd.DataFrame(output, columns=[*IDENTIFIERS, *ORDERED_FEATURES, *TARGETS,
                                               'pts_per_min', 'reb_per_min', 'ast_per_min', 'threes_per_min',
                                               'historical_policy_content_id', 'historical_lineage'])
        frame.attrs['historical_accounting'] = {'population': len(population), 'ready': len(frame), 'excluded': tuple(excluded)}
        frame.attrs['historical_policy_content_id'] = policy.content_id
        if budget is not None:
            frame.attrs['acquisition_limits'] = budget.limits.to_dict()
        if isinstance(request, TrainingFeatureRequest):
            expected_dates = sorted(set(population_dates.values()))
        else:
            first = request.game_date if isinstance(request, DateFeatureRequest) else request.start_date
            last = request.game_date if isinstance(request, DateFeatureRequest) else request.end_date
            expected_dates = [first + timedelta(days=index) for index in range((last - first).days + 1)]
        frame.attrs['historical_date_accounting'] = {
            day: {'population': sum(value == day for value in population_dates.values()),
                  'ready': sum(row['game_date'] == day for row in output),
                  'excluded': tuple(entry for entry in excluded if population_dates[entry[:2]] == day)}
            for day in expected_dates
        }
        frame.attrs['unused_historical_evidence'] = tuple(sorted(set(evidence) - used_evidence))
        frame.attrs['historical_readiness'] = 'controlled_only' if request.controlled_historical_evidence else 'source_integrated'
        if not request.controlled_historical_evidence:
            frame.attrs['historical_contexts'] = dict(contexts)
            frame.attrs['historical_market_routes'] = self.feature_store.historical_sources.market_routes
        frame.attrs['decision_grade'] = False
        return frame

    def assemble(self, row, item, policy, cohort, carryover, authorities):
        context = build_game_context(policy, required_families=REQUIRED_FAMILIES,
                                     game_id=item.context.game_id, game_date=item.context.game_date,
                                     commence_at=item.context.commence_at,
                                     commence_provenance_ref=item.context.commence_provenance_ref)
        if (context != item.context or row['game_id'] != context.game_id or row['game_date'] != context.game_date
                or any(row[key] != getattr(item, key) for key in ('player_id', 'team_id', 'opponent_id', 'season_id', 'is_home'))
                or not item.position_group or item.position_provenance.source_namespace != 'player_position_history'
                or item.position_observed_at.tzinfo is None or item.position_observed_at.utcoffset() is None
                or item.position_observed_at >= context.decision_at):
            raise HistoricalFeatureBlockedError([HistoricalBlock(policy.content_id, row['game_id'], row['player_id'],
                'game_context/player_position_history', 'game_context', 'context/position authority mismatch')])
        values, lineage = dict(row), []
        values['position_group'] = item.position_group
        raw = {}
        for name, snapshot, family, source, entity in (
            ('player', item.player, 'player_rolling', 'player_average_game_stats', item.player_id),
            ('advanced', item.advanced, 'player_rolling', 'player_average_advanced_stats', item.player_id),
            ('team', item.team, 'team_context', 'team_average_game_stats', item.team_id),
            ('opponent', item.opponent, 'opponent_context', 'team_average_game_stats', item.opponent_id),
            ('defense', item.defense, 'opponent_context', 'team_allowed_by_position', item.opponent_id),
        ):
            try:
                raw[name] = snapshot_values(snapshot, context=context, policy=policy, cohort=cohort,
                    carryover=carryover, family=family, source=source, entity_id=entity)
                if snapshot.season_id != item.season_id:
                    raise ValueError('derived snapshot must be for the target season')
                if name == 'defense' and dict(snapshot.provenance.attributes).get('position_group') != item.position_group:
                    raise ValueError('opponent defense position identity mismatch')
                if name == 'player' and carryover['schedule_context']['mode'] != snapshot.carryover_mode:
                    raise ValueError('schedule carryover differs from attested player snapshot')
                if name == 'player' and dict(snapshot.provenance.attributes).get('schedule_evidence_ref') != carryover['schedule_context']['evidence_ref']:
                    raise ValueError('schedule lineage authority absent')
                lineage.append(snapshot)
            except ValueError as exc:
                raise HistoricalFeatureBlockedError([HistoricalBlock(policy.content_id, context.game_id, item.player_id,
                    source, family, str(exc), () if snapshot is None else (snapshot.provenance,))]) from None
        # No partial raw schema can reach a defaulting transform.
        player = {**raw['player'], **raw['advanced']}
        required_player = {'avg_min_l5', 'avg_min_l15', 'avg_pts_l5', 'avg_pts_l15', 'avg_reb_l5', 'avg_ast_l5',
            'avg_fg3m_l5', 'avg_fg3a_l5', 'avg_usg_pct_l5', 'avg_ts_pct_l15', 'avg_reb_pct_l5', 'avg_ast_pct_l5',
            'avg_min_l3', 'avg_pts_l3', 'avg_reb_l3', 'avg_ast_l3', 'avg_fg3m_l3', 'avg_min_szn',
            'std_min_l5', 'std_pts_l5', 'std_reb_l5', 'std_ast_l5', 'std_fg3m_l5', 'min_floor_l5',
            'games_started_l5', 'stored_rest_days', 'games_last_7d'}
        advanced_keys = {'avg_usg_pct_l5', 'avg_ts_pct_l15', 'avg_reb_pct_l5', 'avg_ast_pct_l5'}
        if raw['player'].keys() & raw['advanced'].keys():
            raise HistoricalFeatureBlockedError([HistoricalBlock(policy.content_id, context.game_id, item.player_id,
                'player_average_advanced_stats', 'player_rolling', 'overlapping raw basic/advanced columns')])
        for name, required in (('player', required_player - advanced_keys),
                               ('advanced', {'avg_usg_pct_l5', 'avg_ts_pct_l15', 'avg_reb_pct_l5', 'avg_ast_pct_l5'}),
                               ('team', set(TEAM_ROLLING_DEFAULTS)), ('opponent', set(TEAM_ROLLING_DEFAULTS)),
                               ('defense', set(OPPONENT_POSITIONAL_DEFAULTS))):
            if not required <= raw[name].keys():
                raise HistoricalFeatureBlockedError([HistoricalBlock(policy.content_id, context.game_id, item.player_id,
                    name, 'rolling_context', f'missing raw columns: {sorted(required - raw[name].keys())}')])
        ratio_denominators = ('avg_pts_l15', 'avg_reb_l5', 'avg_ast_l5', 'avg_fg3m_l5', 'avg_min_l5', 'avg_min_l15')
        if (any(player[key] <= 0 for key in ratio_denominators) or player['games_last_7d'] <= 0
                or player['stored_rest_days'] < 0 or not 0 <= player['games_started_l5'] <= 5):
            raise HistoricalFeatureBlockedError([HistoricalBlock(policy.content_id, context.game_id, item.player_id,
                'player_rolling', 'player_rolling', 'raw values would trigger unapproved legacy defaults/cold-start behavior')])
        values.update(build_player_rolling_features(player))
        values.update(row_to_team_rolling_stats(SimpleNamespace(**raw['team']), 'team'))
        values.update(row_to_team_rolling_stats(SimpleNamespace(**raw['opponent']), 'opp'))
        values.update(row_to_opponent_positional_stats(SimpleNamespace(**raw['defense'])))
        for family, source, operation in (
            ('line_context', 'raw_game_lines_staging/raw_player_props_combined', lambda: historical_market_features(
                context=context, player_id=item.player_id, is_home=item.is_home, rows=item.markets,
                selections=item.market_selections, source_policy=authorities['source']['value'], routes=item.market_routes)),
            ('injury_context', 'rapidapi_injuries', lambda: historical_injury_features(
                context=context, player_id=item.player_id, team_id=item.team_id, opponent_id=item.opponent_id,
                position_group=item.position_group, edition=item.injury, injury_policy=authorities['injury']['value'],
                weight_values=lambda injury_row: self.injury_weights(
                    injury_row, context, policy, cohort, carryover, target_season=item.season_id))),
        ):
            try:
                features, source_ledger = operation()
                values.update(features)
                lineage.append(source_ledger)
            except ValueError as exc:
                raise HistoricalFeatureBlockedError([HistoricalBlock(policy.content_id, context.game_id, item.player_id,
                    source, family, str(exc))], ledger=getattr(exc, 'ledger', ())) from None
        if any(feature not in values or not isfinite(values[feature]) for feature in ORDERED_FEATURES):
            raise HistoricalFeatureBlockedError([HistoricalBlock(policy.content_id, context.game_id, item.player_id,
                'assembly', 'all', 'incomplete/nonfinite ordered features')])
        for stat in ('pts', 'reb', 'ast', 'threes'):
            values[f'{stat}_per_min'] = row[f'actual_{stat}'] / row['actual_minutes']
        values['historical_policy_content_id'] = policy.content_id
        values['historical_lineage'] = (context, item.position_provenance, tuple(lineage))
        return values, tuple(lineage)

    @staticmethod
    def injury_weights(row, context, policy, cohort, carryover, *, target_season):
        basic = snapshot_values(row.rolling, context=context, policy=policy, cohort=cohort,
            carryover=carryover, family='injury_context', source='player_average_game_stats', entity_id=row.player_id)
        advanced = snapshot_values(row.advanced, context=context, policy=policy, cohort=cohort,
            carryover=carryover, family='injury_context', source='player_average_advanced_stats', entity_id=row.player_id)
        if row.rolling.season_id != target_season or row.advanced.season_id != target_season:
            raise ValueError('injury contribution snapshots must be for the target season')
        if basic.keys() & advanced.keys():
            raise ValueError('overlapping raw basic/advanced injury contribution columns')
        return {**basic, **advanced}
