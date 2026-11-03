"""PostgreSQL readers for historical NBA features, with explicit source metadata.

Values come from real tables, never from an evidence object supplied to FeatureStore.
Missing lineage/edition metadata is a data error, not an unimplemented-adapter gate.
Metadata may live in each row's historical_metadata JSON object or a repair sidecar.
"""
from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date, datetime, timedelta
from math import isfinite
from pathlib import Path

import pandas as pd
from sqlalchemy import bindparam, text

from src.models.features.nba.contracts import NBA_FEATURE_FAMILIES
from src.models.features.nba.historical_observation_adapters import (
    HistoricalSourceClass,
    InjuryHistoryState,
    SourceProvenance,
)
from src.models.features.nba.historical_policy import build_game_context, policy_payload
from src.models.features.nba.injury_context import HistoricalInjuryEdition, HistoricalInjuryRow
from src.models.features.nba.line_sources import HistoricalMarketRoute, HistoricalMarketRow
from src.models.features.nba.opponent_sources import OPPONENT_POSITIONAL_DEFAULTS
from src.models.features.nba.team_sources import TEAM_ROLLING_DEFAULTS

PLAYER_COLUMNS = ('avg_min_l5', 'avg_min_l15', 'avg_pts_l5', 'avg_pts_l15', 'avg_reb_l5', 'avg_ast_l5',
 'avg_fg3m_l5', 'avg_fg3a_l5', 'avg_min_l3', 'avg_pts_l3', 'avg_reb_l3', 'avg_ast_l3', 'avg_fg3m_l3',
 'avg_min_szn', 'std_min_l5', 'std_pts_l5', 'std_reb_l5', 'std_ast_l5', 'std_fg3m_l5', 'min_floor_l5',
 'games_started_l5', 'stored_rest_days', 'games_last_7d')
ADVANCED_COLUMNS = ('avg_usg_pct_l5', 'avg_ts_pct_l15', 'avg_reb_pct_l5', 'avg_ast_pct_l5')


class HistoricalSourceDataError(ValueError):
    """A named real source record is missing/ambiguous or lacks required metadata."""


def instant(value):
    result = datetime.fromisoformat(value) if isinstance(value, str) else value
    if not isinstance(result, datetime) or result.tzinfo is None or result.utcoffset() is None:
        raise HistoricalSourceDataError('source clock must be an explicit timezone-aware datetime')
    return result


def day(value):
    if isinstance(value, datetime):
        return value.date()
    return date.fromisoformat(value) if isinstance(value, str) else value


def game_aliases(game_id):
    aliases = {game_id}
    if isinstance(game_id, str) and len(game_id) == 10 and game_id.isascii() and game_id.isdigit():
        aliases.update(game_id[-size:] for size in (8, 9) if game_id[-size:].zfill(10) == game_id)
    return sorted(aliases)


class HistoricalSourceMetadata:
    """Owner-maintained facts, not inferred receipts or data repair.

    Sidecar: {"records": [{"table": "...", "key": [...], "metadata": {...}}]}.
    Keys use native identities: rolling(entity_id,game_id), position(player_id,date),
    targets(game_id,player_id), and market/injury rows their source record ID.
    """
    def __init__(self, path=None):
        self.records = {}
        if path is not None:
            payload = json.loads(Path(path).read_text(encoding='utf-8'))
            for record in payload['records']:
                key = (record['table'], tuple(record['key']))
                if key in self.records:
                    raise HistoricalSourceDataError('duplicate source metadata key')
                self.records[key] = record['metadata']

    def get(self, table, raw, key):
        embedded = raw.get('historical_metadata')
        external = self.records.get((table, tuple(key)))
        if embedded is not None and external is not None and embedded != external:
            raise HistoricalSourceDataError(f'{table}:{key}: contradictory embedded/sidecar metadata')
        metadata = embedded if embedded is not None else external
        if not isinstance(metadata, Mapping):
            raise HistoricalSourceDataError(f'{table}:{key}: missing historical_metadata')
        return metadata

    def provenance(self, table, raw, key):
        meta = self.get(table, raw, key)
        default_id = str(key[0]) if len(key)==1 else json.dumps(list(key), separators=(',', ':'))
        record_id = meta.get('source_record_id', default_id)
        return SourceProvenance(table, record_id, tuple(sorted(meta.get('attributes', {}).items())))


class PostgresHistoricalSources:
    """Read raw source rows in bounded target batches; use existing assembly math."""
    def __init__(self, engine, *, metadata_path=None, batch_size=200):
        if type(batch_size) is not int or not 1 <= batch_size <= 1000:
            raise ValueError('source batch_size must be 1..1000')
        self.engine = engine
        self.metadata = HistoricalSourceMetadata(metadata_path)
        self.batch_size = batch_size
        self.contexts = {}
        self.market_routes = ()

    def _query(self, sql, params, expanding=()):
        clause = text(sql).bindparams(*(bindparam(name, expanding=True) for name in expanding))
        with self.engine.connect() as conn, conn.begin():
            conn.execute(text('SET TRANSACTION READ ONLY'))
            conn.execute(text("SET LOCAL statement_timeout = '30s'"))
            return pd.read_sql(clause, conn, params=params)

    @staticmethod
    def _latest(table, alias, condition, date_column='game_date'):
        # Identifiers are compile-time constants from this module, never user SQL.
        return f'''(SELECT jsonb_agg(to_jsonb({alias})) FROM {table} {alias}
          WHERE {condition} AND {alias}.{date_column} =
          (SELECT MAX({alias}2.{date_column}) FROM {table} {alias}2
           WHERE {condition.replace(alias + '.', alias + '2.')}))'''

    def _source_rows(self, keys):
        position = self._latest('player_position_history', 'ph',
            'ph.player_id=p.player_id AND ph.snapshot_date<p.game_date', 'snapshot_date')
        parts = [f'{position} AS position']
        for label, table, alias, condition in (
          ('player', 'player_average_game_stats', 'pb', 'pb.player_id=p.player_id'),
          ('advanced', 'player_average_advanced_stats', 'pa', 'pa.player_id=p.player_id'),
          ('team', 'team_average_game_stats', 'tb', 'tb.team_id=p.team_id'),
          ('opponent', 'team_average_game_stats', 'ob', 'ob.team_id=t.opponent_id')):
            condition += f' AND {alias}.season_id=p.season_id AND {alias}.game_date<=p.game_date'
            parts.append(f'{self._latest(table, alias, condition)} AS {label}')
        # Each positional snapshot is selected below using the actual as-of position.
        defense = '''(SELECT jsonb_agg(to_jsonb(d) || jsonb_build_object('season_id',history.season_id))
          FROM team_allowed_by_position d
          JOIN team_game_stats history ON history.team_id=d.team_id AND history.game_id=d.game_id
          WHERE d.team_id=t.opponent_id AND history.season_id=p.season_id AND d.game_date<=p.game_date
          AND d.game_date=(SELECT MAX(d2.game_date) FROM team_allowed_by_position d2
             WHERE d2.team_id=d.team_id AND d2.position_group=d.position_group AND d2.game_date<=p.game_date))'''
        sql = f'''SELECT p.game_id,p.player_id,to_jsonb(p) AS target,{','.join(parts)}, {defense} AS defense
          FROM player_game_stats p JOIN team_game_stats t ON t.game_id=p.game_id AND t.team_id=p.team_id
          WHERE (p.game_id,p.player_id) IN :keys ORDER BY p.game_date,p.game_id,p.player_id'''
        return self._query(sql, {'keys': keys}, ('keys',))

    @staticmethod
    def _one(rows, source):
        if not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], dict):
            raise HistoricalSourceDataError(f'{source}: expected exactly one as-of row, not missing/tied rows')
        return rows[0]

    def rolling(self, table, raw, entity_id, policy, *, position=None):
        from src.models.features.nba.historical_loader import RollingSnapshot
        key = [entity_id, raw['game_id']]
        if position is not None:
            key.append(position)
        meta = self.metadata.get(table, raw, key)
        columns = (ADVANCED_COLUMNS if table == 'player_average_advanced_stats' else
                   tuple(TEAM_ROLLING_DEFAULTS) if table == 'team_average_game_stats' else
                   tuple(OPPONENT_POSITIONAL_DEFAULTS) if table == 'team_allowed_by_position' else PLAYER_COLUMNS)
        values = []
        for column in columns:
            value = raw.get('rest_days' if column == 'stored_rest_days' else column)
            if isinstance(value, bool) or value is None:
                raise HistoricalSourceDataError(f'{table}:{key}: missing/invalid {column}')
            number = float(value)
            if not isfinite(number):
                raise HistoricalSourceDataError(f'{table}:{key}: nonfinite {column}')
            values.append((column, number))
        return RollingSnapshot(self.metadata.provenance(table, raw, key), entity_id, raw['game_id'],
          day(raw['game_date']), raw['season_id'], policy.content_id, meta['producer_ref'], meta['carryover_mode'],
          tuple(meta['contributing_seasons']), tuple(meta['contribution_game_ids']), instant(meta['latest_contribution_at']),
          instant(meta['source_observed_at']), meta['rolling_min_minutes'], meta['transition_policy_id'],
          meta['cold_start_policy_id'], tuple(values))

    def market(self, table, raw, context):
        key = [str(raw.get('staging_id', raw.get('id')))]
        if key == ['None']:
            raise HistoricalSourceDataError(f'{table}: missing native record ID')
        meta = self.metadata.get(table, raw, key)
        raw_game = raw.get('game_id') if table == 'raw_player_props_combined' else raw.get('nba_game_id')
        if raw_game not in game_aliases(context.game_id):
            raise HistoricalSourceDataError(f'{table}:{key}: game identity mismatch')
        provenance = self.metadata.provenance(table, raw, key)
        return HistoricalMarketRow(provenance, HistoricalSourceClass(meta['source_class']), context.game_id,
          str(raw['player_id']) if table == 'raw_player_props_combined' else 'game', raw['market_key'],
          raw['bookmaker'], meta['snapshot_key'], meta['side'], float(raw['line']), float(raw['odds_american']),
          meta['period'], meta['variant'],
          instant(meta['historical_requested_at']) if meta.get('historical_requested_at') is not None else None,
          instant(raw['market_last_update']) if raw.get('market_last_update') is not None else None,
          instant(raw['bookmaker_last_update']) if raw.get('bookmaker_last_update') is not None else None,
          instant(meta['local_observed_at']) if meta.get('local_observed_at') is not None else None)

    def market_rows(self, contexts, *, bookmakers, markets=('spreads','totals','player_points','player_rebounds','player_assists','player_threes')):
        if not bookmakers:
            raise HistoricalSourceDataError('missing explicit historical bookmaker routes')
        rows = {key: [] for key in contexts}
        aliases = sorted({alias for key in contexts for alias in game_aliases(key)})
        for table, column in (('raw_player_props_combined', 'game_id'), ('raw_game_lines_staging', 'nba_game_id')):
            frame = self._query(f'''SELECT to_jsonb(s) AS raw FROM {table} s
              WHERE {column} IN :games AND bookmaker IN :books AND market_key IN :markets''',
              {'games':aliases,'books':list(bookmakers),'markets':list(markets)}, ('games','books','markets'))
            for raw in frame.get('raw', []):
                matches = [context for key, context in contexts.items() if raw[column] in game_aliases(key)]
                if len(matches) != 1:
                    raise HistoricalSourceDataError(f'{table}: ambiguous source game mapping')
                row = self.market(table, raw, matches[0])
                rows[row.game_id].append(row)
        return {key: tuple(value) for key, value in rows.items()}

    def quote_rows(self, contexts, *, markets, bookmakers, max_rows):
        """Native raw player quotes for the same replay adapter; no source inference."""
        aliases = sorted({alias for key in contexts for alias in game_aliases(key)})
        frame = self._query('''SELECT to_jsonb(s) AS raw FROM raw_player_props_combined s
          WHERE game_id IN :games AND market_key IN :markets AND bookmaker IN :books ORDER BY staging_id LIMIT :row_limit''',
          {'games':aliases,'markets':list(markets),'books':list(bookmakers),'row_limit':max_rows+1}, ('games','markets','books'))
        if len(frame)>max_rows:
            raise HistoricalSourceDataError('native quote candidate cap exceeded; completeness unknown')
        output=[]
        for raw in frame.get('raw',[]):
            matches=[context for key,context in contexts.items() if raw['game_id'] in game_aliases(key)]
            if len(matches)!=1:
                raise HistoricalSourceDataError('raw_player_props_combined: ambiguous context identity')
            context=matches[0]
            row=self.market('raw_player_props_combined',raw,context)
            output.append({'game_id':context.game_id,'player_id':raw['player_id'],'market_key':row.market,
              'bookmaker':row.bookmaker,'period':row.period,'variant':row.variant,'line':raw['line'],
              'side':row.side,'price':raw['odds_american'],'odds_format':'american',
              'source_namespace':row.provenance.source_namespace,'source_record_id':row.provenance.source_record_id,
              'provenance':row.provenance,'source_class':row.source_class,'snapshot_compatibility_key':row.snapshot_key,
              'snapshot_time':instant(raw['snapshot_time']) if raw.get('snapshot_time') is not None else None,
              'historical_requested_at':row.historical_requested_at,'market_updated_at':row.market_updated_at,
              'bookmaker_updated_at':row.bookmaker_updated_at,'local_observed_at':row.local_observed_at,
              'commence_time':instant(raw['commence_time']) if raw.get('commence_time') is not None else None})
        return tuple(output)

    def _injury_rows(self, contexts, population):
        games = sorted(contexts)
        basic = self._latest('player_average_game_stats','ib',
          "ib.player_id=r.player_id AND ib.season_id=g.season_id AND ib.game_date<=g.game_date AND r.status='Out'")
        advanced = self._latest('player_average_advanced_stats','ia',
          "ia.player_id=r.player_id AND ia.season_id=g.season_id AND ia.game_date<=g.game_date AND r.status='Out'")
        sql = f'''WITH games AS (
          SELECT DISTINCT p.game_id,p.game_date::date,p.season_id,
            LEAST(p.team_id,t.opponent_id) AS team1,GREATEST(p.team_id,t.opponent_id) AS team2
          FROM player_game_stats p JOIN team_game_stats t ON p.game_id=t.game_id AND p.team_id=t.team_id
          WHERE p.game_id IN :games)
          SELECT g.game_id,to_jsonb(r) AS raw,{basic} AS player,{advanced} AS advanced
          FROM games g JOIN rapidapi_injuries r ON r.report_date=g.game_date AND r.nba_team_id IN (g.team1,g.team2)
          ORDER BY g.game_id,r.id'''
        frame = self._query(sql, {'games':games}, ('games',))
        records = {}
        for record in frame.to_dict('records'):
            key = (record['game_id'],str(record['raw']['id']))
            if key in records:
                raise HistoricalSourceDataError(f'rapidapi_injuries:{key}: duplicate edition source identity')
            records[key] = record
        return records

    def acquire(self, population, policy):
        from src.models.features.nba.historical_loader import HistoricalPlayerEvidence
        self.contexts = {}
        self.market_routes = ()
        if population.empty:
            return population.copy(), ()
        authorities = policy_payload(policy)['authorities']
        cohort = authorities['cohort_split']['value']
        windows = [(cohort['train_start'],cohort['train_end']),
          (cohort['calibration_start'],(date.fromisoformat(cohort['calibration_end_exclusive'])-timedelta(days=1)).isoformat()),
          (cohort['oos_start'],cohort['oos_end'])]
        if cohort['independent_validation']:
            windows.append((cohort['independent_validation']['start'],cohort['independent_validation']['end']))
        population = population.copy()
        population['season_type'] = None
        output = []
        for offset in range(0,len(population),self.batch_size):
            chunk = population.iloc[offset:offset+self.batch_size]
            chunk = chunk[chunk.apply(lambda row: row.season_id in cohort['seasons'] and
              any(a<=day(row.game_date).isoformat()<=b for a,b in windows),axis=1)]
            if chunk.empty:
                continue
            keys = list(chunk[['game_id','player_id']].itertuples(index=False,name=None))
            sources = self._source_rows(keys)
            lookup = {(row['game_id'],row['player_id']):row for row in sources.to_dict('records')}
            if len(lookup)!=len(sources):
                raise HistoricalSourceDataError('duplicate/ambiguous target source joins')
            contexts, candidates = {}, []
            for index,row in chunk.iterrows():
                key = (row.game_id,row.player_id)
                if pd.notna(row.actual_minutes) and 0<=row.actual_minutes<cohort['target_min_minutes']:
                    continue
                if key not in lookup:
                    raise HistoricalSourceDataError(f'player_game_stats:{key}: missing source/target team join')
                source = lookup[key]
                meta = self.metadata.get('player_game_stats',source['target'],list(key))
                season_type = meta['season_type']
                if season_type not in ('regular_season','play_in','playoffs'):
                    raise HistoricalSourceDataError(f'player_game_stats:{key}: invalid/missing season_type')
                population.at[index,'season_type'] = season_type
                if season_type not in cohort['season_types'] or (pd.notna(row.actual_minutes) and row.actual_minutes < cohort['target_min_minutes']):
                    continue
                game = meta['game_context']
                context = build_game_context(policy, required_families=tuple(sorted(NBA_FEATURE_FAMILIES)),
                  game_id=row.game_id, game_date=day(row.game_date),
                  commence_at=instant(game['commence_at']), commence_provenance_ref=game['commence_provenance_ref'])
                if row.game_id in self.contexts and self.contexts[row.game_id] != context:
                    raise HistoricalSourceDataError('contradictory source commence contexts')
                self.contexts[row.game_id] = contexts[row.game_id] = context
                candidates.append((row,source,meta,context))
            if not candidates:
                continue
            for _,_,meta,_ in candidates:
                routes = tuple(HistoricalMarketRoute(**route) for route in meta['market_routes'])
                self.market_routes = tuple(dict.fromkeys((*self.market_routes,*routes)))
            markets = self.market_rows(contexts,bookmakers=sorted({route.bookmaker for route in self.market_routes}))
            injuries = self._injury_rows(contexts,chunk)
            for row,source,meta,context in candidates:
                position = self._one(source['position'],'player_position_history')
                position_key = [int(row.player_id),str(day(position['snapshot_date']))]
                position_meta = self.metadata.get('player_position_history',position,position_key)
                group = position['position_group']
                snapshots = {}
                for role,table,entity in (('player','player_average_game_stats',row.player_id),
                  ('advanced','player_average_advanced_stats',row.player_id), ('team','team_average_game_stats',row.team_id),
                  ('opponent','team_average_game_stats',row.opponent_id), ('defense','team_allowed_by_position',row.opponent_id)):
                    raw = source[role]
                    if role == 'defense' and isinstance(raw,list):
                        raw = [record for record in raw if record.get('position_group') == group]
                    snapshots[role] = self.rolling(table,self._one(raw,table),int(entity),policy,
                                                   position=group if role=='defense' else None)
                edition_meta = meta['injury_edition']
                injury_records=[]
                for injury_id in edition_meta['row_ids']:
                    injury_source = injuries.get((context.game_id,str(injury_id)))
                    if injury_source is None:
                        raise HistoricalSourceDataError(f'rapidapi_injuries:{injury_id}: missing edition row')
                    raw = injury_source['raw']
                    injury_meta = self.metadata.get('rapidapi_injuries',raw,[str(injury_id)])
                    rolling = advanced = None
                    if raw['status'] == 'Out':
                        # These rows were acquired in bulk, including players with no target-game box score.
                        rolling=self.rolling('player_average_game_stats',self._one(injury_source['player'],'injury basic'),raw['player_id'],policy)
                        advanced=self.rolling('player_average_advanced_stats',self._one(injury_source['advanced'],'injury advanced'),raw['player_id'],policy)
                    injury_records.append(HistoricalInjuryRow(self.metadata.provenance('rapidapi_injuries',raw,[str(injury_id)]),
                      raw['player_id'],raw['nba_team_id'],raw['status'],InjuryHistoryState(injury_meta['history_state']),
                      instant(injury_meta['local_observed_at']),raw.get('report_time'),injury_meta.get('position_group'),rolling,advanced))
                edition = HistoricalInjuryEdition(SourceProvenance(edition_meta['source_namespace'],edition_meta['source_record_id']),
                  context.game_id,tuple(edition_meta['team_ids']),edition_meta['completeness_evidence_ref'],tuple(injury_records),
                  tuple((entry[0],tuple(entry[1])) for entry in edition_meta['roster_membership']))
                routes = tuple(HistoricalMarketRoute(**route) for route in meta['market_routes'])
                self.market_routes = tuple(dict.fromkeys((*self.market_routes,*routes)))
                output.append(HistoricalPlayerEvidence(context,int(row.player_id),int(row.team_id),int(row.opponent_id),row.season_id,
                  meta['season_type'],bool(row.is_home),group,self.metadata.provenance('player_position_history',position,position_key),
                  instant(position_meta['observed_at']),snapshots['player'],snapshots['advanced'],snapshots['team'],snapshots['opponent'],
                  snapshots['defense'],tuple(market for market in markets[row.game_id] if market.player_id in ('game',str(row.player_id))),
                  tuple(tuple(selection) for selection in meta['market_selections']),edition,routes))
        return population,tuple(output)
