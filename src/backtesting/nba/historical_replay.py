"""Bounded historical replay acquisition from compact, price-backed sources."""
from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from decimal import Decimal
from math import isfinite
from numbers import Integral, Real

import pandas as pd

from src.backtesting.nba.historical_quote_adapter import adapt_historical_quotes, resolve_game_context
from src.models.features.nba.historical_policy import HistoricalPolicy, HistoricalPolicyContractError, policy_payload
from src.models.features.nba.line_sources import HistoricalMarketRoute

HISTORICAL_LINE_METADATA = (
    'pair_id', 'policy_content_id', 'snapshot_compatibility_key', 'source_namespace',
    'context_identity', 'over_metadata', 'under_metadata', 'exact_line', 'exact_over_odds', 'exact_under_odds',
    'over_snapshot_time', 'under_snapshot_time', 'over_market_last_update', 'under_market_last_update',
    'over_bookmaker_last_update', 'under_bookmaker_last_update', 'over_decision_time', 'under_decision_time',
    'over_bookmaker', 'under_bookmaker', 'bet_decision_time',
)


class HistoricalReplayBlockedError(RuntimeError):
    def __init__(self, reason, *, ledger=None, summary=None):
        self.ledger = ledger if ledger is not None else pd.DataFrame()
        self.summary = summary or {'status': 'blocked', 'decision_grade': False}
        super().__init__(reason)


def evidence_payload(value):
    """JSON-safe diagnostic evidence without lossy Decimal or nonfinite coercion."""
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, str | bool):
        return value
    if isinstance(value, Decimal):
        return {'decimal': str(value)}
    if isinstance(value, Integral):
        return int(value)
    if isinstance(value, Real):
        return float(value) if isfinite(value) else {'nonfinite': str(value)}
    if isinstance(value, datetime | date):
        return value.isoformat()
    if is_dataclass(value) and not isinstance(value, type):
        return evidence_payload(asdict(value))
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise TypeError('historical evidence keys must be strings')
        return {key: evidence_payload(item) for key, item in value.items()}
    if isinstance(value, tuple | list):
        return [evidence_payload(item) for item in value]
    raise TypeError(f'unsupported historical evidence scalar: {type(value).__name__}')


def write_quote_ledger(frame, path):
    path.write_text(json.dumps(evidence_payload(frame.to_dict('records')), sort_keys=True,
                               allow_nan=False, indent=2), encoding='utf-8')


class HistoricalReplayQuotes:
    """One owner called by both direct and prefetch paths, with bounded inputs.

    Native compact quotes require exact source prices and explicit route authority.
    Explicit controlled rows exercise mechanics only, never source verification.
    """

    def __init__(self, *, engine, policy, contexts, markets, bookmakers, controlled_rows=None,
                 controlled=False, route_authority=(), native_sources=None, max_games=8, max_candidates_per_game=5000):
        if not isinstance(policy, HistoricalPolicy) or not isinstance(contexts, Mapping):
            raise HistoricalPolicyContractError('historical policy and explicit contexts required')
        if type(controlled) is not bool or controlled_rows is not None and not controlled:
            raise HistoricalPolicyContractError('caller quote rows require explicit controlled mode')
        if controlled and not isinstance(controlled_rows, tuple):
            raise HistoricalPolicyContractError('controlled quote rows must be an explicit tuple')
        if not isinstance(route_authority, tuple) or any(not isinstance(route, HistoricalMarketRoute) for route in route_authority):
            raise HistoricalPolicyContractError('historical route authority must be an immutable tuple')
        if controlled and not route_authority:
            raise HistoricalPolicyContractError('controlled replay requires explicit route authority')
        if any(type(value) is not int or value <= 0 for value in (max_games, max_candidates_per_game)):
            raise ValueError('historical acquisition limits must be positive integers')
        self.engine, self.policy, self.contexts = engine, policy, dict(contexts)
        self.markets, self.bookmakers = tuple(markets), tuple(bookmakers)
        self.controlled_rows = controlled_rows
        self.native_sources = native_sources
        source_ref = policy_payload(policy)['authorities']['source'].get('value', {}).get('provenance_evidence_ref')
        self.allowed_routes = {(route.bookmaker, route.period, route.variant) for route in route_authority
                               if route.provenance_evidence_ref == source_ref}
        self.max_games, self.max_candidates_per_game = max_games, max_candidates_per_game
        if not self.markets or not self.bookmakers:
            raise HistoricalPolicyContractError('explicit markets/bookmakers required')
        # Validate all policy/context contracts even on an empty slate.
        adapt_historical_quotes((), contexts=self.contexts, policy=self.policy)
        if controlled_rows is not None:
            if len(controlled_rows) > max_games * max_candidates_per_game:
                raise HistoricalReplayBlockedError('controlled input bundle exceeds acquisition caps')
            global_result = adapt_historical_quotes(controlled_rows, contexts=self.contexts, policy=self.policy)
            if global_result.summary['status'] == 'blocked':
                raise HistoricalReplayBlockedError('controlled quote bundle has source-integrity blocks',
                    ledger=global_result.ledger, summary=global_result.summary)

    def _raw_rows(self, game_ids):
        if len(game_ids) > self.max_games:
            raise HistoricalReplayBlockedError('historical acquisition exceeds explicit game cap')
        if not game_ids:
            return []
        if self.controlled_rows is None and self.native_sources is not None:
            from src.models.features.nba.historical_sources import HistoricalSourceDataError

            try:
                rows=self.native_sources.quote_rows({key:self.contexts[key] for key in game_ids},
                    markets=self.markets,bookmakers=self.bookmakers,
                    max_rows=len(game_ids)*self.max_candidates_per_game)
            except HistoricalSourceDataError as exc:
                raise HistoricalReplayBlockedError(str(exc)) from None
            if any(sum(row['game_id']==key for row in rows)>self.max_candidates_per_game for key in game_ids):
                raise HistoricalReplayBlockedError('native per-game quote candidate cap exceeded')
            return rows
        if self.controlled_rows is not None:
            rows, counts = [], {}
            for row in self.controlled_rows:
                if not isinstance(row, Mapping):
                    raise HistoricalPolicyContractError('controlled outcome rows must be mappings')
                context, _ = resolve_game_context(row.get('game_id'), self.contexts)
                if context is None or context.game_id in game_ids:
                    rows.append(dict(row))
                    key = context.game_id if context else None
                    counts[key] = counts.get(key, 0) + 1
            if any(count > self.max_candidates_per_game for count in counts.values()):
                raise HistoricalReplayBlockedError('controlled quote population exceeds candidate cap')
            return rows
        if getattr(self.engine.dialect, 'name', None) != 'postgresql':
            raise HistoricalReplayBlockedError('native historical acquisition requires read-only PostgreSQL')
        from src.backtesting.nba.compact_quotes import CompactQuoteError, historical_quote_rows, load_compact_pairs

        rows = []
        try:
            for game_id in game_ids:
                pairs = load_compact_pairs(self.engine, game_ids=[game_id], markets=self.markets,
                    bookmakers=self.bookmakers, max_rows=self.max_candidates_per_game // 2)
                rows.extend(historical_quote_rows(pairs, allowed_routes=self.allowed_routes))
        except CompactQuoteError as exc:
            raise HistoricalReplayBlockedError(str(exc)) from exc
        return rows

    def for_date(self, game_date, game_ids=None):
        expected = sorted(key for key, context in self.contexts.items() if context.game_date == game_date)
        if game_ids is not None and set(game_ids) != set(expected):
            # The feature population may select a subset of games, but never a game
            # with no explicit context for the requested date.
            if not set(game_ids) <= set(expected):
                raise HistoricalReplayBlockedError('feature game IDs lack matching historical contexts')
            expected = sorted(set(game_ids))
        result = adapt_historical_quotes(self._raw_rows(expected), contexts=self.contexts, policy=self.policy)
        if result.summary['status'] == 'blocked':
            raise HistoricalReplayBlockedError('historical quote source evidence blocked', ledger=result.ledger,
                                               summary=result.summary)
        disallowed = {row['pair_id'] for row in result.pairs.to_dict('records')
                      if row['market_key'] not in self.markets or row['bookmaker'] not in self.bookmakers
                      or (row['bookmaker'], row['period'], row['variant']) not in self.allowed_routes}
        if disallowed:
            ledger = result.ledger.copy()
            ledger['acquisition_rejection'] = ledger['pair_id'].map(
                lambda pair: 'requested_market_book_or_route_mismatch' if pair in disallowed else None)
            raise HistoricalReplayBlockedError('historical quote route outside explicit request authority', ledger=ledger,
                                               summary={**result.summary, 'status': 'blocked'})
        pairs = result.pairs.copy()
        for name in ('line', 'over_odds', 'under_odds'):
            pairs['exact_' + name] = pairs[name].map(str)
            converted = []
            for value in pairs[name]:
                number = float(value)
                if not isfinite(number) or Decimal(str(number)) != Decimal(str(value)):
                    raise HistoricalReplayBlockedError('quote cannot cross numeric calculator boundary losslessly')
                converted.append(number)
            pairs[name] = pd.Series(converted, index=pairs.index, dtype=float)
        for index, row in pairs.iterrows():
            for side in ('over', 'under'):
                metadata = row[side + '_metadata']
                if metadata['odds_format'] != 'american':
                    raise HistoricalReplayBlockedError('NBA replay requires explicit American quote format')
                clocks = metadata['snapshot_clocks']
                if len(clocks) != 1 or clocks[0][0] != 'utc':
                    raise HistoricalReplayBlockedError('ambiguous paired snapshot clock')
                pairs.at[index, side + '_snapshot_time'] = clocks[0][1]
                for raw_name, output_name in (('market_updated_at', 'market_last_update'),
                                               ('bookmaker_updated_at', 'bookmaker_last_update')):
                    values = {record.get(raw_name).isoformat() if isinstance(record.get(raw_name), datetime)
                              else record.get(raw_name) for record in metadata['raw_occurrences']}
                    if len(values) != 1:
                        raise HistoricalReplayBlockedError('ambiguous side-specific source clock')
                    pairs.at[index, side + '_' + output_name] = next(iter(values))
                pairs.at[index, side + '_decision_time'] = row['context_identity']['decision_at']
                pairs.at[index, side + '_bookmaker'] = row['bookmaker']
            pairs.at[index, 'bet_decision_time'] = row['context_identity']['decision_at']
        ledger = result.ledger.copy()
        ledger['game_date'] = game_date
        pairs.attrs['historical_quote_summary'] = dict(result.summary)
        return pairs, ledger

    def prefetch(self, start_date, end_date):
        if start_date > end_date:
            raise ValueError('historical range is reversed')
        dates = sorted({context.game_date for context in self.contexts.values()
                        if start_date <= context.game_date <= end_date})
        pairs, ledgers = {}, {}
        for day in dates:
            pairs[day], ledgers[day] = self.for_date(day)
        return pairs, ledgers
