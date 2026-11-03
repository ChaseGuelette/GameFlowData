"""NBA population gates and opt-in date-group splits; no model math or I/O."""
from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime
from math import ceil

import numpy as np
import pandas as pd
from sklearn.model_selection import TimeSeriesSplit, train_test_split


def validate_seasons(train_seasons, calibration_season):
    if not isinstance(train_seasons, list | tuple) or not train_seasons:
        raise ValueError('Training seasons must be a nonempty sequence')
    seasons = [*train_seasons, calibration_season]
    if any(type(s) is not str or re.fullmatch(r'[24][0-9]{4}', s) is None for s in seasons):
        raise ValueError('Expected literal NBA season tokens')
    if len(set(seasons)) != len(seasons):
        raise ValueError('Duplicate or overlapping training/calibration seasons')


def _dates(values, size):
    if len(values) != size:
        raise ValueError('Date metadata length mismatch')
    if any(not isinstance(value, date | datetime | pd.Timestamp | str)
           or isinstance(value, str) and re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', value) is None
           for value in values):
        raise ValueError('Expected literal game dates, not numeric/coerced identities')
    days = pd.DatetimeIndex(pd.to_datetime(values, errors='raise'))
    if days.hasnans or days.tz is not None or not (days == days.normalize()).all():
        raise ValueError('Expected valid timezone-naive game dates')
    return days


def temporal_split(*arrays, dates=None, test_size=0.15, **legacy_kwargs):
    """Split whole dates; absent metadata preserves shared callers exactly."""
    if dates is None:
        return train_test_split(*arrays, test_size=test_size, **legacy_kwargs)
    days = _dates(dates, len(arrays[0]))
    unique = days.unique().sort_values()
    n_val = ceil(len(unique) * test_size)
    if not 0 < n_val < len(unique):
        raise ValueError('Insufficient distinct dates for temporal holdout')
    cutoff = unique[-n_val]
    order = np.argsort(days.asi8, kind='stable')
    train = order[days[order] < cutoff]
    valid = order[days[order] >= cutoff]
    result = []
    for array in arrays:
        if len(array) != len(days):
            raise ValueError('Split array length mismatch')
        result.extend(array.iloc[index] if hasattr(array, 'iloc') else array[index] for index in (train, valid))
    return result


def temporal_folds(X, n_splits, dates=None):
    if dates is None:
        yield from TimeSeriesSplit(n_splits=n_splits).split(X)
        return
    days = _dates(dates, len(X))
    unique = days.unique().sort_values()
    order = np.argsort(days.asi8, kind='stable')
    for train, valid in TimeSeriesSplit(n_splits=n_splits).split(unique):
        yield (order[days[order].isin(unique[train])], order[days[order].isin(unique[valid])])


def validate_populations(train, calibration, train_seasons, calibration_season):
    """Reject rather than repair identities; return sorted frames retaining attrs."""
    validate_seasons(train_seasons, calibration_season)
    frames = []
    for frame, seasons in ((train, train_seasons), (calibration, [calibration_season])):
        required = ['game_date', 'game_id', 'player_id', 'season_id']
        if frame.empty or not set(required) <= set(frame.columns):
            raise ValueError('Nonempty training/calibration population with all identities required')
        if frame[required].isna().any().any():
            raise ValueError('Missing population identity')
        _dates(frame['game_date'], len(frame))
        if any(type(s) is not str or s not in seasons for s in frame['season_id']):
            raise ValueError('Population outside requested literal seasons')
        if set(frame['season_id']) != set(seasons):
            raise ValueError('Requested season missing from loaded population')
        if any(type(g) is not str or not g or g != g.strip() for g in frame['game_id']):
            raise ValueError('Invalid game identity')
        if any(isinstance(p, bool | np.bool_) or not isinstance(p, int | np.integer) or p <= 0
               for p in frame['player_id']):
            raise ValueError('Invalid player identity')
        if frame.duplicated(['game_id', 'player_id']).any():
            raise ValueError('Duplicate player-game identity')
        if frame.groupby('game_id')[['game_date', 'season_id']].nunique().gt(1).any().any():
            raise ValueError('Conflicting game date/season identity')
        ordered = frame.sort_values(['game_date', 'game_id', 'player_id'], kind='stable').reset_index(drop=True)
        ordered.attrs = frame.attrs.copy()
        frames.append(ordered)
    fit, cal = frames
    if set(fit['game_id']) & set(cal['game_id']):
        raise ValueError('Training/calibration game overlap')
    if _dates(fit['game_date'], len(fit)).max() >= _dates(cal['game_date'], len(cal)).min():
        raise ValueError('Training must strictly precede calibration by date')
    return fit, cal


def population_proof(frame):
    days = _dates(frame['game_date'], len(frame))
    keys = sorted((str(day.date()), game, int(player), season) for day, game, player, season in
                  zip(days, frame['game_id'], frame['player_id'], frame['season_id'], strict=True))
    return {'rows': len(frame), 'games': int(frame['game_id'].nunique()), 'dates': len(days.unique()),
            'start': days.min().date().isoformat(), 'end': days.max().date().isoformat(),
            'seasons': sorted(frame['season_id'].unique().tolist()),
            'keys_sha256': hashlib.sha256(json.dumps(keys, separators=(',', ':')).encode()).hexdigest()}


def chronology_proof(train, calibration):
    return {'schema_version': 1, 'strict_fit_before_calibration': True,
            'training': population_proof(train), 'calibration': population_proof(calibration),
            'source_verified': False, 'decision_grade': False}
