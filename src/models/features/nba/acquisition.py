"""Operational population limits, independent of historical source eligibility."""
from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class AcquisitionLimits:
    max_dates: int
    max_games: int
    max_population_rows: int
    population_timeout_ms: int

    def __post_init__(self):
        for name, value in asdict(self).items():
            if type(value) is not int or value <= 0:
                raise ValueError(f'{name} must be an explicit positive integer')

    def to_dict(self):
        return asdict(self)

    def check_span(self, start, end):
        if end < start or (end - start).days + 1 > self.max_dates:
            raise ValueError('Requested historical date span exceeds max_dates')


class AcquisitionBudget:
    """Cumulative across a FeatureStore/run, including fitting and calibration.

    Row counts count queries (even repeated rows); dates/games count unions.
    Failed/overflow acquisition is never refunded or published.
    """
    def __init__(self, limits):
        if not isinstance(limits, AcquisitionLimits):
            raise ValueError('Native historical execution requires complete acquisition limits')
        self.limits = limits
        self.rows = 0
        self.dates = set()
        self.games = set()

    @property
    def fetch_limit(self):
        return max(0, self.limits.max_population_rows - self.rows) + 1

    def consume(self, frame):
        self.rows += len(frame)
        self.dates.update(frame['game_date'])
        self.games.update(frame['game_id'])
        if (self.rows > self.limits.max_population_rows or len(self.dates) > self.limits.max_dates
                or len(self.games) > self.limits.max_games):
            raise ValueError('Historical population acquisition budget exceeded before source assembly')


def add_acquisition_arguments(parser):
    for name in AcquisitionLimits.__dataclass_fields__:
        parser.add_argument('--' + name.replace('_', '-'), type=int)


def limits_from_values(values, *, required=False):
    selected = {name: values.get(name) for name in AcquisitionLimits.__dataclass_fields__}
    if not required and all(value is None for value in selected.values()):
        return None
    return AcquisitionLimits(**selected)


def limits_argv(limits):
    return [token for name, value in limits.to_dict().items()
            for token in ('--' + name.replace('_', '-'), str(value))]
