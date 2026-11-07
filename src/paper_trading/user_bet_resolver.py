"""NBA user-bet settlement; general callers retain both real and paper scope."""

import logging
from datetime import date

from src.db.client import get_engine
from src.paper_trading.nba_settlement import resolve_date, resolve_pending

logger = logging.getLogger(__name__)


class UserBetResolver:
    def __init__(self, engine=None, *, paper_only=False, dry_run=False):
        self.engine = engine if engine is not None else get_engine()
        self.paper_only = paper_only
        self.dry_run = dry_run

    @staticmethod
    def _american_to_decimal(american_odds: float) -> float:
        return 1 + (american_odds / 100 if american_odds > 0 else 100 / abs(american_odds))

    def resolve_bets_for_date(self, game_date: date) -> dict:
        result = resolve_date(self.engine, "user_bets", game_date,
                              paper_only=self.paper_only, dry_run=self.dry_run)
        logger.info("NBA user settlement %s: %s", game_date, result)
        return result

    def resolve_all_pending(self, exclude_today: bool = True) -> dict:
        return resolve_pending(self.engine, "user_bets", self.resolve_bets_for_date,
                               exclude_today=exclude_today, paper_only=self.paper_only)
