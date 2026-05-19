"""Pure Kalshi trading policy helpers.

This module is intentionally side-effect free: no DB, no Kalshi API, no
Discord, no environment reads. It owns deterministic live-trade selection
policy for orchestration callers.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, datetime
from typing import Literal

from src.scrapers.kalshi.kalshi_utils import fee_adjusted_edge, kalshi_taker_fee
from src.trading.kalshi.live_trading_config import SPORTSBOOK_LINE_FALLBACK_GAP

Side = Literal["yes", "no"]


@dataclass(frozen=True)
class StrategyConfig:
    """Pure strategy knobs formerly read from the live trader/environment."""

    min_edge: float = 0.15
    max_edge: float = 0.40
    min_price: int = 5
    max_contracts: int = 50
    kelly_fraction: float = 0.125
    bankroll: float = 100.0
    effective_daily_exposure_cap: float = 80.0
    prior_exposure: float = 0.0
    allow_yes: bool = False
    star_hits_yes_price: int = 72
    sportsbook_line_fallback_gap: float = SPORTSBOOK_LINE_FALLBACK_GAP
    supported_stats: dict[str, set[str]] | None = None


@dataclass(frozen=True)
class ExistingPosition:
    """Open Kalshi position summary used for per-ticker capacity checks."""

    total_traded: int = 0


@dataclass(frozen=True)
class TradeCandidate:
    """A side-effect-free candidate row for Kalshi strategy selection."""

    game_date: date
    ticker: str
    sport: str
    player_id: int | None
    player_name: str
    stat_type: str
    line: float | None
    yes_price: int
    model_prob: float
    kalshi_implied: float
    raw_edge: float = 0.0
    volume: int = 0
    bid_ask_spread: int = 100
    sportsbook_consensus_line: float | None = None
    game_start_time: datetime | None = None
    allowed_directions: tuple[str, ...] | None = None
    structural_skip_reason: str | None = None
    structural_skip_directions: tuple[str, ...] = ()


@dataclass(frozen=True)
class TradeIntent:
    """Strategy output that can later be queued or executed by side-effect services."""

    game_date: date
    ticker: str
    sport: str
    player_id: int
    player_name: str
    stat_type: str
    line: float | None
    side: Side
    yes_price: int
    contracts: int
    cost_per: float
    expected_cost: float
    expected_fee: float
    model_prob: float
    kalshi_implied: float
    edge: float
    fee_adjusted_edge: float
    game_start_time: datetime | None = None
    sportsbook_consensus_line: float | None = None

    def as_legacy_dict(self) -> dict:
        """Return the queue-compatible trade dict shape expected by orchestration callers."""
        return {
            "game_date": self.game_date,
            "ticker": self.ticker,
            "sport": self.sport,
            "player_id": self.player_id,
            "player_name": self.player_name,
            "stat_type": self.stat_type,
            "line": self.line,
            "side": self.side,
            "yes_price": self.yes_price,
            "contracts": self.contracts,
            "cost_per": self.cost_per,
            "expected_cost": self.expected_cost,
            "expected_fee": self.expected_fee,
            "model_prob": self.model_prob,
            "kalshi_implied": self.kalshi_implied,
            "edge": self.edge,
            "fee_adjusted_edge": self.fee_adjusted_edge,
            "game_start_time": self.game_start_time,
            "sportsbook_consensus_line": self.sportsbook_consensus_line,
        }


def calculate_kelly_contracts(
    *,
    model_prob: float,
    yes_price: int,
    side: Side,
    bankroll: float,
    kelly_fraction: float,
    max_contracts: int,
) -> int:
    """Calculate Kelly-optimal Kalshi contracts using the existing live formula."""
    if side == "yes":
        cost_per = yes_price / 100.0
        win_per = (100 - yes_price) / 100.0
        prob = model_prob
    else:
        cost_per = (100 - yes_price) / 100.0
        win_per = yes_price / 100.0
        prob = 1.0 - model_prob

    if win_per <= 0 or cost_per <= 0:
        return 0

    fee_per = kalshi_taker_fee(yes_price if side == "yes" else 100 - yes_price)
    net_win_per = win_per - fee_per
    if net_win_per <= 0:
        return 0

    f_net = (prob - cost_per) / net_win_per
    f_fractional = f_net * kelly_fraction
    if f_fractional <= 0:
        return 0

    contracts = int(math.floor(f_fractional * bankroll / cost_per))
    return min(contracts, max_contracts)


def _cost_per_contract(yes_price: int, side: Side) -> float:
    return yes_price / 100.0 if side == "yes" else (100 - yes_price) / 100.0


def _is_sportsbook_aligned(candidate: TradeCandidate) -> bool:
    if candidate.line is None or candidate.sportsbook_consensus_line is None:
        return False
    return int(candidate.line) == math.ceil(candidate.sportsbook_consensus_line)


def _select_side(candidate: TradeCandidate, config: StrategyConfig) -> tuple[Side, float] | None:
    yes_edge = fee_adjusted_edge(candidate.model_prob, candidate.yes_price, is_yes=True, is_maker=False)
    no_edge = fee_adjusted_edge(candidate.model_prob, candidate.yes_price, is_yes=False, is_maker=False)

    if config.allow_yes and yes_edge >= no_edge and yes_edge >= config.min_edge:
        return "yes", yes_edge
    if no_edge >= config.min_edge:
        return "no", no_edge
    return None


def _passes_static_filters(
    candidate: TradeCandidate,
    *,
    side: Side,
    edge: float,
    config: StrategyConfig,
    existing_player_stats: set[tuple[int, str]],
    queued_player_stats: set[tuple[int, str]],
) -> bool:
    if candidate.player_id is None:
        return False

    supported = (config.supported_stats or {}).get(candidate.sport, set())
    if supported and candidate.stat_type not in supported:
        return False

    key = (candidate.player_id, candidate.stat_type)
    if key in existing_player_stats or key in queued_player_stats:
        return False

    if candidate.volume < 20 or candidate.bid_ask_spread > 15:
        return False

    if candidate.yes_price < config.min_price or candidate.yes_price > (100 - config.min_price):
        return False

    direction = "over" if side == "yes" else "under"
    if direction in candidate.structural_skip_directions:
        return False

    if candidate.allowed_directions is not None and direction not in candidate.allowed_directions:
        return False

    if (
        candidate.stat_type == "batter_hits"
        and side == "no"
        and candidate.line == 1.0
        and candidate.yes_price >= config.star_hits_yes_price
    ):
        return False

    return edge <= config.max_edge


def _maybe_replace_candidate(
    existing: tuple[TradeCandidate, Side, float] | None,
    new: tuple[TradeCandidate, Side, float],
    *,
    fallback_gap: float,
) -> tuple[TradeCandidate, Side, float]:
    if existing is None:
        return new

    existing_candidate, _existing_side, existing_edge = existing
    new_candidate, _new_side, new_edge = new
    existing_matching = _is_sportsbook_aligned(existing_candidate)
    new_matching = _is_sportsbook_aligned(new_candidate)

    if new_matching and not existing_matching:
        if existing_edge > new_edge + fallback_gap:
            return existing
        return new
    if existing_matching and not new_matching:
        if new_edge <= existing_edge + fallback_gap:
            return existing
        return new
    if new_edge > existing_edge:
        return new
    return existing


def select_trade_intents(
    candidates: list[TradeCandidate],
    *,
    config: StrategyConfig,
    existing_player_stats: set[tuple[int, str]] | None = None,
    queued_player_stats: set[tuple[int, str]] | None = None,
    held_positions: dict[str, ExistingPosition] | None = None,
) -> list[TradeIntent]:
    """Select sized trade intents from preloaded candidates.

    The caller owns DB/API work.  This function only applies deterministic policy
    and returns legacy-compatible trade intents.
    """
    existing_player_stats = existing_player_stats or set()
    queued_player_stats = queued_player_stats or set()
    held_positions = held_positions or {}

    best_by_player_stat: dict[tuple[int, str], tuple[TradeCandidate, Side, float]] = {}

    for candidate in candidates:
        side_and_edge = _select_side(candidate, config)
        if side_and_edge is None:
            continue
        side, edge = side_and_edge
        if not _passes_static_filters(
            candidate,
            side=side,
            edge=edge,
            config=config,
            existing_player_stats=existing_player_stats,
            queued_player_stats=queued_player_stats,
        ):
            continue
        assert candidate.player_id is not None  # Narrowed by _passes_static_filters.
        key = (candidate.player_id, candidate.stat_type)
        best_by_player_stat[key] = _maybe_replace_candidate(
            best_by_player_stat.get(key),
            (candidate, side, edge),
            fallback_gap=config.sportsbook_line_fallback_gap,
        )

    total_exposure = config.prior_exposure
    intents: list[TradeIntent] = []
    for (player_id, stat_type), (candidate, side, edge) in sorted(
        best_by_player_stat.items(), key=lambda item: item[1][2], reverse=True
    ):
        position = held_positions.get(candidate.ticker)
        remaining_capacity = config.max_contracts - (position.total_traded if position else 0)
        if remaining_capacity <= 0:
            continue

        contracts = calculate_kelly_contracts(
            model_prob=candidate.model_prob,
            yes_price=candidate.yes_price,
            side=side,
            bankroll=config.bankroll,
            kelly_fraction=config.kelly_fraction,
            max_contracts=config.max_contracts,
        )
        contracts = min(contracts, remaining_capacity)
        if contracts <= 0:
            continue

        cost_per = _cost_per_contract(candidate.yes_price, side)
        trade_cost = contracts * cost_per

        if total_exposure + trade_cost > config.effective_daily_exposure_cap:
            remaining = config.effective_daily_exposure_cap - total_exposure
            if remaining <= cost_per:
                continue
            contracts = int(math.floor(remaining / cost_per))
            if contracts <= 0:
                continue
            trade_cost = contracts * cost_per

        if trade_cost > config.bankroll:
            contracts = int(math.floor(config.bankroll / cost_per))
            if contracts <= 0:
                continue
            trade_cost = contracts * cost_per

        total_exposure += trade_cost
        fee_per = kalshi_taker_fee(candidate.yes_price if side == "yes" else 100 - candidate.yes_price)
        intents.append(
            TradeIntent(
                game_date=candidate.game_date,
                ticker=candidate.ticker,
                sport=candidate.sport,
                player_id=player_id,
                player_name=candidate.player_name,
                stat_type=stat_type,
                line=candidate.line,
                side=side,
                yes_price=candidate.yes_price,
                contracts=contracts,
                cost_per=cost_per,
                expected_cost=round(trade_cost, 2),
                expected_fee=round(fee_per * contracts, 4),
                model_prob=round(candidate.model_prob, 4),
                kalshi_implied=round(candidate.kalshi_implied, 4),
                edge=round(candidate.raw_edge, 4),
                fee_adjusted_edge=round(edge, 4),
                game_start_time=candidate.game_start_time,
                sportsbook_consensus_line=candidate.sportsbook_consensus_line,
            )
        )

    return intents
