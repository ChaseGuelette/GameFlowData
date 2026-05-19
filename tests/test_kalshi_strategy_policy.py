from datetime import date

from src.trading.kalshi.strategy import (
    ExistingPosition,
    StrategyConfig,
    TradeCandidate,
    calculate_kelly_contracts,
    select_trade_intents,
)


def test_calculate_kelly_contracts_matches_existing_live_trader_formula_for_no_side():
    contracts = calculate_kelly_contracts(
        model_prob=0.45,
        yes_price=70,
        side="no",
        bankroll=100.0,
        kelly_fraction=0.125,
        max_contracts=50,
    )

    assert contracts == 15


def test_strategy_selects_no_side_when_yes_bets_are_disabled():
    candidate = TradeCandidate(
        game_date=date(2026, 5, 18),
        ticker="KXMLBHIT-26MAY181900PLAYER",
        sport="mlb",
        player_id=7,
        player_name="Test Batter",
        stat_type="batter_hits",
        line=1.0,
        yes_price=70,
        model_prob=0.45,
        kalshi_implied=0.70,
        raw_edge=0.08,
        volume=100,
        bid_ask_spread=5,
        sportsbook_consensus_line=0.5,
    )

    intents = select_trade_intents(
        [candidate],
        config=StrategyConfig(min_edge=0.15, allow_yes=False, bankroll=100.0),
    )

    assert len(intents) == 1
    assert intents[0].side == "no"
    assert intents[0].contracts == 15
    assert intents[0].expected_cost == 4.5
    assert intents[0].fee_adjusted_edge > 0.15


def test_strategy_dedupes_player_stat_and_prefers_sportsbook_aligned_line():
    unaligned = TradeCandidate(
        game_date=date(2026, 5, 18),
        ticker="UNALIGNED",
        sport="mlb",
        player_id=7,
        player_name="Test Batter",
        stat_type="batter_hits",
        line=2.0,
        yes_price=64,
        model_prob=0.70,
        kalshi_implied=0.64,
        raw_edge=0.06,
        volume=100,
        bid_ask_spread=5,
        sportsbook_consensus_line=0.5,
    )
    aligned = TradeCandidate(
        game_date=date(2026, 5, 18),
        ticker="ALIGNED",
        sport="mlb",
        player_id=7,
        player_name="Test Batter",
        stat_type="batter_hits",
        line=1.0,
        yes_price=70,
        model_prob=0.45,
        kalshi_implied=0.70,
        raw_edge=0.08,
        volume=100,
        bid_ask_spread=5,
        sportsbook_consensus_line=0.5,
    )

    intents = select_trade_intents(
        [unaligned, aligned],
        config=StrategyConfig(min_edge=0.15, allow_yes=False, bankroll=100.0),
    )

    assert [intent.ticker for intent in intents] == ["ALIGNED"]


def test_strategy_skips_existing_live_or_queued_player_stat_keys():
    candidate = TradeCandidate(
        game_date=date(2026, 5, 18),
        ticker="KXMLBHIT-26MAY181900PLAYER",
        sport="mlb",
        player_id=7,
        player_name="Test Batter",
        stat_type="batter_hits",
        line=1.0,
        yes_price=70,
        model_prob=0.45,
        kalshi_implied=0.70,
        raw_edge=0.08,
        volume=100,
        bid_ask_spread=5,
        sportsbook_consensus_line=0.5,
    )

    assert select_trade_intents(
        [candidate],
        config=StrategyConfig(min_edge=0.15, allow_yes=False, bankroll=100.0),
        existing_player_stats={(7, "batter_hits")},
    ) == []
    assert select_trade_intents(
        [candidate],
        config=StrategyConfig(min_edge=0.15, allow_yes=False, bankroll=100.0),
        queued_player_stats={(7, "batter_hits")},
    ) == []


def test_strategy_caps_contracts_by_existing_position_capacity():
    candidate = TradeCandidate(
        game_date=date(2026, 5, 18),
        ticker="KXMLBHIT-26MAY181900PLAYER",
        sport="mlb",
        player_id=7,
        player_name="Test Batter",
        stat_type="batter_hits",
        line=1.0,
        yes_price=70,
        model_prob=0.45,
        kalshi_implied=0.70,
        raw_edge=0.08,
        volume=100,
        bid_ask_spread=5,
        sportsbook_consensus_line=0.5,
    )

    intents = select_trade_intents(
        [candidate],
        config=StrategyConfig(min_edge=0.15, allow_yes=False, bankroll=100.0, max_contracts=50),
        held_positions={"KXMLBHIT-26MAY181900PLAYER": ExistingPosition(total_traded=45)},
    )

    assert len(intents) == 1
    assert intents[0].contracts == 5
