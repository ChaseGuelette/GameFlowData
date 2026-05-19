from src.paper_trading import kalshi_paper_trader as paper
from src.trading.kalshi import live_trading_config
from src.trading.kalshi.strategy import calculate_kelly_contracts


def test_paper_trader_uses_shared_live_trading_config_constants():
    assert paper.SUPPORTED_STATS is live_trading_config.SUPPORTED_STATS
    assert paper._SPORTSBOOK_LINE_FALLBACK_GAP == live_trading_config.SPORTSBOOK_LINE_FALLBACK_GAP

    source = paper.__file__
    with open(source, encoding="utf-8") as handle:
        content = handle.read()

    assert "SUPPORTED_STATS: dict" not in content
    assert "_SPORTSBOOK_LINE_FALLBACK_GAP = 0.08" not in content


def test_paper_kelly_sizing_matches_live_strategy_helper_for_yes_and_no():
    trader = paper.KalshiPaperTrader.__new__(paper.KalshiPaperTrader)
    trader.kelly_fraction = 0.125
    trader.max_contracts_per_market = 50

    cases = [
        {"model_prob": 0.45, "yes_price": 70, "side": "no", "bankroll": 100.0},
        {"model_prob": 0.72, "yes_price": 55, "side": "yes", "bankroll": 250.0},
        {"model_prob": 0.20, "yes_price": 80, "side": "no", "bankroll": 80.0},
    ]

    for case in cases:
        paper_contracts = trader._kelly_contracts(
            case["model_prob"],
            case["yes_price"],
            case["side"],
            case["bankroll"],
        )
        live_contracts = calculate_kelly_contracts(
            model_prob=case["model_prob"],
            yes_price=case["yes_price"],
            side=case["side"],
            bankroll=case["bankroll"],
            kelly_fraction=trader.kelly_fraction,
            max_contracts=trader.max_contracts_per_market,
        )

        assert paper_contracts == live_contracts
