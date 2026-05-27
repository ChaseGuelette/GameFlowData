from __future__ import annotations

from pathlib import Path


def test_dfs_paper_trader_queries_do_not_use_sqlalchemy_colon_cast_params():
    content = Path("src/paper_trading/dfs_paper_trader.py").read_text(encoding="utf-8")

    assert ":game_date::" not in content
    assert "CAST(:game_date AS timestamptz)" in content
    assert "CAST(:game_date AS date)" in content
