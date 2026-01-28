"""Tests for visualize_results module."""

import json

import pandas as pd
import pytest

from src.backtesting.visualize_results import generate_chart, load_data


class TestLoadData:
    """Tests for load_data."""

    def test_loads_bets_csv(self, tmp_path):
        """Test loading bets.csv with game_date parsing."""
        bets = pd.DataFrame(
            {
                "game_date": ["2024-01-15", "2024-01-16"],
                "profit": [100.0, -50.0],
                "stat": ["pts", "reb"],
            }
        )
        bets.to_csv(tmp_path / "bets.csv", index=False)

        df, bankroll = load_data(tmp_path)

        assert len(df) == 2
        assert pd.api.types.is_datetime64_any_dtype(df["game_date"])
        assert bankroll == 10000.0  # default

    def test_loads_starting_bankroll_from_config(self, tmp_path):
        """Test loading starting_bankroll from metrics.json config key."""
        bets = pd.DataFrame({"game_date": ["2024-01-15"], "profit": [50.0]})
        bets.to_csv(tmp_path / "bets.csv", index=False)

        metrics = {"config": {"starting_bankroll": 5000.0}}
        with open(tmp_path / "metrics.json", "w") as f:
            json.dump(metrics, f)

        df, bankroll = load_data(tmp_path)
        assert bankroll == 5000.0

    def test_loads_starting_bankroll_from_root(self, tmp_path):
        """Test loading starting_bankroll from root of metrics.json."""
        bets = pd.DataFrame({"game_date": ["2024-01-15"], "profit": [50.0]})
        bets.to_csv(tmp_path / "bets.csv", index=False)

        metrics = {"starting_bankroll": 25000.0}
        with open(tmp_path / "metrics.json", "w") as f:
            json.dump(metrics, f)

        df, bankroll = load_data(tmp_path)
        assert bankroll == 25000.0

    def test_defaults_bankroll_when_no_metrics_json(self, tmp_path):
        """Test default bankroll when metrics.json is absent."""
        bets = pd.DataFrame({"game_date": ["2024-01-15"], "profit": [50.0]})
        bets.to_csv(tmp_path / "bets.csv", index=False)

        df, bankroll = load_data(tmp_path)
        assert bankroll == 10000.0

    def test_raises_if_bets_csv_missing(self, tmp_path):
        """Test error when bets.csv doesn't exist."""
        with pytest.raises(FileNotFoundError, match="Could not find bets.csv"):
            load_data(tmp_path)

    def test_defaults_bankroll_when_metrics_has_no_bankroll_key(self, tmp_path):
        """Test default bankroll when metrics.json lacks the key."""
        bets = pd.DataFrame({"game_date": ["2024-01-15"], "profit": [50.0]})
        bets.to_csv(tmp_path / "bets.csv", index=False)

        metrics = {"betting": {"total_bets": 10}}
        with open(tmp_path / "metrics.json", "w") as f:
            json.dump(metrics, f)

        df, bankroll = load_data(tmp_path)
        assert bankroll == 10000.0


class TestGenerateChart:
    """Tests for generate_chart."""

    def test_creates_html_file(self, tmp_path):
        """Test that generate_chart produces an HTML output file."""
        df = pd.DataFrame(
            {
                "game_date": pd.to_datetime(["2024-01-15", "2024-01-16", "2024-01-17"]),
                "profit": [100.0, -50.0, 75.0],
            }
        )

        output_path = tmp_path / "chart.html"
        generate_chart(df, 10000.0, output_path)

        assert output_path.exists()
        content = output_path.read_text()
        assert "plotly" in content.lower()

    def test_chart_contains_bankroll_data(self, tmp_path):
        """Test chart HTML contains expected data traces."""
        df = pd.DataFrame(
            {
                "game_date": pd.to_datetime(["2024-01-15", "2024-01-16"]),
                "profit": [200.0, -100.0],
            }
        )

        output_path = tmp_path / "chart.html"
        generate_chart(df, 10000.0, output_path)

        content = output_path.read_text()
        assert "Bankroll" in content
        assert "Daily P" in content  # "Daily P&L"

    def test_chart_with_all_positive_days(self, tmp_path):
        """Test chart with all winning days."""
        df = pd.DataFrame(
            {
                "game_date": pd.to_datetime(["2024-01-15", "2024-01-16"]),
                "profit": [100.0, 200.0],
            }
        )

        output_path = tmp_path / "chart.html"
        generate_chart(df, 10000.0, output_path)
        assert output_path.exists()

    def test_chart_with_all_negative_days(self, tmp_path):
        """Test chart with all losing days."""
        df = pd.DataFrame(
            {
                "game_date": pd.to_datetime(["2024-01-15", "2024-01-16"]),
                "profit": [-100.0, -200.0],
            }
        )

        output_path = tmp_path / "chart.html"
        generate_chart(df, 10000.0, output_path)
        assert output_path.exists()

    def test_chart_with_single_day(self, tmp_path):
        """Test chart handles single day of data."""
        df = pd.DataFrame(
            {
                "game_date": pd.to_datetime(["2024-01-15"]),
                "profit": [50.0],
            }
        )

        output_path = tmp_path / "chart.html"
        generate_chart(df, 5000.0, output_path)
        assert output_path.exists()

    def test_chart_title_contains_roi(self, tmp_path):
        """Test chart title shows ROI information."""
        df = pd.DataFrame(
            {
                "game_date": pd.to_datetime(["2024-01-15"]),
                "profit": [1000.0],
            }
        )

        output_path = tmp_path / "chart.html"
        generate_chart(df, 10000.0, output_path)

        content = output_path.read_text()
        assert "ROI" in content


class TestMain:
    """Tests for main() entry point."""

    def test_main_missing_dir(self, capsys, monkeypatch):
        """Test main() handles missing results directory."""
        from src.backtesting.visualize_results import main

        monkeypatch.setattr(
            "sys.argv",
            ["visualize_results.py", "--results-dir", "/nonexistent/path"],
        )
        main()
        captured = capsys.readouterr()
        assert "not found" in captured.out.lower() or "Error" in captured.out
