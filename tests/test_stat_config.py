"""Tests for per-stat configuration."""

import pytest

from src.config.stat_config import StatConfig, StatConfigSet, parse_stat_param


class TestParseStatParam:
    """Tests for parse_stat_param helper function."""

    def test_global_value_only(self):
        """Parse single global value."""
        global_val, per_stat = parse_stat_param(["0.05"])
        assert global_val == 0.05
        assert per_stat == {}

    def test_per_stat_values_only(self):
        """Parse per-stat values without global."""
        global_val, per_stat = parse_stat_param(["pts=0.10", "reb=0.07"])
        assert global_val is None
        assert per_stat == {"pts": 0.10, "reb": 0.07}

    def test_mixed_global_and_per_stat(self):
        """Parse global default with per-stat overrides."""
        global_val, per_stat = parse_stat_param(["0.05", "pts=0.10"])
        assert global_val == 0.05
        assert per_stat == {"pts": 0.10}

    def test_none_value_for_stat(self):
        """Parse 'none' to disable a stat."""
        global_val, per_stat = parse_stat_param(["pts=none"])
        assert global_val is None
        assert per_stat == {"pts": None}

    def test_global_none(self):
        """Parse 'none' as global value."""
        global_val, per_stat = parse_stat_param(["none"])
        assert global_val is None
        assert per_stat == {}

    def test_case_insensitive_stat(self):
        """Stat names are normalized to lowercase."""
        global_val, per_stat = parse_stat_param(["PTS=0.10", "REB=0.07"])
        assert per_stat == {"pts": 0.10, "reb": 0.07}

    def test_invalid_number_raises(self):
        """Invalid number format raises ValueError."""
        with pytest.raises(ValueError, match="Invalid value"):
            parse_stat_param(["abc"])

    def test_invalid_per_stat_format_raises(self):
        """Invalid per-stat format raises ValueError."""
        with pytest.raises(ValueError, match="Invalid value"):
            parse_stat_param(["pts=abc"])


class TestStatConfig:
    """Tests for StatConfig dataclass."""

    def test_defaults(self):
        """Default values are correct."""
        cfg = StatConfig(stat="pts")
        assert cfg.stat == "pts"
        assert cfg.enabled is True
        assert cfg.edge_threshold is None
        assert cfg.bl_tau is None

    def test_custom_values(self):
        """Custom values are stored correctly."""
        cfg = StatConfig(
            stat="reb",
            enabled=False,
            edge_threshold=0.10,
            bl_tau=0.05,
        )
        assert cfg.stat == "reb"
        assert cfg.enabled is False
        assert cfg.edge_threshold == 0.10
        assert cfg.bl_tau == 0.05

    def test_to_dict(self):
        """Serialization to dict works."""
        cfg = StatConfig(stat="ast", edge_threshold=0.15)
        d = cfg.to_dict()
        assert d == {
            "stat": "ast",
            "enabled": True,
            "edge_threshold": 0.15,
            "bl_tau": None,
        }


class TestStatConfigSet:
    """Tests for StatConfigSet container."""

    def test_defaults(self):
        """Default values are correct."""
        cfg_set = StatConfigSet()
        assert cfg_set.global_edge_threshold == 0.05
        assert cfg_set.global_bl_tau is None
        assert cfg_set.configs == {}

    def test_get_edge_threshold_global_fallback(self):
        """Edge threshold falls back to global when no per-stat override."""
        cfg_set = StatConfigSet(global_edge_threshold=0.07)
        assert cfg_set.get_edge_threshold("pts") == 0.07
        assert cfg_set.get_edge_threshold("unknown") == 0.07

    def test_get_edge_threshold_per_stat(self):
        """Edge threshold uses per-stat override when available."""
        cfg_set = StatConfigSet(global_edge_threshold=0.05)
        cfg_set.configs["pts"] = StatConfig(stat="pts", edge_threshold=0.10)
        cfg_set.configs["reb"] = StatConfig(stat="reb")  # No override

        assert cfg_set.get_edge_threshold("pts") == 0.10
        assert cfg_set.get_edge_threshold("reb") == 0.05  # Falls back
        assert cfg_set.get_edge_threshold("ast") == 0.05  # Falls back

    def test_get_bl_tau_global_fallback(self):
        """BL tau falls back to global when no per-stat config."""
        cfg_set = StatConfigSet(global_bl_tau=0.10)
        assert cfg_set.get_bl_tau("pts") == 0.10
        assert cfg_set.get_bl_tau("unknown") == 0.10

    def test_get_bl_tau_per_stat(self):
        """BL tau uses per-stat value when configured."""
        cfg_set = StatConfigSet(global_bl_tau=0.10)
        cfg_set.configs["pts"] = StatConfig(stat="pts", bl_tau=0.05)
        cfg_set.configs["reb"] = StatConfig(stat="reb", bl_tau=None)  # Disabled

        assert cfg_set.get_bl_tau("pts") == 0.05
        assert cfg_set.get_bl_tau("reb") is None  # Per-stat None takes precedence
        assert cfg_set.get_bl_tau("ast") == 0.10  # Falls back to global

    def test_is_stat_enabled_default(self):
        """Stats are enabled by default."""
        cfg_set = StatConfigSet()
        assert cfg_set.is_stat_enabled("pts") is True
        assert cfg_set.is_stat_enabled("unknown") is True

    def test_is_stat_enabled_disabled(self):
        """Disabled stats return False."""
        cfg_set = StatConfigSet()
        cfg_set.configs["ast"] = StatConfig(stat="ast", enabled=False)

        assert cfg_set.is_stat_enabled("pts") is True
        assert cfg_set.is_stat_enabled("ast") is False

    def test_get_enabled_stats(self):
        """Returns list of enabled stats."""
        cfg_set = StatConfigSet()
        cfg_set.configs["pts"] = StatConfig(stat="pts", enabled=True)
        cfg_set.configs["reb"] = StatConfig(stat="reb", enabled=False)
        cfg_set.configs["ast"] = StatConfig(stat="ast", enabled=True)

        enabled = cfg_set.get_enabled_stats()
        assert "pts" in enabled
        assert "ast" in enabled
        assert "reb" not in enabled


class TestStatConfigSetFromCliArgs:
    """Tests for from_cli_args factory method."""

    def test_global_edge_only(self):
        """Parse global edge threshold."""
        cfg = StatConfigSet.from_cli_args(
            edge_values=["0.07"],
            tau_values=None,
            stats=["pts", "reb"],
        )
        assert cfg.global_edge_threshold == 0.07
        assert cfg.get_edge_threshold("pts") == 0.07
        assert cfg.get_edge_threshold("reb") == 0.07

    def test_per_stat_edge(self):
        """Parse per-stat edge thresholds."""
        cfg = StatConfigSet.from_cli_args(
            edge_values=["pts=0.10", "reb=0.07"],
            tau_values=None,
            stats=["pts", "reb", "ast"],
        )
        assert cfg.get_edge_threshold("pts") == 0.10
        assert cfg.get_edge_threshold("reb") == 0.07
        assert cfg.get_edge_threshold("ast") == 0.05  # Default

    def test_mixed_edge(self):
        """Parse global + per-stat edge thresholds."""
        cfg = StatConfigSet.from_cli_args(
            edge_values=["0.06", "pts=0.10"],
            tau_values=None,
            stats=["pts", "reb"],
        )
        assert cfg.global_edge_threshold == 0.06
        assert cfg.get_edge_threshold("pts") == 0.10
        assert cfg.get_edge_threshold("reb") == 0.06

    def test_global_tau(self):
        """Parse global BL tau."""
        cfg = StatConfigSet.from_cli_args(
            edge_values=["0.05"],
            tau_values=["0.10"],
            stats=["pts"],
        )
        assert cfg.global_bl_tau == 0.10
        assert cfg.get_bl_tau("pts") == 0.10

    def test_per_stat_tau(self):
        """Parse per-stat BL tau values."""
        cfg = StatConfigSet.from_cli_args(
            edge_values=["0.05"],
            tau_values=["pts=0.05", "reb=0.10", "ast=none"],
            stats=["pts", "reb", "ast"],
        )
        assert cfg.get_bl_tau("pts") == 0.05
        assert cfg.get_bl_tau("reb") == 0.10
        assert cfg.get_bl_tau("ast") is None

    def test_only_overrides_create_config_entries(self):
        """Only stats with explicit overrides get config entries."""
        cfg = StatConfigSet.from_cli_args(
            edge_values=["pts=0.10"],
            tau_values=["reb=0.05"],
            stats=["pts", "reb", "ast"],
        )
        # Only pts and reb have overrides
        assert "pts" in cfg.configs
        assert "reb" in cfg.configs
        # ast has no override, uses global fallback
        assert "ast" not in cfg.configs
        assert cfg.get_edge_threshold("ast") == 0.05  # Global default

    def test_no_values_uses_defaults(self):
        """None values use defaults."""
        cfg = StatConfigSet.from_cli_args(
            edge_values=None,
            tau_values=None,
            stats=["pts"],
        )
        assert cfg.global_edge_threshold == 0.05
        assert cfg.global_bl_tau is None


class TestStatConfigSetToDict:
    """Tests for to_dict serialization."""

    def test_serialization(self):
        """Full serialization works correctly."""
        cfg_set = StatConfigSet(
            global_edge_threshold=0.06,
            global_bl_tau=0.10,
        )
        cfg_set.configs["pts"] = StatConfig(
            stat="pts",
            edge_threshold=0.10,
            bl_tau=0.05,
        )

        d = cfg_set.to_dict()
        assert d["global_edge_threshold"] == 0.06
        assert d["global_bl_tau"] == 0.10
        assert "pts" in d["configs"]
        assert d["configs"]["pts"]["edge_threshold"] == 0.10
        assert d["configs"]["pts"]["bl_tau"] == 0.05


class TestStatConfigSetRepr:
    """Tests for __repr__ method."""

    def test_basic_repr(self):
        """Basic repr shows global edge."""
        cfg_set = StatConfigSet(global_edge_threshold=0.07)
        r = repr(cfg_set)
        assert "global_edge=0.07" in r

    def test_repr_with_tau(self):
        """Repr includes global tau when set."""
        cfg_set = StatConfigSet(global_bl_tau=0.10)
        r = repr(cfg_set)
        assert "global_tau=0.1" in r

    def test_repr_with_per_stat(self):
        """Repr includes per-stat overrides."""
        cfg_set = StatConfigSet()
        cfg_set.configs["pts"] = StatConfig(
            stat="pts",
            edge_threshold=0.10,
            enabled=False,
        )
        r = repr(cfg_set)
        assert "pts" in r
        assert "edge=0.1" in r
        assert "disabled" in r
