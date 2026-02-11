"""Per-stat configuration for betting parameters.

Enables different edge thresholds and BL tau values for each stat type
(pts, reb, ast) while maintaining backward compatibility with global values.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class StatConfig:
    """Configuration for a single stat type (pts, reb, ast).

    Attributes:
        stat: The stat identifier (e.g., "pts", "reb", "ast")
        enabled: Whether betting is enabled for this stat
        edge_threshold: Minimum edge to place a bet. None = use global default.
        bl_tau: Black-Litterman tau for this stat. None = no BL blending.
    """

    stat: str
    enabled: bool = True
    edge_threshold: float | None = None
    bl_tau: float | None = None

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "stat": self.stat,
            "enabled": self.enabled,
            "edge_threshold": self.edge_threshold,
            "bl_tau": self.bl_tau,
        }


@dataclass
class StatConfigSet:
    """Container for per-stat configurations with global fallbacks.

    Supports both global defaults and per-stat overrides. When a per-stat
    value is None, the global default is used.

    Example usage:
        # Create with global defaults
        config = StatConfigSet(
            global_edge_threshold=0.05,
            global_bl_tau=0.10,
        )

        # Add per-stat overrides
        config.configs["pts"] = StatConfig("pts", edge_threshold=0.10)
        config.configs["reb"] = StatConfig("reb", edge_threshold=0.07)
        config.configs["ast"] = StatConfig("ast", enabled=False)  # Disable AST

        # Get effective values
        config.get_edge_threshold("pts")  # Returns 0.10 (per-stat)
        config.get_edge_threshold("reb")  # Returns 0.07 (per-stat)
        config.get_bl_tau("pts")          # Returns 0.10 (global fallback)
    """

    global_edge_threshold: float = 0.05
    global_bl_tau: float | None = None
    configs: dict[str, StatConfig] = field(default_factory=dict)

    def get_edge_threshold(self, stat: str) -> float:
        """Get edge threshold for stat, falling back to global default."""
        if stat in self.configs:
            threshold = self.configs[stat].edge_threshold
            if threshold is not None:
                return threshold
        return self.global_edge_threshold

    def get_bl_tau(self, stat: str) -> float | None:
        """Get BL tau for stat. Returns None if BL disabled for this stat.

        Per-stat bl_tau=None means no BL blending for that stat.
        If no per-stat config exists, falls back to global.
        """
        if stat in self.configs:
            # Per-stat config exists - use its bl_tau (even if None)
            return self.configs[stat].bl_tau
        # Fall back to global
        return self.global_bl_tau

    def is_stat_enabled(self, stat: str) -> bool:
        """Check if stat is enabled for betting."""
        if stat in self.configs:
            return self.configs[stat].enabled
        return True  # Default: enabled if not explicitly configured

    def get_enabled_stats(self) -> list[str]:
        """Get list of enabled stats."""
        return [s for s in self.configs if self.is_stat_enabled(s)]

    @classmethod
    def from_cli_args(
        cls,
        edge_values: list[str] | None,
        tau_values: list[str] | None,
        stats: list[str],
    ) -> StatConfigSet:
        """Parse CLI arguments into StatConfigSet.

        Args:
            edge_values: List of edge threshold values. Can be:
                - ["0.05"] for global
                - ["pts=0.10", "reb=0.07"] for per-stat
                - ["0.05", "pts=0.10"] for mixed (global default + override)
            tau_values: List of BL tau values. Same format as edge_values.
                Use "none" to disable BL for a stat.
            stats: List of enabled stats (e.g., ["pts", "reb", "ast"])

        Returns:
            Configured StatConfigSet instance
        """
        instance = cls()

        # Parse edge thresholds
        if edge_values:
            global_edge, per_stat_edge = parse_stat_param(edge_values)
            if global_edge is not None:
                instance.global_edge_threshold = global_edge
            # Apply per-stat overrides
            for stat, val in per_stat_edge.items():
                if stat not in instance.configs:
                    instance.configs[stat] = StatConfig(stat=stat)
                instance.configs[stat].edge_threshold = val

        # Parse BL tau values
        if tau_values:
            global_tau, per_stat_tau = parse_stat_param(tau_values)
            if global_tau is not None:
                instance.global_bl_tau = global_tau
            # Apply per-stat overrides
            for stat, val in per_stat_tau.items():
                if stat not in instance.configs:
                    instance.configs[stat] = StatConfig(stat=stat)
                instance.configs[stat].bl_tau = val

        # Note: We don't auto-create config entries for all stats.
        # Stats without explicit per-stat config will use global defaults.

        return instance

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "global_edge_threshold": self.global_edge_threshold,
            "global_bl_tau": self.global_bl_tau,
            "configs": {k: v.to_dict() for k, v in self.configs.items()},
        }

    def __repr__(self) -> str:
        parts = [f"StatConfigSet(global_edge={self.global_edge_threshold}"]
        if self.global_bl_tau is not None:
            parts.append(f"global_tau={self.global_bl_tau}")
        for stat, cfg in self.configs.items():
            stat_parts = [stat]
            if cfg.edge_threshold is not None:
                stat_parts.append(f"edge={cfg.edge_threshold}")
            if cfg.bl_tau is not None:
                stat_parts.append(f"tau={cfg.bl_tau}")
            if not cfg.enabled:
                stat_parts.append("disabled")
            if len(stat_parts) > 1:
                parts.append(f"{':'.join(stat_parts)}")
        return ", ".join(parts) + ")"


def parse_stat_param(values: list[str]) -> tuple[float | None, dict[str, float | None]]:
    """Parse CLI param that can be global or per-stat.

    Args:
        values: List of values from CLI. Can be:
            - ["0.05"] for global value
            - ["pts=0.10", "reb=0.07"] for per-stat values
            - ["0.05", "pts=0.10"] for mixed

    Returns:
        Tuple of (global_value, per_stat_dict)
        global_value is None if no global was provided.
        per_stat_dict values can be None if "none" was specified.

    Examples:
        >>> parse_stat_param(["0.05"])
        (0.05, {})
        >>> parse_stat_param(["pts=0.10", "reb=0.07"])
        (None, {"pts": 0.10, "reb": 0.07})
        >>> parse_stat_param(["0.05", "pts=0.10"])
        (0.05, {"pts": 0.10})
        >>> parse_stat_param(["pts=none"])
        (None, {"pts": None})
    """
    global_val: float | None = None
    per_stat: dict[str, float | None] = {}

    for v in values:
        v = v.strip()
        if "=" in v:
            # Per-stat format: stat=value
            stat, val_str = v.split("=", 1)
            stat = stat.lower().strip()
            val_str = val_str.strip()

            if val_str.lower() == "none":
                per_stat[stat] = None
            else:
                try:
                    per_stat[stat] = float(val_str)
                except ValueError as e:
                    raise ValueError(
                        f"Invalid value '{v}'. Expected format: stat=number or stat=none"
                    ) from e
        else:
            # Global value
            if v.lower() == "none":
                global_val = None
            else:
                try:
                    global_val = float(v)
                except ValueError as e:
                    raise ValueError(
                        f"Invalid value '{v}'. Expected a number or 'none'."
                    ) from e

    return global_val, per_stat
