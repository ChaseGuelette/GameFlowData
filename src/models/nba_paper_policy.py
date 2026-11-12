"""Shared NBA paper policy, loaded from the promoted model's policy sidecar."""

from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pandas as pd

ARTIFACTS_DIR = Path(__file__).resolve().parent / "artifacts"
POLICY_PATH = ARTIFACTS_DIR / "production.policy.json"


@dataclass(frozen=True)
class NBAPaperPolicy:
    stats: tuple[str, ...] = ("pts", "reb", "ast")
    edge_threshold: float = 0.07
    bl_tau: float = 0.75
    bl_z_max: float = 0.5
    bl_max_weight: float = 0.75
    min_odds: int = -200
    max_odds: int = 200
    fixed_stake: float = 10.0
    n_samples: int = 5_000
    random_state: int = 42

    def __post_init__(self):
        object.__setattr__(self, "stats", tuple(self.stats))
        if self.stats != ("pts", "reb", "ast"):
            raise ValueError("This paper policy supports PTS/REB/AST only")
        for name in ("edge_threshold", "bl_tau", "bl_z_max", "bl_max_weight"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int | float) or not math.isfinite(value):
                raise ValueError(f"Invalid policy {name}")
        if not (0 <= self.edge_threshold < 1 and self.bl_tau > 0 and self.bl_z_max > 0
                and 0 < self.bl_max_weight <= 1):
            raise ValueError("Invalid BL policy bounds")
        if (self.min_odds, self.max_odds, self.fixed_stake, self.n_samples, self.random_state) != (-200, 200, 10.0, 5000, 42):
            raise ValueError("Paper odds, flat stake and sampling controls remain frozen")

    def bl_config(self) -> dict[str, float]:
        return {"tau": self.bl_tau, "z_max": self.bl_z_max,
                "max_weight": self.bl_max_weight, "edge_threshold": self.edge_threshold}

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def read_promoted_policy(path: Path = POLICY_PATH) -> dict[str, Any] | None:
    if not path.exists():
        return None
    record = json.loads(path.read_text(encoding="utf-8"))
    if (record.get("schema_version") != 1 or record.get("direction_policy") != "best_then_under"
            or not isinstance(record.get("model_identity"), str) or not record["model_identity"]
            or not isinstance(record.get("evaluation_run_id"), str) or not record["evaluation_run_id"]):
        raise ValueError(f"Invalid promoted policy: {path}")
    NBAPaperPolicy(**record["settings"])
    return record


PROMOTED_POLICY = read_promoted_policy()
NBA_PAPER_POLICY = NBAPaperPolicy(**PROMOTED_POLICY["settings"]) if PROMOTED_POLICY else NBAPaperPolicy()
EVALUATION_RUN_ID = (PROMOTED_POLICY["evaluation_run_id"] if PROMOTED_POLICY
                     else "nba-paper-baseline-under-v1-20261112")


def selected_side(row: Any) -> tuple[str, float] | None:
    """Return the maximum-BL-edge side for one row; ties select OVER."""
    try:
        over_edge = float(row["bl_over_edge"])
        under_edge = float(row["bl_under_edge"])
    except (KeyError, TypeError, ValueError):
        return None
    if not math.isfinite(over_edge) or not math.isfinite(under_edge):
        return None
    return ("over", over_edge) if over_edge >= under_edge else ("under", under_edge)


def is_placeable_selection(row: Any, *, direction: str | None = None, edge: float | None = None) -> bool:
    """Apply final stat, side, edge, and selected-side odds gates."""
    selected = selected_side(row)
    if selected is None:
        return False
    selected_direction, selected_edge = selected
    direction = selected_direction if direction is None else direction
    edge = selected_edge if edge is None else edge
    if direction != selected_direction or edge != selected_edge:
        return False
    if row.get("stat") not in NBA_PAPER_POLICY.stats or direction != "under":
        return False
    try:
        odds = float(row[f"{direction}_odds"])
        edge_value = float(edge)
    except (KeyError, TypeError, ValueError):
        return False
    return (math.isfinite(odds) and math.isfinite(edge_value)
            and NBA_PAPER_POLICY.min_odds <= odds <= NBA_PAPER_POLICY.max_odds
            and edge_value >= NBA_PAPER_POLICY.edge_threshold)


def apply_recommendation_policy(frame: pd.DataFrame) -> pd.DataFrame:
    """Choose one best base-stat opportunity per player/game, then reject OVER."""
    frame["is_recommended"] = False
    frame["sanity_flag"] = None
    if frame.empty:
        return frame
    candidates = []
    for idx, row in frame.iterrows():
        if row.get("stat") not in NBA_PAPER_POLICY.stats:
            continue
        selected = selected_side(row)
        if selected is not None:
            candidates.append((row.get("player_id"), row.get("game_id"), idx, selected[1]))
    if not candidates:
        return frame
    ranked = pd.DataFrame(candidates, columns=["player_id", "game_id", "index", "edge"])
    for position in ranked.groupby(["player_id", "game_id"], sort=False)["edge"].idxmax():
        idx = ranked.loc[position, "index"]
        if is_placeable_selection(frame.loc[idx]):
            frame.at[idx, "is_recommended"] = True
    return frame
