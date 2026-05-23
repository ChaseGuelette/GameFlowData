"""Quote-clean decision-time policy helpers for MLB backtest sweeps.

These functions are intentionally lightweight and side-effect free. They own the
historical decision timestamp policy used by quote-clean replay, while raw line
fetching and sweep orchestration remain in their existing modules.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from datetime import time as datetime_time
from zoneinfo import ZoneInfo
from typing import cast

import pandas as pd

ET_ZONE = ZoneInfo("America/New_York")


def build_fixed_cutoff_ts(game_date: date, cutoff_time_et: str) -> datetime:
    """Build a timezone-aware ET cutoff timestamp for quote-clean line selection.

    The production inference path effectively sees only rows already present in
    `mlb_raw_player_props` when the job runs. For historical quote-clean replay,
    this helper turns a fixed ET inference time (HH:MM) into a timestamp cutoff
    that can be applied to `snapshot_time`.
    """
    try:
        hour_s, minute_s = cutoff_time_et.split(":", 1)
        cutoff_t = datetime_time(hour=int(hour_s), minute=int(minute_s))
    except Exception as exc:
        raise ValueError(
            f"Invalid --quote-cutoff-time-et={cutoff_time_et!r}; expected HH:MM"
        ) from exc

    return datetime.combine(game_date, cutoff_t, tzinfo=ET_ZONE)


def build_slate_decision_ts(commence_ts: datetime, fallback_relative_minutes: int = 60) -> datetime:
    """Return slate-policy decision time with game-relative fallback for early starts."""
    commence_et = cast(datetime, pd.Timestamp(commence_ts).to_pydatetime()).astimezone(ET_ZONE)
    game_day = commence_et.date()
    if commence_et.time() < datetime_time(15, 0):
        candidate = datetime.combine(game_day, datetime_time(9, 30), tzinfo=ET_ZONE)
    elif commence_et.time() < datetime_time(19, 0):
        candidate = datetime.combine(game_day, datetime_time(13, 30), tzinfo=ET_ZONE)
    else:
        candidate = datetime.combine(game_day, datetime_time(17, 30), tzinfo=ET_ZONE)
    if candidate >= commence_et:
        candidate = commence_et - timedelta(minutes=fallback_relative_minutes)
    return candidate


def decision_time_for_game(
    game: dict,
    *,
    policy: str,
    fixed_cutoff_ts: datetime | None,
    relative_minutes: int,
) -> datetime | None:
    """Return the quote-clean line decision timestamp for one game.

    This preserves the original sweep-runner behavior, including falling back to
    the fixed cutoff for unknown policies. Hard validation belongs in a later
    promotion/config contract slice because it changes behavior.
    """
    commence = game.get("game_time_utc")
    commence_ts = pd.to_datetime(commence, utc=True, errors="coerce") if commence is not None else pd.NaT
    if policy == "fixed_et":
        return fixed_cutoff_ts
    if policy == "skip_early_fixed_et":
        if pd.notna(commence_ts) and fixed_cutoff_ts is not None and fixed_cutoff_ts >= commence_ts.to_pydatetime():
            return None
        return fixed_cutoff_ts
    if pd.isna(commence_ts):
        return fixed_cutoff_ts
    commence_dt = cast(datetime, commence_ts.to_pydatetime())
    if policy == "relative_to_commence":
        return commence_dt - timedelta(minutes=relative_minutes)
    if policy == "slate_or_tminus":
        return build_slate_decision_ts(commence_dt, fallback_relative_minutes=relative_minutes)
    return fixed_cutoff_ts
