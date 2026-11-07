"""Explicit NBA provider aliases verified against canonical IDs and game records.

Keep this small and reviewable. No fuzzy matching; canonical-name drift disables
that override, and conflicting manual mappings remain ambiguous. Evidence is in
 docs/operations/nba-props-t60-runbook.md (November 6 alias repair).
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

VERIFIED_PLAYER_ALIASES: dict[str, tuple[int, str]] = {
    "B.J. Boston Jr": (1630527, "Brandon Boston"),
    "Cameron Thomas": (1630560, "Cam Thomas"),
    "Carlton Carrington": (1642267, "Bub Carrington"),
    "Gregory Jackson": (1641713, "GG Jackson"),
    "Herb Jones": (1630529, "Herbert Jones"),
    "James Huff": (1630643, "Jay Huff"),
    "Lonnie Walker": (1629022, "Lonnie Walker IV"),
    "Marcus Morris": (202694, "Marcus Morris Sr."),
    "Moe Wagner": (1629021, "Moritz Wagner"),
    "Mohamed Bamba": (1628964, "Mo Bamba"),
    "Nicolas Claxton": (1629651, "Nic Claxton"),
    "Ron Holland": (1641842, "Ronald Holland II"),
    "Vincent Williams Jr": (1631246, "Vince Williams Jr."),
}


def add_verified_player_aliases(
    players: Iterable[Mapping[str, Any]], manual: dict[str, set[int]],
) -> None:
    """Add only aliases whose expected canonical ID/name is still present."""
    canonical = {int(row["player_id"]): row["player_name"] for row in players if row.get("player_id") is not None}
    for alias, (player_id, expected_name) in VERIFIED_PLAYER_ALIASES.items():
        if canonical.get(player_id) == expected_name:
            manual.setdefault(alias, set()).add(player_id)
