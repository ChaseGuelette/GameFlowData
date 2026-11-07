"""Shared exact-game NBA settlement for automatic and user ledgers.

Unknown results are pending, never DNP. SQL errors deliberately reach callers.
"""

import json
import math
from collections import Counter
from datetime import date
from typing import Any

import pandas as pd
from sqlalchemy import text

COMPONENTS = {
    "pts": ("pts",), "reb": ("reb",), "ast": ("ast",),
    "pra": ("pts", "reb", "ast"), "pr": ("pts", "reb"),
    "pa": ("pts", "ast"), "ra": ("reb", "ast"),
}


def _number(value):
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (ValueError, TypeError):
        return None


def _game_for_bet(conn, bet):
    """Never substitute a date match for a broken explicit prediction identity."""
    context = bet.get("bet_context") or {}
    stored = bet.get("game_id") or context.get("game_id")
    if bet.get("game_id") and bet.get("entry_snapshot"):
        # New automatic entries freeze identity independently of mutable/deleted
        # prediction rows. Manual/legacy rows still validate their prediction link.
        if not isinstance(stored, str) or len(stored) != 10 or not stored.isdigit():
            return None, "invalid_game_id"
        return stored, None
    prediction_id = bet.get("prediction_id")
    if prediction_id is not None:
        row = conn.execute(text("""
            SELECT game_id FROM daily_predictions
            WHERE id = :prediction_id AND player_id = :player_id
              AND prediction_date = :game_date AND stat = :stat_type
        """), bet).fetchone()
        if row is None or (stored is not None and stored != row[0]):
            return None, "prediction_identity_conflict"
        stored = row[0]
    if stored is not None:
        if not isinstance(stored, str) or len(stored) != 10 or not stored.isdigit():
            return None, "invalid_game_id"
        return stored, None
    rows = conn.execute(text("""
        SELECT game_id FROM player_game_stats
        WHERE player_id = :player_id AND game_date = :game_date
        ORDER BY game_id LIMIT 2
    """), bet).fetchall()
    if len(rows) != 1:
        return None, "missing_or_ambiguous_fixture"
    return rows[0][0], None


def _outcome(conn, bet):
    if bet.get("stat_type") not in COMPONENTS:
        return None, "unsupported_stat"
    if bet.get("player_id") is None:
        return None, "unlinked_player"
    game_id, reason = _game_for_bet(conn, bet)
    if reason:
        return None, reason
    # Two reciprocal final team results are required, not any row on the date.
    # Schedule final status is additional corroboration when present; conflicting
    # schedule status is never ignored. Historical stats can predate schedule storage.
    rows = conn.execute(text("""
        SELECT p.pts, p.reb, p.ast, p.min, p.did_not_play, p.comment,
               t.team_wl, t.team_pts, t.team_min,
               o.team_wl AS opp_wl, o.team_pts AS opp_pts, o.team_min AS opp_min,
               s.game_status, s.status_text, s.postponed_status,
               s.observed_at AS schedule_observed_at, s.commence_time_utc,
               s.game_id AS schedule_game_id,
               (s.home_team_id IN (t.team_id, o.team_id)
                AND s.away_team_id IN (t.team_id, o.team_id)
                AND s.game_date = t.game_date) AS schedule_matches
        FROM player_game_stats p
        JOIN team_game_stats t ON t.game_id = p.game_id AND t.team_id = p.team_id
        JOIN team_game_stats o ON o.game_id = t.game_id AND o.team_id = t.opponent_id
          AND o.opponent_id = t.team_id AND o.team_id <> t.team_id
        LEFT JOIN nba_game_schedule s ON s.game_id = p.game_id
        WHERE p.player_id = :player_id AND p.game_id = :game_id
          AND p.game_date = :game_date AND t.game_date = :game_date
          AND o.game_date = :game_date
        LIMIT 2
    """), {**bet, "game_id": game_id}).mappings().all()
    if len(rows) != 1:
        return None, "missing_or_partial_game"
    row = rows[0]
    pts, opp_pts = _number(row["team_pts"]), _number(row["opp_pts"])
    mins, opp_mins = _number(row["team_min"]), _number(row["opp_min"])
    if (pts is None or opp_pts is None or pts == opp_pts
            or mins is None or opp_mins is None or mins < 240 or opp_mins < 240
            or row["team_wl"] != ("W" if pts > opp_pts else "L")
            or row["opp_wl"] != ("L" if pts > opp_pts else "W")):
        return None, "incomplete_final_team_result"
    if row["schedule_game_id"] is not None:
        status = (row["status_text"] or "").lower()
        # The schedule job may still hold yesterday's pregame snapshot at the
        # next-morning resolver. CDN ingestion explicitly checks final status;
        # legacy LeagueGameFinder supplies W/L. Two complete reciprocal results
        # supersede a pregame snapshot, not an observed live/cancelled fixture.
        pregame_snapshot = (row["game_status"] == 1
                            and row.get("schedule_observed_at") is not None
                            and row.get("commence_time_utc") is not None
                            and row["schedule_observed_at"] < row["commence_time_utc"])
        if ((row["game_status"] != 3 and not pregame_snapshot) or not row["schedule_matches"]
                or row["postponed_status"] not in (None, "", "N")
                or "postpon" in status or "cancel" in status):
            return None, "schedule_not_final_or_conflicting"
    # Some older ingestion inferred did_not_play from rounded zero minutes.
    # Require an explicit nonparticipation comment as well as the flag.
    comment = (row["comment"] or "").strip().lower()
    explicit_dnp = comment.startswith(("dnp", "dnd", "nwt", "inactive", "did not play", "did not dress"))
    minutes = _number(row["min"])
    if row["did_not_play"] and explicit_dnp and (minutes is None or minutes == 0):
        if any(_number(row[c]) not in (None, 0) for c in ("pts", "reb", "ast")):
            return None, "participation_unknown_or_conflicting"
        return ("cancelled", None, 0.0), None
    if row["did_not_play"] or minutes is None or minutes <= 0:
        return None, "participation_unknown_or_conflicting"
    values = [_number(row[c]) for c in COMPONENTS[bet["stat_type"]]]
    if any(v is None for v in values):
        return None, "missing_stat_component"
    actual = sum(v for v in values if v is not None)
    line, odds, stake = (_number(bet.get(k)) for k in ("line", "odds_at_bet", "stake"))
    if (line is None or odds is None or odds == 0 or stake is None or stake < 0
            or bet.get("bet_direction") not in ("over", "under")):
        return None, "invalid_bet_terms"
    side = "over" if actual > line else "under"
    status = "push" if actual == line else "won" if side == bet["bet_direction"] else "lost"
    pnl = stake * (odds / 100 if odds > 0 else 100 / abs(odds)) if status == "won" else -stake if status == "lost" else 0
    return (status, actual, round(pnl, 2)), None


def resolve_date(engine, table, game_date, *, paper_only=False, dry_run=False):
    if table not in ("paper_bets", "user_bets"):
        raise ValueError("Unsupported settlement ledger")
    scope = "AND b.is_paper_trade = true" if paper_only and table == "user_bets" else ""
    result: dict[str, Any] = dict.fromkeys(("resolved", "won", "lost", "push", "cancelled", "pending", "blocked", "would_resolve"), 0)
    reasons = Counter()
    with engine.begin() as conn:
        bets = pd.read_sql(text(f"""
            SELECT to_jsonb(b) AS captured FROM {table} b
            WHERE b.game_date = :game_date AND b.status = 'pending'
              AND b.stat_type = ANY(:stats) {scope}
            ORDER BY b.id
        """), conn, params={"game_date": game_date, "stats": list(COMPONENTS)})
        for record in bets.to_dict("records"):
            bet = record.get("captured")
            if not isinstance(bet, dict):
                raise ValueError("Settlement reader requires captured ledger identity")
            outcome, reason = _outcome(conn, bet)
            if reason:
                result["pending"] += 1
                result["blocked"] += 1
                reasons[reason] += 1
                continue
            if dry_run:
                result["would_resolve"] += 1
                continue
            status, actual, pnl = outcome
            changed = conn.execute(text(f"""
                UPDATE {table} AS b SET status = :status, actual_value = :actual,
                    pnl = :pnl, resolved_at = NOW()
                WHERE b.id = :id AND b.status = 'pending'
                  AND to_jsonb(b) = CAST(:captured AS jsonb) {scope}
                RETURNING b.id
            """), {"id": bet["id"], "captured": json.dumps(bet),
                   "status": status, "actual": actual, "pnl": pnl}).fetchone()
            if changed is not None:
                result["resolved"] += 1
                result[status] += 1
            else:
                result["pending"] += 1
                reasons["concurrent_change"] += 1
    result["reasons"] = dict(reasons)
    return result


def resolve_pending(engine, table, resolve_one, *, exclude_today=True, paper_only=False):
    if table not in ("paper_bets", "user_bets"):
        raise ValueError("Unsupported settlement ledger")
    scope = "AND is_paper_trade = true" if paper_only and table == "user_bets" else ""
    with engine.connect() as conn:
        dates = conn.execute(text(f"""
            SELECT DISTINCT game_date FROM {table}
            WHERE status = 'pending' AND stat_type = ANY(:stats) {scope}
            ORDER BY game_date
        """), {"stats": list(COMPONENTS)}).fetchall()
    counts = ("resolved", "won", "lost", "push", "cancelled", "pending", "blocked", "would_resolve")
    summary = {"dates_processed": 0, "dates_skipped": 0, "by_date": {}, **{f"total_{k}": 0 for k in counts}}
    for (day,) in dates:
        if exclude_today and day >= date.today():
            summary["dates_skipped"] += 1
            summary["by_date"][str(day)] = {"skipped": True, "reason": "today_or_future"}
            continue
        result = resolve_one(day)
        summary["dates_processed"] += 1
        summary["by_date"][str(day)] = result
        for key in counts:
            summary[f"total_{key}"] += result.get(key, 0)
    return summary
