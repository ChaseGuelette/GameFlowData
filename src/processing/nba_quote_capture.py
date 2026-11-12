"""Immutable live quote states and compact, authoritative event confirmations.

No environment loading or I/O on import. Callers own provider access; this module
validates the complete response before acquiring an event transaction lock.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation

from psycopg2.extras import Json, execute_values
from sqlalchemy import text

LIVE_MARKETS = (
    "player_points", "player_rebounds", "player_assists", "player_threes",
    "player_points_rebounds_assists", "player_points_rebounds",
    "player_points_assists", "player_rebounds_assists",
)
EXCLUDED_BOOKS = ("prizepicks", "underdog", "pick6", "betr_us_dfs", "fliff")
LIVE_REGIONS = "us,us2,us_ex"
CAPTURE_POLICY = "live_change_v1"
SCOPE = {"markets": sorted(LIVE_MARKETS), "regions": LIVE_REGIONS, "excluded_books": list(EXCLUDED_BOOKS)}
QUOTE_COLUMNS = (
    "api_game_id", "api_player_name", "bookmaker", "market_key", "outcome_label", "line",
    "odds_american", "commence_time", "home_team", "away_team", "snapshot_time",
    "market_last_update", "bookmaker_last_update", "bookmaker_name",
    "requested_snapshot_time", "source_snapshot_time", "source_kind", "observed_at",
)


class CaptureError(ValueError):
    """Unknown/invalid capture: must not refresh or withdraw durable state."""


def clock(value):
    try:
        result = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        raise CaptureError("Invalid capture/provider clock") from None
    if result.tzinfo is None:
        raise CaptureError("Naive capture/provider clock")
    return result.astimezone(UTC)


def numeric(value):
    if isinstance(value, bool) or value is None:
        raise CaptureError("Missing/invalid quote numeric value")
    try:
        number = Decimal(str(value))
    except InvalidOperation:
        raise CaptureError("Invalid quote numeric value") from None
    if not number.is_finite():
        raise CaptureError("Nonfinite quote numeric value")
    return number


def normalize_response(data, metadata, *, expected_event_id, markets=LIVE_MARKETS):
    """Return deterministic full groups; an empty list is an explicit withdrawal."""
    if set(markets) != set(LIVE_MARKETS) or metadata.get("source_kind") != "live":
        raise CaptureError("Change-only capture requires the reviewed live market scope")
    if not isinstance(data, dict) or data.get("id") != expected_event_id or not expected_event_id:
        raise CaptureError("Response event identity mismatch")
    if not isinstance(data.get("bookmakers"), list):
        raise CaptureError("Missing/ambiguous response bookmaker scope")
    received = clock(metadata.get("observed_at"))
    requested = clock(metadata.get("requested_snapshot_time"))
    if clock(metadata.get("source_snapshot_time")) != received or requested > received:
        raise CaptureError("Response clocks do not describe an actual live receipt")
    commence = clock(data.get("commence_time"))
    if received >= commence:
        raise CaptureError("Response is not pregame")
    if not all(isinstance(data.get(k), str) and data[k] for k in ("home_team", "away_team")) or data["home_team"] == data["away_team"]:
        raise CaptureError("Invalid response teams")
    groups = {}
    seen_books = set()
    for book in data["bookmakers"]:
        if not isinstance(book, dict) or not isinstance(book.get("key"), str) or not book["key"]:
            raise CaptureError("Malformed bookmaker")
        key = book["key"]
        if key in EXCLUDED_BOOKS:
            continue
        if key in seen_books or not isinstance(book.get("markets"), list):
            raise CaptureError("Duplicate/malformed bookmaker scope")
        seen_books.add(key)
        book_clock = clock(book["last_update"]) if book.get("last_update") else None
        seen_markets = set()
        for market in book["markets"]:
            if not isinstance(market, dict) or market.get("key") not in LIVE_MARKETS:
                raise CaptureError("Unexpected/malformed market scope")
            market_key = market["key"]
            if market_key in seen_markets or not isinstance(market.get("outcomes"), list):
                raise CaptureError("Duplicate/malformed market")
            seen_markets.add(market_key)
            market_clock = clock(market["last_update"]) if market.get("last_update") else None
            if any(c and (c > received or c >= commence) for c in (book_clock, market_clock)):
                raise CaptureError("Provider clock exceeds actual receipt/pregame boundary")
            for outcome in market["outcomes"]:
                if not isinstance(outcome, dict) or not isinstance(outcome.get("description"), str) or not outcome["description"] or outcome.get("name") not in ("Over", "Under"):
                    raise CaptureError("Malformed quote outcome")
                line, price = numeric(outcome.get("point")), numeric(outcome.get("price"))
                if price != price.to_integral_value() or price == 0:
                    raise CaptureError("Invalid American price")
                group_key = (outcome["description"], key, market_key)
                group = groups.setdefault(group_key, {
                    "key": list(group_key), "state": [], "rows": [],
                    "market_last_update": market_clock.isoformat() if market_clock else None,
                    "bookmaker_last_update": book_clock.isoformat() if book_clock else None,
                })
                # Decimal equality, not binary floats or formatted provider strings.
                state = [format(line.normalize(), "f"), outcome["name"], int(price)]
                if any(s[:2] == state[:2] for s in group["state"]):
                    raise CaptureError("Duplicate/conflicting quote side")
                group["state"].append(state)
                group["rows"].append((
                    expected_event_id, outcome["description"], key, market_key, outcome["name"], line,
                    int(price), commence, data["home_team"], data["away_team"], received,
                    market_clock, book_clock, book.get("title"), requested, received, "live", received,
                ))
    result = []
    for key in sorted(groups):
        group = groups[key]
        ordered = sorted(zip(group["state"], group["rows"], strict=True), key=lambda item: item[0])
        group["state"], group["rows"] = map(list, zip(*ordered, strict=True))
        sides = defaultdict(set)
        for line, side, _price in group["state"]:
            sides[line].add(side)
        group["complete"] = all(s == {"Over", "Under"} for s in sides.values())
        result.append(group)
    if len(result) > 20000:
        raise CaptureError("Capture group safety cap exceeded")
    return result


def plan_capture(groups, previous):
    """Compare only the latest durable membership, never all historical states."""
    old = {tuple(g["key"]): g for g in previous}
    metrics = dict(parsed=sum(len(g["state"]) for g in groups), changed=0, unchanged=0,
                   incomplete=sum(not g["complete"] for g in groups),
                   withdrawn=len(set(old) - {tuple(g["key"]) for g in groups}), inserted=0, receipts=0)
    for group in groups:
        prior = old.get(tuple(group["key"]))
        group["reuse_ids"] = prior["ids"] if prior and prior["state"] == group["state"] else None
        metrics["unchanged" if group["reuse_ids"] is not None else "changed"] += 1
    return metrics


def persist_capture(engine, data, metadata, *, expected_event_id, markets=LIVE_MARKETS):
    groups = normalize_response(data, metadata, expected_event_id=expected_event_id, markets=markets)
    received, requested, commence = clock(metadata["observed_at"]), clock(metadata["requested_snapshot_time"]), clock(data["commence_time"])
    conn = engine.raw_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout = '20s'")
            cur.execute("SET LOCAL lock_timeout = '2s'")
            cur.execute("SELECT to_regclass('public.nba_props_live_captures'), to_regclass('public.nba_player_props_current')")
            if not all(cur.fetchone()):
                raise CaptureError("Change-only producer requires migration 20261112_nba_change_only_capture.sql and consumer rollout first")
            cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", ("nba-live:" + expected_event_id,))
            cur.execute("""SELECT received_at, groups, scope, commence_time, home_team, away_team
                FROM public.nba_props_live_captures WHERE api_game_id=%s
                ORDER BY received_at DESC, capture_id DESC LIMIT 1""", (expected_event_id,))
            previous = cur.fetchone()
            if previous and (received <= previous[0] or previous[2] != SCOPE or previous[3:] != (commence, data["home_team"], data["away_team"])):
                raise CaptureError("Out-of-order response or capture scope/event identity changed")
            metrics = plan_capture(groups, previous[1] if previous else [])
            reused_ids = [i for g in groups if g["reuse_ids"] is not None for i in g["reuse_ids"]]
            if reused_ids:
                cur.execute("SELECT count(*) FROM public.nba_player_props_inference WHERE staging_id = ANY(%s)", (reused_ids,))
                if cur.fetchone()[0] != len(reused_ids):
                    raise CaptureError("Prior receipt references pruned quotes; retention coordination required")
            new_rows = [row for g in groups if g["reuse_ids"] is None for row in g["rows"]]
            new_ids = defaultdict(list)
            if new_rows:
                # One paged batch, not a round trip per player/book/market.
                # No ON CONFLICT: same-clock corrections fail and roll back.
                inserted = execute_values(cur,
                    f"INSERT INTO public.nba_player_props_inference ({', '.join(QUOTE_COLUMNS)}) VALUES %s "
                    "RETURNING staging_id, api_player_name, bookmaker, market_key",
                    new_rows, page_size=1000, fetch=True)
                metrics["inserted"] = len(inserted)
                for staging_id, player, book, market in inserted:
                    new_ids[(player, book, market)].append(staging_id)
            membership = []
            for group in groups:
                ids = group["reuse_ids"]
                if ids is None:
                    ids = new_ids[tuple(group["key"])]
                    if len(ids) != len(group["rows"]):
                        raise CaptureError("Changed group insert was incomplete")
                membership.append({**{k: group[k] for k in ("key", "state", "complete", "market_last_update", "bookmaker_last_update")}, "ids": ids})
            cur.execute("""INSERT INTO public.nba_props_live_captures
                (api_game_id, commence_time, home_team, away_team, requested_at, received_at, source_kind, policy_version, scope, groups)
                VALUES (%s,%s,%s,%s,%s,%s,'live',%s,%s,%s) RETURNING capture_id""",
                (expected_event_id, commence, data["home_team"], data["away_team"], requested, received,
                 CAPTURE_POLICY, Json(SCOPE), Json(membership)))
            if cur.fetchone() is None:
                raise CaptureError("Capture receipt was not inserted")
            metrics["receipts"] = 1
        conn.commit()
        return metrics
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def has_capture_surface(conn):
    """Migration compatibility check, not an empty-result fallback."""
    return bool(conn.execute(text("SELECT to_regclass('public.nba_player_props_current')")).scalar())


def current_lines_sql(*, capture_surface):
    """Shared coherent pair query used by prediction and edge-refresh readers."""
    relation = "nba_player_props_current" if capture_surface else "nba_player_props_inference"
    confirmation = "confirmed_at" if capture_surface else "observed_at"
    capture = "CASE WHEN capture_id IS NOT NULL THEN jsonb_build_object('capture_id', capture_id, 'confirmed_at', confirmed_at, 'market_last_update', confirmation_market_last_update, 'bookmaker_last_update', confirmation_bookmaker_last_update) ELSE NULL::jsonb END" if capture_surface else "NULL::jsonb"
    original = "to_jsonb(q) - ARRAY['capture_id','confirmed_at','confirmation_market_last_update','confirmation_bookmaker_last_update']" if capture_surface else "to_jsonb(q)"
    return f"""
        WITH eligible AS (
            SELECT q.*, {original} AS quote_row, {capture} AS capture_metadata,
                {confirmation} AS confirmation_time
            FROM {relation} q
            WHERE game_id IN :game_ids AND market_key IN :markets AND player_id IS NOT NULL
              AND bookmaker NOT IN ('prizepicks','underdog','pick6','betr_us_dfs','fliff')
              AND {confirmation} >= NOW() - interval '48 hours' AND {confirmation} <= NOW()
              AND source_snapshot_time <= NOW() AND source_snapshot_time < commence_time
              AND observed_at <= NOW() AND observed_at < commence_time
              AND (market_last_update IS NULL OR market_last_update <= NOW())
              AND (bookmaker_last_update IS NULL OR bookmaker_last_update <= NOW())
              AND (market_last_update IS NULL OR market_last_update < commence_time)
              AND (bookmaker_last_update IS NULL OR bookmaker_last_update < commence_time)
        ), pairs AS (
            SELECT player_id, game_id, bookmaker, market_key, line, source_snapshot_time,
                confirmation_time, api_game_id, commence_time,
                jsonb_build_object('decision_time', NOW(), 'rows', jsonb_agg(quote_row ORDER BY outcome_label),
                    'capture', (jsonb_agg(capture_metadata)->0)) AS quote_snapshot,
                max(odds_american) FILTER (WHERE outcome_label='Over') AS over_odds,
                max(odds_american) FILTER (WHERE outcome_label='Under') AS under_odds
            FROM eligible
            GROUP BY player_id, game_id, bookmaker, market_key, line, source_snapshot_time,
                confirmation_time, api_game_id, commence_time
            HAVING count(*)=2 AND count(DISTINCT outcome_label)=2
               AND count(*) FILTER (WHERE outcome_label IN ('Over','Under'))=2
        ), ranked AS (
            SELECT *, row_number() OVER (PARTITION BY player_id,game_id,bookmaker,market_key,line
                ORDER BY confirmation_time DESC, source_snapshot_time DESC) AS rn FROM pairs
        ) SELECT player_id, lpad(game_id,10,'0') AS game_id, bookmaker, market_key, line,
            over_odds, under_odds, quote_snapshot FROM ranked WHERE rn=1
    """


def pair_in_capture(groups, quotes):
    """Check exact economic pair AND immutable IDs, independent of receipt ID."""
    if len(quotes) != 2 or {q["outcome_label"] for q in quotes} != {"Over", "Under"}:
        return False
    q = quotes[0]
    key = [q["api_player_name"], q["bookmaker"], q["market_key"]]
    for group in groups:
        if group["key"] == key:
            return all(q["staging_id"] in group["ids"] and
                       [format(numeric(q["line"]).normalize(), "f"), q["outcome_label"], int(q["odds_american"])] in group["state"]
                       for q in quotes)
    return False
