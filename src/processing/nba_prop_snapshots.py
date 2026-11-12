"""Shared NBA player-prop parsing, exact identity linking, and T-60 selection.

This module is intentionally policy-small. Both the one-time historical importer
and the daily inference materializer call the same selector. It performs no
environment loading and no network access.
"""

from __future__ import annotations

import csv
import unicodedata
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from psycopg2.extras import execute_values
from sqlalchemy import text

from src.models.features.nba.season import resolve_nba_season
from src.processing.nba_player_aliases import add_verified_player_aliases

POLICY_VERSION = "t60_v1"
T60 = timedelta(minutes=60)
FEATURE_MARKETS = ("player_points", "player_rebounds", "player_assists", "player_threes")
ALLOWED_BOOKMAKERS = ("draftkings", "pinnacle")
BOOK_PRIORITY = {"draftkings": 0, "pinnacle": 1}
ET_TZ = "America/New_York"
EPOCH_UTC = datetime(1970, 1, 1, tzinfo=UTC)
TEAM_NAME_ALIASES = {
    "Atlanta Hawks": "ATL", "Boston Celtics": "BOS", "Brooklyn Nets": "BKN",
    "Charlotte Hornets": "CHA", "Chicago Bulls": "CHI", "Cleveland Cavaliers": "CLE",
    "Dallas Mavericks": "DAL", "Denver Nuggets": "DEN", "Detroit Pistons": "DET",
    "Golden State Warriors": "GSW", "Houston Rockets": "HOU", "Indiana Pacers": "IND",
    "Los Angeles Clippers": "LAC", "Los Angeles Lakers": "LAL", "Memphis Grizzlies": "MEM",
    "Miami Heat": "MIA", "Milwaukee Bucks": "MIL", "Minnesota Timberwolves": "MIN",
    "New Orleans Pelicans": "NOP", "New York Knicks": "NYK", "Oklahoma City Thunder": "OKC",
    "Orlando Magic": "ORL", "Philadelphia 76ers": "PHI", "Phoenix Suns": "PHX",
    "Portland Trail Blazers": "POR", "Sacramento Kings": "SAC", "San Antonio Spurs": "SAS",
    "Toronto Raptors": "TOR", "Utah Jazz": "UTA", "Washington Wizards": "WAS",
    "LA Clippers": "LAC", "L.A. Clippers": "LAC", "LA Lakers": "LAL", "L.A. Lakers": "LAL",
    "New Jersey Nets": "BKN", "Charlotte Bobcats": "CHA", "New Orleans Hornets": "NOP",
    "New Orleans/Oklahoma City Hornets": "NOP", "Seattle SuperSonics": "OKC",
    "Vancouver Grizzlies": "MEM", "NOH": "NOP",
}


def normalize_team(name: Any) -> Any:
    """Side-effect-free parity with nba_linker_local.normalize_team."""
    if name is None:
        return None
    value = str(name).strip()
    return TEAM_NAME_ALIASES.get(value, value)


def normalize_player(name: Any) -> Any:
    """Side-effect-free parity with nba_linker_local.normalize_player."""
    if name is None:
        return None
    value = unicodedata.normalize("NFKD", str(name)).encode("ASCII", "ignore").decode("utf-8")
    value = value.lower().strip()
    for old, new in ((".", ""), ("'", ""), ("-", " "), (" jr", ""), (" iii", ""), (" ii", "")):
        value = value.replace(old, new)
    return " ".join(value.split())


class SnapshotContractError(ValueError):
    """The provider row cannot safely participate in T-60 selection."""


@dataclass(frozen=True)
class PropQuote:
    api_game_id: str
    api_player_name: str
    bookmaker: str
    market_key: str
    outcome_label: str
    line: Decimal | None
    odds_american: int
    commence_time: datetime
    home_team: str
    away_team: str
    snapshot_time: datetime
    requested_snapshot_time: datetime | None
    source_snapshot_time: datetime
    source_kind: str
    observed_at: datetime
    market_last_update: datetime | None = None
    bookmaker_last_update: datetime | None = None
    bookmaker_name: str | None = None
    game_id: str | None = None
    player_id: int | None = None
    team_id: int | None = None
    source_staging_id: int | None = None
    capture_id: int | None = None
    capture_received_at: datetime | None = None
    capture_market_last_update: datetime | None = None
    capture_bookmaker_last_update: datetime | None = None


@dataclass(frozen=True)
class FeatureLine:
    game_id: str
    player_id: int
    market_key: str
    line: Decimal
    bookmaker: str
    game_date: date
    season_id: str
    selection_policy_version: str
    decision_time: datetime
    commence_time: datetime
    source_snapshot_time: datetime
    team_id: int | None
    api_game_id: str
    api_player_name: str
    source_kind: str
    requested_snapshot_time: datetime | None
    market_last_update: datetime | None
    bookmaker_last_update: datetime | None
    observed_at: datetime
    source_staging_id: int | None = None
    capture_id: int | None = None
    capture_received_at: datetime | None = None
    capture_market_last_update: datetime | None = None
    capture_bookmaker_last_update: datetime | None = None


def parse_utc(value: Any, *, field: str, required: bool = True) -> datetime | None:
    if value in (None, ""):
        if required:
            raise SnapshotContractError(f"missing {field}")
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip().replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise SnapshotContractError(f"invalid {field}: {value!r}") from exc
    if parsed.tzinfo is None:
        raise SnapshotContractError(f"naive {field}: {value!r}")
    return parsed.astimezone(UTC)


def iso_z(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise SnapshotContractError(f"invalid numeric line: {value!r}") from exc
    if not result.is_finite():
        raise SnapshotContractError(f"non-finite numeric line: {value!r}")
    return result


def parse_odds_event(
    data: Mapping[str, Any],
    *,
    requested_snapshot_time: datetime | None,
    source_snapshot_time: datetime,
    source_kind: str,
    observed_at: datetime,
    expected_event_id: str | None = None,
    expected_home_team: str | None = None,
    expected_away_team: str | None = None,
    allowed_markets: Sequence[str] = FEATURE_MARKETS,
    allowed_bookmakers: Sequence[str] = ALLOWED_BOOKMAKERS,
) -> list[PropQuote]:
    """Parse a real Odds API event while preserving all source clocks.

    ``source_snapshot_time`` is the historical wrapper timestamp or actual live
    provider snapshot. ``observed_at`` is local availability for live rows and
    the same historical wrapper clock for historical rows. Fetch time belongs
    only in audit/task metadata and is not accepted here as availability.
    """
    if source_kind not in {"live", "historical"}:
        raise SnapshotContractError(f"unsupported source_kind: {source_kind!r}")
    event_id = str(data.get("id") or "")
    if not event_id or (expected_event_id is not None and event_id != expected_event_id):
        raise SnapshotContractError("API event identity mismatch")
    commence = parse_utc(data.get("commence_time"), field="commence_time")
    assert commence is not None
    home = str(data.get("home_team") or "").strip()
    away = str(data.get("away_team") or "").strip()
    if not home or not away or home.casefold() == away.casefold():
        raise SnapshotContractError("missing or contradictory event teams")
    if expected_home_team and home.casefold() != expected_home_team.strip().casefold():
        raise SnapshotContractError("API home-team identity mismatch")
    if expected_away_team and away.casefold() != expected_away_team.strip().casefold():
        raise SnapshotContractError("API away-team identity mismatch")
    parsed_source_snapshot_time = parse_utc(source_snapshot_time, field="source_snapshot_time")
    parsed_observed_at = parse_utc(observed_at, field="observed_at")
    parsed_requested_snapshot_time = parse_utc(
        requested_snapshot_time, field="requested_snapshot_time", required=False
    )
    assert parsed_source_snapshot_time is not None and parsed_observed_at is not None
    if parsed_source_snapshot_time >= commence or parsed_observed_at >= commence:
        raise SnapshotContractError("source availability is not pregame")

    rows: list[PropQuote] = []
    market_allow = set(allowed_markets)
    book_allow = set(allowed_bookmakers)
    for book in data.get("bookmakers") or []:
        bookmaker = str(book.get("key") or "")
        if bookmaker not in book_allow:
            continue
        book_update = parse_utc(book.get("last_update"), field="bookmaker_last_update", required=False)
        for market in book.get("markets") or []:
            market_key = str(market.get("key") or "")
            if market_key not in market_allow:
                continue
            market_update = parse_utc(market.get("last_update"), field="market_last_update", required=False)
            for outcome in market.get("outcomes") or []:
                player_name = str(outcome.get("description") or "").strip()
                side = str(outcome.get("name") or "").strip()
                price = outcome.get("price")
                if not player_name or side not in {"Over", "Under"} or isinstance(price, bool):
                    continue
                try:
                    odds = int(price)
                except (TypeError, ValueError):
                    continue
                rows.append(
                    PropQuote(
                        api_game_id=event_id,
                        api_player_name=player_name,
                        bookmaker=bookmaker,
                        bookmaker_name=book.get("title"),
                        market_key=market_key,
                        outcome_label=side,
                        line=_decimal(outcome.get("point")),
                        odds_american=odds,
                        commence_time=commence,
                        home_team=home,
                        away_team=away,
                        snapshot_time=parsed_source_snapshot_time,
                        requested_snapshot_time=parsed_requested_snapshot_time,
                        source_snapshot_time=parsed_source_snapshot_time,
                        source_kind=source_kind,
                        observed_at=parsed_observed_at,
                        market_last_update=market_update,
                        bookmaker_last_update=book_update,
                    )
                )
    return rows


def quote_from_record(row: Mapping[str, Any]) -> PropQuote:
    """Build a quote from a linked inference-table record."""
    kwargs = {
        "api_game_id": str(row["api_game_id"]),
        "api_player_name": str(row["api_player_name"]),
        "bookmaker": str(row["bookmaker"]),
        "market_key": str(row["market_key"]),
        "outcome_label": str(row["outcome_label"]),
        "line": _decimal(row.get("line")),
        "odds_american": int(row["odds_american"]),
        "commence_time": parse_utc(row["commence_time"], field="commence_time"),
        "home_team": str(row.get("home_team") or ""),
        "away_team": str(row.get("away_team") or ""),
        "snapshot_time": parse_utc(row["snapshot_time"], field="snapshot_time"),
        "requested_snapshot_time": parse_utc(
            row.get("requested_snapshot_time"), field="requested_snapshot_time", required=False
        ),
        "source_snapshot_time": parse_utc(row["source_snapshot_time"], field="source_snapshot_time"),
        "source_kind": str(row["source_kind"]),
        "observed_at": parse_utc(row["observed_at"], field="observed_at"),
        "market_last_update": parse_utc(
            row.get("market_last_update"), field="market_last_update", required=False
        ),
        "bookmaker_last_update": parse_utc(
            row.get("bookmaker_last_update"), field="bookmaker_last_update", required=False
        ),
        "bookmaker_name": row.get("bookmaker_name"),
        "game_id": str(row["game_id"]) if row.get("game_id") is not None else None,
        "player_id": int(row["player_id"]) if row.get("player_id") is not None else None,
        "team_id": int(row["team_id"]) if row.get("team_id") is not None else None,
        "source_staging_id": int(row["staging_id"]) if row.get("staging_id") is not None else None,
        "capture_id": int(row["capture_id"]) if row.get("capture_id") is not None else None,
        "capture_received_at": parse_utc(row.get("capture_received_at"), field="capture_received_at", required=False),
        "capture_market_last_update": parse_utc(row.get("capture_market_last_update"), field="capture_market_last_update", required=False),
        "capture_bookmaker_last_update": parse_utc(row.get("capture_bookmaker_last_update"), field="capture_bookmaker_last_update", required=False),
    }
    return PropQuote(**kwargs)  # type: ignore[arg-type]


def exact_link_quote(quote: PropQuote, context: Mapping[str, Any]) -> PropQuote:
    """Resolve exact player/game identity using nba_linker_local context.

    There is no fuzzy date fallback. Team ID remains nullable when a unique
    player-game relation is unavailable; callers may fill it from a bounded
    ``player_game_stats`` lookup.
    """
    from dataclasses import replace

    game_date = quote.commence_time.astimezone(ZoneInfo(ET_TZ)).date().isoformat()
    match_key = (normalize_team(quote.home_team), normalize_team(quote.away_team), game_date)
    games = context["by_match"].get(match_key, set())
    if len(games) != 1:
        raise SnapshotContractError(f"unresolved exact game identity: {match_key!r}")
    game_id = next(iter(games))
    game_keys = context["by_game"].get(str(game_id), set())
    if game_keys != {match_key}:
        raise SnapshotContractError(f"canonical game identity is not uniquely coherent: {game_id!r}")
    if quote.game_id is not None and str(quote.game_id) != str(game_id):
        raise SnapshotContractError("existing linked game identity conflicts with exact match")
    normalized_player = normalize_player(quote.api_player_name)
    players = context["manual"].get(
        quote.api_player_name, context["player_names"].get(normalized_player, set())
    )
    if len(players) != 1:
        raise SnapshotContractError(f"unresolved exact player identity: {quote.api_player_name!r}")
    player_id = int(next(iter(players)))
    if player_id not in context["known_players"]:
        raise SnapshotContractError("resolved player is absent from canonical players")
    if quote.player_id is not None and int(quote.player_id) != player_id:
        raise SnapshotContractError("existing linked player identity conflicts with exact match")
    return replace(quote, game_id=str(game_id), player_id=player_id)


def load_exact_linker_context(engine: Any, start_date: date, end_date: date) -> dict[str, Any]:
    """Load the exact historical subset of the existing linker context.

    This deliberately does not call the existing schedule/CDN fallback: old
    games are established from bounded ``team_game_stats`` rows, and absence
    from the current schedule does not invalidate historical identity.
    """
    with engine.begin() as conn:
        conn.execute(text("SET LOCAL statement_timeout = '15s'"))
        conn.execute(text("SET LOCAL lock_timeout = '2s'"))
        teams = [dict(row._mapping) for row in conn.execute(text("SELECT team_id, team_name FROM teams"))]
        players = [dict(row._mapping) for row in conn.execute(text("SELECT player_id, player_name FROM players"))]
        games = [dict(row._mapping) for row in conn.execute(text("""
            SELECT game_id, game_date, team_matchup
            FROM team_game_stats
            WHERE game_date >= :start_date AND game_date < :end_date
        """), {"start_date": start_date, "end_date": end_date})]

    team_ids: defaultdict[str, set[int]] = defaultdict(set)
    player_names: defaultdict[str, set[int]] = defaultdict(set)
    manual: defaultdict[str, set[int]] = defaultdict(set)
    for row in teams:
        normalized = normalize_team(row.get("team_name"))
        if isinstance(normalized, str) and normalized and row.get("team_id") is not None:
            team_ids[normalized].add(int(row["team_id"]))
    known_players = {int(row["player_id"]) for row in players if row.get("player_id") is not None}
    for row in players:
        normalized = normalize_player(row.get("player_name"))
        if isinstance(normalized, str) and normalized and row.get("player_id") is not None:
            player_names[normalized].add(int(row["player_id"]))
    mappings_file = Path(__file__).resolve().parents[2] / "linker_data" / "player_mappings.csv"
    if mappings_file.exists():
        with mappings_file.open("r", encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle):
                try:
                    manual[str(row.get("api_name") or "")].add(int(row["player_id"]))
                except (KeyError, TypeError, ValueError):
                    continue

    add_verified_player_aliases(players, manual)

    by_match: defaultdict[tuple[str, str, str], set[str]] = defaultdict(set)
    by_game: defaultdict[str, set[tuple[str, str, str]]] = defaultdict(set)
    for row in games:
        game_id = str(row.get("game_id") or "")
        matchup = row.get("team_matchup")
        if not isinstance(matchup, str):
            continue
        if " vs. " in matchup:
            home, away = matchup.split(" vs. ", 1)
        elif " @ " in matchup:
            away, home = matchup.split(" @ ", 1)
        else:
            continue
        key = (normalize_team(home), normalize_team(away), str(row.get("game_date"))[:10])
        if game_id and all(isinstance(value, str) and value for value in key) and key[0] != key[1]:
            by_match[key].add(game_id)
            by_game[game_id].add(key)
    return {
        "team_ids": team_ids,
        "player_names": player_names,
        "manual": manual,
        "known_players": known_players,
        "by_match": by_match,
        "by_game": by_game,
        "schedule_status": {"reference_available": False, "source": "bounded_team_game_stats"},
    }


def _eligible(quote: PropQuote, decision: datetime) -> bool:
    if quote.line is None or quote.line <= 0:
        return False
    if quote.bookmaker not in BOOK_PRIORITY or quote.market_key not in FEATURE_MARKETS:
        return False
    if quote.game_id is None or quote.player_id is None:
        return False
    clocks = (
        quote.source_snapshot_time,
        quote.observed_at,
        quote.market_last_update,
        quote.bookmaker_last_update,
        quote.capture_received_at,
        quote.capture_market_last_update,
        quote.capture_bookmaker_last_update,
    )
    if quote.capture_id is not None and (quote.capture_received_at is None or quote.source_kind != "live"
                                       or quote.capture_received_at < quote.source_snapshot_time):
        return False
    return all(clock is None or (clock <= decision and clock < quote.commence_time) for clock in clocks)


def _selection_clock(quote: PropQuote) -> datetime:
    return quote.capture_received_at or quote.source_snapshot_time


def _selection_market_clock(quote: PropQuote) -> datetime:
    return (quote.capture_market_last_update if quote.capture_id is not None else quote.market_last_update) or EPOCH_UTC


def select_t60_features(quotes: Iterable[PropQuote]) -> tuple[list[FeatureLine], list[str]]:
    """Select one deterministic line per linked game/player/market.

    Outcome pairing is not required. Contradictory numeric lines at an identical
    game/player/book/market/source snapshot are rejected before ranking. Ranking
    is newest source snapshot, then newest market clock, then draftkings before
    pinnacle, then stable source staging ID.
    """
    quotes = list(quotes)
    conflicts: list[str] = []
    by_snapshot: defaultdict[tuple[Any, ...], list[PropQuote]] = defaultdict(list)
    for quote in quotes:
        if quote.market_key not in FEATURE_MARKETS or quote.bookmaker not in BOOK_PRIORITY:
            continue
        key = (
            quote.game_id,
            quote.player_id,
            quote.market_key,
            quote.bookmaker,
            _selection_clock(quote),
        )
        by_snapshot[key].append(quote)

    candidates: defaultdict[tuple[str, int, str], list[PropQuote]] = defaultdict(list)
    for key, same_snapshot in by_snapshot.items():
        numeric_lines = {quote.line for quote in same_snapshot if quote.line is not None}
        if len(numeric_lines) > 1:
            conflicts.append("contradictory identical snapshot: " + repr(key))
            continue
        # Over/Under rows normally duplicate the same line; one row is enough for a feature.
        quote = min(same_snapshot, key=lambda item: (item.outcome_label, item.odds_american))
        decision = quote.commence_time - T60
        if _eligible(quote, decision):
            assert quote.game_id is not None and quote.player_id is not None
            candidates[(quote.game_id, quote.player_id, quote.market_key)].append(quote)

    selected: list[FeatureLine] = []
    for (game_id, player_id, market_key), group in sorted(candidates.items()):
        def rank(item: PropQuote) -> tuple[Any, ...]:
            market_clock = _selection_market_clock(item)
            staging = item.source_staging_id if item.source_staging_id is not None else 2**63 - 1
            return (-_selection_clock(item).timestamp(), -market_clock.timestamp(), BOOK_PRIORITY[item.bookmaker], staging)

        winner = min(group, key=rank)
        game_date = winner.commence_time.astimezone(ZoneInfo(ET_TZ)).date()
        season = resolve_nba_season(game_id, game_date)
        assert winner.line is not None
        selected.append(
            FeatureLine(
                game_id=game_id,
                player_id=player_id,
                market_key=market_key,
                line=winner.line,
                bookmaker=winner.bookmaker,
                game_date=game_date,
                season_id=season.season_id,
                selection_policy_version=POLICY_VERSION,
                decision_time=winner.commence_time - T60,
                commence_time=winner.commence_time,
                source_snapshot_time=winner.source_snapshot_time,
                team_id=winner.team_id,
                api_game_id=winner.api_game_id,
                api_player_name=winner.api_player_name,
                source_kind=winner.source_kind,
                requested_snapshot_time=winner.requested_snapshot_time,
                market_last_update=winner.market_last_update,
                bookmaker_last_update=winner.bookmaker_last_update,
                observed_at=winner.observed_at,
                source_staging_id=winner.source_staging_id,
                capture_id=winner.capture_id,
                capture_received_at=winner.capture_received_at,
                capture_market_last_update=winner.capture_market_last_update,
                capture_bookmaker_last_update=winner.capture_bookmaker_last_update,
            )
        )
    return selected, conflicts


def feature_as_dict(feature: FeatureLine) -> dict[str, Any]:
    return asdict(feature)


def insert_inference_quotes(conn: Any, quotes: Sequence[PropQuote]) -> int:
    """Idempotently append raw quotes to the new short-retention table."""
    if not quotes:
        return 0
    columns = (
        "api_game_id", "api_player_name", "bookmaker", "market_key", "outcome_label", "line",
        "odds_american", "commence_time", "home_team", "away_team", "market_last_update",
        "bookmaker_last_update", "bookmaker_name", "snapshot_time", "game_id", "player_id", "team_id",
        "requested_snapshot_time", "source_snapshot_time", "source_kind", "observed_at",
    )
    values = [tuple(getattr(quote, column) for column in columns) for quote in quotes]
    with conn.cursor() as cur:
        execute_values(
            cur,
            f"INSERT INTO public.nba_player_props_inference ({', '.join(columns)}) VALUES %s ON CONFLICT DO NOTHING",
            values,
            page_size=1000,
        )
        return max(cur.rowcount, 0)


def upsert_feature_lines(conn: Any, features: Sequence[FeatureLine], *, task_id: str | None = None) -> int:
    """Atomically upsert selector output; caller owns transaction boundaries.

    Each canonical grain is transaction-advisory-locked before comparison. Equal
    rank is an idempotent no-op only for the same semantic row; an equal-rank
    disagreement or cross-event/commence replacement fails closed.
    """
    if not features:
        return 0
    columns = (
        "game_id", "player_id", "market_key", "line", "bookmaker", "game_date", "season_id",
        "selection_policy_version", "decision_time", "commence_time", "source_snapshot_time", "team_id",
        "api_game_id", "api_player_name", "source_kind", "requested_snapshot_time", "market_last_update",
        "bookmaker_last_update", "observed_at", "task_id", "source_staging_id",
    )
    key_columns = ("game_id", "player_id", "market_key", "selection_policy_version")
    compare_columns = (
        "api_game_id", "commence_time", "line", "bookmaker", "source_snapshot_time",
        "market_last_update", "bookmaker_last_update", "observed_at", "decision_time",
        "source_kind", "requested_snapshot_time", "team_id", "api_player_name",
    )
    capture_columns = ("capture_id", "capture_received_at", "capture_market_last_update", "capture_bookmaker_last_update")
    # Historical-only callers stay compatible before the additive migration.
    capture_mode = any(feature.capture_id is not None for feature in features)
    if not capture_mode:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('public.nba_props_live_captures')")
            capture_mode = bool(cur.fetchone()[0])
    if capture_mode:
        columns += capture_columns
        compare_columns += capture_columns
    incoming_clock = "COALESCE(EXCLUDED.capture_received_at, EXCLUDED.source_snapshot_time)" if capture_mode else "EXCLUDED.source_snapshot_time"
    current_clock = "COALESCE(nba_player_prop_feature_lines.capture_received_at, nba_player_prop_feature_lines.source_snapshot_time)" if capture_mode else "nba_player_prop_feature_lines.source_snapshot_time"
    incoming_market = "CASE WHEN EXCLUDED.capture_id IS NOT NULL THEN EXCLUDED.capture_market_last_update ELSE EXCLUDED.market_last_update END" if capture_mode else "EXCLUDED.market_last_update"
    current_market = "CASE WHEN nba_player_prop_feature_lines.capture_id IS NOT NULL THEN nba_player_prop_feature_lines.capture_market_last_update ELSE nba_player_prop_feature_lines.market_last_update END" if capture_mode else "nba_player_prop_feature_lines.market_last_update"
    accepted: list[FeatureLine] = []
    with conn.cursor() as cur:
        for feature in features:
            lock_key = "|".join(str(getattr(feature, column)) for column in key_columns)
            cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (lock_key,))
            cur.execute(
                f"SELECT {', '.join(compare_columns)} FROM public.nba_player_prop_feature_lines "
                "WHERE game_id=%s AND player_id=%s AND market_key=%s AND selection_policy_version=%s FOR UPDATE",
                tuple(getattr(feature, column) for column in key_columns),
            )
            existing = cur.fetchone()
            if existing is None:
                accepted.append(feature)
                continue
            current = dict(zip(compare_columns, existing, strict=True))
            if current["api_game_id"] != feature.api_game_id or current["commence_time"] != feature.commence_time:
                raise SnapshotContractError("feature upsert conflicts with canonical event/commence identity")
            current_rank = (
                current.get("capture_received_at") or current["source_snapshot_time"],
                (current.get("capture_market_last_update") if current.get("capture_id") is not None else current["market_last_update"]) or EPOCH_UTC,
                -BOOK_PRIORITY.get(str(current["bookmaker"]), 999),
            )
            incoming_rank = (
                feature.capture_received_at or feature.source_snapshot_time,
                (feature.capture_market_last_update if feature.capture_id is not None else feature.market_last_update) or EPOCH_UTC,
                -BOOK_PRIORITY[feature.bookmaker],
            )
            if incoming_rank < current_rank:
                continue
            if incoming_rank == current_rank:
                incoming = {column: getattr(feature, column) for column in compare_columns}
                if any(current[column] != incoming[column] for column in compare_columns):
                    raise SnapshotContractError("equal-rank feature upsert has conflicting semantic content")
                continue
            accepted.append(feature)
    if not accepted:
        return 0
    values = [
        tuple(getattr(feature, column) if column != "task_id" else task_id for column in columns)
        for feature in accepted
    ]
    update_columns = [column for column in columns if column not in {"game_id", "player_id", "market_key", "selection_policy_version"}]
    assignments = ", ".join(f"{column} = EXCLUDED.{column}" for column in update_columns)
    # Advisory locks above make the compare-and-write safe across concurrent runs.
    # Keep a strict SQL guard as defense in depth.
    with conn.cursor() as cur:
        execute_values(
            cur,
            f"""INSERT INTO public.nba_player_prop_feature_lines ({', '.join(columns)}) VALUES %s
            ON CONFLICT (game_id, player_id, market_key, selection_policy_version) DO UPDATE SET
                {assignments}, updated_at = now()
            WHERE nba_player_prop_feature_lines.api_game_id = EXCLUDED.api_game_id
              AND nba_player_prop_feature_lines.commence_time = EXCLUDED.commence_time
              AND (
                {incoming_clock},
                COALESCE({incoming_market}, '-infinity'::timestamptz),
                CASE EXCLUDED.bookmaker WHEN 'draftkings' THEN 0 ELSE -1 END
            ) > (
                {current_clock},
                COALESCE({current_market}, '-infinity'::timestamptz),
                CASE nba_player_prop_feature_lines.bookmaker WHEN 'draftkings' THEN 0 ELSE -1 END
            )""",
            values,
            page_size=500,
        )
        return max(cur.rowcount, 0)
