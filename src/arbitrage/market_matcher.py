"""
Market Matcher
==============
Cross-platform market matching logic for Kalshi ↔ Polymarket pairs.

Matching modes:
  Player props:
    - Exact: same player_id + stat_type + line
    - Near: same player_id + stat_type + line within 0.5
    - Fuzzy: name match + stat_type + line (when player_id is NULL on Poly side)

  Game-level markets:
    - frozenset({canonical_team1, canonical_team2}) + date + market_type
    - Additional key for totals: + line

  Non-sports markets:
    - Per-series category-aware grouping + structured field extraction (price, direction, period)
    - Falls back to SequenceMatcher >= 0.80 when extraction fails on either side
"""

import logging
import re
from dataclasses import dataclass
from datetime import date, timedelta
from difflib import SequenceMatcher

from sqlalchemy import text

from src.arbitrage.non_sports_extractor import (
    extract_kalshi,
    extract_poly,
)
from src.arbitrage.non_sports_extractor import (
    match_score as structured_match_score,
)

logger = logging.getLogger(__name__)

# Maximum line difference to consider a "near match"
NEAR_LINE_TOLERANCE = 0.5

# SequenceMatcher fallback threshold when structured extraction fails on either side
NON_SPORTS_FALLBACK_THRESHOLD = 0.80

# Month abbreviation map for Kalshi ticker date parsing
_KALSHI_MONTH_MAP = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}

# Kalshi game ticker: KXMLBGAME-26APR191340BALCLE-CLE → date 2026-04-19, time 13:40
# Pattern: 2-digit year + 3-letter month + 2-digit day (+ 4-digit HHMM optional)
_KALSHI_DATE_RE = re.compile(r"(\d{2})([A-Z]{3})(\d{2})(\d{4})?")

# Polymarket slug date: mlb-bal-cle-2026-04-19 → date 2026-04-19
_POLY_SLUG_DATE_RE = re.compile(r"(\d{4})-(\d{2})-(\d{2})$")

# Candidate name extractor: "Will [NAME] win ..." → captures NAME
# Used to reject same-race/different-candidate false positives in election matching.
_CANDIDATE_RE = re.compile(r"will\s+(.+?)\s+win\b", re.IGNORECASE)


def _extract_candidate(question: str) -> str | None:
    """Extract candidate name from 'Will [NAME] win ...' election questions."""
    m = _CANDIDATE_RE.search(question)
    return m.group(1).strip() if m else None


# Placement rank extractor: "Will X finish 1st?" vs "Will X finish 2nd?"
# Only fires when "finish"/"place" keyword is present to avoid false positives on "1st quarter".
_PLACEMENT_RE = re.compile(
    r'(?:finish(?:ed|es)?|place[sd]?|ranked?)\s+'
    r'(1st|first|2nd|second|3rd|third|4th|fourth)',
    re.IGNORECASE,
)
_PLACE_MAP = {
    '1st': 1, 'first': 1,
    '2nd': 2, 'second': 2,
    '3rd': 3, 'third': 3,
    '4th': 4, 'fourth': 4,
}


def _extract_placement(text: str) -> int | None:
    """Extract ordinal placement rank from race/competition questions.

    Returns 1-4 if found, None otherwise.
    """
    m = _PLACEMENT_RE.search(text)
    if not m:
        return None
    return _PLACE_MAP.get(m.group(1).lower())


def _extract_date_from_kalshi_ticker(ticker: str) -> date | None:
    """Extract game date from a Kalshi game ticker.

    Example: KXMLBGAME-26APR191340BALCLE-CLE → 2026-04-19
    Returns None if the ticker doesn't contain a parseable date.
    """
    m = _KALSHI_DATE_RE.search(ticker.upper())
    if not m:
        return None
    yy, mon_str, dd = int(m.group(1)), m.group(2), int(m.group(3))
    month = _KALSHI_MONTH_MAP.get(mon_str)
    if month is None:
        return None
    year = 2000 + yy
    try:
        return date(year, month, dd)
    except ValueError:
        return None


def _extract_time_from_kalshi_ticker(ticker: str) -> tuple[int, int] | None:
    """Extract game start time (hour, minute) ET from a Kalshi game ticker.

    Example: KXMLBGAME-26APR162040SEASD → (20, 40) ET
    Returns None if no time is encoded.
    """
    m = _KALSHI_DATE_RE.search(ticker.upper())
    if not m or not m.group(4):
        return None
    time_str = m.group(4)
    if len(time_str) == 4:
        try:
            hh, mm = int(time_str[:2]), int(time_str[2:])
            if 0 <= hh <= 23 and 0 <= mm <= 59:
                return (hh, mm)
        except ValueError:
            pass
    return None


def _extract_date_from_poly_slug(slug: str) -> date | None:
    """Extract game date from a Polymarket event slug.

    Example: mlb-bal-cle-2026-04-19 → 2026-04-19
    Returns None if no parseable date found.
    """
    m = _POLY_SLUG_DATE_RE.search(slug)
    if not m:
        return None
    try:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None


@dataclass
class MatchedMarket:
    """A matched Kalshi + Polymarket pair for any market type."""
    kalshi_ticker: str
    kalshi_yes_price: float
    kalshi_no_price: float
    kalshi_volume: int
    kalshi_yes_bid: float
    kalshi_yes_ask: float
    poly_condition_id: str
    poly_yes_price: float
    poly_no_price: float
    poly_liquidity: float
    poly_yes_bid: float | None
    poly_yes_ask: float | None
    player_id: int | None
    player_name: str
    stat_type: str
    line: float
    sport: str
    match_type: str | None  # 'exact', 'near', 'fuzzy', 'game', 'text_similarity'
    match_confidence: float  # 0.0 - 1.0
    # New game-level / non-sports fields (backward-compatible defaults)
    market_type: str = "player_prop"
    team1: str | None = None
    team2: str | None = None
    game_date: date | None = None
    description: str | None = None  # human-readable match context


class MarketMatcher:
    """Matches Polymarket player prop markets against Kalshi markets."""

    def __init__(self, engine=None):
        """Initialize the matcher.

        Args:
            engine: SQLAlchemy engine (auto-created if None).
        """
        if engine is None:
            from src.db.client import get_engine
            engine = get_engine()
        self.engine = engine

    # ------------------------------------------------------------------
    # Kalshi ↔ Polymarket Matching
    # ------------------------------------------------------------------

    def match_kalshi_markets(
        self,
        target_date: date,
        sport: str = "nba",
    ) -> list[MatchedMarket]:
        """Find Kalshi ↔ Polymarket player prop pairs for a given date.

        Loads the most recent snapshot from each platform for target_date
        and finds matching pairs by (player_id, stat_type, line).

        Args:
            target_date: Date to load snapshots for.
            sport: Sport to match ('nba' or 'mlb').

        Returns:
            List of MatchedMarket instances.
        """
        # Load Kalshi player prop snapshot (most recent for target_date)
        kalshi_rows = self._load_kalshi_props(target_date, sport)
        if not kalshi_rows:
            logger.info(f"No Kalshi props found for {target_date} {sport}")
            return []

        # Load Polymarket player prop snapshot (most recent for target_date)
        poly_rows = self._load_poly_props(target_date, sport)
        if not poly_rows:
            logger.info(f"No Polymarket props found for {target_date} {sport}")
            return []

        logger.info(
            f"Matching {len(kalshi_rows)} Kalshi props × {len(poly_rows)} Poly props "
            f"for {sport.upper()} {target_date}"
        )

        # Build Kalshi lookup: {(player_id, stat_type, line) -> kalshi_row}
        kalshi_by_pid: dict[tuple, dict] = {}
        kalshi_by_name: dict[tuple, list[dict]] = {}

        for row in kalshi_rows:
            pid = row.get("player_id")
            stat = row.get("stat_type") or ""
            line = float(row.get("line") or 0)
            if pid:
                kalshi_by_pid[(pid, stat, line)] = row
                # Also index by stat+line for near-match lookups
                for line_var in _line_variants(line):
                    kalshi_by_pid.setdefault((pid, stat, line_var), row)
            # Also build name-based fallback
            norm_name = _norm(row.get("player_name") or "")
            if norm_name:
                kalshi_by_name.setdefault((norm_name, stat, line), []).append(row)

        # Match each Polymarket prop
        matched: list[MatchedMarket] = []
        seen_poly: set[str] = set()

        for poly in poly_rows:
            cid = poly.get("condition_id", "")
            if cid in seen_poly:
                continue

            pid = poly.get("player_id")
            stat = poly.get("stat_type") or ""
            line = float(poly.get("line") or 0)
            poly_name = poly.get("player_name") or ""

            kalshi_row = None
            match_type = None
            confidence = 0.0

            # 1. Exact match (same player_id + stat + line)
            if pid:
                key = (pid, stat, line)
                if key in kalshi_by_pid:
                    kalshi_row = kalshi_by_pid[key]
                    match_type = "exact"
                    confidence = 1.0

            # 2. Near match (same player_id + stat + line within tolerance)
            if kalshi_row is None and pid:
                for line_var in _line_variants(line, tolerance=NEAR_LINE_TOLERANCE, steps=2):
                    key = (pid, stat, line_var)
                    if key in kalshi_by_pid:
                        kalshi_row = kalshi_by_pid[key]
                        match_type = "near"
                        confidence = 0.85
                        break

            # 3. Fuzzy name match (player_id NULL or no direct match)
            if kalshi_row is None and poly_name:
                norm_poly = _norm(poly_name)
                best_score = 0.0
                best_row = None
                for (k_name, k_stat, k_line), rows in kalshi_by_name.items():
                    if k_stat != stat:
                        continue
                    if abs(k_line - line) > NEAR_LINE_TOLERANCE:
                        continue
                    score = _name_similarity(norm_poly, k_name)
                    if score > best_score:
                        best_score = score
                        best_row = rows[0]
                if best_row and best_score >= 0.80:
                    kalshi_row = best_row
                    match_type = "fuzzy"
                    confidence = best_score * 0.9

            if kalshi_row is None:
                continue

            seen_poly.add(cid)

            matched.append(MatchedMarket(
                kalshi_ticker=kalshi_row.get("ticker", ""),
                kalshi_yes_price=float(kalshi_row.get("yes_price") or 0),
                kalshi_no_price=float(kalshi_row.get("no_price") or 0),
                kalshi_volume=int(kalshi_row.get("volume") or 0),
                kalshi_yes_bid=float(kalshi_row.get("yes_bid") or 0),
                kalshi_yes_ask=float(kalshi_row.get("yes_ask") or 0),
                poly_condition_id=cid,
                poly_yes_price=float(poly.get("yes_price") or 0),
                poly_no_price=float(poly.get("no_price") or 0),
                poly_liquidity=float(poly.get("liquidity") or 0),
                poly_yes_bid=_opt_float(poly.get("yes_bid")),
                poly_yes_ask=_opt_float(poly.get("yes_ask")),
                player_id=pid,
                player_name=poly_name or kalshi_row.get("player_name", ""),
                stat_type=stat,
                line=line,
                sport=sport,
                match_type=match_type,
                match_confidence=confidence,
            ))

        logger.info(f"Found {len(matched)} Kalshi↔Poly matched pairs")
        return matched

    # ------------------------------------------------------------------
    # DB Loaders
    # ------------------------------------------------------------------

    def _load_kalshi_props(self, target_date: date, sport: str) -> list[dict]:
        """Load most recent Kalshi player prop snapshot for target_date."""
        query = text("""
            SELECT DISTINCT ON (ticker)
                ticker, player_name, stat_type, line, yes_price, no_price,
                yes_bid, yes_ask, volume, player_id
            FROM kalshi_markets
            WHERE sport = :sport
              AND snapshot_time::date = :target_date
              AND market_status = 'open'
              AND player_name IS NOT NULL
            ORDER BY ticker, snapshot_time DESC
        """)
        with self.engine.connect() as conn:
            rows = conn.execute(query, {"sport": sport, "target_date": target_date}).fetchall()
        return [dict(row._mapping) for row in rows]

    def _load_poly_props(self, target_date: date, sport: str) -> list[dict]:
        """Load most recent Polymarket player prop snapshot for target_date."""
        # Look back up to 3 days for the most recent snapshot (markets may not update daily)
        for days_back in range(4):
            check_date = target_date - timedelta(days=days_back)
            query = text("""
                SELECT DISTINCT ON (condition_id)
                    condition_id, player_name, stat_type, line, yes_price, no_price,
                    yes_bid, yes_ask, liquidity, player_id
                FROM polymarket_markets
                WHERE sport = :sport
                  AND snapshot_time::date = :target_date
                  AND market_type = 'player_prop'
                  AND market_status = 'open'
                ORDER BY condition_id, snapshot_time DESC
            """)
            with self.engine.connect() as conn:
                rows = conn.execute(query, {"sport": sport, "target_date": check_date}).fetchall()
            if rows:
                return [dict(row._mapping) for row in rows]
        return []

    # ------------------------------------------------------------------
    # Game-Level Matching
    # ------------------------------------------------------------------

    def match_game_markets(
        self,
        target_date: date,
        sport: str,
    ) -> list["MatchedMarket"]:
        """Match Kalshi ↔ Polymarket on game-level markets for a given date.

        Match key: frozenset({canonical_team1, canonical_team2}) + market_type
        For totals: also match on line (within 0.5 tolerance).

        Args:
            target_date: Date to load snapshots for.
            sport: Sport to match ('nba' or 'mlb').

        Returns:
            List of MatchedMarket instances.
        """
        from src.arbitrage.team_normalizer import normalize_team

        kalshi_rows = self._load_kalshi_game_markets(target_date, sport)
        if not kalshi_rows:
            logger.info(f"No Kalshi game markets for {target_date} {sport}")
            return []

        poly_rows = self._load_poly_game_markets(target_date, sport)
        if not poly_rows:
            logger.info(f"No Poly game markets for {target_date} {sport}")
            return []

        logger.info(
            f"Matching {len(kalshi_rows)} Kalshi game mkts × {len(poly_rows)} Poly game mkts "
            f"for {sport.upper()} {target_date}"
        )

        def _team_key(t1, t2, mtype, game_date_val, line=None):
            """Build a match key from canonical team abbreviations, market type, and date."""
            # Include game_date in key to prevent cross-date matching.
            # date may be None for rows where we can't parse it — those will only match
            # other None-date rows (i.e., they won't cross-match dated rows).
            base = (frozenset({(t1 or "").upper(), (t2 or "").upper()}), mtype, game_date_val)
            if mtype == "total" and line is not None:
                return base + (round(float(line) * 2) / 2,)  # round to nearest 0.5
            return base

        # Build Kalshi lookup: key includes game date extracted from ticker
        kalshi_by_key: dict[tuple, dict] = {}
        for row in kalshi_rows:
            t1 = normalize_team(row.get("team1") or "", sport=sport) or (row.get("team1") or "")
            t2 = normalize_team(row.get("team2") or "", sport=sport) or (row.get("team2") or "")
            mtype = row.get("market_type") or "moneyline"
            line = row.get("line")
            # Extract date from Kalshi ticker (most reliable source)
            kalshi_date = _extract_date_from_kalshi_ticker(row.get("ticker") or "")
            key = _team_key(t1, t2, mtype, kalshi_date, line)
            kalshi_by_key[key] = row

        matched: list[MatchedMarket] = []
        seen_poly: set[str] = set()

        for poly in poly_rows:
            cid = poly.get("condition_id", "")
            if cid in seen_poly:
                continue

            t1 = normalize_team(poly.get("team1") or "", sport=sport) or (poly.get("team1") or "")
            t2 = normalize_team(poly.get("team2") or "", sport=sport) or (poly.get("team2") or "")
            mtype = poly.get("market_type") or "moneyline"
            line = poly.get("line")
            # Extract date from Polymarket event slug
            poly_date = _extract_date_from_poly_slug(poly.get("event_slug") or "")

            key = _team_key(t1, t2, mtype, poly_date, line)
            kalshi_row = kalshi_by_key.get(key)

            # For totals: try without line (sometimes lines differ slightly)
            if kalshi_row is None and mtype == "total":
                base_key = (frozenset({t1.upper(), t2.upper()}), mtype, poly_date)
                for k, v in kalshi_by_key.items():
                    if len(k) >= 3 and k[:3] == base_key:
                        k_line = k[3] if len(k) > 3 else None
                        p_line = float(line or 0)
                        if k_line is not None and abs(float(k_line) - p_line) <= NEAR_LINE_TOLERANCE:
                            kalshi_row = v
                            break

            if kalshi_row is None:
                continue

            seen_poly.add(cid)

            # Resolved game date: prefer Kalshi ticker date, fall back to poly slug date,
            # then target_date as last resort
            kalshi_date_resolved = _extract_date_from_kalshi_ticker(kalshi_row.get("ticker") or "")
            resolved_game_date = kalshi_date_resolved or poly_date or target_date

            desc = f"{t1 or '?'} vs {t2 or '?'} [{mtype.upper()}]"
            if line is not None:
                desc += f" {line}"
            if resolved_game_date:
                desc += f" on {resolved_game_date}"

            matched.append(MatchedMarket(
                kalshi_ticker=kalshi_row.get("ticker", ""),
                kalshi_yes_price=float(kalshi_row.get("yes_price") or 0),
                kalshi_no_price=float(kalshi_row.get("no_price") or 0),
                kalshi_volume=int(kalshi_row.get("volume") or 0),
                kalshi_yes_bid=float(kalshi_row.get("yes_bid") or 0),
                kalshi_yes_ask=float(kalshi_row.get("yes_ask") or 0),
                poly_condition_id=cid,
                poly_yes_price=float(poly.get("yes_price") or 0),
                poly_no_price=float(poly.get("no_price") or 0),
                poly_liquidity=float(poly.get("liquidity") or 0),
                poly_yes_bid=_opt_float(poly.get("yes_bid")),
                poly_yes_ask=_opt_float(poly.get("yes_ask")),
                player_id=None,
                player_name="",
                stat_type="",
                line=float(line or 0),
                sport=sport,
                match_type="game",
                match_confidence=0.95,
                market_type=mtype,
                team1=t1 or None,
                team2=t2 or None,
                game_date=resolved_game_date,
                description=desc,
            ))

        logger.info(f"Found {len(matched)} Kalshi↔Poly game-level matched pairs")
        return matched

    # ------------------------------------------------------------------
    # Non-Sports Matching
    # ------------------------------------------------------------------

    def match_non_sports_markets(
        self,
        categories: list[str] | None = None,
    ) -> list["MatchedMarket"]:
        """Match Kalshi ↔ Polymarket on non-sports binary markets via structured field extraction.

        Extracts (price, direction, period) from both sides and matches deterministically.
        Falls back to SequenceMatcher >= NON_SPORTS_FALLBACK_THRESHOLD (0.80) when extraction
        fails on either side.
        Each Kalshi series is matched only against the corresponding Polymarket categories.

        Args:
            categories: Unused — retained for API compatibility.
                        Category filtering is now done per-series via KALSHI_SERIES_POLY_CONFIG.

        Returns:
            List of MatchedMarket instances.
        """
        from src.scrapers.kalshi.kalshi_utils import KALSHI_SERIES_POLY_CONFIG

        kalshi_rows = self._load_kalshi_non_sports()
        if not kalshi_rows:
            logger.info("No Kalshi non-sports markets found")
            return []

        # Group Kalshi markets by their series prefix (KXGDP, KXBTC, etc.)
        series_groups: dict[str, list] = {}
        for row in kalshi_rows:
            ticker = row.get("ticker", "")
            series = row.get("series_ticker") or ticker.split("-")[0]
            series_groups.setdefault(series, []).append(row)

        matched: list[MatchedMarket] = []
        seen_poly: set[str] = set()

        for series, k_rows in series_groups.items():
            cfg = KALSHI_SERIES_POLY_CONFIG.get(series)
            if cfg is None:
                logger.debug(f"[{series}] No Poly config — skipping")
                continue

            poly_categories = cfg["poly_categories"]
            poly_keywords = cfg["poly_keywords"]
            min_kalshi_volume = cfg.get("min_kalshi_volume", 0)
            min_poly_liquidity = cfg.get("min_poly_liquidity", 0)

            # Apply per-config Kalshi volume filter (keeps O(n×m) manageable for large categories)
            if min_kalshi_volume > 0:
                k_rows = [r for r in k_rows if (r.get("volume") or 0) >= min_kalshi_volume]
            if not k_rows:
                logger.info(f"[{series}] 0 Kalshi markets after volume filter — skipping")
                continue

            # Load Polymarket markets scoped to this series's categories
            poly_rows = self._load_poly_non_sports(
                categories=poly_categories,
                min_liquidity=min_poly_liquidity if min_poly_liquidity > 0 else None,
            )
            if not poly_rows:
                logger.info(f"[{series}] No Poly markets for categories {poly_categories}")
                continue

            # Apply series-specific keyword filter
            poly_filtered = [
                row for row in poly_rows
                if any(kw in (row.get("question") or "").lower() for kw in poly_keywords)
            ]
            if not poly_filtered:
                logger.info(f"[{series}] 0 Poly markets after keyword filter — skipping")
                continue

            logger.info(f"[{series}] {len(k_rows)} Kalshi × {len(poly_filtered)} Poly")

            # Pre-extract Kalshi fields and normalize questions for this series
            k_data = [
                (r, _norm_q(r.get("market_title") or ""),
                 extract_kalshi(series, r.get("ticker", ""), r.get("market_title") or ""))
                for r in k_rows
            ]

            for poly in poly_filtered:
                cid = poly.get("condition_id", "")
                if cid in seen_poly:
                    continue
                poly_q_norm = _norm_q(poly.get("question") or "")
                if not poly_q_norm:
                    continue

                p_fields = extract_poly(series, poly.get("question") or "")

                best_score, best_k = 0.0, None
                for k_row, k_n, k_fields in k_data:
                    if not k_n:
                        continue
                    if k_fields is not None and p_fields is not None:
                        score = structured_match_score(k_fields, p_fields)
                    else:
                        # Extraction failed on one side — fuzzy fallback (threshold per config)
                        fallback_threshold = cfg.get("fallback_threshold", NON_SPORTS_FALLBACK_THRESHOLD)
                        sm = SequenceMatcher(None, poly_q_norm, k_n).ratio()
                        if sm >= fallback_threshold:
                            # Candidate disambiguation: reject "Will X win?" vs "Will Y win?"
                            # when X and Y are clearly different people (same-race false positives).
                            k_cand = _extract_candidate(k_row.get("market_title") or "")
                            p_cand = _extract_candidate(poly.get("question") or "")
                            if k_cand and p_cand:
                                # Both sides name a candidate — require names to match.
                                # Threshold 0.65 (not 0.5): Korean/Spanish names share too many
                                # characters at 0.5, causing false positives (Chong Won-o ≈ Kang Hoon-sik).
                                name_sim = SequenceMatcher(None, k_cand.lower(), p_cand.lower()).ratio()
                                score = sm if name_sim >= 0.65 else 0.0
                            elif k_cand or p_cand:
                                # One side is "Will X win?", other has different structure → reject
                                score = 0.0
                            else:
                                score = sm
                        else:
                            score = 0.0
                    # Placement disambiguation: "finish 1st" vs "finish 2nd" are different markets
                    if score > 0.0:
                        k_place = _extract_placement(k_row.get("market_title") or "")
                        p_place = _extract_placement(poly.get("question") or "")
                        if k_place and p_place and k_place != p_place:
                            score = 0.0
                    if score > best_score:
                        best_score, best_k = score, k_row

                if best_k is None or best_score == 0.0:
                    continue

                seen_poly.add(cid)
                desc = f"[{series}] {poly.get('question', '')[:60]}"

                matched.append(MatchedMarket(
                    kalshi_ticker=best_k.get("ticker", ""),
                    kalshi_yes_price=float(best_k.get("yes_price") or 0),
                    kalshi_no_price=float(best_k.get("no_price") or 0),
                    kalshi_volume=int(best_k.get("volume") or 0),
                    kalshi_yes_bid=float(best_k.get("yes_bid") or 0),
                    kalshi_yes_ask=float(best_k.get("yes_ask") or 0),
                    poly_condition_id=cid,
                    poly_yes_price=float(poly.get("yes_price") or 0),
                    poly_no_price=float(poly.get("no_price") or 0),
                    poly_liquidity=float(poly.get("liquidity") or 0),
                    poly_yes_bid=_opt_float(poly.get("yes_bid")),
                    poly_yes_ask=_opt_float(poly.get("yes_ask")),
                    player_id=None,
                    player_name="",
                    stat_type="",
                    line=0.0,
                    sport="",
                    match_type="text_similarity",
                    match_confidence=best_score,
                    market_type=poly.get("market_type") or "binary",
                    team1=None,
                    team2=None,
                    game_date=None,
                    description=desc,
                ))

        logger.info(f"Found {len(matched)} Kalshi↔Poly non-sports matched pairs")
        return matched

    # ------------------------------------------------------------------
    # Additional DB Loaders
    # ------------------------------------------------------------------

    def _load_kalshi_game_markets(self, target_date: date, sport: str) -> list[dict]:
        """Load most recent Kalshi game-level (non-prop) snapshot for target_date.

        Checks target_date ± 1 day to handle UTC/ET timezone boundary crossings
        (snapshots stored after midnight UTC may have next-day dates).
        """
        # Check target_date +1 first (most recent), then today, then 3 days back
        check_dates = [target_date + timedelta(days=1), target_date] + [
            target_date - timedelta(days=d) for d in range(1, 4)
        ]
        query = text("""
            SELECT DISTINCT ON (ticker)
                ticker, market_type, team1, team2, line,
                yes_price, no_price, yes_bid, yes_ask, volume, market_title
            FROM kalshi_markets
            WHERE sport = :sport
              AND snapshot_time::date = :target_date
              AND market_status = 'open'
              AND market_type != 'player_prop'
            ORDER BY ticker, snapshot_time DESC
        """)
        for check_date in check_dates:
            with self.engine.connect() as conn:
                rows = conn.execute(query, {"sport": sport, "target_date": check_date}).fetchall()
            if rows:
                return [dict(row._mapping) for row in rows]
        return []

    def _load_poly_game_markets(self, target_date: date, sport: str) -> list[dict]:
        """Load most recent Polymarket game-level snapshot for target_date."""
        for days_back in range(4):
            check_date = target_date - timedelta(days=days_back)
            query = text("""
                SELECT DISTINCT ON (condition_id)
                    condition_id, event_slug, market_type, team1, team2, line,
                    yes_price, no_price, yes_bid, yes_ask, liquidity, question
                FROM polymarket_markets
                WHERE sport = :sport
                  AND snapshot_time::date = :target_date
                  AND market_type IN ('moneyline', 'nrfi', 'total', 'spread', 'season_future')
                  AND market_status = 'open'
                ORDER BY condition_id, snapshot_time DESC
            """)
            with self.engine.connect() as conn:
                rows = conn.execute(query, {"sport": sport, "target_date": check_date}).fetchall()
            if rows:
                return [dict(row._mapping) for row in rows]
        return []

    def _load_kalshi_non_sports(self) -> list[dict]:
        """Load recent Kalshi non-sports markets (no sport tag, or KALSHI_GAME_SERIES non-sports).

        Uses a 4-hour window to tolerate gaps in the Kalshi refresh job (which runs every 10 min).
        """
        query = text("""
            SELECT DISTINCT ON (ticker)
                ticker, series_ticker, market_title, market_type,
                yes_price, no_price, yes_bid, yes_ask, volume
            FROM kalshi_markets
            WHERE (sport IS NULL OR sport = '')
              AND market_status = 'open'
              AND snapshot_time >= NOW() - INTERVAL '4 hours'
            ORDER BY ticker, snapshot_time DESC
        """)
        with self.engine.connect() as conn:
            rows = conn.execute(query).fetchall()
        return [dict(row._mapping) for row in rows]

    def _load_poly_non_sports(
        self,
        categories: list[str] | None = None,
        min_liquidity: int | None = None,
    ) -> list[dict]:
        """Load Polymarket non-sports markets (most recent snapshot per market).

        No freshness filter — polymarket_markets is populated by a dedicated scrape job
        that runs 2x/day. The DISTINCT ON guarantees we get the latest snapshot for each
        market regardless of when it was scraped. Only markets with actual prices are returned.
        Filters to markets with liquidity > min_liquidity (default 100) to reduce noise
        and O(n×m) matching cost.
        """
        liq = min_liquidity if min_liquidity is not None else 100
        if categories:
            query = text("""
                SELECT DISTINCT ON (condition_id)
                    condition_id, question, category, market_type,
                    yes_price, no_price, yes_bid, yes_ask, liquidity
                FROM polymarket_markets
                WHERE category = ANY(:categories)
                  AND market_status = 'open'
                  AND yes_price IS NOT NULL
                  AND liquidity > :liq
                ORDER BY condition_id, snapshot_time DESC
            """)
            with self.engine.connect() as conn:
                rows = conn.execute(query, {"categories": categories, "liq": liq}).fetchall()
        else:
            query = text("""
                SELECT DISTINCT ON (condition_id)
                    condition_id, question, category, market_type,
                    yes_price, no_price, yes_bid, yes_ask, liquidity
                FROM polymarket_markets
                WHERE category NOT IN ('sports')
                  AND market_status = 'open'
                  AND yes_price IS NOT NULL
                  AND liquidity > :liq
                ORDER BY condition_id, snapshot_time DESC
            """)
            with self.engine.connect() as conn:
                rows = conn.execute(query, {"liq": liq}).fetchall()
        return [dict(row._mapping) for row in rows]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _norm_q(q: str) -> str:
    """Normalize a market question for text similarity comparison."""
    import unicodedata
    q = unicodedata.normalize("NFKD", q).encode("ASCII", "ignore").decode("utf-8")
    q = q.lower().strip()
    q = re.sub(r"[^\w\s]", " ", q)
    q = re.sub(r"\s+", " ", q)
    return q


def _norm(name: str) -> str:
    """Basic name normalization for matching."""
    import unicodedata
    if not name:
        return ""
    name = unicodedata.normalize("NFKD", name).encode("ASCII", "ignore").decode("utf-8")
    name = name.lower().strip()
    for old, new in [(".", ""), ("'", ""), ("-", " "), (" jr", ""), (" iii", ""), (" ii", ""), (" iv", "")]:
        name = name.replace(old, new)
    return " ".join(name.split())


def _name_similarity(a: str, b: str) -> float:
    """Compute name similarity using SequenceMatcher with last-name boost."""
    from difflib import SequenceMatcher
    score = SequenceMatcher(None, a, b).ratio()
    parts_a = a.split()
    parts_b = b.split()
    if parts_a and parts_b and parts_a[-1] == parts_b[-1]:
        score += 0.15
    return min(score, 1.0)


def _line_variants(line: float, tolerance: float = 0.0, steps: int = 1) -> list[float]:
    """Generate line variants within tolerance for near-match lookups."""
    variants = [line]
    if tolerance > 0:
        for step in range(1, steps + 1):
            delta = tolerance * step / steps
            variants.extend([line + delta, line - delta])
    return variants


def _opt_float(val) -> float | None:
    """Safely convert to float or return None."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None
