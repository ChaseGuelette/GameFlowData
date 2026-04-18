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
    - Fuzzy question-text similarity (SequenceMatcher >= 0.80)
"""

import logging
import re
from dataclasses import dataclass
from datetime import date, timedelta
from difflib import SequenceMatcher

from sqlalchemy import text

logger = logging.getLogger(__name__)

# Maximum line difference to consider a "near match"
NEAR_LINE_TOLERANCE = 0.5

# Non-sports fuzzy match threshold
NON_SPORTS_SIMILARITY_THRESHOLD = 0.80

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
        """Match Kalshi ↔ Polymarket on non-sports binary markets via question similarity.

        Uses SequenceMatcher with normalized question text.
        Threshold: NON_SPORTS_SIMILARITY_THRESHOLD (0.80).

        Args:
            categories: Optional list of Polymarket categories to include
                        (e.g., ['politics', 'crypto', 'economics']). None = all non-sports.

        Returns:
            List of MatchedMarket instances.
        """
        kalshi_rows = self._load_kalshi_non_sports()
        if not kalshi_rows:
            logger.info("No Kalshi non-sports markets found")
            return []

        poly_rows = self._load_poly_non_sports(categories)
        if not poly_rows:
            logger.info("No Poly non-sports markets found")
            return []

        logger.info(
            f"Matching {len(kalshi_rows)} Kalshi non-sports × {len(poly_rows)} Poly non-sports"
        )

        def _norm_q(q: str) -> str:
            """Normalize question for comparison."""
            import re
            import unicodedata
            q = unicodedata.normalize("NFKD", q).encode("ASCII", "ignore").decode("utf-8")
            q = q.lower().strip()
            q = re.sub(r"[^\w\s]", " ", q)
            q = re.sub(r"\s+", " ", q)
            return q

        # Keyword pre-filter: only compare Poly markets that contain at least one
        # term associated with our Kalshi non-sports series. Reduces 27k+ → ~few hundred
        # before the expensive O(n×m) SequenceMatcher pass.
        _NON_SPORTS_KEYWORDS = {
            # Economics / macro
            "gdp", "gross domestic", "cpi", "inflation", "consumer price",
            "federal funds", "fomc", "rate cut", "rate hike", "interest rate",
            "fed rate", "fed funds",
            # Crypto
            "bitcoin", "btc", "ethereum", "eth", "dogecoin", "doge",
            "ripple", "xrp", "crypto",
        }
        poly_filtered = [
            row for row in poly_rows
            if any(kw in (row.get("question") or "").lower() for kw in _NON_SPORTS_KEYWORDS)
        ]
        if len(poly_filtered) < len(poly_rows):
            logger.info(
                f"Keyword pre-filter: {len(poly_rows)} → {len(poly_filtered)} Poly markets"
            )
        poly_rows = poly_filtered

        # Normalize Kalshi questions
        kalshi_norm = [(row, _norm_q(row.get("market_title") or "")) for row in kalshi_rows]

        matched: list[MatchedMarket] = []
        seen_poly: set[str] = set()

        for poly in poly_rows:
            cid = poly.get("condition_id", "")
            if cid in seen_poly:
                continue

            poly_q_norm = _norm_q(poly.get("question") or "")
            if not poly_q_norm:
                continue

            best_score = 0.0
            best_kalshi = None

            for k_row, k_norm in kalshi_norm:
                if not k_norm:
                    continue
                score = SequenceMatcher(None, poly_q_norm, k_norm).ratio()
                if score > best_score:
                    best_score = score
                    best_kalshi = k_row

            if best_kalshi is None or best_score < NON_SPORTS_SIMILARITY_THRESHOLD:
                continue

            seen_poly.add(cid)
            desc = f"[{poly.get('category', 'other').upper()}] {poly.get('question', '')[:60]}"

            matched.append(MatchedMarket(
                kalshi_ticker=best_kalshi.get("ticker", ""),
                kalshi_yes_price=float(best_kalshi.get("yes_price") or 0),
                kalshi_no_price=float(best_kalshi.get("no_price") or 0),
                kalshi_volume=int(best_kalshi.get("volume") or 0),
                kalshi_yes_bid=float(best_kalshi.get("yes_bid") or 0),
                kalshi_yes_ask=float(best_kalshi.get("yes_ask") or 0),
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
                ticker, market_title, market_type,
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

    def _load_poly_non_sports(self, categories: list[str] | None = None) -> list[dict]:
        """Load Polymarket non-sports markets (most recent snapshot per market).

        No freshness filter — polymarket_markets is populated by a dedicated scrape job
        that runs 2x/day. The DISTINCT ON guarantees we get the latest snapshot for each
        market regardless of when it was scraped. Only markets with actual prices are returned.
        Filters to markets with liquidity > 100 to reduce noise and O(n×m) matching cost.
        """
        if categories:
            query = text("""
                SELECT DISTINCT ON (condition_id)
                    condition_id, question, category, market_type,
                    yes_price, no_price, yes_bid, yes_ask, liquidity
                FROM polymarket_markets
                WHERE category = ANY(:categories)
                  AND market_status = 'open'
                  AND yes_price IS NOT NULL
                  AND liquidity > 100
                ORDER BY condition_id, snapshot_time DESC
            """)
            with self.engine.connect() as conn:
                rows = conn.execute(query, {"categories": categories}).fetchall()
        else:
            query = text("""
                SELECT DISTINCT ON (condition_id)
                    condition_id, question, category, market_type,
                    yes_price, no_price, yes_bid, yes_ask, liquidity
                FROM polymarket_markets
                WHERE category NOT IN ('sports')
                  AND market_status = 'open'
                  AND yes_price IS NOT NULL
                  AND liquidity > 100
                ORDER BY condition_id, snapshot_time DESC
            """)
            with self.engine.connect() as conn:
                rows = conn.execute(query).fetchall()
        return [dict(row._mapping) for row in rows]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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
