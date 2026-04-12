"""
Market Matcher
==============
Cross-platform market matching logic:
  1. Kalshi ↔ Polymarket player prop pairs (for arb detection)
  2. Polymarket ↔ sportsbook consensus (for mispricing detection)

Matching modes:
  - Exact: same player_id + stat_type + line
  - Near: same player_id + stat_type + line within 0.5
  - Fuzzy: name match + stat_type + line (when player_id is NULL on Poly side)
"""

import logging
from dataclasses import dataclass, field
from datetime import date, timedelta

from sqlalchemy import text

logger = logging.getLogger(__name__)

# Maximum line difference to consider a "near match"
NEAR_LINE_TOLERANCE = 0.5


@dataclass
class MatchedMarket:
    """A matched Kalshi + Polymarket pair for the same player prop."""
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
    match_type: str | None  # 'exact', 'near', 'fuzzy'
    match_confidence: float  # 0.0 - 1.0


@dataclass
class SportsbookComparison:
    """A Polymarket market compared against sportsbook consensus."""
    poly_condition_id: str
    poly_yes_price: float
    poly_liquidity: float
    sportsbook_implied: float      # Sportsbook implied probability (0-1)
    price_discrepancy: float       # |poly_implied - sportsbook_implied|
    sport: str
    market_type: str
    player_name: str | None = None
    stat_type: str | None = None
    line: float | None = None
    team1: str | None = None
    team2: str | None = None
    question: str | None = None
    sportsbook_source: str = "consensus"
    extra: dict = field(default_factory=dict)


class MarketMatcher:
    """Matches Polymarket markets against Kalshi and sportsbook data."""

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
    # Kalshi Matching
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
        # Keyed by normalized tuples
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
    # Sportsbook Comparison
    # ------------------------------------------------------------------

    def match_sportsbook_markets(
        self,
        target_date: date,
        sport: str = "nba",
    ) -> list[SportsbookComparison]:
        """Compare all Polymarket markets against sportsbook consensus lines.

        For player props: joins against daily_predictions / mlb_daily_predictions
        to get sportsbook-implied probability (derived from odds).

        Args:
            target_date: Date to compare.
            sport: Sport ('nba' or 'mlb').

        Returns:
            List of SportsbookComparison instances where discrepancy >= 3%.
        """
        poly_rows = self._load_poly_all(target_date, sport)
        if not poly_rows:
            logger.info(f"No Polymarket markets for {target_date} {sport}")
            return []

        comparisons: list[SportsbookComparison] = []

        # Separate props vs game markets
        props = [m for m in poly_rows if m.get("market_type") == "player_prop"]
        others = [m for m in poly_rows if m.get("market_type") != "player_prop"]

        # Compare player props vs sportsbook
        if props:
            prop_comps = self._compare_props_vs_sportsbook(props, target_date, sport)
            comparisons.extend(prop_comps)

        # Compare game-level markets vs sportsbook (best-effort)
        if others:
            game_comps = self._compare_game_markets_vs_sportsbook(others, target_date, sport)
            comparisons.extend(game_comps)

        # Filter to meaningful discrepancies (>= 3%)
        min_disc = 0.03
        filtered = [c for c in comparisons if c.price_discrepancy >= min_disc]
        logger.info(
            f"Sportsbook comparison: {len(comparisons)} pairs, "
            f"{len(filtered)} with >= {min_disc:.0%} discrepancy"
        )
        return filtered

    def _compare_props_vs_sportsbook(
        self,
        poly_props: list[dict],
        target_date: date,
        sport: str,
    ) -> list[SportsbookComparison]:
        """Compare Polymarket player props against sportsbook consensus.

        Loads the most recent sportsbook odds from daily_predictions
        (NBA) or mlb_daily_predictions (MLB) and computes discrepancy.
        """
        # Build lookup from sportsbook predictions: (player_id, stat_type) -> implied_prob
        sb_lookup: dict[tuple, float] = {}
        try:
            sb_lookup = self._load_sportsbook_props(target_date, sport)
        except Exception as e:
            logger.warning(f"Could not load sportsbook props: {e}")

        comparisons = []
        for poly in poly_props:
            pid = poly.get("player_id")
            stat = poly.get("stat_type") or ""
            poly_yes_price = float(poly.get("yes_price") or 0)

            if poly_yes_price <= 0:
                continue

            poly_implied = poly_yes_price / 100.0

            # Try to find sportsbook implied prob
            sb_implied = None
            if pid:
                key = (pid, stat)
                sb_implied = sb_lookup.get(key)

            if sb_implied is None:
                continue

            discrepancy = abs(poly_implied - sb_implied)

            comparisons.append(SportsbookComparison(
                poly_condition_id=poly.get("condition_id", ""),
                poly_yes_price=poly_yes_price,
                poly_liquidity=float(poly.get("liquidity") or 0),
                sportsbook_implied=sb_implied,
                price_discrepancy=discrepancy,
                sport=sport,
                market_type="player_prop",
                player_name=poly.get("player_name"),
                stat_type=stat,
                line=_opt_float(poly.get("line")),
                question=poly.get("question"),
            ))

        return comparisons

    def _compare_game_markets_vs_sportsbook(
        self,
        poly_games: list[dict],  # noqa: ARG002
        target_date: date,  # noqa: ARG002
        sport: str,  # noqa: ARG002
    ) -> list[SportsbookComparison]:
        """Compare Polymarket game-level markets against sportsbook data.

        Best-effort: tries to match team names from Polymarket questions
        against data in raw_player_props_combined game columns.
        Game-level matching requires sport-specific team normalization — Phase 2.
        """
        return []

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

    def _load_poly_all(self, target_date: date, sport: str) -> list[dict]:
        """Load most recent Polymarket snapshot (all types) for target_date."""
        for days_back in range(4):
            check_date = target_date - timedelta(days=days_back)
            query = text("""
                SELECT DISTINCT ON (condition_id)
                    condition_id, player_name, stat_type, line, yes_price, no_price,
                    yes_bid, yes_ask, liquidity, player_id, market_type, team1, team2, question
                FROM polymarket_markets
                WHERE sport = :sport
                  AND snapshot_time::date = :target_date
                  AND market_status = 'open'
                ORDER BY condition_id, snapshot_time DESC
            """)
            with self.engine.connect() as conn:
                rows = conn.execute(query, {"sport": sport, "target_date": check_date}).fetchall()
            if rows:
                return [dict(row._mapping) for row in rows]
        return []

    def _load_sportsbook_props(self, target_date: date, sport: str) -> dict[tuple, float]:
        """Load sportsbook-implied probability for (player_id, stat_type) pairs.

        For NBA: uses daily_predictions (over_prob column).
        For MLB: uses mlb_daily_predictions (over_prob column).

        Returns:
            Dict mapping (player_id, stat_type) -> implied probability (0-1).
        """
        lookup: dict[tuple, float] = {}

        if sport == "nba":
            query = text("""
                SELECT DISTINCT ON (player_id, stat)
                    player_id, stat,
                    over_prob AS implied_prob
                FROM daily_predictions
                WHERE prediction_date = :target_date
                  AND player_id IS NOT NULL
                  AND over_prob IS NOT NULL
                ORDER BY player_id, stat, created_at DESC
            """)
        else:
            query = text("""
                SELECT DISTINCT ON (player_id, stat)
                    player_id, stat,
                    over_prob AS implied_prob
                FROM mlb_daily_predictions
                WHERE prediction_date = :target_date
                  AND player_id IS NOT NULL
                  AND over_prob IS NOT NULL
                ORDER BY player_id, stat, created_at DESC
            """)

        try:
            with self.engine.connect() as conn:
                rows = conn.execute(query, {"target_date": target_date}).fetchall()
            for row in rows:
                pid = row[0]
                stat = row[1] or ""
                prob = row[2]
                if pid and stat and prob is not None:
                    lookup[(pid, stat)] = float(prob)
        except Exception as e:
            logger.warning(f"Sportsbook props query failed: {e}")

        return lookup


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
