"""
Market Matcher
==============
Cross-platform market matching logic for Kalshi ↔ Polymarket player prop pairs.

Matching modes:
  - Exact: same player_id + stat_type + line
  - Near: same player_id + stat_type + line within 0.5
  - Fuzzy: name match + stat_type + line (when player_id is NULL on Poly side)
"""

import logging
from dataclasses import dataclass
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
