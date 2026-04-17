"""
Arbitrage Scanner
=================
Detects arbitrage opportunities between Kalshi and Polymarket prediction markets.

Arb types:
  pure arb:  net cost < 100 (guaranteed profit regardless of outcome)
  soft arb:  >= 5% price discrepancy between platforms

Usage:
    from src.arbitrage.arb_scanner import ArbScanner
    scanner = ArbScanner()
    result = scanner.scan(date.today(), "nba")
"""

import logging
from dataclasses import dataclass, field
from datetime import UTC, date, datetime

from sqlalchemy import text

logger = logging.getLogger(__name__)

# Thresholds
SOFT_ARB_THRESHOLD = 0.05       # 5% price discrepancy = soft arb
MIN_KALSHI_VOLUME = 20          # Minimum Kalshi volume for consideration
MIN_POLY_LIQUIDITY = 100.0      # Minimum Polymarket liquidity (USD)
MIN_KALSHI_BID = 3              # Min bid on the Kalshi side we want to buy (cents).
                                # YES bid < 3c or NO bid < 3c means no real market exists.


@dataclass
class ArbOpportunity:
    """A detected arbitrage opportunity between Kalshi and Polymarket."""
    sport: str
    arb_type: str               # 'pure' or 'soft'
    market_type: str
    player_name: str | None
    stat_type: str | None
    line: float | None
    poly_condition_id: str
    poly_side: str              # 'yes' or 'no'
    poly_price: float           # Cost in cents
    poly_liquidity: float
    poly_fee: float = 0.0
    kalshi_ticker: str | None = None
    kalshi_side: str | None = None
    kalshi_price: int | None = None
    kalshi_volume: int | None = None
    kalshi_fee: float | None = None
    combined_cost: float | None = None
    gross_margin: float | None = None
    net_margin: float | None = None
    price_discrepancy: float | None = None
    min_fillable: int | None = None
    estimated_profit: float | None = None
    extra: dict = field(default_factory=dict)


@dataclass
class ScanResult:
    """Results from a full arbitrage scan."""
    sport: str
    scan_date: date
    scan_time: datetime
    n_kalshi_matched: int
    pure_arbs: list[ArbOpportunity]
    soft_arbs: list[ArbOpportunity]
    total_opportunities: int
    best_margin: float | None = None
    errors: list[str] = field(default_factory=list)


class ArbScanner:
    """Detects and stores arbitrage opportunities between Kalshi and Polymarket."""

    def __init__(self, engine=None):
        """Initialize the scanner.

        Args:
            engine: SQLAlchemy engine (auto-created if None).
        """
        if engine is None:
            from src.db.client import get_engine
            engine = get_engine()
        self.engine = engine

    def scan(
        self,
        target_date: date,
        sport: str = "nba",
        dry_run: bool = False,
        include_game: bool = True,
        include_non_sports: bool = False,
    ) -> ScanResult:
        """Run a full arbitrage scan for the given date and sport.

        Args:
            target_date: Date to scan.
            sport: Sport to scan ('nba' or 'mlb').
            dry_run: Skip DB writes if True.
            include_game: Also match game-level markets (moneyline, NRFI, etc.).
            include_non_sports: Also match non-sports binary markets.

        Returns:
            ScanResult with all detected opportunities.
        """
        from src.arbitrage.market_matcher import MarketMatcher

        scan_time = datetime.now(UTC)
        errors: list[str] = []

        # Step 1: Match Kalshi ↔ Polymarket
        logger.info(f"Scanning {sport.upper()} arbs for {target_date}...")
        matcher = MarketMatcher(engine=self.engine)

        all_matched = []

        # Player prop matching (always on)
        try:
            prop_matched = matcher.match_kalshi_markets(target_date, sport)
            all_matched.extend(prop_matched)
        except Exception as e:
            logger.error(f"Kalshi prop matching failed: {e}")
            errors.append(f"kalshi_prop_match: {e}")

        # Game-level matching
        if include_game:
            try:
                game_matched = matcher.match_game_markets(target_date, sport)
                all_matched.extend(game_matched)
                if game_matched:
                    logger.info(f"Game-level matched: {len(game_matched)} pairs")
            except Exception as e:
                logger.warning(f"Game-level matching failed (non-fatal): {e}")
                errors.append(f"game_match: {e}")

        # Non-sports matching
        if include_non_sports:
            try:
                ns_matched = matcher.match_non_sports_markets()
                all_matched.extend(ns_matched)
                if ns_matched:
                    logger.info(f"Non-sports matched: {len(ns_matched)} pairs")
            except Exception as e:
                logger.warning(f"Non-sports matching failed (non-fatal): {e}")
                errors.append(f"non_sports_match: {e}")

        kalshi_matched = all_matched  # for ScanResult.n_kalshi_matched

        # Step 2: Detect arbs from matched pairs
        pure_arbs: list[ArbOpportunity] = []
        soft_arbs: list[ArbOpportunity] = []

        for pair in kalshi_matched:
            opps = self._detect_kalshi_arbs(pair)
            for opp in opps:
                if opp.arb_type == "pure":
                    pure_arbs.append(opp)
                else:
                    soft_arbs.append(opp)

        # Sort by best margin/discrepancy
        pure_arbs.sort(key=lambda o: o.net_margin or 0, reverse=True)
        soft_arbs.sort(key=lambda o: abs(o.price_discrepancy or 0), reverse=True)

        all_opps = pure_arbs + soft_arbs

        # Step 3: Store to DB
        if not dry_run and all_opps:
            try:
                stored = self._store_opportunities(all_opps, scan_time)
                logger.info(f"Stored {stored} arb opportunities")
            except Exception as e:
                logger.error(f"Failed to store arb opportunities: {e}")
                errors.append(f"store: {e}")

        best_margin = None
        if pure_arbs:
            best_margin = max(o.net_margin or 0 for o in pure_arbs)
        elif soft_arbs:
            best_margin = max(abs(o.price_discrepancy or 0) for o in soft_arbs)

        result = ScanResult(
            sport=sport,
            scan_date=target_date,
            scan_time=scan_time,
            n_kalshi_matched=len(kalshi_matched),
            pure_arbs=pure_arbs,
            soft_arbs=soft_arbs,
            total_opportunities=len(all_opps),
            best_margin=best_margin,
            errors=errors,
        )

        logger.info(
            f"Scan complete: {len(pure_arbs)} pure arbs, {len(soft_arbs)} soft arbs"
        )
        return result

    # ------------------------------------------------------------------
    # Arb Detection
    # ------------------------------------------------------------------

    def _detect_kalshi_arbs(self, pair) -> list[ArbOpportunity]:
        """Detect arb opportunities from a Kalshi↔Poly matched pair.

        Checks both directions:
          Direction A: Buy Kalshi YES + Buy Poly NO (combined cost = k_yes + poly_no)
          Direction B: Buy Kalshi NO + Buy Poly YES (combined cost = k_no + poly_yes)

        For game markets, skips pairs where the game has already started to avoid
        stale-price contamination from live/post-game states.

        Args:
            pair: MatchedMarket instance.

        Returns:
            List of ArbOpportunity (0, 1, or 2 per pair).
        """
        from src.scrapers.kalshi.kalshi_utils import kalshi_taker_fee
        from src.scrapers.polymarket.polymarket_utils import polymarket_fee

        opps = []

        # Pre-game filter for game-level markets
        # Kalshi tickers encode game start time: KXMLBGAME-26APR162040SEASD → 20:40 ET
        # Skip if the game has already started (live/post-game prices are stale on Poly).
        if getattr(pair, "market_type", "player_prop") != "player_prop":
            ticker = getattr(pair, "kalshi_ticker", "") or ""
            if ticker:
                from src.arbitrage.market_matcher import (
                    _extract_date_from_kalshi_ticker,
                    _extract_time_from_kalshi_ticker,
                )
                game_date_val = _extract_date_from_kalshi_ticker(ticker)
                game_time_val = _extract_time_from_kalshi_ticker(ticker)
                if game_date_val is not None and game_time_val is not None:
                    import zoneinfo
                    et_tz = zoneinfo.ZoneInfo("America/New_York")
                    game_start_et = datetime(
                        game_date_val.year, game_date_val.month, game_date_val.day,
                        game_time_val[0], game_time_val[1], tzinfo=et_tz,
                    )
                    now_et = datetime.now(et_tz)
                    if now_et >= game_start_et:
                        return []  # Game has started or ended — skip

        k_yes = pair.kalshi_yes_price
        k_no = pair.kalshi_no_price or (100 - k_yes)
        p_yes = pair.poly_yes_price
        p_no = pair.poly_no_price or (100 - p_yes)

        # Bid available on each Kalshi side (NO bid ≈ 100 - YES ask)
        k_yes_bid = pair.kalshi_yes_bid or 0
        k_no_bid = max(100 - (pair.kalshi_yes_ask or 100), 0)

        # Liquidity checks
        if pair.kalshi_volume < MIN_KALSHI_VOLUME:
            return []
        if pair.poly_liquidity < MIN_POLY_LIQUIDITY:
            return []

        # Direction A: Buy Kalshi YES + Buy Poly NO
        k_fee_a = kalshi_taker_fee(k_yes) * 100  # convert $ to cents
        p_fee_a = polymarket_fee() * 100
        combined_a = k_yes + p_no
        gross_a = 100 - combined_a
        net_a = gross_a - k_fee_a - p_fee_a

        # Direction B: Buy Kalshi NO + Buy Poly YES
        k_fee_b = kalshi_taker_fee(k_no) * 100
        p_fee_b = polymarket_fee() * 100
        combined_b = k_no + p_yes
        gross_b = 100 - combined_b
        net_b = gross_b - k_fee_b - p_fee_b

        # Price discrepancy (regardless of arb)
        k_implied = k_yes / 100.0
        p_implied = p_yes / 100.0
        discrepancy = abs(k_implied - p_implied)

        # Depth on thinner side (rough estimate)
        min_depth = min(pair.kalshi_volume, int(pair.poly_liquidity / max(p_yes, 1)))

        for direction, combined, gross, net, k_side, p_side, k_price_for_fee, p_price, k_bid in [
            ("A", combined_a, gross_a, net_a, "yes", "no", k_yes, p_no, k_yes_bid),
            ("B", combined_b, gross_b, net_b, "no", "yes", k_no, p_yes, k_no_bid),
        ]:
            # Skip if no real bid exists on the Kalshi side we'd be buying.
            # A bid of 1-2c means no actual buyer — the price is a stale placeholder.
            if k_bid < MIN_KALSHI_BID:
                continue

            is_pure = net > 0
            is_soft = discrepancy >= SOFT_ARB_THRESHOLD and not is_pure

            if not (is_pure or is_soft):
                continue

            k_fee_dollars = kalshi_taker_fee(k_price_for_fee)
            est_profit = net * min_depth / 100 if is_pure and min_depth else None

            opp = ArbOpportunity(
                sport=pair.sport,
                arb_type="pure" if is_pure else "soft",
                market_type=getattr(pair, "market_type", "player_prop"),
                player_name=pair.player_name,
                stat_type=pair.stat_type,
                line=pair.line,
                poly_condition_id=pair.poly_condition_id,
                poly_side=p_side,
                poly_price=p_price,
                poly_liquidity=pair.poly_liquidity,
                poly_fee=0.0,
                kalshi_ticker=pair.kalshi_ticker,
                kalshi_side=k_side,
                kalshi_price=int(k_yes if k_side == "yes" else k_no),
                kalshi_volume=pair.kalshi_volume,
                kalshi_fee=k_fee_dollars,
                combined_cost=combined,
                gross_margin=gross,
                net_margin=net,
                price_discrepancy=discrepancy,
                min_fillable=min_depth,
                estimated_profit=est_profit,
                extra={
                    "direction": direction,
                    "match_type": pair.match_type,
                    "team1": getattr(pair, "team1", None),
                    "team2": getattr(pair, "team2", None),
                    "description": getattr(pair, "description", None),
                },
            )
            opps.append(opp)

        return opps

    # ------------------------------------------------------------------
    # DB Storage
    # ------------------------------------------------------------------

    def _store_opportunities(
        self,
        opportunities: list[ArbOpportunity],
        scan_time: datetime,
    ) -> int:
        """Bulk insert arb opportunities into arb_opportunities table.

        Args:
            opportunities: List of ArbOpportunity instances.
            scan_time: Timestamp of this scan.

        Returns:
            Number of rows inserted.
        """
        stmt = text("""
            INSERT INTO arb_opportunities (
                scan_time, sport, arb_type, market_type, player_name, stat_type, line,
                team1, team2, description,
                kalshi_ticker, kalshi_side, kalshi_price, kalshi_volume, kalshi_fee,
                poly_condition_id, poly_side, poly_price, poly_liquidity, poly_fee,
                combined_cost, gross_margin, net_margin,
                price_discrepancy, min_fillable, estimated_profit, status
            ) VALUES (
                :scan_time, :sport, :arb_type, :market_type, :player_name, :stat_type, :line,
                :team1, :team2, :description,
                :kalshi_ticker, :kalshi_side, :kalshi_price, :kalshi_volume, :kalshi_fee,
                :poly_condition_id, :poly_side, :poly_price, :poly_liquidity, :poly_fee,
                :combined_cost, :gross_margin, :net_margin,
                :price_discrepancy, :min_fillable, :estimated_profit, 'detected'
            )
        """)

        count = 0
        with self.engine.begin() as conn:
            for opp in opportunities:
                conn.execute(stmt, {
                    "scan_time": scan_time,
                    "sport": opp.sport,
                    "arb_type": opp.arb_type,
                    "market_type": opp.market_type,
                    "player_name": opp.player_name,
                    "stat_type": opp.stat_type,
                    "line": opp.line,
                    "team1": opp.extra.get("team1"),
                    "team2": opp.extra.get("team2"),
                    "description": opp.extra.get("description"),
                    "kalshi_ticker": opp.kalshi_ticker,
                    "kalshi_side": opp.kalshi_side,
                    "kalshi_price": opp.kalshi_price,
                    "kalshi_volume": opp.kalshi_volume,
                    "kalshi_fee": opp.kalshi_fee,
                    "poly_condition_id": opp.poly_condition_id,
                    "poly_side": opp.poly_side,
                    "poly_price": opp.poly_price,
                    "poly_liquidity": opp.poly_liquidity,
                    "poly_fee": opp.poly_fee,
                    "combined_cost": opp.combined_cost,
                    "gross_margin": opp.gross_margin,
                    "net_margin": opp.net_margin,
                    "price_discrepancy": opp.price_discrepancy,
                    "min_fillable": opp.min_fillable,
                    "estimated_profit": opp.estimated_profit,
                })
                count += 1
        return count
