"""
Arbitrage Scanner
=================
Detects arbitrage opportunities and mispricings between:
  1. Kalshi ↔ Polymarket (player prop cross-platform arb)
  2. Polymarket ↔ sportsbook consensus (market intel)

Arb types:
  pure arb:           net cost < 100 (guaranteed profit regardless of outcome)
  soft arb:           >= 5% price discrepancy between platforms
  sportsbook_mispricing: Polymarket price differs >= 8% from sportsbook consensus

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
SPORTSBOOK_DISC_THRESHOLD = 0.08  # 8% discrepancy = sportsbook mispricing alert
MIN_KALSHI_VOLUME = 20          # Minimum Kalshi volume for consideration
MIN_POLY_LIQUIDITY = 100.0      # Minimum Polymarket liquidity (USD)


@dataclass
class ArbOpportunity:
    """A detected arbitrage or mispricing opportunity."""
    sport: str
    arb_type: str               # 'pure', 'soft', 'sportsbook_mispricing'
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
    sportsbook_implied: float | None = None
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
    n_poly_sportsbook: int
    pure_arbs: list[ArbOpportunity]
    soft_arbs: list[ArbOpportunity]
    sportsbook_mispricings: list[ArbOpportunity]
    total_opportunities: int
    best_margin: float | None = None
    errors: list[str] = field(default_factory=list)


class ArbScanner:
    """Detects and stores arbitrage opportunities across prediction markets."""

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
    ) -> ScanResult:
        """Run a full arbitrage scan for the given date and sport.

        Args:
            target_date: Date to scan.
            sport: Sport to scan ('nba' or 'mlb').
            dry_run: Skip DB writes if True.

        Returns:
            ScanResult with all detected opportunities.
        """
        from src.arbitrage.market_matcher import MarketMatcher

        scan_time = datetime.now(UTC)
        errors: list[str] = []

        # Step 1: Match Kalshi ↔ Polymarket
        logger.info(f"Scanning {sport.upper()} arbs for {target_date}...")
        matcher = MarketMatcher(engine=self.engine)

        kalshi_matched = []
        try:
            kalshi_matched = matcher.match_kalshi_markets(target_date, sport)
        except Exception as e:
            logger.error(f"Kalshi matching failed: {e}")
            errors.append(f"kalshi_match: {e}")

        # Step 2: Match Polymarket ↔ sportsbook
        sb_comparisons = []
        try:
            sb_comparisons = matcher.match_sportsbook_markets(target_date, sport)
        except Exception as e:
            logger.error(f"Sportsbook comparison failed: {e}")
            errors.append(f"sb_comparison: {e}")

        # Step 3: Detect arbs from Kalshi matches
        pure_arbs: list[ArbOpportunity] = []
        soft_arbs: list[ArbOpportunity] = []

        for pair in kalshi_matched:
            opps = self._detect_kalshi_arbs(pair)
            for opp in opps:
                if opp.arb_type == "pure":
                    pure_arbs.append(opp)
                else:
                    soft_arbs.append(opp)

        # Step 4: Detect mispricings from sportsbook comparisons
        sb_mispricings: list[ArbOpportunity] = []
        for comp in sb_comparisons:
            opp = self._detect_sportsbook_mispricing(comp)
            if opp:
                sb_mispricings.append(opp)

        # Sort by best margin/discrepancy
        pure_arbs.sort(key=lambda o: o.net_margin or 0, reverse=True)
        soft_arbs.sort(key=lambda o: abs(o.price_discrepancy or 0), reverse=True)
        sb_mispricings.sort(key=lambda o: o.price_discrepancy or 0, reverse=True)

        all_opps = pure_arbs + soft_arbs + sb_mispricings

        # Step 5: Store to DB
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
            n_poly_sportsbook=len(sb_comparisons),
            pure_arbs=pure_arbs,
            soft_arbs=soft_arbs,
            sportsbook_mispricings=sb_mispricings,
            total_opportunities=len(all_opps),
            best_margin=best_margin,
            errors=errors,
        )

        logger.info(
            f"Scan complete: {len(pure_arbs)} pure arbs, "
            f"{len(soft_arbs)} soft arbs, "
            f"{len(sb_mispricings)} sportsbook mispricings"
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

        Args:
            pair: MatchedMarket instance.

        Returns:
            List of ArbOpportunity (0, 1, or 2 per pair).
        """
        from src.scrapers.kalshi.kalshi_utils import kalshi_taker_fee
        from src.scrapers.polymarket.polymarket_utils import polymarket_fee

        opps = []

        k_yes = pair.kalshi_yes_price
        k_no = pair.kalshi_no_price or (100 - k_yes)
        p_yes = pair.poly_yes_price
        p_no = pair.poly_no_price or (100 - p_yes)

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

        for direction, combined, gross, net, k_side, p_side, k_price_for_fee, p_price in [
            ("A", combined_a, gross_a, net_a, "yes", "no", k_yes, p_no),
            ("B", combined_b, gross_b, net_b, "no", "yes", k_no, p_yes),
        ]:
            is_pure = net > 0
            is_soft = discrepancy >= SOFT_ARB_THRESHOLD and not is_pure

            if not (is_pure or is_soft):
                continue

            k_fee_dollars = kalshi_taker_fee(k_price_for_fee)
            est_profit = net * min_depth / 100 if is_pure and min_depth else None

            opp = ArbOpportunity(
                sport=pair.sport,
                arb_type="pure" if is_pure else "soft",
                market_type="player_prop",
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
                extra={"direction": direction, "match_type": pair.match_type},
            )
            opps.append(opp)

        return opps

    def _detect_sportsbook_mispricing(self, comp) -> ArbOpportunity | None:
        """Convert a SportsbookComparison into an ArbOpportunity if threshold met.

        Args:
            comp: SportsbookComparison instance.

        Returns:
            ArbOpportunity if discrepancy >= threshold, else None.
        """
        if comp.price_discrepancy < SPORTSBOOK_DISC_THRESHOLD:
            return None

        poly_implied = comp.poly_yes_price / 100.0
        sb_implied = comp.sportsbook_implied

        # Determine which side looks favorable
        if poly_implied > sb_implied:
            # Poly overpriced → buy NO (poly thinks it's more likely than sportsbook)
            poly_side = "no"
            poly_price = 100 - comp.poly_yes_price
        else:
            # Poly underpriced → buy YES
            poly_side = "yes"
            poly_price = comp.poly_yes_price

        return ArbOpportunity(
            sport=comp.sport,
            arb_type="sportsbook_mispricing",
            market_type=comp.market_type,
            player_name=comp.player_name,
            stat_type=comp.stat_type,
            line=comp.line,
            poly_condition_id=comp.poly_condition_id,
            poly_side=poly_side,
            poly_price=poly_price,
            poly_liquidity=comp.poly_liquidity,
            poly_fee=0.0,
            sportsbook_implied=sb_implied,
            price_discrepancy=comp.price_discrepancy,
        )

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
                kalshi_ticker, kalshi_side, kalshi_price, kalshi_volume, kalshi_fee,
                poly_condition_id, poly_side, poly_price, poly_liquidity, poly_fee,
                sportsbook_implied, combined_cost, gross_margin, net_margin,
                price_discrepancy, min_fillable, estimated_profit, status
            ) VALUES (
                :scan_time, :sport, :arb_type, :market_type, :player_name, :stat_type, :line,
                :kalshi_ticker, :kalshi_side, :kalshi_price, :kalshi_volume, :kalshi_fee,
                :poly_condition_id, :poly_side, :poly_price, :poly_liquidity, :poly_fee,
                :sportsbook_implied, :combined_cost, :gross_margin, :net_margin,
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
                    "sportsbook_implied": opp.sportsbook_implied,
                    "combined_cost": opp.combined_cost,
                    "gross_margin": opp.gross_margin,
                    "net_margin": opp.net_margin,
                    "price_discrepancy": opp.price_discrepancy,
                    "min_fillable": opp.min_fillable,
                    "estimated_profit": opp.estimated_profit,
                })
                count += 1
        return count
