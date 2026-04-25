"""
Kalshi Live Trading Module
==========================
Places real taker orders on Kalshi prediction markets using model edges.

Strategy: Sniper Mode — 15%+ fee-adjusted edge, taker orders (instant fill),
Kelly sizing, circuit breakers (drawdown, daily loss, consecutive losses).

CRITICAL: KALSHI_LIVE_TRADING_ENABLED must be explicitly "true" to place orders.

Configuration via environment variables:
    KALSHI_LIVE_TRADING_ENABLED: Must be "true" to trade (default: false)
    KALSHI_LIVE_STARTING_BANKROLL: Starting bankroll in dollars (default: 100)
    KALSHI_LIVE_KELLY_FRACTION: Kelly fraction (default: 0.125)
    KALSHI_LIVE_MIN_EDGE: Minimum fee-adjusted edge (default: 0.15)
    KALSHI_LIVE_MAX_CONTRACTS: Max contracts per market (default: 50)
    KALSHI_LIVE_MAX_DAILY_EXPOSURE: Max $ exposure per day (default: 30)
    KALSHI_LIVE_DRAWDOWN_LIMIT: Halt at this drawdown % (default: 0.30)
    KALSHI_LIVE_DAILY_LOSS_LIMIT: Daily loss limit in $ (default: 15)
    KALSHI_LIVE_CONSEC_LOSS_LIMIT: Pause after N consecutive losses (default: 5)
"""

from __future__ import annotations

import logging
import math
import os
from datetime import date, datetime
from typing import Any

import pandas as pd
from sqlalchemy import text

from src.db.client import get_engine
from src.models.daily_runner import should_skip_recommendation
from src.scrapers.kalshi.kalshi_client import KalshiClient
from src.scrapers.kalshi.kalshi_utils import fee_adjusted_edge, kalshi_taker_fee

logger = logging.getLogger(__name__)

# When a non-sportsbook-aligned Kalshi line beats the aligned line by more
# than this gap, allow the override.  Otherwise prefer the aligned line.
_SPORTSBOOK_LINE_FALLBACK_GAP = 0.08  # 8 percentage points

KALSHI_SWEEP_MAX_CENTS = _env_int("KALSHI_SWEEP_MAX_CENTS", 10)
KALSHI_SWEEP_EDGE_RETENTION = _env_float("KALSHI_SWEEP_EDGE_RETENTION", 0.50)

# Supported stat types per sport (skip markets without trained models)
SUPPORTED_STATS: dict[str, set[str]] = {
    "nba": {"pts", "reb", "ast", "pra", "pr", "pa", "ra", "stl", "blk", "3pm"},
    "mlb": {"pitcher_strikeouts", "batter_hits", "batter_hrr"},
}


def _env_float(name: str, default: float) -> float:
    val = os.environ.get(name)
    if val is None:
        return default
    try:
        return float(val)
    except ValueError:
        logger.warning(f"Invalid {name}={val}, using default {default}")
        return default


def _env_int(name: str, default: int) -> int:
    val = os.environ.get(name)
    if val is None:
        return default
    try:
        return int(val)
    except ValueError:
        logger.warning(f"Invalid {name}={val}, using default {default}")
        return default


class KalshiLiveTrader:
    """Places real taker orders on Kalshi with circuit breaker protection."""

    def __init__(self, resolve_only: bool = False):
        # Gate: must be explicitly enabled (skipped for resolve-only mode)
        if not resolve_only:
            enabled = os.getenv("KALSHI_LIVE_TRADING_ENABLED", "false").lower()
            if enabled != "true":
                raise RuntimeError(
                    "KALSHI_LIVE_TRADING_ENABLED is not 'true'. "
                    "Set KALSHI_LIVE_TRADING_ENABLED=true to enable live trading."
                )

        # Configuration
        self.starting_bankroll = _env_float("KALSHI_LIVE_STARTING_BANKROLL", 100.0)
        self.kelly_fraction = _env_float("KALSHI_LIVE_KELLY_FRACTION", 0.125)
        self.min_edge = _env_float("KALSHI_LIVE_MIN_EDGE", 0.15)
        self.max_contracts = _env_int("KALSHI_LIVE_MAX_CONTRACTS", 50)
        self.max_daily_exposure = _env_float("KALSHI_LIVE_MAX_DAILY_EXPOSURE", 80.0)
        self.min_price = _env_int("KALSHI_LIVE_MIN_PRICE", 5)
        self.drawdown_limit = _env_float("KALSHI_LIVE_DRAWDOWN_LIMIT", 0.30)
        self.daily_loss_limit = _env_float("KALSHI_LIVE_DAILY_LOSS_LIMIT", 15.0)
        self.consec_loss_limit = _env_int("KALSHI_LIVE_CONSEC_LOSS_LIMIT", 5)

        # Initialize API client + DB
        self.client = KalshiClient()
        if not self.client.is_authenticated:
            raise RuntimeError("Kalshi API credentials not configured for live trading")
        self.engine = get_engine()

        # Ensure config row exists
        self._ensure_config()

        logger.info(
            f"KalshiLiveTrader initialized: bankroll=${self.starting_bankroll:.0f}, "
            f"kelly={self.kelly_fraction}, min_edge={self.min_edge:.0%}, "
            f"max_contracts={self.max_contracts}, drawdown_limit={self.drawdown_limit:.0%}"
        )

    # ------------------------------------------------------------------
    # Config helpers
    # ------------------------------------------------------------------

    def _ensure_config(self) -> None:
        """Ensure the singleton config row exists."""
        with self.engine.connect() as conn:
            result = conn.execute(
                text("SELECT id FROM kalshi_live_trading_config WHERE id = 1")
            ).fetchone()
            if result is None:
                conn.execute(text("""
                    INSERT INTO kalshi_live_trading_config (id, starting_bankroll, hwm_dollars)
                    VALUES (1, :bankroll, :bankroll)
                """), {"bankroll": self.starting_bankroll})
                conn.commit()

    def _get_config(self) -> dict:
        """Read the singleton config row."""
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT * FROM kalshi_live_trading_config WHERE id = 1")
            ).fetchone()
        if row is None:
            return {"is_halted": False, "starting_bankroll": self.starting_bankroll}
        return dict(row._mapping)

    def _set_halted(self, reason: str) -> None:
        """Set the trading halt flag in config."""
        with self.engine.connect() as conn:
            conn.execute(text("""
                UPDATE kalshi_live_trading_config
                SET is_halted = true,
                    halt_reason = :reason,
                    halted_at = now(),
                    last_updated = now()
                WHERE id = 1
            """), {"reason": reason})
            conn.commit()
        logger.warning(f"TRADING HALTED: {reason}")

    def _update_streak(self, streak_count: int) -> None:
        """Update the consecutive loss streak count."""
        with self.engine.connect() as conn:
            conn.execute(text("""
                UPDATE kalshi_live_trading_config
                SET streak_count = :count, last_updated = now()
                WHERE id = 1
            """), {"count": streak_count})
            conn.commit()

    # ------------------------------------------------------------------
    # Circuit breakers
    # ------------------------------------------------------------------

    def check_circuit_breakers(self) -> tuple[bool, str]:
        """Check all circuit breakers before trading.

        Returns:
            (can_trade, reason_if_halted)
        """
        config = self._get_config()

        # 0. Manual halt flag
        if config.get("is_halted"):
            # Check for force resume
            if os.getenv("KALSHI_LIVE_FORCE_RESUME", "").lower() == "true":
                with self.engine.connect() as conn:
                    conn.execute(text("""
                        UPDATE kalshi_live_trading_config
                        SET is_halted = false, halt_reason = null,
                            halted_at = null, last_updated = now()
                        WHERE id = 1
                    """))
                    conn.commit()
                logger.info("Force resume: cleared halt flag")
            else:
                reason = config.get("halt_reason", "Manual halt")
                return False, f"Halted: {reason}"

        # 1. Drawdown breaker
        balance_data = self.client.get_balance()
        if balance_data is None:
            logger.error(
                "CIRCUIT BREAKER ABORT: get_balance() returned None — "
                "portfolio API unreachable or API key lacks portfolio permissions."
            )
            return False, "Cannot check balance — API error"

        # Use total portfolio value (cash + open positions), not just cash.
        # Using cash alone would false-trigger after placing many open positions.
        balance_cents = balance_data.get("balance", 0)
        portfolio_cents = balance_data.get("portfolio_value", 0)
        total_dollars = (balance_cents + portfolio_cents) / 100.0
        balance_dollars = balance_cents / 100.0  # kept for logging / select_trades bankroll

        # High-water mark drawdown: ratchet up on new highs, halt if we fall
        # too far from peak. Avoids the static-anchor problem where a bad day
        # permanently anchors the drawdown check to the original starting balance.
        hwm = float(config.get("hwm_dollars") or total_dollars)
        if total_dollars > hwm:
            hwm = total_dollars
            with self.engine.begin() as conn:
                conn.execute(text("""
                    UPDATE kalshi_live_trading_config
                    SET hwm_dollars = :hwm, last_updated = now()
                    WHERE id = 1
                """), {"hwm": hwm})
            logger.info(f"New portfolio HWM: ${hwm:.2f}")

        min_balance = hwm * (1 - self.drawdown_limit)
        logger.info(
            f"Drawdown check: portfolio ${total_dollars:.2f} vs HWM ${hwm:.2f} "
            f"(floor ${min_balance:.2f} = {self.drawdown_limit:.0%} drawdown)"
        )

        if total_dollars < min_balance:
            reason = (
                f"Drawdown limit reached: portfolio ${total_dollars:.2f} "
                f"(cash ${balance_dollars:.2f} + positions ${portfolio_cents/100:.2f}) "
                f"< ${min_balance:.2f} ({self.drawdown_limit:.0%} from HWM ${hwm:.2f})"
            )
            self._set_halted(reason)
            self._send_circuit_breaker_alert(reason, balance_dollars, "All trading HALTED. Manual review required.")
            return False, reason

        # 2. Daily loss limit
        today = date.today()
        daily_pnl = self._get_daily_pnl(today)
        if daily_pnl < -self.daily_loss_limit:
            reason = (
                f"Daily loss limit reached: ${daily_pnl:.2f} "
                f"(limit: -${self.daily_loss_limit:.0f})"
            )
            self._send_circuit_breaker_alert(reason, balance_dollars, "Pausing until tomorrow.")
            return False, reason

        # 3. Consecutive loss streak
        streak = self._get_consecutive_losses()
        if streak >= self.consec_loss_limit:
            reason = f"{streak} consecutive losses (limit: {self.consec_loss_limit})"
            self._send_circuit_breaker_alert(reason, balance_dollars, "Pausing for review.")
            return False, reason

        return True, ""

    def _get_daily_pnl(self, target_date: date) -> float:
        """Sum today's realized P&L from kalshi_live_orders."""
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT COALESCE(SUM(pnl), 0)
                FROM kalshi_live_orders
                WHERE game_date = :d AND status IN ('won', 'lost')
            """), {"d": target_date}).scalar()
        return float(result or 0)

    def _get_consecutive_losses(self) -> int:
        """Count consecutive losses from most recent resolved trades."""
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT status
                FROM kalshi_live_orders
                WHERE status IN ('won', 'lost')
                ORDER BY resolved_at DESC
                LIMIT :limit
            """), {"limit": self.consec_loss_limit}).fetchall()

        streak = 0
        for row in rows:
            if row[0] == "lost":
                streak += 1
            else:
                break
        return streak

    def _send_circuit_breaker_alert(self, reason: str, balance: float, action: str) -> None:
        """Send Discord alert for circuit breaker trigger."""
        try:
            from src.discord_bot.alerts import send_kalshi_trade_alert_sync
            send_kalshi_trade_alert_sync("circuit_breaker", {
                "reason": reason,
                "balance": balance,
                "action": action,
            })
        except Exception as e:
            logger.warning(f"Failed to send circuit breaker alert: {e}")

    # ------------------------------------------------------------------
    # Kelly sizing (taker fees)
    # ------------------------------------------------------------------

    def _get_best_available_price(self, ticker: str, side: str, target_cents: int) -> int | None:
        try:
            ob = self.client.get_orderbook(ticker, depth=10)
            if ob is None:
                return None

            orderbook = ob.get("orderbook", {})

            if side == "yes":
                no_bids = orderbook.get("no", [])
                for no_bid, qty in no_bids:
                    yes_ask = 100 - no_bid
                    if yes_ask >= target_cents and qty > 0:
                        return yes_ask
                return None
            else:
                yes_bids = orderbook.get("yes", [])
                for yes_bid, qty in yes_bids:
                    if yes_bid <= target_cents and qty > 0:
                        return yes_bid
                return None
        except Exception as e:
            logger.warning(f"Orderbook query failed for {ticker}: {e}")
            return None

    def _kelly_contracts(
        self, model_prob: float, price_cents: int, side: str, bankroll: float,
    ) -> int:
        """Calculate Kelly-optimal contracts using taker fees."""
        if side == "yes":
            cost_per = price_cents / 100.0
            win_per = (100 - price_cents) / 100.0
            prob = model_prob
        else:
            cost_per = (100 - price_cents) / 100.0
            win_per = price_cents / 100.0
            prob = 1.0 - model_prob

        if win_per <= 0 or cost_per <= 0:
            return 0

        # Deduct taker fee from effective return
        fee_per = kalshi_taker_fee(price_cents if side == "yes" else 100 - price_cents)
        net_win_per = win_per - fee_per
        if net_win_per <= 0:
            return 0

        f_net = (prob - cost_per) / net_win_per
        f_fractional = f_net * self.kelly_fraction
        if f_fractional <= 0:
            return 0

        contracts = int(math.floor(f_fractional * bankroll / cost_per))
        return min(contracts, self.max_contracts)

    # ------------------------------------------------------------------
    # Trade selection
    # ------------------------------------------------------------------

    def select_trades(self, target_date: date, sport: str = "nba", prior_exposure: float = 0.0) -> list[dict[str, Any]]:
        """Select trades from kalshi_markets with sufficient taker-fee-adjusted edge.

        Uses real API balance, checks existing positions, enforces all limits.

        MLB priority rule: to ensure MLB trades take precedence over NBA within the
        shared daily exposure cap, call with sport="mlb" first, then pass the
        resulting total cost as prior_exposure= when calling with sport="nba".
        """
        # Per-sport trading gate: refuse to trade a sport that's disabled
        sport_gate_var = f"{sport.upper()}_TRADING_ENABLED"
        if os.getenv(sport_gate_var, "true").lower() != "true":
            logger.info(f"Live trading disabled for {sport} ({sport_gate_var}=false)")
            return []

        # Get real balance from API
        balance_data = self.client.get_balance()
        if balance_data is None:
            logger.error(
                "LIVE TRADE ABORT: get_balance() returned None — "
                "Kalshi portfolio API unreachable or API key lacks portfolio permissions. "
                "Check KALSHI_API_KEY scope and account status."
            )
            return []
        bankroll = balance_data.get("balance", 0) / 100.0
        logger.info(f"Kalshi balance: ${bankroll:.2f}")

        # Bankroll-proportional exposure cap
        daily_exposure_pct = _env_float("KALSHI_DAILY_EXPOSURE_PCT", 0.60)
        min_exposure = _env_float("KALSHI_MIN_DAILY_EXPOSURE", 80.0)
        max_exposure = _env_float("KALSHI_MAX_DAILY_EXPOSURE", 500.0)
        dynamic_cap = bankroll * daily_exposure_pct
        effective_cap = max(min_exposure, min(dynamic_cap, max_exposure))
        # NO-only mode control
        _allow_yes = os.environ.get("KALSHI_ALLOW_YES_BETS", "false").lower() == "true"
        _mode_str = "YES+NO" if _allow_yes else "NO-only"
        logger.info(
            f"Mode: {_mode_str} | Exposure cap: ${bankroll:.2f} × {daily_exposure_pct:.0%} "
            f"= ${dynamic_cap:.2f} → effective ${effective_cap:.2f} "
            f"(floor ${min_exposure:.0f}, ceiling ${max_exposure:.0f})"
        )

        # Get current open positions to avoid over-accumulation
        positions = self.client.get_positions(settlement_status="open")
        held_tickers = {p.get("ticker"): p for p in positions}

        # Get edge candidates from latest snapshot
        query = text("""
            SELECT DISTINCT ON (ticker)
                ticker, sport, player_id, player_name, stat_type, line,
                yes_price, model_prob, kalshi_implied,
                raw_edge, maker_fee_adjusted_edge,
                volume, bid_ask_spread, market_status,
                bl_model_prob, bl_edge,
                sportsbook_consensus_line, line_vs_sportsbook
            FROM kalshi_markets
            WHERE sport = :sport
              AND (snapshot_time AT TIME ZONE 'America/New_York')::date = :target_date
              AND market_status = 'open'
              AND model_prob IS NOT NULL
            ORDER BY ticker, snapshot_time DESC
        """)

        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"sport": sport, "target_date": target_date})

        if df.empty:
            logger.info(f"No Kalshi markets with model probs for {target_date}")
            return []

        # Check what we already traded today (by player+stat)
        today_positions: set[tuple[int, str]] = set()
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT DISTINCT player_id, stat_type
                FROM kalshi_live_orders
                WHERE game_date = :d AND status != 'cancelled'
                  AND player_id IS NOT NULL
            """), {"d": target_date}).fetchall()
            today_positions = {(int(row[0]), row[1]) for row in rows}

        # Skip player+stat combos already sitting in the approval queue
        with self.engine.connect() as conn:
            queued_rows = conn.execute(text("""
                SELECT player_id, stat_type FROM kalshi_trade_queue
                WHERE game_date = :d AND sport = :sport
                  AND status = 'pending_approval'
                  AND expires_at > now()
                  AND player_id IS NOT NULL
            """), {"d": target_date, "sport": sport}).fetchall()
        queued_player_stats: set[tuple[int, str]] = {(int(r[0]), r[1]) for r in queued_rows}
        if queued_player_stats:
            logger.info(f"Skipping {len(queued_player_stats)} player+stat combos already pending approval")

        # Shared cross-sport exposure cap: ALL sports share one daily pool.
        # MLB fires first (minute :00) and gets full access. NBA fires at :02
        # and sees what's left after MLB. prior_exposure can also be passed in.
        total_exposure = 0.0
        with self.engine.connect() as conn:
            existing_exposure = conn.execute(text("""
                SELECT COALESCE(SUM(total_cost), 0)
                FROM kalshi_live_orders
                WHERE game_date = :d AND status != 'cancelled'
            """), {"d": target_date}).scalar()
        total_exposure = float(existing_exposure or 0) + prior_exposure

        # First pass: find best-edge candidate per (player_id, stat_type)
        run_candidates: dict[tuple[int, str], dict] = {}
        _pool_no_edges: list[float] = []  # All NO edges (for pool logging)

        for _, row in df.iterrows():
            player_id = int(row["player_id"]) if pd.notna(row["player_id"]) else None
            stat_type = row["stat_type"]

            if player_id is None:
                continue

            pos_key = (player_id, stat_type)

            # Stat whitelist: skip unsupported stat types
            supported = SUPPORTED_STATS.get(sport, set())
            if supported and stat_type not in supported:
                continue

            # Skip if already traded today
            if pos_key in today_positions:
                continue

            # Skip if already sitting in approval queue
            if pos_key in queued_player_stats:
                continue

            volume = int(row["volume"] or 0)
            spread = int(row["bid_ask_spread"] or 100)

            # Liquidity filters (relaxed vs paper — we're taker anyway)
            if volume < 20:
                continue
            if spread > 15:
                continue

            yes_price = int(row["yes_price"])

            # Price floor: skip extreme prices
            if yes_price < self.min_price or yes_price > (100 - self.min_price):
                continue

            # Use BL-blended probability if available, else fall back to raw
            if pd.notna(row.get("bl_model_prob")):
                model_prob = float(row["bl_model_prob"])
            else:
                model_prob = float(row["model_prob"])

            # Calculate taker fee-adjusted edges for both sides
            yes_edge = fee_adjusted_edge(model_prob, yes_price, is_yes=True, is_maker=False)
            no_edge = fee_adjusted_edge(model_prob, yes_price, is_yes=False, is_maker=False)

            # Track all NO edges for bet pool logging (before threshold filter)
            if no_edge > 0:
                _pool_no_edges.append(no_edge)

            # NO-only by default; re-enable YES via KALSHI_ALLOW_YES_BETS=true
            if _allow_yes and yes_edge >= no_edge and yes_edge >= self.min_edge:
                side = "yes"
                edge = yes_edge
            elif no_edge >= self.min_edge:
                side = "no"
                edge = no_edge
            else:
                continue

            # Structural filters — same rules as daily_runner / edge_refresh_job
            direction = "over" if side == "yes" else "under"
            line_val = float(row["line"]) if pd.notna(row.get("line")) else None
            skip, reason = should_skip_recommendation(
                stat=stat_type,
                direction=direction,
                line=line_val,
            )
            if skip:
                logger.debug(f"SKIP {row['player_name']} {stat_type} {direction}: {reason}")
                continue

            # Star-batter filter: NO on 1+ hits with high yes_price means
            # betting a likely star goes hitless.  Model tail P(0 hits) is
            # miscalibrated for elite contact hitters (28.8% win rate vs
            # 39.4% for non-stars in paper trading).  Threshold from data:
            # stars avg yes_price 72+, non-stars 70-.
            _STAR_YES_PRICE = int(os.environ.get("KALSHI_STAR_HITS_YES_PRICE", "72"))
            if stat_type == "batter_hits" and side == "no" and line_val == 1.0 and yes_price >= _STAR_YES_PRICE:
                logger.debug(
                    f"SKIP star-hitter filter: {row['player_name']} batter_hits "
                    f"NO@1+ yes_price={yes_price} >= {_STAR_YES_PRICE}"
                )
                continue

            # Edge sanity cap: reject absurd edges that indicate model garbage
            MAX_EDGE = _env_float("KALSHI_LIVE_MAX_EDGE", 0.40)
            if edge > MAX_EDGE:
                logger.warning(
                    f"SUSPICIOUS EDGE {edge:.1%} on {row['ticker']} "
                    f"({row['player_name']} {stat_type}) — skipping (max {MAX_EDGE:.1%})"
                )
                continue

            # Per-stat direction restriction (matches daily runner allowed_directions)
            if sport == "mlb":
                from src.models.mlb.mlb_stat_config import MLB_STATS as _MLB_STATS
                _allowed = _MLB_STATS.get(stat_type, {}).get("allowed_directions", ["over", "under"])
                _direction = "over" if side == "yes" else "under"
                if _direction not in _allowed:
                    continue

            # Sportsbook-alignment check
            kalshi_line = float(row["line"])
            sb_line = float(row["sportsbook_consensus_line"]) if pd.notna(row.get("sportsbook_consensus_line")) else None

            is_matching = False
            if sb_line is not None:
                matching_target = math.ceil(sb_line)
                is_matching = (int(kalshi_line) == matching_target)

            # Same-run dedup: prefer sportsbook-aligned line per (player_id, stat_type)
            if pos_key in run_candidates:
                existing = run_candidates[pos_key]
                ex_edge = existing["fee_adjusted_edge"]
                ex_matching = existing.get("is_matching_line", False)

                if is_matching and not ex_matching:
                    # New is sportsbook-aligned, existing is not
                    if ex_edge > edge + _SPORTSBOOK_LINE_FALLBACK_GAP:
                        continue  # keep existing (huge edge advantage)
                    logger.info(
                        f"SB-align: {row['player_name']} {stat_type} — "
                        f"replacing line {existing['line']} (edge={ex_edge:.3f}) "
                        f"with SB-aligned line {kalshi_line} (edge={edge:.3f})"
                    )
                elif ex_matching and not is_matching:
                    # Existing is sportsbook-aligned, new is not
                    if edge <= ex_edge + _SPORTSBOOK_LINE_FALLBACK_GAP:
                        continue
                    logger.info(
                        f"SB-override: {row['player_name']} {stat_type} — "
                        f"overriding SB-aligned line {existing['line']} (edge={ex_edge:.3f}) "
                        f"with line {kalshi_line} (edge={edge:.3f}, gap={edge - ex_edge:.3f})"
                    )
                else:
                    # Both same alignment — pick higher edge
                    if edge <= ex_edge:
                        continue

            run_candidates[pos_key] = {
                "ticker": row["ticker"],
                "player_name": row["player_name"],
                "line": kalshi_line,
                "is_matching_line": is_matching,
                "sportsbook_line": sb_line,
                "side": side,
                "yes_price": yes_price,
                "model_prob": model_prob,
                "kalshi_implied": float(row["kalshi_implied"]) if pd.notna(row["kalshi_implied"]) else yes_price / 100.0,
                "raw_edge": float(row["raw_edge"]) if pd.notna(row["raw_edge"]) else 0,
                "fee_adjusted_edge": edge,
            }

        # Log bet pool statistics (all NO edges, before threshold filter)
        _tiers = [0, 3, 5, 10, 15]
        _tier_str = ", ".join(
            f">{t}%: {sum(1 for e in _pool_no_edges if e >= t / 100.0)}"
            for t in _tiers
        )
        logger.info(f"BET POOL ({target_date} {_mode_str}): {_tier_str}")

        # Lookup game start times for all tickers (best-effort, non-fatal)
        game_start_times = self._lookup_game_start_times(target_date, sport)

        # Second pass: Kelly size and apply exposure/balance caps
        trades: list[dict[str, Any]] = []

        for pos_key, cand in sorted(run_candidates.items(), key=lambda kv: kv[1]["fee_adjusted_edge"], reverse=True):
            ticker = cand["ticker"]
            side = cand["side"]
            yes_price = cand["yes_price"]
            model_prob = cand["model_prob"]

            # Check position accumulation via API
            if ticker in held_tickers:
                existing = held_tickers[ticker]
                existing_contracts = existing.get("total_traded", 0)
                if existing_contracts >= self.max_contracts:
                    continue
                remaining_capacity = self.max_contracts - existing_contracts
            else:
                remaining_capacity = self.max_contracts

            contracts = self._kelly_contracts(model_prob, yes_price, side, bankroll)
            if contracts <= 0:
                continue
            contracts = min(contracts, remaining_capacity)

            # Cost for this trade
            cost_per = (yes_price / 100.0) if side == "yes" else ((100 - yes_price) / 100.0)
            trade_cost = contracts * cost_per

            # Enforce daily exposure cap
            if total_exposure + trade_cost > effective_cap:
                remaining = effective_cap - total_exposure
                if remaining <= cost_per:
                    continue
                contracts = int(math.floor(remaining / cost_per))
                if contracts <= 0:
                    continue
                trade_cost = contracts * cost_per

            # Ensure we can afford it
            if trade_cost > bankroll:
                contracts = int(math.floor(bankroll / cost_per))
                if contracts <= 0:
                    continue
                trade_cost = contracts * cost_per

            total_exposure += trade_cost
            fee_per = kalshi_taker_fee(yes_price if side == "yes" else 100 - yes_price)

            trades.append({
                "game_date": target_date,
                "ticker": ticker,
                "sport": sport,
                "player_id": pos_key[0],
                "player_name": cand["player_name"],
                "stat_type": pos_key[1],
                "line": cand["line"],
                "side": side,
                "yes_price": yes_price,
                "contracts": contracts,
                "cost_per": cost_per,
                "expected_cost": round(trade_cost, 2),
                "expected_fee": round(fee_per * contracts, 4),
                "model_prob": round(model_prob, 4),
                "kalshi_implied": round(cand["kalshi_implied"], 4),
                "edge": round(cand["raw_edge"], 4),
                "fee_adjusted_edge": round(cand["fee_adjusted_edge"], 4),
                "game_start_time": game_start_times.get(ticker),
                "sportsbook_consensus_line": cand.get("sportsbook_line"),
            })

        logger.info(
            f"Selected {len(trades)} Kalshi live trades [{_mode_str}] for {target_date} "
            f"(exposure: ${total_exposure:.2f}/${effective_cap:.2f} cap)"
        )
        return trades

    def _lookup_game_start_times(self, target_date: date, sport: str) -> dict[str, datetime | None]:
        """Look up game start times from kalshi_markets.close_time, keyed by ticker.

        Kalshi sports markets close at game start, so close_time is the game start time.
        """
        start_times: dict[str, datetime | None] = {}
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(text("""
                    SELECT DISTINCT ON (ticker) ticker, close_time
                    FROM kalshi_markets
                    WHERE sport = :sport
                      AND (snapshot_time AT TIME ZONE 'America/New_York')::date = :d
                      AND close_time IS NOT NULL
                    ORDER BY ticker, snapshot_time DESC
                """), {"d": target_date, "sport": sport}).fetchall()
                for row in rows:
                    start_times[row[0]] = row[1]
        except Exception as e:
            logger.debug(f"Game start time lookup failed (non-fatal): {e}")
        return start_times

    # ------------------------------------------------------------------
    # Trade execution
    # ------------------------------------------------------------------

    def execute_trades(self, trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Place real taker orders via the Kalshi API.

        For each trade:
        1. Final balance check
        2. Place market order
        3. Record to DB
        4. Send Discord alert
        """
        if not trades:
            return []

        results: list[dict[str, Any]] = []

        for trade in trades:
            # Fresh balance check before each order
            balance_data = self.client.get_balance()
            if balance_data is None:
                logger.error("Balance check failed — aborting remaining trades")
                break
            balance = balance_data.get("balance", 0) / 100.0

            if balance < trade["expected_cost"]:
                logger.warning(
                    f"Insufficient balance (${balance:.2f}) for {trade['ticker']} "
                    f"(cost: ${trade['expected_cost']:.2f}) — skipping"
                )
                continue

            # --- Orderbook sweep check ---
            snapshot_price = trade["yes_price"]
            actual_price = self._get_best_available_price(trade["ticker"], trade["side"], snapshot_price)
            swept_from: int | None = None
            swept_to: int | None = None
            recalc_edge_val: float | None = None

            if actual_price is not None and actual_price != snapshot_price:
                price_delta = abs(actual_price - snapshot_price)

                if price_delta > KALSHI_SWEEP_MAX_CENTS:
                    logger.warning(
                        f"SWEEP REJECTED [{trade['ticker']}]: price moved {price_delta}c "
                        f"({snapshot_price}c -> {actual_price}c) exceeds max {KALSHI_SWEEP_MAX_CENTS}c — skipping"
                    )
                    continue

                recalc_edge_val = fee_adjusted_edge(
                    trade["model_prob"], actual_price,
                    is_yes=(trade["side"] == "yes"), is_maker=False,
                )
                original_edge = trade["fee_adjusted_edge"]
                edge_floor = original_edge * KALSHI_SWEEP_EDGE_RETENTION

                if recalc_edge_val < edge_floor:
                    logger.warning(
                        f"SWEEP REJECTED [{trade['ticker']}]: edge at {actual_price}c = "
                        f"{recalc_edge_val:.1%} below {KALSHI_SWEEP_EDGE_RETENTION:.0%} retention "
                        f"floor {edge_floor:.1%} (original: {original_edge:.1%}) — skipping"
                    )
                    continue

                logger.info(
                    f"SWEEP ACCEPTED [{trade['ticker']}]: {snapshot_price}c -> {actual_price}c, "
                    f"recalc edge {recalc_edge_val:.1%} (was {original_edge:.1%})"
                )
                swept_from = snapshot_price
                swept_to = actual_price
                new_contracts = self._kelly_contracts(
                    trade["model_prob"], actual_price, trade["side"], balance
                )
                price_per = actual_price / 100.0 if trade["side"] == "yes" else (100 - actual_price) / 100.0
                new_expected_cost = new_contracts * price_per
                new_expected_fee = kalshi_taker_fee(
                    actual_price if trade["side"] == "yes" else 100 - actual_price
                ) * new_contracts

                if new_expected_cost > balance:
                    new_contracts = max(0, int(balance / price_per))
                    new_expected_cost = new_contracts * price_per
                    new_expected_fee = kalshi_taker_fee(
                        actual_price if trade["side"] == "yes" else 100 - actual_price
                    ) * new_contracts

                if new_contracts == 0:
                    logger.warning(
                        f"SWEEP RESIZE: 0 contracts after resize for {trade['ticker']} — skipping"
                    )
                    continue

                old_contracts = trade["contracts"]
                logger.info(
                    f"SWEEP RESIZE [{trade['ticker']}]: {old_contracts} -> {new_contracts} contracts "
                    f"(price: {snapshot_price}c -> {actual_price}c, edge: {recalc_edge_val:.1%})"
                )
                trade = {
                    **trade,
                    "yes_price": actual_price,
                    "fee_adjusted_edge": recalc_edge_val,
                    "contracts": new_contracts,
                    "expected_cost": new_expected_cost,
                    "expected_fee": new_expected_fee,
                }

            elif actual_price is None:
                logger.warning(f"Orderbook unavailable for {trade['ticker']} — using snapshot+buffer fallback")
            # --- End sweep check ---

            # Place market order with a 3-cent sweep buffer so the taker order
            # fills immediately (Kalshi "market" orders require a price field).
            sweep_buffer = 3
            yes_px = trade["yes_price"]
            if trade["side"] == "yes":
                order_result = self.client.create_order(
                    ticker=trade["ticker"],
                    action="buy",
                    side="yes",
                    order_type="market",
                    count=trade["contracts"],
                    yes_price=min(yes_px + sweep_buffer, 99),
                )
            else:
                order_result = self.client.create_order(
                    ticker=trade["ticker"],
                    action="buy",
                    side="no",
                    order_type="market",
                    count=trade["contracts"],
                    no_price=min(100 - yes_px + sweep_buffer, 99),
                )

            if order_result is None:
                logger.error(f"Order failed for {trade['ticker']} — no API response")
                continue

            order = order_result.get("order", order_result)
            order_id = order.get("order_id", order.get("id", ""))
            status = order.get("status", "unknown")

            # Determine fill details
            fill_price = None
            fill_count = 0
            total_cost = 0.0
            fee_paid = 0.0

            if status in ("executed", "filled"):
                # Order fully filled
                fill_price = order.get("yes_price") or order.get("avg_price")
                if not fill_price:
                    # API didn't return fill price — fall back to snapshot price
                    fill_price = trade["yes_price"]
                fill_count = order.get("count", trade["contracts"])
                # Calculate actual cost from fill
                if fill_price:
                    if trade["side"] == "yes":
                        total_cost = fill_count * fill_price / 100.0
                    else:
                        total_cost = fill_count * (100 - fill_price) / 100.0
                    fee_paid = kalshi_taker_fee(fill_price if trade["side"] == "yes" else 100 - fill_price) * fill_count
                else:
                    total_cost = trade["expected_cost"]
                    fee_paid = trade["expected_fee"]
            else:
                # Partial or pending — use expected values
                fill_price = trade["yes_price"]
                fill_count = trade["contracts"]
                total_cost = trade["expected_cost"]
                fee_paid = trade["expected_fee"]

            # Record to DB
            db_status = "filled" if status in ("executed", "filled") else "pending"
            self._record_order(trade, order_id, db_status, fill_price, fill_count, total_cost, fee_paid)

            # Get updated balance for alert
            new_balance_data = self.client.get_balance()
            new_balance = (new_balance_data.get("balance", 0) / 100.0) if new_balance_data else balance - total_cost

            # Discord alert
            self._send_trade_placed_alert(
                trade, fill_price, fill_count, total_cost, new_balance,
                swept_from=swept_from, swept_to=swept_to, recalc_edge=recalc_edge_val,
            )

            results.append({
                "ticker": trade["ticker"],
                "side": trade["side"],
                "contracts": fill_count,
                "fill_price": fill_price,
                "total_cost": total_cost,
                "order_id": order_id,
                "status": db_status,
            })

            logger.info(
                f"LIVE TRADE: {trade['player_name']} {trade['stat_type']} "
                f"{trade['side'].upper()} @ {fill_price}c x{fill_count} "
                f"= ${total_cost:.2f} (edge: {trade['fee_adjusted_edge']:.1%})"
            )

        logger.info(f"Executed {len(results)}/{len(trades)} live trades")
        return results

    # ------------------------------------------------------------------
    # Trade queue (approval flow)
    # ------------------------------------------------------------------

    def propose_trades(self, trades: list[dict[str, Any]]) -> int:
        """Write trades to kalshi_trade_queue for human approval instead of executing.

        Returns:
            Number of trades proposed.
        """
        if not trades:
            return 0

        with self.engine.begin() as conn:
            for trade in trades:
                conn.execute(text("""
                    INSERT INTO kalshi_trade_queue (
                        game_date, ticker, sport, player_id, player_name,
                        stat_type, line, side, yes_price, contracts,
                        expected_cost, expected_fee, model_prob, kalshi_implied,
                        edge, fee_adjusted_edge, sportsbook_consensus_line,
                        status, expires_at
                    ) VALUES (
                        :game_date, :ticker, :sport, :player_id, :player_name,
                        :stat_type, :line, :side, :yes_price, :contracts,
                        :expected_cost, :expected_fee, :model_prob, :kalshi_implied,
                        :edge, :fee_adjusted_edge, :sportsbook_consensus_line,
                        'pending_approval',
                        now() + interval '30 minutes'
                    )
                """), {
                    "game_date": trade["game_date"],
                    "ticker": trade["ticker"],
                    "sport": trade["sport"],
                    "player_id": trade.get("player_id"),
                    "player_name": trade.get("player_name"),
                    "stat_type": trade["stat_type"],
                    "line": trade["line"],
                    "side": trade["side"],
                    "yes_price": trade["yes_price"],
                    "contracts": trade["contracts"],
                    "expected_cost": trade["expected_cost"],
                    "expected_fee": trade.get("expected_fee"),
                    "model_prob": trade["model_prob"],
                    "kalshi_implied": trade["kalshi_implied"],
                    "edge": trade["edge"],
                    "fee_adjusted_edge": trade["fee_adjusted_edge"],
                    "sportsbook_consensus_line": trade.get("sportsbook_consensus_line"),
                })

        logger.info(f"Proposed {len(trades)} trades to approval queue")
        return len(trades)

    def renew_expired_queue_trades(self, target_date: date, sport: str) -> int:
        """Extend the expiry of pending trades the user hasn't acted on yet.

        When a 30-minute approval window expires without the user approving or
        rejecting, this resets expires_at so the trades stay visible on the
        dashboard without a gap — as long as their Kalshi market is still open.

        Returns the number of trades renewed (0 = nothing to carry forward).
        """
        with self.engine.begin() as conn:
            result = conn.execute(text("""
                UPDATE kalshi_trade_queue q
                SET expires_at = now() + interval '30 minutes'
                WHERE q.game_date = :d
                  AND q.sport = :sport
                  AND q.status = 'pending_approval'
                  AND q.expires_at BETWEEN now() - interval '60 minutes' AND now()
                  AND EXISTS (
                      SELECT 1 FROM kalshi_markets m
                      WHERE m.ticker = q.ticker
                        AND m.market_status = 'open'
                        AND m.snapshot_time >= now() - interval '15 minutes'
                  )
            """), {"d": target_date, "sport": sport})
            count = result.rowcount
        if count > 0:
            logger.info(f"Renewed {count} expired pending trades for {sport.upper()} (markets still open)")
        return count

    def execute_approved_trades(self, trade_ids: list[int]) -> list[dict[str, Any]]:
        """Execute trades from the queue that have been approved.

        Reads trade details from kalshi_trade_queue, executes via Kalshi API,
        and updates queue status.
        """
        if not trade_ids:
            return []

        # Fetch approved trades from queue
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT id, game_date, ticker, sport, player_id, player_name,
                       stat_type, line, side, yes_price, contracts,
                       expected_cost, expected_fee, model_prob, kalshi_implied,
                       edge, fee_adjusted_edge, expires_at
                FROM kalshi_trade_queue
                WHERE id = ANY(:ids) AND status = 'approved'
            """), {"ids": trade_ids}).fetchall()

        if not rows:
            logger.warning("No approved trades found in queue")
            return []

        # Convert to trade dicts for execute_trades()
        trades = []
        expired_ids = []
        now = datetime.utcnow()

        for row in rows:
            row_dict = dict(row._mapping)
            # Check expiry
            if row_dict["expires_at"] and row_dict["expires_at"].replace(tzinfo=None) < now:
                expired_ids.append(row_dict["id"])
                continue

            trades.append({
                "game_date": row_dict["game_date"],
                "ticker": row_dict["ticker"],
                "sport": row_dict["sport"],
                "player_id": row_dict["player_id"],
                "player_name": row_dict["player_name"],
                "stat_type": row_dict["stat_type"],
                "line": float(row_dict["line"]),
                "side": row_dict["side"],
                "yes_price": row_dict["yes_price"],
                "contracts": row_dict["contracts"],
                "expected_cost": float(row_dict["expected_cost"]),
                "expected_fee": float(row_dict["expected_fee"]) if row_dict["expected_fee"] else 0,
                "model_prob": float(row_dict["model_prob"]) if row_dict["model_prob"] else 0,
                "kalshi_implied": float(row_dict["kalshi_implied"]) if row_dict["kalshi_implied"] else 0,
                "edge": float(row_dict["edge"]) if row_dict["edge"] else 0,
                "fee_adjusted_edge": float(row_dict["fee_adjusted_edge"]) if row_dict["fee_adjusted_edge"] else 0,
                "_queue_id": row_dict["id"],
            })

        # Mark expired trades
        if expired_ids:
            with self.engine.begin() as conn:
                conn.execute(text("""
                    UPDATE kalshi_trade_queue SET status = 'expired'
                    WHERE id = ANY(:ids)
                """), {"ids": expired_ids})
            logger.warning(f"Expired {len(expired_ids)} trades from queue")

        if not trades:
            return []

        # Execute via normal path
        results = self.execute_trades(trades)

        # Update queue status for executed trades
        executed_tickers = {r["ticker"] for r in results}
        with self.engine.begin() as conn:
            for trade in trades:
                queue_id = trade["_queue_id"]
                if trade["ticker"] in executed_tickers:
                    conn.execute(text("""
                        UPDATE kalshi_trade_queue
                        SET status = 'executed', executed_at = now()
                        WHERE id = :id
                    """), {"id": queue_id})
                else:
                    conn.execute(text("""
                        UPDATE kalshi_trade_queue
                        SET status = 'failed'
                        WHERE id = :id
                    """), {"id": queue_id})

        return results

    def _record_order(
        self,
        trade: dict,
        order_id: str,
        status: str,
        fill_price: int | None,
        fill_count: int,
        total_cost: float,
        fee_paid: float,
    ) -> None:
        """Insert a live order record into the database."""
        with self.engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO kalshi_live_orders (
                    game_date, ticker, sport, player_id, player_name,
                    stat_type, line, side, order_type, contracts,
                    kalshi_order_id, fill_price, fill_count, total_cost, fee_paid,
                    model_prob, kalshi_implied, edge, fee_adjusted_edge,
                    status, filled_at, game_start_time
                ) VALUES (
                    :game_date, :ticker, :sport, :player_id, :player_name,
                    :stat_type, :line, :side, 'market', :contracts,
                    :order_id, :fill_price, :fill_count, :total_cost, :fee_paid,
                    :model_prob, :kalshi_implied, :edge, :fee_adjusted_edge,
                    :status, :filled_at, :game_start_time
                )
            """), {
                "game_date": trade["game_date"],
                "ticker": trade["ticker"],
                "sport": trade["sport"],
                "player_id": trade.get("player_id"),
                "player_name": trade.get("player_name"),
                "stat_type": trade["stat_type"],
                "line": trade["line"],
                "side": trade["side"],
                "contracts": trade["contracts"],
                "order_id": order_id,
                "fill_price": fill_price,
                "fill_count": fill_count,
                "total_cost": round(total_cost, 2),
                "fee_paid": round(fee_paid, 4),
                "model_prob": trade["model_prob"],
                "kalshi_implied": trade["kalshi_implied"],
                "edge": trade["edge"],
                "fee_adjusted_edge": trade["fee_adjusted_edge"],
                "status": status,
                "filled_at": datetime.utcnow() if status == "filled" else None,
                "game_start_time": trade.get("game_start_time"),
            })
            conn.commit()

    def _send_trade_placed_alert(
        self, trade: dict, fill_price: int | None, contracts: int,
        total_cost: float, balance: float,
        swept_from: int | None = None, swept_to: int | None = None,
        recalc_edge: float | None = None,
    ) -> None:
        """Send Discord alert for a placed trade."""
        try:
            from src.discord_bot.alerts import send_kalshi_trade_alert_sync
            payload = {
                "player_name": trade.get("player_name", "Unknown"),
                "stat_type": trade["stat_type"],
                "line": trade["line"],
                "side": trade["side"],
                "fill_price": fill_price or trade["yes_price"],
                "contracts": contracts,
                "total_cost": total_cost,
                "fee_adjusted_edge": trade["fee_adjusted_edge"],
                "balance_after": balance,
            }
            if swept_from is not None and swept_to is not None and swept_from != swept_to:
                payload["swept"] = f"{swept_from}c -> {swept_to}c (edge: {recalc_edge:.1%})"
            send_kalshi_trade_alert_sync("placed", payload)
        except Exception as e:
            logger.warning(f"Failed to send trade placed alert: {e}")

    # ------------------------------------------------------------------
    # Fill reconciliation
    # ------------------------------------------------------------------

    def reconcile_fills(self, target_date: date | None = None) -> dict[str, Any]:
        """Fetch fills from API and reconcile with our order records.

        Args:
            target_date: If provided, only reconcile pending orders for that date.
                         If None, reconcile ALL pending orders regardless of game_date.
        """
        if target_date is not None:
            where_clause = "WHERE game_date = :d AND status = 'pending'"
            params: dict = {"d": target_date}
        else:
            where_clause = "WHERE status = 'pending'"
            params = {}

        with self.engine.connect() as conn:
            pending = conn.execute(text(f"""
                SELECT id, kalshi_order_id, ticker, side, contracts
                FROM kalshi_live_orders
                {where_clause}
            """), params).fetchall()

        if not pending:
            return {"reconciled": 0}

        reconciled = 0
        for row in pending:
            order_id = row[1]
            if not order_id:
                continue

            fills = self.client.get_fills(order_id=order_id)
            if not fills:
                continue

            total_filled = sum(f.get("count", 0) for f in fills)
            avg_price = None
            if total_filled > 0:
                weighted = sum(f.get("yes_price", 0) * f.get("count", 0) for f in fills)
                avg_price = int(weighted / total_filled)

            if total_filled > 0:
                side = row[3]
                if side == "yes":
                    total_cost = total_filled * avg_price / 100.0
                else:
                    total_cost = total_filled * (100 - avg_price) / 100.0
                fee_paid = kalshi_taker_fee(avg_price if side == "yes" else 100 - avg_price) * total_filled

                with self.engine.connect() as conn:
                    conn.execute(text("""
                        UPDATE kalshi_live_orders
                        SET status = 'filled',
                            fill_price = :price,
                            fill_count = :count,
                            total_cost = :cost,
                            fee_paid = :fee,
                            filled_at = now()
                        WHERE id = :id
                    """), {
                        "price": avg_price,
                        "count": total_filled,
                        "cost": round(total_cost, 2),
                        "fee": round(fee_paid, 4),
                        "id": row[0],
                    })
                    conn.commit()
                reconciled += 1

        logger.info(f"Reconciled {reconciled} fills" + (f" for {target_date}" if target_date else ""))
        return {"reconciled": reconciled}

    # ------------------------------------------------------------------
    # Resolution
    # ------------------------------------------------------------------

    def resolve_settled(self) -> dict[str, Any]:
        """Check for settled Kalshi positions and resolve our orders.

        Uses the same actual-stats resolution logic as the paper trader.
        """
        # Get all filled (unresolved) orders
        with self.engine.connect() as conn:
            orders = pd.read_sql(text("""
                SELECT id, game_date, ticker, player_id, player_name,
                       stat_type, line, side, fill_price, fill_count,
                       total_cost, fee_paid, sport
                FROM kalshi_live_orders
                WHERE status = 'filled'
                ORDER BY game_date ASC
            """), conn)

        if orders.empty:
            logger.info("No filled Kalshi live orders to resolve")
            return {"resolved": 0, "won": 0, "lost": 0, "cancelled": 0}

        # Group by date for resolution
        dates = orders["game_date"].unique()
        totals = {"resolved": 0, "won": 0, "lost": 0, "cancelled": 0}

        for game_date in dates:
            # Don't resolve today's games — wait for stats
            if game_date >= date.today():
                continue

            date_orders = orders[orders["game_date"] == game_date]
            sport = date_orders.iloc[0]["sport"]

            # Fetch actuals (reuse paper trader's logic)
            actuals = self._fetch_actuals(game_date, date_orders, sport)

            for _, order in date_orders.iterrows():
                player_id = int(order["player_id"]) if pd.notna(order["player_id"]) else None
                stat_type = order["stat_type"]
                line = float(order["line"])
                side = order["side"]

                # Guard: null fill_price/fill_count means order was never properly
                # reconciled. Defaulting to 0 produces garbage PnL — skip and warn.
                if pd.isna(order["fill_price"]) or pd.isna(order["fill_count"]):
                    logger.warning(
                        f"SKIP RESOLUTION: order {int(order['id'])} ({order.get('ticker','?')}) "
                        f"has null fill_price={order['fill_price']} or "
                        f"fill_count={order['fill_count']} — run reconcile_fills() first"
                    )
                    continue

                fill_price = int(order["fill_price"])
                fill_count = int(order["fill_count"])
                fee = float(order["fee_paid"]) if pd.notna(order["fee_paid"]) else 0.0

                actual = actuals.get((player_id, stat_type)) if player_id else None

                if actual is None:
                    status = "cancelled"
                    pnl = 0.0
                    totals["cancelled"] += 1
                else:
                    yes_wins = actual >= line

                    if side == "yes":
                        if yes_wins:
                            pnl = fill_count * (100 - fill_price) / 100.0 - fee
                            status = "won"
                            totals["won"] += 1
                        else:
                            pnl = -(fill_count * fill_price / 100.0)
                            status = "lost"
                            totals["lost"] += 1
                    else:
                        if not yes_wins:
                            pnl = fill_count * fill_price / 100.0 - fee
                            status = "won"
                            totals["won"] += 1
                        else:
                            pnl = -(fill_count * (100 - fill_price) / 100.0)
                            status = "lost"
                            totals["lost"] += 1

                # Update order
                with self.engine.connect() as conn:
                    conn.execute(text("""
                        UPDATE kalshi_live_orders
                        SET status = :status,
                            actual_value = :actual,
                            pnl = :pnl,
                            resolved_at = now()
                        WHERE id = :id
                    """), {
                        "status": status,
                        "actual": actual,
                        "pnl": round(pnl, 2),
                        "id": int(order["id"]),
                    })
                    conn.commit()

                totals["resolved"] += 1

                # Discord alert for resolution
                if status in ("won", "lost"):
                    balance_data = self.client.get_balance()
                    balance = (balance_data.get("balance", 0) / 100.0) if balance_data else 0
                    self._send_resolution_alert(order, status, actual, pnl, balance)

            # Update daily log for this date
            self._update_daily_log(game_date)

        # Update streak count
        streak = self._get_consecutive_losses()
        self._update_streak(streak)

        logger.info(
            f"Resolved {totals['resolved']} Kalshi live orders: "
            f"{totals['won']}W {totals['lost']}L {totals['cancelled']}C"
        )
        return totals

    def _fetch_actuals(
        self, game_date: date, orders_df: pd.DataFrame, sport: str,
    ) -> dict[tuple[int, str], float | None]:
        """Fetch actual stat values for resolution (same logic as paper trader)."""
        from src.paper_trading.kalshi_paper_trader import (
            MLB_STAT_RESOLUTION,
            NBA_STAT_RESOLUTION,
        )

        actuals: dict[tuple[int, str], float | None] = {}
        stats_needed = orders_df["stat_type"].unique()

        for stat_type in stats_needed:
            nba_res = NBA_STAT_RESOLUTION.get(stat_type)
            if nba_res is not None:
                table, columns = nba_res
                col_expr = " + ".join(f"s.{c}" for c in columns)
                with self.engine.connect() as conn:
                    rows = conn.execute(text(f"""
                        SELECT s.player_id, ({col_expr}) as actual_value
                        FROM {table} s
                        WHERE s.game_date = :game_date AND s.player_id IS NOT NULL
                          AND s.min > 0
                    """), {"game_date": game_date}).fetchall()
                for row in rows:
                    actuals[(int(row[0]), stat_type)] = float(row[1]) if row[1] is not None else None
                continue

            mlb_res = MLB_STAT_RESOLUTION.get(stat_type)
            if mlb_res is not None:
                table, columns = mlb_res
                col_expr = " + ".join(f"s.{c}" for c in columns)
                with self.engine.connect() as conn:
                    rows = conn.execute(text(f"""
                        SELECT s.player_id, {col_expr} as actual_value, s.did_not_play
                        FROM {table} s
                        WHERE s.game_date = :game_date
                    """), {"game_date": game_date}).fetchall()
                for row in rows:
                    if row[2]:  # DNP
                        actuals[(int(row[0]), stat_type)] = None
                    else:
                        actuals[(int(row[0]), stat_type)] = float(row[1]) if row[1] is not None else None
                continue

            logger.warning(f"No resolution mapping for stat_type: {stat_type}")

        return actuals

    def _send_resolution_alert(
        self, order: pd.Series, status: str, actual: float | None, pnl: float, balance: float,
    ) -> None:
        """Send Discord alert for a resolved trade."""
        try:
            from src.discord_bot.alerts import send_kalshi_trade_alert_sync
            send_kalshi_trade_alert_sync("resolved", {
                "player_name": order.get("player_name", "Unknown"),
                "stat_type": order["stat_type"],
                "line": float(order["line"]),
                "side": order["side"],
                "actual_value": actual,
                "pnl": pnl,
                "status": status,
                "balance_after": balance,
            })
        except Exception as e:
            logger.warning(f"Failed to send resolution alert: {e}")

    # ------------------------------------------------------------------
    # Daily log
    # ------------------------------------------------------------------

    def _update_daily_log(self, game_date: date) -> None:
        """Aggregate live order results and upsert daily log entry."""
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE status = 'won') as won,
                    COUNT(*) FILTER (WHERE status = 'lost') as lost,
                    COUNT(*) FILTER (WHERE status = 'cancelled') as cancelled,
                    COUNT(*) FILTER (WHERE status IN ('pending', 'filled')) as pending,
                    COALESCE(SUM(total_cost) FILTER (WHERE status IN ('won', 'lost')), 0) as total_cost,
                    COALESCE(SUM(pnl), 0) as total_pnl
                FROM kalshi_live_orders
                WHERE game_date = :d
            """), {"d": game_date}).fetchone()

        if result is None or result[0] == 0:
            return

        total, won, lost, cancelled, pending, total_cost, total_pnl = result
        total_cost = float(total_cost)
        total_pnl = float(total_pnl)
        roi_pct = (total_pnl / total_cost * 100) if total_cost > 0 else 0

        # Get previous cumulative values
        with self.engine.connect() as conn:
            prev = conn.execute(text("""
                SELECT cumulative_pnl, balance_after
                FROM kalshi_live_trading_daily_log
                WHERE game_date < :d
                ORDER BY game_date DESC LIMIT 1
            """), {"d": game_date}).fetchone()

        prev_cum = float(prev[0]) if prev else 0.0
        prev_bal = float(prev[1]) if prev else self.starting_bankroll
        cumulative_pnl = prev_cum + total_pnl
        balance_after = prev_bal + total_pnl

        with self.engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO kalshi_live_trading_daily_log (
                    game_date, total_trades, trades_won, trades_lost,
                    trades_cancelled, trades_pending, total_cost, total_pnl,
                    roi_pct, cumulative_pnl, balance_after
                ) VALUES (
                    :d, :total, :won, :lost,
                    :cancelled, :pending, :cost, :pnl,
                    :roi, :cum_pnl, :bal
                )
                ON CONFLICT (game_date) DO UPDATE SET
                    total_trades = EXCLUDED.total_trades,
                    trades_won = EXCLUDED.trades_won,
                    trades_lost = EXCLUDED.trades_lost,
                    trades_cancelled = EXCLUDED.trades_cancelled,
                    trades_pending = EXCLUDED.trades_pending,
                    total_cost = EXCLUDED.total_cost,
                    total_pnl = EXCLUDED.total_pnl,
                    roi_pct = EXCLUDED.roi_pct,
                    cumulative_pnl = EXCLUDED.cumulative_pnl,
                    balance_after = EXCLUDED.balance_after,
                    updated_at = now()
            """), {
                "d": game_date,
                "total": total,
                "won": won,
                "lost": lost,
                "cancelled": cancelled,
                "pending": pending,
                "cost": round(total_cost, 2),
                "pnl": round(total_pnl, 2),
                "roi": round(roi_pct, 2),
                "cum_pnl": round(cumulative_pnl, 2),
                "bal": round(balance_after, 2),
            })
            conn.commit()

        logger.info(
            f"Updated Kalshi live daily log for {game_date}: "
            f"P&L=${total_pnl:.2f}, balance=${balance_after:.2f}"
        )
