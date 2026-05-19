"""Kalshi live-trading risk and circuit-breaker service."""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import date
from typing import Any

from sqlalchemy import text

logger = logging.getLogger(__name__)


class KalshiRiskService:
    """Owns live-trading config, halt flags, and circuit-breaker checks."""

    def __init__(
        self,
        *,
        engine: Any,
        client: Any,
        starting_bankroll: float,
        drawdown_limit: float,
        daily_loss_limit: float,
        consec_loss_limit: int,
        send_circuit_breaker_alert: Callable[[str, float, str], None] | None = None,
        force_resume: bool = False,
    ):
        self.engine = engine
        self.client = client
        self.starting_bankroll = starting_bankroll
        self.drawdown_limit = drawdown_limit
        self.daily_loss_limit = daily_loss_limit
        self.consec_loss_limit = consec_loss_limit
        self.send_circuit_breaker_alert = send_circuit_breaker_alert
        self.force_resume = force_resume

    def ensure_config(self) -> None:
        """Ensure the singleton live-trading config row exists."""
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

    def get_config(self) -> dict[str, Any]:
        """Read the singleton live-trading config row."""
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT * FROM kalshi_live_trading_config WHERE id = 1")
            ).fetchone()
        if row is None:
            return {"is_halted": False, "starting_bankroll": self.starting_bankroll}
        return dict(row._mapping)

    def set_halted(self, reason: str) -> None:
        """Set the persistent trading halt flag."""
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

    def clear_halt(self) -> None:
        """Clear the persistent trading halt flag for force-resume mode."""
        with self.engine.connect() as conn:
            conn.execute(text("""
                UPDATE kalshi_live_trading_config
                SET is_halted = false, halt_reason = null,
                    halted_at = null, last_updated = now()
                WHERE id = 1
            """))
            conn.commit()
        logger.info("Force resume: cleared halt flag")

    def update_streak(self, streak_count: int) -> None:
        """Persist the consecutive-loss streak count."""
        with self.engine.connect() as conn:
            conn.execute(text("""
                UPDATE kalshi_live_trading_config
                SET streak_count = :count, last_updated = now()
                WHERE id = 1
            """), {"count": streak_count})
            conn.commit()

    def get_daily_pnl(self, target_date: date) -> float:
        """Sum realized P&L for the target date from live orders."""
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT COALESCE(SUM(pnl), 0)
                FROM kalshi_live_orders
                WHERE game_date = :d AND status IN ('won', 'lost')
            """), {"d": target_date}).scalar()
        return float(result or 0)

    def get_consecutive_losses(self, target_date: date | None = None) -> int:
        """Count consecutive losses from most recent resolved trades for a date."""
        if target_date is None:
            target_date = date.today()
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT status
                FROM kalshi_live_orders
                WHERE status IN ('won', 'lost')
                  AND game_date = :d
                ORDER BY resolved_at DESC
                LIMIT :limit
            """), {"d": target_date, "limit": self.consec_loss_limit}).fetchall()

        streak = 0
        for row in rows:
            if row[0] == "lost":
                streak += 1
            else:
                break
        return streak

    def check_circuit_breakers(self, *, today: date | None = None) -> tuple[bool, str]:
        """Check manual halt, drawdown, daily loss, and loss-streak breakers."""
        config = self.get_config()

        if config.get("is_halted"):
            if self.force_resume:
                self.clear_halt()
            else:
                reason = config.get("halt_reason", "Manual halt")
                return False, f"Halted: {reason}"

        balance_data = self.client.get_balance()
        if balance_data is None:
            logger.error(
                "CIRCUIT BREAKER ABORT: get_balance() returned None — "
                "portfolio API unreachable or API key lacks portfolio permissions."
            )
            return False, "Cannot check balance — API error"

        balance_cents = balance_data.get("balance", 0)
        portfolio_cents = balance_data.get("portfolio_value", 0)
        total_dollars = (balance_cents + portfolio_cents) / 100.0
        balance_dollars = balance_cents / 100.0

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
            self.set_halted(reason)
            self._send_alert(reason, balance_dollars, "All trading HALTED. Manual review required.")
            return False, reason

        today = today or date.today()
        daily_pnl = self.get_daily_pnl(today)
        if daily_pnl < -self.daily_loss_limit:
            reason = (
                f"Daily loss limit reached: ${daily_pnl:.2f} "
                f"(limit: -${self.daily_loss_limit:.0f})"
            )
            self._send_alert(reason, balance_dollars, "Pausing until tomorrow.")
            return False, reason

        streak = self.get_consecutive_losses(today)
        if streak >= self.consec_loss_limit:
            reason = f"{streak} consecutive losses (limit: {self.consec_loss_limit})"
            self._send_alert(reason, balance_dollars, "Pausing for review.")
            return False, reason

        return True, ""

    def _send_alert(self, reason: str, balance: float, action: str) -> None:
        if self.send_circuit_breaker_alert is not None:
            self.send_circuit_breaker_alert(reason, balance, action)
