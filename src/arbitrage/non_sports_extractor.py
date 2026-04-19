"""
Non-Sports Market Field Extractor
==================================
Extracts structured fields from Kalshi and Polymarket non-sports questions
for deterministic matching instead of fuzzy text similarity.

Series groupings:
  Crypto  (KXBTC, KXBTCD, KXETH, KXETHD, KXDOGE, KXXRP):
    extract (price_usd, direction, date)
  Rate    (KXFED, KXCPI):
    extract (rate_pct, direction, month, year)
  GDP     (KXGDP):
    extract (growth_pct, direction, quarter, year)
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date

# ---------------------------------------------------------------------------
# Regex helpers
# ---------------------------------------------------------------------------

# Dollar prices: "$95,000" | "$95k" | "95000" | "$1.5M"
_PRICE_USD_RE = re.compile(r'\$\s*([\d,]+(?:\.\d+)?)\s*([kKmM])?')

# Percentages: "4.50%" | "3.5 percent"
_PCT_RE = re.compile(r'([\d]+(?:\.\d+)?)\s*(?:%|percent)', re.IGNORECASE)

# Month names (full + abbreviated)
_MONTH_MAP = {
    'january': 1, 'february': 2, 'march': 3, 'april': 4,
    'may': 5, 'june': 6, 'july': 7, 'august': 8,
    'september': 9, 'october': 10, 'november': 11, 'december': 12,
    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4,
    'jun': 6, 'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12,
}

# Kalshi ticker date segment: 24APR26 → 2026-04-24
_TICKER_DATE_RE = re.compile(
    r'(\d{2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(\d{2})',
    re.IGNORECASE,
)
_TICKER_MONTH_MAP = {
    'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
    'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12,
}

# Kalshi ticker price suffix: -B95000 or -95000 or -4P50 (4.50%)
# Letter prefix is direction hint (B=above), digits are value, P = decimal point
_TICKER_PRICE_RE = re.compile(r'-([A-Z]?)(\d+(?:P\d+)?)$', re.IGNORECASE)

# Quarter patterns
_QUARTER_ORDINAL_MAP = {
    'first': 1, 'second': 2, 'third': 3, 'fourth': 4,
    '1st': 1, '2nd': 2, '3rd': 3, '4th': 4,
}
_QUARTER_RE = re.compile(r'q([1-4])\s*(\d{4})', re.IGNORECASE)
_QUARTER_ORDINAL_RE = re.compile(
    r'(first|second|third|fourth|1st|2nd|3rd|4th)\s+quarter\s+(\d{4})',
    re.IGNORECASE,
)

# Month+year: "March 2026", "Mar 2026"
_MONTH_YEAR_RE = re.compile(
    r'(january|february|march|april|may|june|july|august|september|october|november|december'
    r'|jan|feb|mar|apr|jun|jul|aug|sep|oct|nov|dec)\s+(\d{4})',
    re.IGNORECASE,
)

# Date in free text: "April 24, 2026" or "24 April 2026"
_DATE_TEXT_MDY_RE = re.compile(
    r'(january|february|march|april|may|june|july|august|september|october|november|december'
    r'|jan|feb|mar|apr|jun|jul|aug|sep|oct|nov|dec)\s+(\d{1,2}),?\s+(\d{4})',
    re.IGNORECASE,
)
_DATE_TEXT_DMY_RE = re.compile(
    r'(\d{1,2})\s+(january|february|march|april|may|june|july|august|september|october|november|december'
    r'|jan|feb|mar|apr|jun|jul|aug|sep|oct|nov|dec)\s+(\d{4})',
    re.IGNORECASE,
)

# Direction indicators
_ABOVE_RE = re.compile(
    r'\b(above|over|exceed|exceeds|higher|at least|more than|greater than|atleast)\b|[≥>]',
    re.IGNORECASE,
)
_BELOW_RE = re.compile(
    r'\b(below|under|less than|lower|beneath|at most|no more than)\b|[<≤]',
    re.IGNORECASE,
)

# Series classification
CRYPTO_SERIES = frozenset({'KXBTC', 'KXBTCD', 'KXETH', 'KXETHD', 'KXDOGE', 'KXXRP'})
RATE_SERIES = frozenset({'KXFED', 'KXCPI'})
GDP_SERIES = frozenset({'KXGDP'})


# ---------------------------------------------------------------------------
# Shared parsing helpers
# ---------------------------------------------------------------------------

def _norm(text: str) -> str:
    """Lowercase, collapse whitespace."""
    return re.sub(r'\s+', ' ', text.lower().strip())


def _parse_price_usd(text: str) -> float | None:
    """$95k → 95000.0, $1.5M → 1500000.0, $95,000 → 95000.0"""
    for m in _PRICE_USD_RE.finditer(text):
        raw = m.group(1).replace(',', '')
        try:
            val = float(raw)
        except ValueError:
            continue
        suffix = (m.group(2) or '').upper()
        if suffix == 'K':
            val *= 1_000
        elif suffix == 'M':
            val *= 1_000_000
        return val
    return None


def _parse_pct(text: str) -> float | None:
    """Extract first percentage. '4.50%' → 4.5"""
    m = _PCT_RE.search(text)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            pass
    return None


def _parse_month_year(text: str) -> tuple[int, int] | None:
    """'March 2026' → (3, 2026)"""
    m = _MONTH_YEAR_RE.search(text)
    if m:
        month = _MONTH_MAP.get(m.group(1).lower())
        if month:
            try:
                return (month, int(m.group(2)))
            except ValueError:
                pass
    return None


def _parse_quarter_year(text: str) -> tuple[int, int] | None:
    """'Q1 2026' or 'first quarter 2026' → (1, 2026)"""
    m = _QUARTER_RE.search(text)
    if m:
        try:
            return (int(m.group(1)), int(m.group(2)))
        except ValueError:
            pass
    m = _QUARTER_ORDINAL_RE.search(text)
    if m:
        quarter = _QUARTER_ORDINAL_MAP.get(m.group(1).lower())
        if quarter:
            try:
                return (quarter, int(m.group(2)))
            except ValueError:
                pass
    return None


def _parse_date_from_ticker(ticker: str) -> date | None:
    """KXBTC-24APR26-B95000 → 2026-04-24"""
    m = _TICKER_DATE_RE.search(ticker.upper())
    if m:
        day = int(m.group(1))
        month = _TICKER_MONTH_MAP.get(m.group(2).upper())
        year = 2000 + int(m.group(3))
        if month:
            try:
                return date(year, month, day)
            except ValueError:
                pass
    return None


def _parse_price_from_ticker(ticker: str) -> float | None:
    """Extract numeric price from Kalshi ticker suffix.
    B95000 → 95000.0, 4P50 → 4.5 (P is decimal point for sub-$1 values)
    """
    m = _TICKER_PRICE_RE.search(ticker)
    if not m:
        return None
    raw = m.group(2)
    if 'P' in raw.upper():
        raw = raw.upper().replace('P', '.')
    try:
        return float(raw)
    except ValueError:
        return None


def _parse_date_from_text(text: str) -> date | None:
    """'April 24, 2026' or '24 April 2026' → date(2026, 4, 24)"""
    m = _DATE_TEXT_MDY_RE.search(text)
    if m:
        month = _MONTH_MAP.get(m.group(1).lower())
        if month:
            try:
                return date(int(m.group(3)), month, int(m.group(2)))
            except ValueError:
                pass
    m = _DATE_TEXT_DMY_RE.search(text)
    if m:
        month = _MONTH_MAP.get(m.group(2).lower())
        if month:
            try:
                return date(int(m.group(3)), month, int(m.group(1)))
            except ValueError:
                pass
    return None


def _parse_direction(text: str) -> str | None:
    """Detect ABOVE/BELOW from question text."""
    has_above = bool(_ABOVE_RE.search(text))
    has_below = bool(_BELOW_RE.search(text))
    if has_above and not has_below:
        return 'ABOVE'
    if has_below and not has_above:
        return 'BELOW'
    return None  # ambiguous or neither


# ---------------------------------------------------------------------------
# Structured fields dataclass
# ---------------------------------------------------------------------------

@dataclass
class MarketFields:
    series: str
    price: float | None = None     # USD (crypto) or pct (macro)
    direction: str | None = None   # 'ABOVE' | 'BELOW'
    date: date | None = None       # crypto: specific settlement date
    month: int | None = None       # macro: FOMC/CPI month (1-12)
    year: int | None = None        # macro: year
    quarter: int | None = None     # GDP: Q1-Q4


# ---------------------------------------------------------------------------
# Extraction functions
# ---------------------------------------------------------------------------

def extract_kalshi(series: str, ticker: str, title: str) -> MarketFields | None:
    """Extract structured fields from a Kalshi non-sports market.

    Returns None if price or direction cannot be extracted (triggers fallback).
    """
    series_upper = series.upper()
    title_norm = _norm(title)

    if series_upper in CRYPTO_SERIES:
        price = _parse_price_from_ticker(ticker)
        if price is None:
            price = _parse_price_usd(title_norm)
        direction = _parse_direction(title_norm)
        dt = _parse_date_from_ticker(ticker)
        if price is None or direction is None:
            return None
        return MarketFields(series=series_upper, price=price, direction=direction, date=dt)

    if series_upper in RATE_SERIES:
        price = _parse_pct(title_norm)
        direction = _parse_direction(title_norm)
        my = _parse_month_year(title_norm)
        if price is None or direction is None:
            return None
        month, year = my if my else (None, None)
        return MarketFields(series=series_upper, price=price, direction=direction,
                            month=month, year=year)

    if series_upper in GDP_SERIES:
        price = _parse_pct(title_norm)
        direction = _parse_direction(title_norm)
        qy = _parse_quarter_year(title_norm)
        if price is None or direction is None:
            return None
        quarter, year = qy if qy else (None, None)
        return MarketFields(series=series_upper, price=price, direction=direction,
                            quarter=quarter, year=year)

    return None


def extract_poly(series: str, question: str) -> MarketFields | None:
    """Extract structured fields from a Polymarket question string.

    The question should be raw (not pre-normalised) — this function normalises it.
    Returns None if price or direction cannot be extracted (triggers fallback).
    """
    series_upper = series.upper()
    q_norm = _norm(question)

    if series_upper in CRYPTO_SERIES:
        price = _parse_price_usd(q_norm)
        direction = _parse_direction(q_norm)
        dt = _parse_date_from_text(q_norm)
        if price is None or direction is None:
            return None
        return MarketFields(series=series_upper, price=price, direction=direction, date=dt)

    if series_upper in RATE_SERIES:
        price = _parse_pct(q_norm)
        direction = _parse_direction(q_norm)
        my = _parse_month_year(q_norm)
        if price is None or direction is None:
            return None
        month, year = my if my else (None, None)
        return MarketFields(series=series_upper, price=price, direction=direction,
                            month=month, year=year)

    if series_upper in GDP_SERIES:
        price = _parse_pct(q_norm)
        direction = _parse_direction(q_norm)
        qy = _parse_quarter_year(q_norm)
        if price is None or direction is None:
            return None
        quarter, year = qy if qy else (None, None)
        return MarketFields(series=series_upper, price=price, direction=direction,
                            quarter=quarter, year=year)

    return None


# ---------------------------------------------------------------------------
# Match scoring
# ---------------------------------------------------------------------------

PRICE_TOLERANCE = 0.005  # 0.5% relative — $95,000 matches $95,475 but not $90,000


def _periods_match(k: MarketFields, p: MarketFields) -> bool | None:
    """Returns True if periods match, False if they conflict, None if unknown on one side."""
    series = k.series

    if series in CRYPTO_SERIES:
        if k.date is None or p.date is None:
            return None
        return k.date == p.date

    if series in RATE_SERIES:
        k_known = k.month is not None and k.year is not None
        p_known = p.month is not None and p.year is not None
        if not k_known or not p_known:
            return None
        return k.month == p.month and k.year == p.year

    if series in GDP_SERIES:
        k_known = k.quarter is not None and k.year is not None
        p_known = p.quarter is not None and p.year is not None
        if not k_known or not p_known:
            return None
        return k.quarter == p.quarter and k.year == p.year

    return None


def match_score(k: MarketFields, p: MarketFields) -> float:
    """Deterministic match score between a Kalshi and Polymarket market.

    Returns:
        0.0  — definitively different markets
        0.85 — price + direction confirmed, period unknown on one side
        1.0  — price + direction + period all confirmed
    """
    # 1. Price must match (primary discriminator)
    if k.price is None or p.price is None:
        return 0.0
    denom = max(k.price, p.price, 1.0)
    if abs(k.price - p.price) / denom > PRICE_TOLERANCE:
        return 0.0

    # 2. Direction must match when both known
    if k.direction and p.direction and k.direction != p.direction:
        return 0.0

    # 3. Period
    period_match = _periods_match(k, p)
    if period_match is False:
        return 0.0
    if period_match is True:
        return 1.0
    return 0.85  # period unknown on one side
