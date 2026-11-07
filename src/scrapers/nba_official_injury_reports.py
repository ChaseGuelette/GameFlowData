"""Bounded parser/fetcher for official NBA injury-report PDFs."""

from __future__ import annotations

import hashlib
import io
import re
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from typing import Any
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import requests
from pypdf import PdfReader

OFFICIAL_HOST = "ak-static.cms.nba.com"
OFFICIAL_PATH_RE = re.compile(
    r"^/referee/injury/Injury-Report_(?P<date>\d{4}-\d{2}-\d{2})_(?P<token>[A-Za-z0-9_-]+)\.pdf$"
)
HEADER_RE = re.compile(
    r"^Injury Report:\s*(?P<date>\d{2}/\d{2}/\d{2})\s+(?P<time>\d{1,2}:\d{2})\s+(?P<ampm>AM|PM)$"
)
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
VALID_STATUSES = {"Out", "Doubtful", "Questionable", "Probable", "Available"}
MAX_PDF_BYTES = 5_000_000
MAX_PDF_PAGES = 25
REQUEST_TIMEOUT_SECONDS = 15
_Y_TOLERANCE = 1.5


class UnsupportedOfficialInjuryReportError(ValueError):
    """The PDF did not satisfy the supported official report layout."""


@dataclass(frozen=True, slots=True)
class OfficialInjuryRow:
    game_date: str
    game_time: str
    matchup: str
    team: str
    player: str
    status: str
    reason: str


@dataclass(frozen=True, slots=True)
class OfficialInjuryReport:
    source_url: str
    capture_sha256: str
    printed_header: str
    publication_date: str
    publication_time: str
    published_at_utc: str
    page_count: int
    rows: tuple[OfficialInjuryRow, ...]

    def to_dict(self) -> dict:
        return {**asdict(self), "rows": [asdict(row) for row in self.rows]}


def _validated_url(url: str) -> tuple[date, re.Match[str]]:
    parsed = urlparse(url)
    if parsed.scheme != "https" or parsed.hostname != OFFICIAL_HOST or parsed.port is not None:
        raise ValueError("Official report URL must use HTTPS on ak-static.cms.nba.com")
    if parsed.query or parsed.fragment or parsed.username or parsed.password:
        raise ValueError("Official report URL must not contain credentials, query, or fragment")
    match = OFFICIAL_PATH_RE.fullmatch(parsed.path)
    if not match:
        raise ValueError("Unsupported official NBA injury-report URL path")
    return date.fromisoformat(match.group("date")), match


def _publication_from_header(printed_header: str, filename_date: date) -> tuple[date, str, datetime]:
    match = HEADER_RE.fullmatch(" ".join(printed_header.split()))
    if not match:
        raise UnsupportedOfficialInjuryReportError("Missing or unsupported printed Injury Report header")
    publication_date = datetime.strptime(match.group("date"), "%m/%d/%y").date()
    if publication_date != filename_date:
        raise UnsupportedOfficialInjuryReportError(
            f"Printed publication date {publication_date} does not equal filename date {filename_date}"
        )
    printed_time = f"{match.group('time')} {match.group('ampm')}"
    local_naive = datetime.strptime(
        f"{publication_date.isoformat()} {printed_time}", "%Y-%m-%d %I:%M %p"
    )
    local = local_naive.replace(tzinfo=ZoneInfo("America/New_York"))
    if local.utcoffset() != local.replace(fold=1).utcoffset():
        raise UnsupportedOfficialInjuryReportError(
            "Printed report clock is ambiguous or nonexistent at a daylight-saving transition"
        )
    return publication_date, printed_time, local.astimezone(UTC)


def official_observation_publication(observation: dict) -> tuple[date, str, datetime]:
    """Validate official observation authority and return its publication instant."""
    if observation.get("source") != "official_nba_pdf":
        raise ValueError("Not an official NBA PDF observation")
    if observation.get("lineage") != "official_reconstruction":
        raise ValueError("Official reconstruction lineage is required")
    source_url = observation.get("source_url")
    if not isinstance(source_url, str):
        raise ValueError("Official observation has no source URL")
    filename_date, _ = _validated_url(source_url)
    digest = observation.get("capture_sha256")
    if not isinstance(digest, str) or not SHA256_RE.fullmatch(digest):
        raise ValueError("Official observation has no valid PDF SHA-256")
    header = observation.get("official_report_header")
    if not isinstance(header, dict):
        raise ValueError("Official observation has no structured printed header")
    printed = header.get("printed")
    if not isinstance(printed, str):
        raise ValueError("Official observation has no printed report header")
    publication_date, printed_time, published_at = _publication_from_header(printed, filename_date)
    if header.get("publication_date") != publication_date.isoformat():
        raise ValueError("Official header publication date is inconsistent")
    if header.get("publication_time") != printed_time or header.get("timezone") != "America/New_York":
        raise ValueError("Official header clock or timezone is inconsistent")
    if observation.get("requested_date") != publication_date.isoformat():
        raise ValueError("Official requested date must be the printed publication date")
    supplied = observation.get("published_at_utc")
    try:
        supplied_at = datetime.fromisoformat(str(supplied).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("Official publication instant is invalid") from exc
    if supplied_at.tzinfo is None or supplied_at.astimezone(UTC) != published_at:
        raise ValueError("Official publication instant is inconsistent with the printed header")
    return publication_date, printed_time, published_at


def fetch_official_injury_report(
    url: str,
    *,
    timeout_seconds: int = REQUEST_TIMEOUT_SECONDS,
    max_bytes: int = MAX_PDF_BYTES,
    max_pages: int = MAX_PDF_PAGES,
) -> OfficialInjuryReport:
    """Fetch and parse one explicitly supplied official NBA PDF under hard bounds."""
    _validated_url(url)
    if not 1 <= timeout_seconds <= REQUEST_TIMEOUT_SECONDS:
        raise ValueError("timeout_seconds must be between 1 and 15")
    if not 1 <= max_bytes <= MAX_PDF_BYTES or not 1 <= max_pages <= MAX_PDF_PAGES:
        raise ValueError("PDF size/page limits exceed the supported bounds")
    with requests.get(url, timeout=timeout_seconds, stream=True, allow_redirects=False) as response:
        if response.status_code != 200:
            raise requests.HTTPError(f"Official NBA PDF returned HTTP {response.status_code}", response=response)
        if response.headers.get("content-type", "").split(";", 1)[0].strip().lower() != "application/pdf":
            raise UnsupportedOfficialInjuryReportError("Official report response is not application/pdf")
        declared = response.headers.get("content-length")
        if declared and int(declared) > max_bytes:
            raise UnsupportedOfficialInjuryReportError("Official report exceeds the byte limit")
        chunks: list[bytes] = []
        total = 0
        for chunk in response.iter_content(64 * 1024):
            if not chunk:
                continue
            total += len(chunk)
            if total > max_bytes:
                raise UnsupportedOfficialInjuryReportError("Official report exceeds the byte limit")
            chunks.append(chunk)
    return parse_official_injury_report(b"".join(chunks), url=url, max_pages=max_pages)


def _page_lines(page) -> list[tuple[float, list[tuple[float, str]]]]:
    words: list[tuple[float, float, str]] = []

    def visitor(text, cm, tm, _font, _size):
        value = " ".join(text.split())
        if not value:
            return
        x = tm[4] * cm[0] + tm[5] * cm[2] + cm[4]
        y = tm[4] * cm[1] + tm[5] * cm[3] + cm[5]
        words.append((float(x), float(y), value))

    page.extract_text(visitor_text=visitor)
    lines: list[list] = []
    for x, y, value in sorted(words, key=lambda item: (-item[1], item[0])):
        if not lines or abs(lines[-1][0] - y) > _Y_TOLERANCE:
            lines.append([y, [(x, value)]])
        else:
            lines[-1][1].append((x, value))
    return [(float(y), sorted(parts)) for y, parts in lines]


def _column_text(parts: list[tuple[float, str]], left: float, right: float = float("inf")) -> str:
    return " ".join(value for x, value in parts if left <= x < right).strip()


def _canonical_player(value: str) -> str:
    if value.count(",") != 1:
        raise UnsupportedOfficialInjuryReportError(f"Unsupported official player name layout: {value!r}")
    family, given = (part.strip() for part in value.split(",", 1))
    if not family or not given:
        raise UnsupportedOfficialInjuryReportError(f"Incomplete official player name: {value!r}")
    return f"{given} {family}"


def parse_official_injury_report(
    pdf_bytes: bytes, *, url: str, max_pages: int = MAX_PDF_PAGES
) -> OfficialInjuryReport:
    """Parse the supported tabular NBA PDF layout or fail without partial success."""
    filename_date, _ = _validated_url(url)
    if not pdf_bytes or len(pdf_bytes) > MAX_PDF_BYTES:
        raise UnsupportedOfficialInjuryReportError("Official report PDF is empty or exceeds the byte limit")
    try:
        reader = PdfReader(io.BytesIO(pdf_bytes), strict=True)
    except Exception as exc:
        raise UnsupportedOfficialInjuryReportError("Official report is not a readable strict PDF") from exc
    if not 1 <= len(reader.pages) <= min(max_pages, MAX_PDF_PAGES):
        raise UnsupportedOfficialInjuryReportError("Official report page count is outside the supported bounds")

    header: str | None = None
    context: dict[str, str] = {"game_date": "", "game_time": "", "matchup": "", "team": ""}
    records: list[dict[str, Any]] = []
    pending_reasons: list[tuple[int, float, str]] = []
    saw_columns = False

    def flush_before_anchor(page_index: int, anchor_y: float) -> list[str]:
        nonlocal pending_reasons
        upcoming: list[str] = []
        previous: list[str] = []
        for reason_page, reason_y, text in pending_reasons:
            if reason_page == page_index and reason_y > anchor_y and reason_y - anchor_y <= 12.0:
                upcoming.append(text)
            else:
                previous.append(text)
        if previous:
            if not records:
                raise UnsupportedOfficialInjuryReportError("Reason continuation appeared before any player row")
            records[-1]["reason_parts"].extend(previous)
        pending_reasons = []
        return upcoming

    for page_index, page in enumerate(reader.pages):
        lines = _page_lines(page)
        page_header = None
        for _y, parts in lines:
            candidate = " ".join(value for _x, value in parts)
            if candidate.startswith("Injury Report:"):
                page_header = candidate
                break
        if page_header is None:
            raise UnsupportedOfficialInjuryReportError(f"Page {page_index + 1} has no printed Injury Report header")
        if header is None:
            header = page_header
        elif page_header != header:
            raise UnsupportedOfficialInjuryReportError("Printed report header changes between pages")

        for y, parts in lines:
            whole = " ".join(value for _x, value in parts)
            if whole == page_header or whole.startswith("Page "):
                continue
            if all(token in whole for token in ("Game", "Date", "Time", "Matchup", "Team", "Player", "Status", "Reason")):
                saw_columns = True
                continue

            date_text = _column_text(parts, 0, 110)
            time_text = _column_text(parts, 110, 190)
            matchup_text = _column_text(parts, 190, 255)
            team_text = _column_text(parts, 255, 415)
            player_text = _column_text(parts, 415, 575)
            status_text = _column_text(parts, 575, 660)
            reason_text = _column_text(parts, 660)

            if date_text:
                try:
                    parsed_game_date = datetime.strptime(date_text, "%m/%d/%Y").date()
                except ValueError as exc:
                    raise UnsupportedOfficialInjuryReportError(f"Unsupported game date cell: {date_text!r}") from exc
                context["game_date"] = parsed_game_date.isoformat()
            if time_text:
                if not re.fullmatch(r"\d{1,2}:\d{2} \(ET\)", time_text):
                    raise UnsupportedOfficialInjuryReportError(f"Unsupported game time cell: {time_text!r}")
                context["game_time"] = time_text
            if matchup_text:
                if not re.fullmatch(r"[A-Z]{2,3}@[A-Z]{2,3}", matchup_text):
                    raise UnsupportedOfficialInjuryReportError(f"Unsupported matchup cell: {matchup_text!r}")
                context["matchup"] = matchup_text
            if team_text:
                context["team"] = team_text

            if status_text:
                if status_text not in VALID_STATUSES:
                    raise UnsupportedOfficialInjuryReportError(f"Unsupported status cell: {status_text!r}")
                if not player_text:
                    raise UnsupportedOfficialInjuryReportError("Status cell has no player name")
                upcoming_reasons = flush_before_anchor(page_index, y)
                if not all(context.values()):
                    raise UnsupportedOfficialInjuryReportError(
                        f"Player row lacks inherited game context: {player_text!r}"
                    )
                records.append({
                    **context,
                    "player": _canonical_player(player_text),
                    "status": status_text,
                    "reason_parts": [*upcoming_reasons, *([reason_text] if reason_text else [])],
                })
            elif player_text:
                raise UnsupportedOfficialInjuryReportError(f"Player cell has no supported status: {player_text!r}")
            elif reason_text and reason_text != "NOT YET SUBMITTED":
                pending_reasons.append((page_index, y, reason_text))

    if pending_reasons:
        if not records:
            raise UnsupportedOfficialInjuryReportError("Reason continuation appeared without a player row")
        records[-1]["reason_parts"].extend(text for _page, _y, text in pending_reasons)
    if header is None or not saw_columns or not records:
        raise UnsupportedOfficialInjuryReportError("Official report has no supported table rows")

    publication_date, publication_time, published_at = _publication_from_header(header, filename_date)
    rows: list[OfficialInjuryRow] = []
    for ordinal, record in enumerate(records, 1):
        reason = " ".join(record.pop("reason_parts")).strip()
        if not reason:
            raise UnsupportedOfficialInjuryReportError(f"Official row {ordinal} has no complete reason")
        rows.append(OfficialInjuryRow(reason=reason, **record))
    digest = hashlib.sha256(pdf_bytes).hexdigest()
    return OfficialInjuryReport(
        source_url=url,
        capture_sha256=digest,
        printed_header=header,
        publication_date=publication_date.isoformat(),
        publication_time=publication_time,
        published_at_utc=published_at.isoformat(),
        page_count=len(reader.pages),
        rows=tuple(rows),
    )
