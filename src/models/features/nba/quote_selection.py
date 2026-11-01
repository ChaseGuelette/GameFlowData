"""Deterministic NBA Over/Under quote pairing and optional selection."""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Any

from src.models.features.nba.observation_policy import (
    ObservationDecision,
    observation_payload,
    validate_observation_decision,
)

QUOTE_SELECTION_SCHEMA_VERSION = "nba-quote-selection/v1"


class QuoteContractError(ValueError):
    """Raised when a caller violates the quote-selection API contract."""


class Side(StrEnum):
    OVER = "over"
    UNDER = "under"


class OddsFormat(StrEnum):
    AMERICAN = "american"
    DECIMAL = "decimal"


class QuoteRejectionReason(StrEnum):
    EMPTY_IDENTITY = "empty_identity"
    INVALID_SIDE = "invalid_side"
    INVALID_LINE = "invalid_line"
    UNKNOWN_ODDS_FORMAT = "unknown_odds_format"
    INVALID_PRICE = "invalid_price"
    OBSERVATION_INELIGIBLE = "observation_ineligible"
    MISSING_SIDE = "missing_side"
    INELIGIBLE_COUNTERPART = "ineligible_counterpart"
    SNAPSHOT_CONFLICT = "snapshot_conflict"
    REUSED_SOURCE_IDENTITY = "reused_source_identity"
    INCOMPATIBLE_OBSERVATION_CONTEXT = "incompatible_observation_context"


class LedgerDisposition(StrEnum):
    SELECTED = "selected"
    ELIGIBLE_NOT_SELECTED = "eligible_not_selected"
    DUPLICATE = "duplicate"
    REJECTED = "rejected"


class SelectionStatus(StrEnum):
    NOT_REQUESTED = "not_requested"
    SELECTED = "selected"
    NO_ELIGIBLE_PAIRS = "no_eligible_pairs"
    AMBIGUOUS_TOP_RANK = "ambiguous_top_rank"


@dataclass(frozen=True, slots=True)
class SourceRecordRef:
    source_namespace: str
    source_record_id: str


@dataclass(frozen=True, slots=True)
class QuoteInput:
    game_id: str
    player_id: str
    market: str
    period: str
    variant: str
    bookmaker: str
    line: Decimal | int | float | str
    side: Side | str
    source_namespace: str
    snapshot_compatibility_key: str
    source_record_id: str
    odds_format: OddsFormat | str
    odds_value: Decimal | int | float | str
    observation: ObservationDecision


@dataclass(frozen=True, slots=True)
class PairedQuoteSide:
    semantic_quote_id: str
    side: Side
    line: Decimal
    odds_format: OddsFormat
    odds_value: Decimal
    source_refs: tuple[SourceRecordRef, ...]
    occurrence_count: int
    raw_quote: QuoteInput

    @property
    def raw_provenance(self) -> tuple[Any, ...]:
        return self.raw_quote.observation.raw_evidence

    @property
    def interpreted_provenance(self) -> tuple[tuple[str, str], ...]:
        interpretation = self.raw_quote.observation.interpretation
        return () if interpretation is None else interpretation.provenance


@dataclass(frozen=True, slots=True)
class QuotePair:
    schema_version: str
    pair_id: str
    game_id: str
    player_id: str
    market: str
    period: str
    variant: str
    bookmaker: str
    line: Decimal
    source_namespace: str
    snapshot_compatibility_key: str
    over: PairedQuoteSide
    under: PairedQuoteSide


@dataclass(frozen=True, slots=True)
class LedgerEntry:
    occurrence_index: int
    source_ref: SourceRecordRef
    disposition: LedgerDisposition
    semantic_quote_id: str | None
    pair_id: str | None
    canonical_source_ref: SourceRecordRef | None
    raw_quote: QuoteInput
    reasons: tuple[QuoteRejectionReason, ...] = ()
    observation_reasons: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class QuoteSelectionResult:
    schema_version: str
    eligible_pairs: tuple[QuotePair, ...]
    selected_pair: QuotePair | None
    selection_status: SelectionStatus
    top_rank: Decimal | None
    ledger: tuple[LedgerEntry, ...]


QuoteRanker = Callable[[QuotePair], int | float | Decimal]


@dataclass(frozen=True, slots=True)
class _NormalizedQuote:
    occurrence_index: int
    raw: QuoteInput
    identities: tuple[str, str, str, str, str, str, str, str]
    line: Decimal | None
    side: Side | None
    odds_format: OddsFormat | None
    odds_value: Decimal | None
    source_ref: SourceRecordRef
    validation_reasons: tuple[QuoteRejectionReason, ...]
    semantic_payload: dict[str, Any]
    semantic_quote_id: str
    group_key: tuple[str, str, str, str, str, str, str, str, str] | None


@dataclass(frozen=True, slots=True)
class _Candidate:
    normalized: _NormalizedQuote
    source_refs: tuple[SourceRecordRef, ...]
    occurrence_indices: tuple[int, ...]


_IDENTITY_FIELDS = (
    "game_id",
    "player_id",
    "market",
    "period",
    "variant",
    "bookmaker",
    "source_namespace",
    "snapshot_compatibility_key",
)


def _identity(value: Any) -> str:
    # Opaque identifiers are case-sensitive; adapters own source mappings.
    return value if isinstance(value, str) and value.strip() else ""


def _decimal(value: Decimal | int | float | str) -> Decimal | None:
    if isinstance(value, bool):
        return None
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return result if result.is_finite() else None


def _decimal_text(value: Decimal | None, raw: Any) -> str | None:
    if value is not None:
        if value == 0:
            return "0"
        text = format(value, "f")
        if "." in text:
            text = text.rstrip("0").rstrip(".")
        return text
    if isinstance(raw, str | int | float | Decimal) and not isinstance(raw, bool):
        return str(raw)
    return None


def _enum_value(value: Any, enum_type: type[StrEnum]) -> StrEnum | None:
    if isinstance(value, enum_type):
        return value
    if isinstance(value, str):
        normalized = value.strip().casefold()
        for item in enum_type:
            if item.value == normalized:
                return item
    return None


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def _content_id(kind: str, payload: dict[str, Any]) -> str:
    material = {"schema_version": QUOTE_SELECTION_SCHEMA_VERSION, "kind": kind, "content": payload}
    return hashlib.sha256(_canonical_json(material).encode("utf-8")).hexdigest()


def _normalize_quote(raw: QuoteInput, occurrence_index: int) -> _NormalizedQuote:
    if not isinstance(raw, QuoteInput):
        raise QuoteContractError("every input must be a QuoteInput")
    if not isinstance(raw.observation, ObservationDecision):
        raise QuoteContractError("QuoteInput.observation must be an ObservationDecision")
    validate_observation_decision(raw.observation)
    if any(not isinstance(getattr(raw, field), str) for field in (*_IDENTITY_FIELDS, "source_record_id", "side", "odds_format")):
        raise QuoteContractError("quote identity, side and format fields must be strings")
    if any(not isinstance(value, Decimal | int | float | str) for value in (raw.line, raw.odds_value)):
        raise QuoteContractError("line and price must be immutable numeric scalars or strings")

    identities = (
        _identity(raw.game_id),
        _identity(raw.player_id),
        _identity(raw.market),
        _identity(raw.period),
        _identity(raw.variant),
        _identity(raw.bookmaker),
        _identity(raw.source_namespace),
        _identity(raw.snapshot_compatibility_key),
    )
    reasons: list[QuoteRejectionReason] = []
    if any(not value for value in identities) or not _identity(raw.source_record_id):
        reasons.append(QuoteRejectionReason.EMPTY_IDENTITY)

    line = _decimal(raw.line)
    if line is None:
        reasons.append(QuoteRejectionReason.INVALID_LINE)

    side_value = _enum_value(raw.side, Side)
    side = side_value if isinstance(side_value, Side) else None
    if side is None:
        reasons.append(QuoteRejectionReason.INVALID_SIDE)

    format_value = _enum_value(raw.odds_format, OddsFormat)
    odds_format = format_value if isinstance(format_value, OddsFormat) else None
    if odds_format is None:
        reasons.append(QuoteRejectionReason.UNKNOWN_ODDS_FORMAT)

    odds_value = _decimal(raw.odds_value)
    if odds_value is None:
        reasons.append(QuoteRejectionReason.INVALID_PRICE)
    elif odds_format is OddsFormat.AMERICAN and odds_value.copy_abs() < Decimal(100):
        reasons.append(QuoteRejectionReason.INVALID_PRICE)
    elif odds_format is OddsFormat.DECIMAL and odds_value <= Decimal(1):
        reasons.append(QuoteRejectionReason.INVALID_PRICE)

    source_ref = SourceRecordRef(identities[6], raw.source_record_id)
    payload = {
        "identity": dict(zip(_IDENTITY_FIELDS, identities, strict=True)),
        "line": _decimal_text(line, raw.line),
        "side": side.value if side is not None else (raw.side.strip().casefold() if isinstance(raw.side, str) else None),
        "odds_format": odds_format.value
        if odds_format is not None
        else (raw.odds_format.strip().casefold() if isinstance(raw.odds_format, str) else None),
        "odds_value": _decimal_text(odds_value, raw.odds_value),
        "observation": observation_payload(raw.observation),
        "validation_reasons": sorted(reason.value for reason in reasons),
    }
    semantic_quote_id = _content_id("quote", payload)
    group_key = None if line is None or any(not value for value in identities) else (*identities[:6], _decimal_text(line, raw.line) or "", *identities[6:])
    return _NormalizedQuote(
        occurrence_index,
        raw,
        identities,
        line,
        side,
        odds_format,
        odds_value,
        source_ref,
        tuple(dict.fromkeys(reasons)),
        payload,
        semantic_quote_id,
        group_key,
    )


def _source_ref_key(ref: SourceRecordRef) -> tuple[str, str]:
    return (ref.source_namespace, ref.source_record_id)


def _raw_representation_key(quote: _NormalizedQuote) -> str:
    """Deterministically retain a raw representative of normalized duplicates."""
    payload = {
        field: {"type": type(getattr(quote.raw, field)).__name__, "value": str(getattr(quote.raw, field))}
        for field in ("line", "odds_value", "side", "odds_format")
    }
    interpretation = quote.raw.observation.interpretation
    return _canonical_json({
        "scalars": payload,
        "provenance_order": None if interpretation is None else interpretation.provenance,
        "rejection_order": quote.raw.observation.reasons,
    })


def _candidate_side(candidate: _Candidate) -> PairedQuoteSide:
    quote = candidate.normalized
    assert quote.side is not None and quote.line is not None and quote.odds_format is not None and quote.odds_value is not None
    return PairedQuoteSide(
        quote.semantic_quote_id,
        quote.side,
        quote.line,
        quote.odds_format,
        quote.odds_value,
        candidate.source_refs,
        len(candidate.occurrence_indices),
        quote.raw,
    )


def _pair_from_candidates(over: _Candidate, under: _Candidate) -> QuotePair:
    quote = over.normalized
    assert quote.group_key is not None and quote.line is not None
    identities = quote.identities
    payload = {
        "pair_key": {
            "game_id": identities[0],
            "player_id": identities[1],
            "market": identities[2],
            "period": identities[3],
            "variant": identities[4],
            "bookmaker": identities[5],
            "line": _decimal_text(quote.line, quote.raw.line),
            "source_namespace": identities[6],
            "snapshot_compatibility_key": identities[7],
        },
        "over_semantic_quote_id": over.normalized.semantic_quote_id,
        "under_semantic_quote_id": under.normalized.semantic_quote_id,
    }
    pair_id = _content_id("pair", payload)
    return QuotePair(
        QUOTE_SELECTION_SCHEMA_VERSION,
        pair_id,
        *identities[:6],
        quote.line,
        *identities[6:],
        _candidate_side(over),
        _candidate_side(under),
    )


def _pair_sort_key(pair: QuotePair) -> tuple[Any, ...]:
    return (
        pair.game_id,
        pair.player_id,
        pair.market,
        pair.period,
        pair.variant,
        pair.bookmaker,
        pair.line,
        pair.source_namespace,
        pair.snapshot_compatibility_key,
        pair.pair_id,
    )


def select_quote_pairs(
    quotes: tuple[QuoteInput, ...],
    *,
    ranker: QuoteRanker | None = None,
) -> QuoteSelectionResult:
    """Return every eligible complete pair and optionally select the max-ranked pair.

    Conflict detection precedes eligibility filtering. ``ranker`` values must be
    finite numeric values; the maximum wins, and a materially different top-rank
    tie produces ``AMBIGUOUS_TOP_RANK`` with no selected pair.
    """
    if not isinstance(quotes, tuple):
        raise QuoteContractError("quotes must be an immutable tuple")
    if ranker is not None and not callable(ranker):
        raise QuoteContractError("ranker must be callable")

    normalized = tuple(_normalize_quote(raw, index) for index, raw in enumerate(quotes))
    by_semantic: dict[str, list[_NormalizedQuote]] = defaultdict(list)
    for quote in normalized:
        by_semantic[quote.semantic_quote_id].append(quote)

    candidates: list[_Candidate] = []
    occurrence_to_candidate: dict[int, _Candidate] = {}
    duplicate_indices: set[int] = set()
    for records in by_semantic.values():
        refs = tuple(sorted({item.source_ref for item in records}, key=_source_ref_key))
        canonical_ref = refs[0]
        canonical_records = sorted(
            records,
            key=lambda item: (_source_ref_key(item.source_ref), _raw_representation_key(item), item.occurrence_index),
        )
        canonical = canonical_records[0]
        occurrence_indices = tuple(sorted(item.occurrence_index for item in records))
        candidate = _Candidate(canonical, refs, occurrence_indices)
        candidates.append(candidate)
        for item in records:
            occurrence_to_candidate[item.occurrence_index] = candidate
            if item.occurrence_index != canonical.occurrence_index:
                duplicate_indices.add(item.occurrence_index)
        if canonical.source_ref != canonical_ref:
            raise QuoteContractError("internal canonical source-reference mismatch")

    source_material: dict[SourceRecordRef, set[str]] = defaultdict(set)
    source_groups: dict[SourceRecordRef, set[tuple[str, ...]]] = defaultdict(set)
    for quote in normalized:
        if not _identity(quote.source_ref.source_namespace) or not _identity(quote.source_ref.source_record_id):
            continue
        source_material[quote.source_ref].add(quote.semantic_quote_id)
        if quote.group_key is not None:
            source_groups[quote.source_ref].add(quote.group_key)
    reused_refs = {ref for ref, material in source_material.items() if len(material) > 1}
    reused_groups = {group for ref in reused_refs for group in source_groups[ref]}

    grouped: dict[tuple[str, ...], list[_Candidate]] = defaultdict(list)
    ungrouped: list[_Candidate] = []
    for candidate in candidates:
        if candidate.normalized.group_key is None:
            ungrouped.append(candidate)
        else:
            grouped[candidate.normalized.group_key].append(candidate)

    canonical_disposition: dict[int, tuple[LedgerDisposition, tuple[QuoteRejectionReason, ...], str | None]] = {}
    eligible_pairs: list[QuotePair] = []
    pair_candidates: dict[str, tuple[_Candidate, _Candidate]] = {}

    for candidate in ungrouped:
        canonical_disposition[candidate.normalized.occurrence_index] = (
            LedgerDisposition.REJECTED,
            candidate.normalized.validation_reasons,
            None,
        )

    for group_key, group_candidates in grouped.items():
        canonical_indices = [candidate.normalized.occurrence_index for candidate in group_candidates]
        if group_key in reused_groups:
            for index in canonical_indices:
                canonical_disposition[index] = (
                    LedgerDisposition.REJECTED,
                    (QuoteRejectionReason.REUSED_SOURCE_IDENTITY, *normalized[index].validation_reasons),
                    None,
                )
            continue

        by_side: dict[Side, list[_Candidate]] = defaultdict(list)
        invalid_candidates: list[_Candidate] = []
        for candidate in group_candidates:
            if candidate.normalized.side is None or candidate.normalized.validation_reasons:
                invalid_candidates.append(candidate)
            else:
                by_side[candidate.normalized.side].append(candidate)

        # Malformed-price records can still contradict an identifiable side.
        conflict = any(
            sum(candidate.normalized.side is side for candidate in group_candidates) > 1
            for side in Side
        )
        if conflict:
            for index in canonical_indices:
                canonical_disposition[index] = (
                    LedgerDisposition.REJECTED,
                    (QuoteRejectionReason.SNAPSHOT_CONFLICT, *normalized[index].validation_reasons),
                    None,
                )
            continue

        for candidate in invalid_candidates:
            canonical_disposition[candidate.normalized.occurrence_index] = (
                LedgerDisposition.REJECTED,
                candidate.normalized.validation_reasons,
                None,
            )

        over = by_side.get(Side.OVER, [])
        under = by_side.get(Side.UNDER, [])
        if not over or not under:
            for candidate in (*over, *under):
                canonical_disposition[candidate.normalized.occurrence_index] = (
                    LedgerDisposition.REJECTED,
                    (QuoteRejectionReason.MISSING_SIDE,),
                    None,
                )
            continue

        over_candidate, under_candidate = over[0], under[0]
        over_eligible = over_candidate.normalized.raw.observation.accepted
        under_eligible = under_candidate.normalized.raw.observation.accepted
        if not over_eligible or not under_eligible:
            for candidate, eligible, counterpart_eligible in (
                (over_candidate, over_eligible, under_eligible),
                (under_candidate, under_eligible, over_eligible),
            ):
                reason = (
                    QuoteRejectionReason.INELIGIBLE_COUNTERPART
                    if eligible and not counterpart_eligible
                    else QuoteRejectionReason.OBSERVATION_INELIGIBLE
                )
                canonical_disposition[candidate.normalized.occurrence_index] = (
                    LedgerDisposition.REJECTED,
                    (reason,),
                    None,
                )
            continue

        over_observation = over_candidate.normalized.raw.observation
        under_observation = under_candidate.normalized.raw.observation
        assert over_observation.decision_at is not None and under_observation.decision_at is not None
        assert over_observation.commence_at is not None and under_observation.commence_at is not None
        if (
            over_observation.interpreter_id != under_observation.interpreter_id
            or over_observation.decision_at.astimezone(UTC) != under_observation.decision_at.astimezone(UTC)
            or over_observation.commence_at.astimezone(UTC) != under_observation.commence_at.astimezone(UTC)
        ):
            for candidate in (over_candidate, under_candidate):
                canonical_disposition[candidate.normalized.occurrence_index] = (
                    LedgerDisposition.REJECTED,
                    (QuoteRejectionReason.INCOMPATIBLE_OBSERVATION_CONTEXT,),
                    None,
                )
            continue

        pair = _pair_from_candidates(over_candidate, under_candidate)
        eligible_pairs.append(pair)
        pair_candidates[pair.pair_id] = (over_candidate, under_candidate)
        for candidate in (over_candidate, under_candidate):
            canonical_disposition[candidate.normalized.occurrence_index] = (
                LedgerDisposition.ELIGIBLE_NOT_SELECTED,
                (),
                pair.pair_id,
            )

    eligible_pairs.sort(key=_pair_sort_key)
    selected_pair: QuotePair | None = None
    top_rank: Decimal | None = None
    if ranker is None:
        selection_status = SelectionStatus.NOT_REQUESTED
    elif not eligible_pairs:
        selection_status = SelectionStatus.NO_ELIGIBLE_PAIRS
    else:
        ranked: list[tuple[Decimal, QuotePair]] = []
        for pair in eligible_pairs:
            raw_rank = ranker(pair)
            if isinstance(raw_rank, bool) or not isinstance(raw_rank, int | float | Decimal):
                raise QuoteContractError("ranker must return a finite numeric value")
            rank = raw_rank if isinstance(raw_rank, Decimal) else Decimal(str(raw_rank))
            if not rank.is_finite():
                raise QuoteContractError("ranker must return a finite numeric value")
            ranked.append((rank, pair))
        top_rank = max(rank for rank, _ in ranked)
        top_pairs = [pair for rank, pair in ranked if rank == top_rank]
        if len(top_pairs) != 1:
            selection_status = SelectionStatus.AMBIGUOUS_TOP_RANK
        else:
            selected_pair = top_pairs[0]
            selection_status = SelectionStatus.SELECTED
            for candidate in pair_candidates[selected_pair.pair_id]:
                canonical_disposition[candidate.normalized.occurrence_index] = (
                    LedgerDisposition.SELECTED,
                    (),
                    selected_pair.pair_id,
                )

    ledger: list[LedgerEntry] = []
    for quote in normalized:
        candidate = occurrence_to_candidate[quote.occurrence_index]
        canonical = candidate.normalized
        canonical_ref = candidate.source_refs[0]
        if quote.occurrence_index in duplicate_indices:
            pair_id = canonical_disposition.get(canonical.occurrence_index, (LedgerDisposition.REJECTED, (), None))[2]
            ledger.append(
                LedgerEntry(
                    quote.occurrence_index,
                    quote.source_ref,
                    LedgerDisposition.DUPLICATE,
                    quote.semantic_quote_id,
                    pair_id,
                    canonical_ref,
                    quote.raw,
                    canonical_disposition[canonical.occurrence_index][1],
                    tuple(reason.value for reason in quote.raw.observation.reasons),
                )
            )
            continue
        disposition, reasons, pair_id = canonical_disposition.get(
            quote.occurrence_index,
            (LedgerDisposition.REJECTED, quote.validation_reasons, None),
        )
        observation_reasons = tuple(reason.value for reason in quote.raw.observation.reasons)
        ledger.append(
            LedgerEntry(
                quote.occurrence_index,
                quote.source_ref,
                disposition,
                quote.semantic_quote_id,
                pair_id,
                canonical_ref,
                quote.raw,
                reasons,
                observation_reasons,
            )
        )

    return QuoteSelectionResult(
        QUOTE_SELECTION_SCHEMA_VERSION,
        tuple(eligible_pairs),
        selected_pair,
        selection_status,
        top_rank,
        tuple(ledger),
    )


__all__ = [
    "QUOTE_SELECTION_SCHEMA_VERSION",
    "LedgerDisposition",
    "LedgerEntry",
    "OddsFormat",
    "PairedQuoteSide",
    "QuoteContractError",
    "QuoteInput",
    "QuotePair",
    "QuoteRanker",
    "QuoteRejectionReason",
    "QuoteSelectionResult",
    "SelectionStatus",
    "Side",
    "SourceRecordRef",
    "select_quote_pairs",
]
