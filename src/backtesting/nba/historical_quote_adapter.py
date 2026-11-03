"""Offline historical outcome-row adapter; source authentication belongs upstream."""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from numbers import Integral
from typing import Any

import pandas as pd

from src.models.features.nba.contracts import NBA_FEATURE_FAMILIES
from src.models.features.nba.historical_observation_adapters import (
    HistoricalSourceClass,
    SourceProvenance,
    evaluate_market_observation,
)
from src.models.features.nba.historical_policy import (
    HistoricalGameContext,
    HistoricalPolicy,
    HistoricalPolicyContractError,
    policy_payload,
)
from src.models.features.nba.observation_policy import (
    AvailabilityInterpretation,
    ClockEvidence,
    InterpretationStatus,
    ObservationContext,
    evaluate_observation,
    observation_payload,
)
from src.models.features.nba.quote_selection import (
    QUOTE_SELECTION_SCHEMA_VERSION,
    LedgerDisposition,
    QuoteInput,
    QuoteRejectionReason,
    select_quote_pairs,
)

HISTORICAL_QUOTE_ADAPTER_SCHEMA_VERSION = "nba-historical-quote-adapter/v1"
_CLOCKS = ("historical_requested_at", "market_updated_at", "bookmaker_updated_at", "local_observed_at", "snapshot_time")
_PAIR_COLUMNS = [
    "player_id", "game_id", "market_key", "line", "bookmaker", "over_odds", "under_odds",
    "adapter_schema_version", "quote_schema_version", "pair_id", "period", "variant",
    "source_namespace", "snapshot_compatibility_key", "policy_content_id", "context_identity",
    "over_metadata", "under_metadata",
]
_LEDGER_COLUMNS = [
    "occurrence_index", "raw_record", "raw_game_id", "game_id", "game_id_mapping",
    "policy_content_id", "context_identity", "source_ref", "disposition", "selector_disposition",
    "adapter_reasons", "quote_reasons", "observation_reasons", "observation", "semantic_quote_id",
    "pair_id", "canonical_source_ref", "blocked", "adapter_schema_version", "quote_schema_version",
]


class AdapterReason(StrEnum):
    INVALID_GAME_ID = "invalid_game_id"
    MISSING_CONTEXT = "missing_context"
    MISSING_PROVENANCE = "missing_provenance"
    PROVENANCE_MISMATCH = "provenance_mismatch"
    INVALID_IDENTITY = "invalid_identity"
    INVALID_CLOCK = "invalid_clock"
    MISSING_SNAPSHOT_TIME = "missing_snapshot_time"
    MISSING_MARKET_UPDATE = "missing_market_update"
    MISSING_SOURCE_CLOCK = "missing_source_clock"
    SOURCE_NOT_AUTHORIZED = "source_not_authorized"
    INCOHERENT_SOURCE_CLOCKS = "incoherent_source_clocks"
    AT_OR_AFTER_DECISION = "at_or_after_decision"
    AT_OR_AFTER_COMMENCE = "at_or_after_commence"
    STALE_QUOTE = "stale_quote"
    COMMENCE_MISMATCH = "commence_mismatch"
    INELIGIBLE_COUNTERPART = "adapter_ineligible_counterpart"
    SOURCE_RECORD_CONFLICT = "source_record_conflict"


_BLOCKING = {
    AdapterReason.INVALID_GAME_ID, AdapterReason.MISSING_CONTEXT, AdapterReason.MISSING_PROVENANCE,
    AdapterReason.PROVENANCE_MISMATCH, AdapterReason.INVALID_IDENTITY, AdapterReason.INVALID_CLOCK,
    AdapterReason.MISSING_SNAPSHOT_TIME, AdapterReason.MISSING_MARKET_UPDATE,
    AdapterReason.MISSING_SOURCE_CLOCK,
    AdapterReason.SOURCE_NOT_AUTHORIZED, AdapterReason.INCOHERENT_SOURCE_CLOCKS,
    AdapterReason.SOURCE_RECORD_CONFLICT,
}


@dataclass(frozen=True)
class HistoricalQuoteResult:
    """All eligible pairs, one ledger row per occurrence, and explicit accounting."""

    pairs: pd.DataFrame
    ledger: pd.DataFrame
    summary: Mapping[str, Any]


def _exact(value: Any) -> bool:
    return isinstance(value, str) and bool(value) and value == value.strip()


def resolve_game_context(
    raw_game_id: Any, contexts: Mapping[str, HistoricalGameContext],
) -> tuple[HistoricalGameContext | None, str]:
    """Exact match or ASCII 8–10 digit left-padding to a supplied 10-digit context.

    No integer coercion, stripping, opaque-ID repair, or inferred context creation.
    The adapter checks the context mapping contract before calling this seam.
    """
    if not _exact(raw_game_id):
        return None, AdapterReason.INVALID_GAME_ID.value
    if re.fullmatch(r"[0-9]{8,10}", raw_game_id):
        canonical = raw_game_id.zfill(10)
        if raw_game_id != canonical and raw_game_id in contexts:
            raise HistoricalPolicyContractError("noncanonical decimal context alias would make mapping ambiguous")
        context = contexts.get(canonical)
        if context is not None and (not isinstance(context, HistoricalGameContext) or context.game_id != canonical):
            raise HistoricalPolicyContractError("mapped context identity mismatch")
        return (context, "exact" if canonical == raw_game_id else "nba_ascii_decimal_left_zero_pad/v1") if context else (
            None, AdapterReason.MISSING_CONTEXT.value,
        )
    context = contexts.get(raw_game_id)
    if context is not None and (not isinstance(context, HistoricalGameContext) or context.game_id != raw_game_id):
        raise HistoricalPolicyContractError("exact context identity mismatch")
    return (context, "exact") if context else (None, AdapterReason.MISSING_CONTEXT.value)


def _clock(value: Any) -> datetime | None:
    # Preserve raw text in the ledger; parse ISO text without assigning a timezone.
    if value is None or value is pd.NaT or value is pd.NA:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
    return None


def _aware(value: datetime | None) -> bool:
    return value is not None and value.tzinfo is not None and value.utcoffset() is not None


def _context_identity(context: HistoricalGameContext | None) -> dict[str, Any] | None:
    if context is None:
        return None
    return {
        "game_id": context.game_id, "game_date": context.game_date.isoformat(),
        "decision_at": context.decision_at.isoformat(), "commence_at": context.commence_at.isoformat(),
        "commence_provenance_ref": context.commence_provenance_ref, "policy_content_id": context.policy_content_id,
    }


def _text(value: Any) -> str:
    return value if isinstance(value, str) else ""


def _number(value: Any) -> Decimal | int | float | str:
    # Nonrepresentable source scalars become invalid input, never a valid price/line.
    if isinstance(value, Integral) and not isinstance(value, bool):
        return int(value)
    return value if isinstance(value, Decimal | int | float | str) else ""


def _player(value: Any) -> str:
    # Type-tagging avoids pairing integer 123 with opaque string "123".
    if isinstance(value, Integral) and not isinstance(value, bool):
        return f"int:{int(value)}"
    return f"str:{value}" if _exact(value) else ""


def _snapshot_identity(value: Any) -> tuple[str, str]:
    """Bind source identity to its required snapshot clock, including missingness."""
    clock = _clock(value)
    if _aware(clock):
        return "utc", clock.astimezone(UTC).isoformat()
    if value is None or value is pd.NaT or value is pd.NA:
        return "missing", ""
    return type(value).__name__, repr(value)


def _metadata_record(raw: Mapping[str, Any]) -> dict[str, Any]:
    """Use a deterministic, merge-compatible representation for integral IDs."""
    record = dict(raw)
    player = record.get("player_id")
    if isinstance(player, Integral) and not isinstance(player, bool):
        record["player_id"] = int(player)
    return record


def _metadata_order(raw: Mapping[str, Any]) -> tuple[str, str, tuple[str, str], str]:
    record = _metadata_record(raw)
    return (
        _text(record.get("source_namespace")), _text(record.get("source_record_id")),
        _snapshot_identity(record.get("snapshot_time")),
        repr(tuple(sorted(record.items(), key=lambda item: item[0]))),
    )


def adapt_historical_quotes(
    rows: pd.DataFrame | Iterable[Mapping[str, Any]],
    *,
    contexts: Mapping[str, HistoricalGameContext],
    policy: HistoricalPolicy,
) -> HistoricalQuoteResult:
    """Adapt explicit raw outcome occurrences, without acquisition or ranking.

    Required raw fields and the upstream trust boundary are documented in
    docs/understanding/nba-s8-quote-adapter.md. Contract/programming errors raise;
    malformed source values are measured, not silently dropped.
    """
    if not isinstance(policy, HistoricalPolicy):
        raise HistoricalPolicyContractError("policy must be HistoricalPolicy")
    policy.require_execution_ready(required_families=tuple(sorted(NBA_FEATURE_FAMILIES)))
    policy_id = policy.content_id
    if not isinstance(contexts, Mapping):
        raise HistoricalPolicyContractError("contexts must be an explicit mapping")
    for key, context in contexts.items():
        if not isinstance(context, HistoricalGameContext) or key != context.game_id:
            raise HistoricalPolicyContractError("context keys must exactly equal their context game_id")
        if context.policy_content_id != policy_id:
            raise HistoricalPolicyContractError("context policy identity mismatch")
        if re.fullmatch(r"[0-9]+", key) and re.fullmatch(r"[0-9]{10}", key) is None:
            raise HistoricalPolicyContractError("decimal context IDs must be canonical 10-digit NBA tokens")
    source_policy = policy_payload(policy)["authorities"]["source"]["value"]
    raw_rows = rows.to_dict("records") if isinstance(rows, pd.DataFrame) else list(rows)
    quotes: list[QuoteInput] = []
    evidence: list[dict[str, Any]] = []
    player_values: dict[str, Any] = {}
    source_snapshots: dict[tuple[str, str], set[tuple[str, str]]] = {}
    for index, raw in enumerate(raw_rows):
        if not isinstance(raw, Mapping):
            raise TypeError("each raw occurrence must be a mapping")
        raw = dict(raw)
        reasons: list[AdapterReason] = []
        context, mapping = resolve_game_context(raw.get("game_id"), contexts)
        if context is None:
            reasons.append(AdapterReason(mapping))
        clocks = {key: _clock(raw.get(key)) for key in _CLOCKS}
        for key, value in clocks.items():
            supplied = raw.get(key)
            if supplied is not None and supplied is not pd.NaT and supplied is not pd.NA and not _aware(value):
                reasons.append(AdapterReason.INVALID_CLOCK)
        if clocks["snapshot_time"] is None:
            reasons.append(AdapterReason.MISSING_SNAPSHOT_TIME)
        if source_policy["require_market_update"] and clocks["market_updated_at"] is None:
            reasons.append(AdapterReason.MISSING_MARKET_UPDATE)
        provenance = raw.get("provenance")
        namespace, record_id = _text(raw.get("source_namespace")), _text(raw.get("source_record_id"))
        provenance_ref = dict(provenance.attributes).get("provenance_evidence_ref") if isinstance(provenance, SourceProvenance) else None
        if not isinstance(provenance, SourceProvenance) or not _exact(provenance_ref):
            reasons.append(AdapterReason.MISSING_PROVENANCE)
        elif provenance_ref != source_policy["provenance_evidence_ref"]:
            reasons.append(AdapterReason.PROVENANCE_MISMATCH)
        if isinstance(provenance, SourceProvenance) and (
            provenance.source_namespace != namespace or provenance.source_record_id != record_id
        ):
            reasons.append(AdapterReason.PROVENANCE_MISMATCH)
        player = _player(raw.get("player_id"))
        if player:
            player_value = int(raw["player_id"]) if player.startswith("int:") else raw["player_id"]
            player_values.setdefault(player, player_value)
        identity_fields = ("market_key", "period", "variant", "bookmaker", "source_namespace",
                           "snapshot_compatibility_key", "source_record_id")
        if not player or any(not _exact(raw.get(key)) for key in identity_fields):
            reasons.append(AdapterReason.INVALID_IDENTITY)
        source = _text(raw.get("source_class"))
        historical = source == HistoricalSourceClass.AUTHENTICATED_HISTORICAL_REQUEST
        local = source == HistoricalSourceClass.LOCAL_OBSERVED_AT
        required_clocks = []
        if historical:
            required_clocks.extend(("historical_requested_at", "market_updated_at"))
        if local:
            required_clocks.append("local_observed_at")
        if source_policy["require_bookmaker_update"]:
            required_clocks.append("bookmaker_updated_at")
        if any(clocks[key] is None for key in required_clocks):
            reasons.append(AdapterReason.MISSING_SOURCE_CLOCK)
        incoherent = (historical and clocks["local_observed_at"] is not None) or (
            local and clocks["historical_requested_at"] is not None
        )
        if incoherent:
            reasons.append(AdapterReason.INCOHERENT_SOURCE_CLOCKS)
        market_clocks = {key: clocks[key] for key in _CLOCKS if key != "snapshot_time"}
        if context is not None and isinstance(provenance, SourceProvenance) and not incoherent:
            observation = evaluate_market_observation(
                context=context, source_class=source if isinstance(source, str) else HistoricalSourceClass.UNKNOWN,
                provenance=provenance, require_book_update=source_policy["require_bookmaker_update"], **market_clocks,
            )
        else:
            # No invented SourceProvenance or clock. Still forward this occurrence
            # to conflict detection using a genuine unknown-source observation.
            observation = evaluate_observation(
                context=ObservationContext(context.decision_at if context else None, context.commence_at if context else None),
                raw_evidence=tuple(ClockEvidence(key, value) for key, value in clocks.items()),
                interpreter=lambda _: AvailabilityInterpretation(InterpretationStatus.UNKNOWN_SOURCE),
                interpreter_id="nba-unsupported-market-source/v1",
            )
        if observation.interpreter_id not in source_policy["interpreters"] or not (historical or local):
            reasons.append(AdapterReason.SOURCE_NOT_AUTHORIZED)
        if context is not None:
            for value in clocks.values():
                if _aware(value):
                    assert value is not None
                    if value.astimezone(UTC) >= context.decision_at.astimezone(UTC):
                        reasons.append(AdapterReason.AT_OR_AFTER_DECISION)
                    if value.astimezone(UTC) >= context.commence_at.astimezone(UTC):
                        reasons.append(AdapterReason.AT_OR_AFTER_COMMENCE)
            age_clocks = [clocks["snapshot_time"], clocks["market_updated_at"], clocks["bookmaker_updated_at"]]
            if observation.interpretation is not None:
                age_clocks.append(observation.interpretation.availability_at)
            max_age = timedelta(minutes=source_policy["max_quote_age_minutes"])
            if any(_aware(value) and context.decision_at.astimezone(UTC) - value.astimezone(UTC) > max_age
                   for value in age_clocks):
                reasons.append(AdapterReason.STALE_QUOTE)
            if raw.get("commence_time") is not None:
                commence = _clock(raw["commence_time"])
                if not _aware(commence):
                    reasons.append(AdapterReason.INVALID_CLOCK)
                elif commence.astimezone(UTC) != context.commence_at.astimezone(UTC):
                    reasons.append(AdapterReason.COMMENCE_MISMATCH)
        quotes.append(QuoteInput(
            context.game_id if context else _text(raw.get("game_id")), player, _text(raw.get("market_key")),
            _text(raw.get("period")), _text(raw.get("variant")), _text(raw.get("bookmaker")),
            _number(raw.get("line")), _text(raw.get("side")), _text(namespace),
            _text(raw.get("snapshot_compatibility_key")), _text(record_id), _text(raw.get("odds_format")),
            _number(raw.get("price")), observation,
        ))
        if _exact(namespace) and _exact(record_id):
            source_snapshots.setdefault((namespace, record_id), set()).add(_snapshot_identity(raw.get("snapshot_time")))
        evidence.append({
            "occurrence_index": index, "raw_record": raw, "raw_game_id": raw.get("game_id"),
            "game_id": context.game_id if context else None, "game_id_mapping": mapping,
            "policy_content_id": policy_id, "context_identity": _context_identity(context),
            "adapter_reasons": tuple(dict.fromkeys(reason.value for reason in reasons)),
            "blocked": any(reason in _BLOCKING for reason in reasons),
            "adapter_schema_version": HISTORICAL_QUOTE_ADAPTER_SCHEMA_VERSION,
            "quote_schema_version": QUOTE_SELECTION_SCHEMA_VERSION,
        })
    # Never prefilter by observation, freshness, provenance, or duplicates.
    selection = select_quote_pairs(tuple(quotes), ranker=None)
    conflicting_sources = {ref for ref, snapshots in source_snapshots.items() if len(snapshots) != 1}
    for entry in selection.ledger:
        ref = (entry.source_ref.source_namespace, entry.source_ref.source_record_id)
        if ref in conflicting_sources or QuoteRejectionReason.REUSED_SOURCE_IDENTITY in entry.reasons:
            row = evidence[entry.occurrence_index]
            row["adapter_reasons"] = tuple(dict.fromkeys((*row["adapter_reasons"], AdapterReason.SOURCE_RECORD_CONFLICT.value)))
            row["blocked"] = True
    vetoed = {entry.pair_id for entry in selection.ledger
              if entry.pair_id is not None and evidence[entry.occurrence_index]["adapter_reasons"]}
    pair_rows = []
    for pair in selection.eligible_pairs:
        if pair.pair_id in vetoed:
            continue
        context = contexts[pair.game_id]
        sides = {}
        for side in (pair.over, pair.under):
            sides[f"{side.side.value}_metadata"] = {
                "semantic_quote_id": side.semantic_quote_id, "source_refs": side.source_refs,
                "occurrence_count": side.occurrence_count, "odds_format": side.odds_format.value,
                "observation": observation_payload(side.raw_quote.observation),
                "snapshot_clocks": tuple(sorted({
                    _snapshot_identity(evidence[entry.occurrence_index]["raw_record"].get("snapshot_time"))
                    for entry in selection.ledger
                    if entry.pair_id == pair.pair_id and entry.raw_quote.side.strip().casefold() == side.side.value
                })),
                "raw_occurrences": tuple(_metadata_record(raw) for raw in sorted(
                    (evidence[entry.occurrence_index]["raw_record"] for entry in selection.ledger
                     if entry.pair_id == pair.pair_id and entry.raw_quote.side.strip().casefold() == side.side.value),
                    key=_metadata_order,
                )),
            }
        pair_rows.append({
            "player_id": player_values[pair.player_id], "game_id": pair.game_id, "market_key": pair.market,
            "line": pair.line, "bookmaker": pair.bookmaker, "over_odds": pair.over.odds_value,
            "under_odds": pair.under.odds_value, "adapter_schema_version": HISTORICAL_QUOTE_ADAPTER_SCHEMA_VERSION,
            "quote_schema_version": pair.schema_version, "pair_id": pair.pair_id,
            "period": pair.period, "variant": pair.variant, "source_namespace": pair.source_namespace,
            "snapshot_compatibility_key": pair.snapshot_compatibility_key, "policy_content_id": policy_id,
            "context_identity": _context_identity(context), **sides,
        })
    for entry in selection.ledger:
        row = evidence[entry.occurrence_index]
        veto = bool(row["adapter_reasons"]) or entry.pair_id in vetoed
        if entry.pair_id in vetoed and not row["adapter_reasons"]:
            row["adapter_reasons"] = (AdapterReason.INELIGIBLE_COUNTERPART.value,)
        row.update({
            "source_ref": entry.source_ref, "selector_disposition": entry.disposition.value,
            "disposition": (LedgerDisposition.REJECTED.value if veto and entry.disposition is not LedgerDisposition.DUPLICATE
                            else entry.disposition.value),
            "quote_reasons": tuple(reason.value for reason in entry.reasons),
            "observation_reasons": entry.observation_reasons, "observation": observation_payload(entry.raw_quote.observation),
            "semantic_quote_id": entry.semantic_quote_id, "pair_id": entry.pair_id,
            "canonical_source_ref": entry.canonical_source_ref,
        })
    counts = Counter(row["disposition"] for row in evidence)
    blocked = sum(row["blocked"] for row in evidence)
    summary = {
        "status": "blocked" if blocked else ("eligible_pairs" if pair_rows else "no_eligible_pairs"),
        "decision_grade": False, "historical_readiness": "unverified_upstream_source_authority",
        "input_occurrences": len(evidence), "eligible_pairs": len(pair_rows),
        "eligible_occurrences": counts[LedgerDisposition.ELIGIBLE_NOT_SELECTED.value],
        "rejected_occurrences": counts[LedgerDisposition.REJECTED.value],
        "duplicate_occurrences": counts[LedgerDisposition.DUPLICATE.value], "blocked_occurrences": blocked,
        "policy_content_id": policy_id, "selection_status": selection.selection_status.value,
    }
    # Object dtype retains exact Decimal and player-ID values (including empty schemas).
    return HistoricalQuoteResult(pd.DataFrame(pair_rows, columns=_PAIR_COLUMNS, dtype=object),
                                 pd.DataFrame(evidence, columns=_LEDGER_COLUMNS, dtype=object), summary)


__all__ = [
    "HISTORICAL_QUOTE_ADAPTER_SCHEMA_VERSION", "AdapterReason", "HistoricalQuoteResult",
    "adapt_historical_quotes", "resolve_game_context",
]
