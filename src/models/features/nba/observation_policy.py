"""Pure decision-time observation eligibility primitives for NBA inputs."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from types import MappingProxyType
from typing import Any

OBSERVATION_SCHEMA_VERSION = "nba-observation-policy/v1"


class ObservationContractError(ValueError):
    """Raised for caller/programming contract violations."""


class InterpretationStatus(StrEnum):
    AVAILABLE = "available"
    UNKNOWN_SOURCE = "unknown_source"
    MISSING_HISTORY = "missing_history"
    AMBIGUOUS_HISTORY = "ambiguous_history"


class ObservationRejectionReason(StrEnum):
    MISSING_DECISION_AT = "missing_decision_at"
    NAIVE_DECISION_AT = "naive_decision_at"
    MISSING_COMMENCE_AT = "missing_commence_at"
    NAIVE_COMMENCE_AT = "naive_commence_at"
    DECISION_NOT_BEFORE_COMMENCE = "decision_not_before_commence"
    INVALID_SOURCE_TIMESTAMP = "invalid_source_timestamp"
    UNKNOWN_SOURCE = "unknown_source"
    MISSING_HISTORY = "missing_history"
    AMBIGUOUS_HISTORY = "ambiguous_history"
    MISSING_AVAILABILITY = "missing_availability"
    NAIVE_AVAILABILITY = "naive_availability"
    AVAILABLE_AFTER_DECISION = "available_after_decision"
    AVAILABLE_AT_OR_AFTER_COMMENCE = "available_at_or_after_commence"
    MISSING_CUTOFF_EVIDENCE = "missing_cutoff_evidence"
    NAIVE_CUTOFF_EVIDENCE = "naive_cutoff_evidence"
    CUTOFF_EVIDENCE_AFTER_DECISION = "cutoff_evidence_after_decision"
    CUTOFF_EVIDENCE_AT_OR_AFTER_COMMENCE = "cutoff_evidence_at_or_after_commence"


@dataclass(frozen=True, slots=True)
class ClockEvidence:
    """A labeled source clock retained exactly as supplied."""

    label: str
    observed_at: datetime | None

    def __post_init__(self) -> None:
        if not isinstance(self.label, str) or not self.label.strip():
            raise ObservationContractError("ClockEvidence.label must be nonempty")
        if self.observed_at is not None and not isinstance(self.observed_at, datetime):
            raise ObservationContractError("ClockEvidence.observed_at must be a datetime or None")


@dataclass(frozen=True, slots=True)
class ObservationContext:
    decision_at: datetime | None
    commence_at: datetime | None

    def __post_init__(self) -> None:
        if self.decision_at is not None and not isinstance(self.decision_at, datetime):
            raise ObservationContractError("decision_at must be a datetime or None")
        if self.commence_at is not None and not isinstance(self.commence_at, datetime):
            raise ObservationContractError("commence_at must be a datetime or None")


@dataclass(frozen=True, slots=True)
class AvailabilityInterpretation:
    """The explicit result returned by a caller-supplied interpreter."""

    status: InterpretationStatus
    availability_at: datetime | None = None
    cutoff_evidence: tuple[ClockEvidence, ...] = ()
    provenance: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.status, InterpretationStatus):
            raise ObservationContractError("status must be an InterpretationStatus")
        if self.availability_at is not None and not isinstance(self.availability_at, datetime):
            raise ObservationContractError("availability_at must be a datetime or None")
        if not isinstance(self.cutoff_evidence, tuple) or not all(
            isinstance(item, ClockEvidence) for item in self.cutoff_evidence
        ):
            raise ObservationContractError("cutoff_evidence must be a tuple of ClockEvidence")
        if not isinstance(self.provenance, tuple):
            raise ObservationContractError("provenance must be an immutable tuple")
        for item in self.provenance:
            if (
                not isinstance(item, tuple)
                or len(item) != 2
                or not all(isinstance(value, str) for value in item)
                or not item[0]
            ):
                raise ObservationContractError("provenance entries must be nonempty string key/value pairs")
        if len({key for key, _ in self.provenance}) != len(self.provenance):
            raise ObservationContractError("provenance keys must be unique")


@dataclass(frozen=True, slots=True)
class ObservationDecision:
    schema_version: str
    interpreter_id: str
    accepted: bool
    reasons: tuple[ObservationRejectionReason, ...]
    decision_at: datetime | None
    commence_at: datetime | None
    raw_evidence: tuple[ClockEvidence, ...]
    interpretation: AvailabilityInterpretation | None

    @property
    def provenance(self) -> Mapping[str, str]:
        if self.interpretation is None:
            return MappingProxyType({})
        return MappingProxyType(dict(self.interpretation.provenance))


ObservationInterpreter = Callable[[tuple[ClockEvidence, ...]], AvailabilityInterpretation]


def _is_aware(value: datetime | None) -> bool:
    return value is not None and value.tzinfo is not None and value.utcoffset() is not None


def _utc(value: datetime) -> datetime:
    return value.astimezone(UTC)


def _context_reasons(context: ObservationContext) -> list[ObservationRejectionReason]:
    reasons: list[ObservationRejectionReason] = []
    if context.decision_at is None:
        reasons.append(ObservationRejectionReason.MISSING_DECISION_AT)
    elif not _is_aware(context.decision_at):
        reasons.append(ObservationRejectionReason.NAIVE_DECISION_AT)
    if context.commence_at is None:
        reasons.append(ObservationRejectionReason.MISSING_COMMENCE_AT)
    elif not _is_aware(context.commence_at):
        reasons.append(ObservationRejectionReason.NAIVE_COMMENCE_AT)
    if _is_aware(context.decision_at) and _is_aware(context.commence_at):
        assert context.decision_at is not None and context.commence_at is not None
        if _utc(context.decision_at) >= _utc(context.commence_at):
            reasons.append(ObservationRejectionReason.DECISION_NOT_BEFORE_COMMENCE)
    return reasons


def _unknown_status_reason(status: InterpretationStatus) -> ObservationRejectionReason:
    return {
        InterpretationStatus.UNKNOWN_SOURCE: ObservationRejectionReason.UNKNOWN_SOURCE,
        InterpretationStatus.MISSING_HISTORY: ObservationRejectionReason.MISSING_HISTORY,
        InterpretationStatus.AMBIGUOUS_HISTORY: ObservationRejectionReason.AMBIGUOUS_HISTORY,
    }[status]


def evaluate_observation(
    *,
    context: ObservationContext,
    raw_evidence: tuple[ClockEvidence, ...],
    interpreter: ObservationInterpreter,
    interpreter_id: str,
) -> ObservationDecision:
    """Interpret and validate one observation without inferring decision context.

    Unexpected interpreter exceptions deliberately propagate. Invalid decision context
    and interpreted data are returned as structured data rejections; malformed
    interpreter values and invalid caller arguments raise ``ObservationContractError``.
    """
    if not isinstance(context, ObservationContext):
        raise ObservationContractError("context must be an ObservationContext")
    if not callable(interpreter):
        raise ObservationContractError("interpreter must be callable")
    if not isinstance(interpreter_id, str) or not interpreter_id.strip():
        raise ObservationContractError("interpreter_id must be nonempty")
    if not isinstance(raw_evidence, tuple) or not all(isinstance(item, ClockEvidence) for item in raw_evidence):
        raise ObservationContractError("raw_evidence must be a tuple of ClockEvidence")

    reasons = _context_reasons(context)
    if any(item.observed_at is not None and not _is_aware(item.observed_at) for item in raw_evidence):
        reasons.append(ObservationRejectionReason.INVALID_SOURCE_TIMESTAMP)
    if reasons:
        return ObservationDecision(
            OBSERVATION_SCHEMA_VERSION,
            interpreter_id,
            False,
            tuple(dict.fromkeys(reasons)),
            context.decision_at,
            context.commence_at,
            raw_evidence,
            None,
        )

    interpretation = interpreter(raw_evidence)
    if not isinstance(interpretation, AvailabilityInterpretation):
        raise ObservationContractError("interpreter must return AvailabilityInterpretation")

    if interpretation.status is not InterpretationStatus.AVAILABLE:
        reasons.append(_unknown_status_reason(interpretation.status))
    else:
        availability = interpretation.availability_at
        if availability is None:
            reasons.append(ObservationRejectionReason.MISSING_AVAILABILITY)
        elif not _is_aware(availability):
            reasons.append(ObservationRejectionReason.NAIVE_AVAILABILITY)
        else:
            assert context.decision_at is not None and context.commence_at is not None
            if _utc(availability) > _utc(context.decision_at):
                reasons.append(ObservationRejectionReason.AVAILABLE_AFTER_DECISION)
            if _utc(availability) >= _utc(context.commence_at):
                reasons.append(ObservationRejectionReason.AVAILABLE_AT_OR_AFTER_COMMENCE)

        if not interpretation.cutoff_evidence:
            reasons.append(ObservationRejectionReason.MISSING_CUTOFF_EVIDENCE)
        for evidence in interpretation.cutoff_evidence:
            timestamp = evidence.observed_at
            if timestamp is None:
                reasons.append(ObservationRejectionReason.MISSING_CUTOFF_EVIDENCE)
            elif not _is_aware(timestamp):
                reasons.append(ObservationRejectionReason.NAIVE_CUTOFF_EVIDENCE)
            else:
                assert context.decision_at is not None and context.commence_at is not None
                if _utc(timestamp) > _utc(context.decision_at):
                    reasons.append(ObservationRejectionReason.CUTOFF_EVIDENCE_AFTER_DECISION)
                if _utc(timestamp) >= _utc(context.commence_at):
                    reasons.append(ObservationRejectionReason.CUTOFF_EVIDENCE_AT_OR_AFTER_COMMENCE)

    return ObservationDecision(
        OBSERVATION_SCHEMA_VERSION,
        interpreter_id,
        not reasons,
        tuple(dict.fromkeys(reasons)),
        context.decision_at,
        context.commence_at,
        raw_evidence,
        interpretation,
    )


def validate_observation_decision(decision: ObservationDecision) -> None:
    """Recheck supplied evidence instead of trusting a caller-set accepted flag."""
    if not isinstance(decision, ObservationDecision):
        raise ObservationContractError("observation must be an ObservationDecision")
    if decision.schema_version != OBSERVATION_SCHEMA_VERSION:
        raise ObservationContractError("unsupported observation schema")
    if type(decision.accepted) is not bool or not isinstance(decision.reasons, tuple):
        raise ObservationContractError("invalid observation result fields")
    if not all(isinstance(reason, ObservationRejectionReason) for reason in decision.reasons):
        raise ObservationContractError("invalid observation rejection reason")
    checked = evaluate_observation(
        context=ObservationContext(decision.decision_at, decision.commence_at),
        raw_evidence=decision.raw_evidence,
        interpreter=lambda evidence: decision.interpretation,  # type: ignore[return-value]
        interpreter_id=decision.interpreter_id,
    )
    if checked.accepted != decision.accepted or checked.reasons != decision.reasons:
        raise ObservationContractError("observation eligibility contradicts its evidence")
    if checked.interpretation is None and decision.interpretation is not None:
        raise ObservationContractError("invalid-context observation carries an interpretation")


def clock_evidence_payload(evidence: ClockEvidence) -> dict[str, Any]:
    """Return stable, explicit clock material suitable for canonical JSON."""
    value = evidence.observed_at
    if value is None:
        return {"label": evidence.label, "instant": None}
    if not _is_aware(value):
        return {"label": evidence.label, "instant": {"naive_iso": value.isoformat(), "fold": value.fold}}
    return {
        "label": evidence.label,
        "instant": {
            "original_iso": value.isoformat(),
            "utc_iso": _utc(value).isoformat(),
            "fold": value.fold,
            "timezone_name": value.tzname(),
        },
    }


def observation_payload(decision: ObservationDecision) -> dict[str, Any]:
    """Return all immutable observation evidence in a stable primitive shape."""

    def instant(value: datetime | None) -> dict[str, Any] | None:
        if value is None:
            return None
        if not _is_aware(value):
            return {"naive_iso": value.isoformat(), "fold": value.fold}
        return {
            "original_iso": value.isoformat(),
            "utc_iso": _utc(value).isoformat(),
            "fold": value.fold,
            "timezone_name": value.tzname(),
        }

    interpretation = decision.interpretation
    return {
        "schema_version": decision.schema_version,
        "interpreter_id": decision.interpreter_id,
        "accepted": decision.accepted,
        "reasons": sorted(reason.value for reason in decision.reasons),
        "decision_at": instant(decision.decision_at),
        "commence_at": instant(decision.commence_at),
        "raw_evidence": [clock_evidence_payload(item) for item in decision.raw_evidence],
        "interpretation": None
        if interpretation is None
        else {
            "status": interpretation.status.value,
            "availability_at": instant(interpretation.availability_at),
            "cutoff_evidence": [clock_evidence_payload(item) for item in interpretation.cutoff_evidence],
            "provenance": sorted([list(item) for item in interpretation.provenance]),
        },
    }


__all__ = [
    "OBSERVATION_SCHEMA_VERSION",
    "AvailabilityInterpretation",
    "ClockEvidence",
    "InterpretationStatus",
    "ObservationContext",
    "ObservationContractError",
    "ObservationDecision",
    "ObservationInterpreter",
    "ObservationRejectionReason",
    "clock_evidence_payload",
    "evaluate_observation",
    "observation_payload",
    "validate_observation_decision",
]
