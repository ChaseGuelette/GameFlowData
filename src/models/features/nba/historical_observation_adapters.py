"""Pure source adapters for historical NBA observation eligibility."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum

from src.models.features.nba.historical_policy import HistoricalGameContext
from src.models.features.nba.observation_policy import (
    AvailabilityInterpretation,
    ClockEvidence,
    InterpretationStatus,
    ObservationContext,
    ObservationContractError,
    ObservationDecision,
    evaluate_observation,
)


class HistoricalSourceClass(StrEnum):
    AUTHENTICATED_HISTORICAL_REQUEST = "authenticated_historical_request_upper_bound"
    LOCAL_OBSERVED_AT = "local_observed_at"
    LIVE_PROPS_INVOCATION_START = "live_props_invocation_start_unsupported"
    UNKNOWN = "unknown"


class InjuryHistoryState(StrEnum):
    LISTED = "listed"
    UNKNOWN_COMPLETENESS = "unknown_completeness"
    OMITTED = "omitted"


@dataclass(frozen=True, slots=True)
class SourceProvenance:
    source_namespace: str
    source_record_id: str
    attributes: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        for field, value in (
            ("source_namespace", self.source_namespace),
            ("source_record_id", self.source_record_id),
        ):
            if not isinstance(value, str) or not value.strip() or value != value.strip():
                raise ObservationContractError(f"{field} must be a nonempty opaque identifier")
        if not isinstance(self.attributes, tuple):
            raise ObservationContractError("provenance attributes must be an immutable tuple")
        keys = []
        for item in self.attributes:
            if (
                not isinstance(item, tuple)
                or len(item) != 2
                or not all(isinstance(value, str) for value in item)
                or not item[0]
            ):
                raise ObservationContractError("provenance attributes must be string key/value pairs")
            keys.append(item[0])
        if len(keys) != len(set(keys)) or {"source_namespace", "source_record_id"} & set(keys):
            raise ObservationContractError("provenance keys must be unique")

    @property
    def primitive_provenance(self) -> tuple[tuple[str, str], ...]:
        return (
            ("source_namespace", self.source_namespace),
            ("source_record_id", self.source_record_id),
            *self.attributes,
        )


def _context(context: HistoricalGameContext) -> ObservationContext:
    if not isinstance(context, HistoricalGameContext):
        raise ObservationContractError("context must be a HistoricalGameContext")
    return ObservationContext(context.decision_at, context.commence_at)


def _clock(label: str, value: datetime | None) -> ClockEvidence:
    if value is not None and not isinstance(value, datetime):
        raise ObservationContractError(f"{label} must be a datetime or None")
    return ClockEvidence(label, value)


def _source_class(value: HistoricalSourceClass | str) -> HistoricalSourceClass:
    if isinstance(value, HistoricalSourceClass):
        return value
    if not isinstance(value, str):
        raise ObservationContractError("source_class must be a string or HistoricalSourceClass")
    try:
        return HistoricalSourceClass(value)
    except ValueError:
        return HistoricalSourceClass.UNKNOWN


def evaluate_market_observation(
    *,
    context: HistoricalGameContext,
    source_class: HistoricalSourceClass | str,
    provenance: SourceProvenance,
    historical_requested_at: datetime | None = None,
    market_updated_at: datetime | None = None,
    bookmaker_updated_at: datetime | None = None,
    local_observed_at: datetime | None = None,
    require_book_update: bool,
) -> ObservationDecision:
    """Map explicit market clocks to the shared observation primitive.

    Authenticated historical requests always require both the request upper bound and
    market-update clock. Bookmaker-update evidence is independently optional or
    required by the caller. Every supplied clock remains cutoff evidence.
    """
    if not isinstance(provenance, SourceProvenance):
        raise ObservationContractError("provenance must be SourceProvenance")
    if type(require_book_update) is not bool:
        raise ObservationContractError("require_book_update must be an explicit boolean")
    source = _source_class(source_class)
    supplied = {
        "historical_requested_at": historical_requested_at,
        "market_updated_at": market_updated_at,
        "bookmaker_updated_at": bookmaker_updated_at,
        "local_observed_at": local_observed_at,
    }
    for label, value in supplied.items():
        if value is not None and not isinstance(value, datetime):
            raise ObservationContractError(f"{label} must be a datetime or None")

    if source is HistoricalSourceClass.AUTHENTICATED_HISTORICAL_REQUEST:
        if local_observed_at is not None:
            raise ObservationContractError("historical-request source cannot carry local observed-at authority")
        raw = (
            _clock("historical_requested_at_upper_bound", historical_requested_at),
            _clock("market_updated_at", market_updated_at),
        )
        if require_book_update or bookmaker_updated_at is not None:
            raw += (_clock("bookmaker_updated_at", bookmaker_updated_at),)

        def interpret(evidence: tuple[ClockEvidence, ...]) -> AvailabilityInterpretation:
            availability = None
            if all(item.observed_at is not None for item in evidence):
                availability = max(
                    (item.observed_at for item in evidence if item.observed_at is not None),
                    key=lambda observed_at: observed_at.astimezone(UTC),
                )
            return AvailabilityInterpretation(
                InterpretationStatus.AVAILABLE,
                availability,
                evidence,
                provenance.primitive_provenance,
            )

        interpreter_id = "nba-historical-request-upper-bound/v1"
    elif source is HistoricalSourceClass.LOCAL_OBSERVED_AT:
        if historical_requested_at is not None:
            raise ObservationContractError("local observed-at source cannot carry historical request authority")
        raw = (_clock("local_observed_at", local_observed_at),)
        if market_updated_at is not None:
            raw += (_clock("market_updated_at", market_updated_at),)
        if require_book_update or bookmaker_updated_at is not None:
            raw += (_clock("bookmaker_updated_at", bookmaker_updated_at),)

        def interpret(evidence: tuple[ClockEvidence, ...]) -> AvailabilityInterpretation:
            availability = evidence[0].observed_at
            return AvailabilityInterpretation(
                InterpretationStatus.AVAILABLE,
                availability,
                evidence,
                provenance.primitive_provenance,
            )

        interpreter_id = "nba-local-observed-at/v1"
    else:
        raw = tuple(_clock(label, value) for label, value in supplied.items())

        def interpret(evidence: tuple[ClockEvidence, ...]) -> AvailabilityInterpretation:
            return AvailabilityInterpretation(
                InterpretationStatus.UNKNOWN_SOURCE,
                provenance=provenance.primitive_provenance + (("unsupported_source_class", source.value),),
            )

        interpreter_id = "nba-unsupported-market-source/v1"

    return evaluate_observation(
        context=_context(context), raw_evidence=raw, interpreter=interpret, interpreter_id=interpreter_id
    )


def evaluate_injury_observation(
    *,
    context: HistoricalGameContext,
    provenance: SourceProvenance,
    history_state: InjuryHistoryState,
    local_observed_at: datetime | None,
    report_cycle_text: str | None,
) -> ObservationDecision:
    """Evaluate local injury availability without inferring report-cycle clocks or health."""
    if not isinstance(provenance, SourceProvenance):
        raise ObservationContractError("provenance must be SourceProvenance")
    if not isinstance(history_state, InjuryHistoryState):
        raise ObservationContractError("history_state must be an InjuryHistoryState")
    if local_observed_at is not None and not isinstance(local_observed_at, datetime):
        raise ObservationContractError("local_observed_at must be a datetime or None")
    if report_cycle_text is not None and not isinstance(report_cycle_text, str):
        raise ObservationContractError("report_cycle_text must be text or None")
    attributes = provenance.primitive_provenance
    if report_cycle_text is not None:
        attributes += (("report_cycle_text", report_cycle_text),)
    raw = (_clock("injury_local_observed_at", local_observed_at),)

    def interpret(evidence: tuple[ClockEvidence, ...]) -> AvailabilityInterpretation:
        if history_state is InjuryHistoryState.UNKNOWN_COMPLETENESS:
            return AvailabilityInterpretation(InterpretationStatus.AMBIGUOUS_HISTORY, provenance=attributes)
        if history_state is InjuryHistoryState.OMITTED:
            return AvailabilityInterpretation(InterpretationStatus.MISSING_HISTORY, provenance=attributes)
        return AvailabilityInterpretation(
            InterpretationStatus.AVAILABLE,
            evidence[0].observed_at,
            evidence,
            attributes,
        )

    return evaluate_observation(
        context=_context(context),
        raw_evidence=raw,
        interpreter=interpret,
        interpreter_id="nba-injury-local-observed-at/v1",
    )


__all__ = [
    "HistoricalSourceClass",
    "InjuryHistoryState",
    "SourceProvenance",
    "evaluate_injury_observation",
    "evaluate_market_observation",
]
