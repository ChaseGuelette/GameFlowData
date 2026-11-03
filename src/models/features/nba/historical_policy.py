"""Immutable caller-supplied policy for historical NBA feature evaluation."""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from enum import StrEnum
from typing import Any, TypeAlias
from zoneinfo import ZoneInfo

HISTORICAL_POLICY_SCHEMA_VERSION = "nba-historical-policy/v1"
_SLATE_ZONE = ZoneInfo("America/New_York")

JsonScalar: TypeAlias = None | bool | int | float | str


@dataclass(frozen=True, slots=True)
class _FrozenArray:
    items: tuple[Any, ...]


@dataclass(frozen=True, slots=True)
class _FrozenObject:
    items: tuple[tuple[str, Any], ...]


FrozenJson: TypeAlias = JsonScalar | _FrozenArray | _FrozenObject


class HistoricalPolicyContractError(ValueError):
    """Raised when policy material is malformed or internally contradictory."""


class HistoricalPolicyBlockedError(RuntimeError):
    """Raised when a valid policy still has unresolved execution authority."""

    def __init__(self, reasons: tuple[PolicyBlockingReason, ...]) -> None:
        self.reasons = reasons
        super().__init__("historical policy is blocked: " + ", ".join(reason.setting_id for reason in reasons))


class DecisionRuleKind(StrEnum):
    GAME_RELATIVE = "game_relative_t_minus"
    SLATE_CLOCK = "america_new_york_slate_clock"


class EarlyGameBehavior(StrEnum):
    BLOCK = "block"
    USE_GAME_RELATIVE = "use_game_relative_t_minus"


class ResolutionState(StrEnum):
    ABSENT = "absent"
    BLOCKED = "blocked"
    RESOLVED = "resolved"


class BlockingCategory(StrEnum):
    COHORT_SPLIT = "cohort_split"
    INJURY = "injury"
    SOURCE = "source"
    CARRYOVER = "carryover"


def _identifier(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise HistoricalPolicyContractError(f"{field} must be a nonempty exact identifier without repaired whitespace")
    return value


def _finite_number(value: Any, field: str) -> int | float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise HistoricalPolicyContractError(f"{field} must be numeric and not boolean")
    if isinstance(value, float) and not math.isfinite(value):
        raise HistoricalPolicyContractError(f"{field} must be finite")
    return value


def _freeze(value: Any) -> FrozenJson:
    if isinstance(value, _FrozenArray | _FrozenObject):
        return value
    if value is None or isinstance(value, bool | str):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise HistoricalPolicyContractError("policy values must not contain nonfinite numbers")
        return value
    if isinstance(value, list | tuple):
        return _FrozenArray(tuple(_freeze(item) for item in value))
    if isinstance(value, dict):
        items: list[tuple[str, FrozenJson]] = []
        for key, item in value.items():
            if not isinstance(key, str) or not key:
                raise HistoricalPolicyContractError("policy object keys must be nonempty strings")
            items.append((key, _freeze(item)))
        return _FrozenObject(tuple(sorted(items)))
    raise HistoricalPolicyContractError("policy values must be JSON-compatible immutable data")


def _contains_mutable_container(value: Any) -> bool:
    if isinstance(value, list | dict | set | bytearray):
        return True
    if isinstance(value, tuple):
        return any(_contains_mutable_container(item) for item in value)
    return False


def _freeze_required_value(value: Any) -> FrozenJson:
    if isinstance(value, tuple) and all(
        isinstance(item, tuple) and len(item) == 2 and isinstance(item[0], str) and item[0]
        for item in value
    ):
        keys = tuple(item[0] for item in value)
        if len(set(keys)) != len(keys):
            raise HistoricalPolicyContractError("resolved object keys must be unique")
        return _FrozenObject(tuple(sorted((key, _freeze_required_value(item)) for key, item in value)))
    if isinstance(value, tuple):
        return _FrozenArray(tuple(_freeze_required_value(item) for item in value))
    return _freeze(value)


def _thaw(value: FrozenJson) -> Any:
    if isinstance(value, _FrozenObject):
        return {key: _thaw(item) for key, item in value.items}
    if isinstance(value, _FrozenArray):
        return [_thaw(item) for item in value.items]
    return value


def _exact_object(value: FrozenJson | None, field: str, keys: set[str]) -> dict[str, Any]:
    if not isinstance(value, _FrozenObject):
        raise HistoricalPolicyContractError(f"resolved {field} authority must be an object")
    payload = _thaw(value)
    if set(payload) != keys:
        raise HistoricalPolicyContractError(f"resolved {field} authority has missing or unknown fields")
    return payload


def _positive(value: Any, field: str) -> int | float:
    number = _finite_number(value, field)
    if number <= 0:
        raise HistoricalPolicyContractError(f"{field} must be positive")
    return number


def _iso_date(value: Any, field: str) -> date:
    if not isinstance(value, str):
        raise HistoricalPolicyContractError(f"{field} must be an exact ISO date string")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise HistoricalPolicyContractError(f"{field} must be an exact ISO date string") from exc
    if parsed.isoformat() != value:
        raise HistoricalPolicyContractError(f"{field} must be an exact ISO date string")
    return parsed


def _string_list(value: Any, field: str) -> list[str]:
    if not isinstance(value, list) or not value:
        raise HistoricalPolicyContractError(f"{field} must be a nonempty array")
    items = [_identifier(item, field) for item in value]
    if len(set(items)) != len(items):
        raise HistoricalPolicyContractError(f"{field} values must be unique")
    return items


def _validate_cohort(value: FrozenJson | None) -> None:
    payload = _exact_object(value, "cohort_split", {
        "seasons", "season_types", "train_start", "train_end", "calibration_start",
        "calibration_end_exclusive", "oos_start", "oos_end", "independent_validation",
        "target_min_minutes", "rolling_min_minutes", "transition_policy_id", "cold_start_policy_id",
    })
    seasons = _string_list(payload["seasons"], "cohort_split.seasons")
    previous_start = -1
    for season in seasons:
        if re.fullmatch(r"2[0-9]{4}", season) is None:
            raise HistoricalPolicyContractError("cohort_split.seasons must use literal NBA season tokens")
        start = int(season[1:])
        if start <= previous_start:
            raise HistoricalPolicyContractError("cohort_split.seasons must be ordered")
        previous_start = start
    season_types = _string_list(payload["season_types"], "cohort_split.season_types")
    if any(item not in {"regular_season", "play_in", "playoffs"} for item in season_types):
        raise HistoricalPolicyContractError("cohort_split.season_types contains an unsupported value")
    dates = {key: _iso_date(payload[key], f"cohort_split.{key}") for key in (
        "train_start", "train_end", "calibration_start", "calibration_end_exclusive", "oos_start", "oos_end",
    )}
    if not (
        dates["train_start"] <= dates["train_end"] < dates["calibration_start"]
        < dates["calibration_end_exclusive"] <= dates["oos_start"] <= dates["oos_end"]
    ):
        raise HistoricalPolicyContractError("cohort_split windows are not strictly ordered")
    independent = payload["independent_validation"]
    if independent is not None:
        if not isinstance(independent, dict) or set(independent) != {"start", "end"}:
            raise HistoricalPolicyContractError("independent_validation must be null or an exact start/end object")
        start = _iso_date(independent["start"], "cohort_split.independent_validation.start")
        end = _iso_date(independent["end"], "cohort_split.independent_validation.end")
        if start > end or not (end < dates["train_start"] or start >= dates["calibration_end_exclusive"]):
            raise HistoricalPolicyContractError("independent_validation must be ordered and outside train/calibration")
    _positive(payload["target_min_minutes"], "cohort_split.target_min_minutes")
    _positive(payload["rolling_min_minutes"], "cohort_split.rolling_min_minutes")
    _identifier(payload["transition_policy_id"], "cohort_split.transition_policy_id")
    _identifier(payload["cold_start_policy_id"], "cohort_split.cold_start_policy_id")


def _validate_injury(value: FrozenJson | None) -> None:
    payload = _exact_object(
        value, "injury", {"interpreter_id", "missing_history", "omitted_player", "completeness_evidence_ref"}
    )
    _identifier(payload["interpreter_id"], "injury.interpreter_id")
    _identifier(payload["completeness_evidence_ref"], "injury.completeness_evidence_ref")
    for key in ("missing_history", "omitted_player"):
        if payload[key] not in {"block", "unknown"}:
            raise HistoricalPolicyContractError(f"injury.{key} must be block or unknown")


def _validate_source(value: FrozenJson | None) -> None:
    payload = _exact_object(value, "source", {
        "interpreters", "provenance_evidence_ref", "missing_commence", "require_market_update",
        "require_bookmaker_update", "max_quote_age_minutes",
    })
    _string_list(payload["interpreters"], "source.interpreters")
    _identifier(payload["provenance_evidence_ref"], "source.provenance_evidence_ref")
    if payload["missing_commence"] != "block":
        raise HistoricalPolicyContractError("source.missing_commence must be block")
    for key in ("require_market_update", "require_bookmaker_update"):
        if type(payload[key]) is not bool:
            raise HistoricalPolicyContractError(f"source.{key} must be an explicit boolean")
    _positive(payload["max_quote_age_minutes"], "source.max_quote_age_minutes")


def _validate_carryover(value: FrozenJson | None, family_id: str) -> None:
    if not isinstance(value, _FrozenObject):
        raise HistoricalPolicyContractError(f"resolved carryover authority for {family_id} must be an object")
    payload = _thaw(value)
    mode = payload.get("mode")
    common = {"mode", "evidence_ref"}
    expected = {
        "current_season_only": common,
        "prior_season_carryover": common,
        "carryover_with_decay": common | {"decay", "indicator_feature"},
        "neutral_default": common | {"value"},
        "block": common,
    }.get(mode)
    if expected is None or set(payload) != expected:
        raise HistoricalPolicyContractError(f"resolved carryover authority for {family_id} has invalid mode or fields")
    _identifier(payload["evidence_ref"], f"carryover.{family_id}.evidence_ref")
    if mode == "carryover_with_decay":
        decay = _finite_number(payload["decay"], f"carryover.{family_id}.decay")
        if not 0 <= decay <= 1:
            raise HistoricalPolicyContractError(f"carryover.{family_id}.decay must be between zero and one")
        _identifier(payload["indicator_feature"], f"carryover.{family_id}.indicator_feature")
    elif mode == "neutral_default":
        _finite_number(payload["value"], f"carryover.{family_id}.value")


@dataclass(frozen=True, slots=True)
class RequiredPolicyValue:
    state: ResolutionState
    value: FrozenJson | None = None
    reason: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.state, ResolutionState):
            raise HistoricalPolicyContractError("required value state must be a ResolutionState")
        if self.state is ResolutionState.RESOLVED:
            if self.value is None or self.reason is not None:
                raise HistoricalPolicyContractError("resolved values require value and forbid blocking reason")
            if _contains_mutable_container(self.value):
                raise HistoricalPolicyContractError("caller-supplied resolved authority must be deeply immutable")
            object.__setattr__(self, "value", _freeze_required_value(self.value))
        elif self.value is not None:
            raise HistoricalPolicyContractError("absent or blocked values cannot carry an executable value")
        elif self.state is ResolutionState.BLOCKED:
            object.__setattr__(self, "reason", _identifier(self.reason, "blocking reason"))
        elif self.reason is not None:
            raise HistoricalPolicyContractError("absent values must remain distinct from explicit blocked values")


@dataclass(frozen=True, slots=True)
class PolicyBlockingReason:
    category: BlockingCategory
    setting_id: str
    state: ResolutionState
    detail: str


@dataclass(frozen=True, slots=True)
class HistoricalDecisionRule:
    kind: DecisionRuleKind
    t_minus_minutes: int | float | None = None
    slate_clock: time | None = None
    early_game_behavior: EarlyGameBehavior | None = None
    early_game_t_minus_minutes: int | float | None = None
    early_game_trigger_margin_minutes: int | float | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.kind, DecisionRuleKind):
            raise HistoricalPolicyContractError("decision rule kind is invalid")
        if self.kind is DecisionRuleKind.GAME_RELATIVE:
            minutes = _finite_number(self.t_minus_minutes, "t_minus_minutes")
            if minutes <= 0:
                raise HistoricalPolicyContractError("t_minus_minutes must be positive")
            if any(value is not None for value in (
                self.slate_clock,
                self.early_game_behavior,
                self.early_game_t_minus_minutes,
                self.early_game_trigger_margin_minutes,
            )):
                raise HistoricalPolicyContractError("game-relative rule cannot carry slate-clock settings")
            return
        if self.t_minus_minutes is not None or not isinstance(self.slate_clock, time) or self.slate_clock.tzinfo is not None:
            raise HistoricalPolicyContractError("slate-clock rule requires one naive wall clock and no primary t-minus")
        if not isinstance(self.early_game_behavior, EarlyGameBehavior):
            raise HistoricalPolicyContractError("slate-clock early-game behavior must be explicit")
        margin = _finite_number(self.early_game_trigger_margin_minutes, "early_game_trigger_margin_minutes")
        if margin < 0:
            raise HistoricalPolicyContractError("early_game_trigger_margin_minutes must be nonnegative")
        if self.early_game_behavior is EarlyGameBehavior.USE_GAME_RELATIVE:
            minutes = _finite_number(self.early_game_t_minus_minutes, "early_game_t_minus_minutes")
            if minutes <= 0:
                raise HistoricalPolicyContractError("early_game_t_minus_minutes must be positive")
        elif self.early_game_t_minus_minutes is not None:
            raise HistoricalPolicyContractError("blocking early-game behavior cannot carry fallback minutes")

    def decision_at(self, game_date: date, commence_at: datetime) -> datetime:
        if not isinstance(game_date, date) or isinstance(game_date, datetime):
            raise HistoricalPolicyContractError("game_date must be a date")
        if not isinstance(commence_at, datetime) or commence_at.tzinfo is None or commence_at.utcoffset() is None:
            raise HistoricalPolicyContractError("commence_at must be timezone-aware")
        if self.kind is DecisionRuleKind.GAME_RELATIVE:
            assert self.t_minus_minutes is not None
            decision = commence_at.astimezone(UTC) - timedelta(minutes=self.t_minus_minutes)
        else:
            assert (
                self.slate_clock is not None
                and self.early_game_behavior is not None
                and self.early_game_trigger_margin_minutes is not None
            )
            naive = datetime.combine(game_date, self.slate_clock)
            candidates = []
            for fold in (0, 1):
                candidate = naive.replace(tzinfo=_SLATE_ZONE, fold=fold)
                if candidate.astimezone(UTC).astimezone(_SLATE_ZONE).replace(tzinfo=None) == naive:
                    candidates.append(candidate)
            unique = {candidate.astimezone(UTC) for candidate in candidates}
            if len(unique) != 1:
                raise HistoricalPolicyContractError("slate clock is ambiguous or nonexistent in America/New_York")
            decision = candidates[0].astimezone(UTC)
            commence_utc = commence_at.astimezone(UTC)
            early_game_boundary = decision + timedelta(minutes=self.early_game_trigger_margin_minutes)
            if commence_utc <= decision or commence_utc < early_game_boundary:
                if self.early_game_behavior is EarlyGameBehavior.BLOCK:
                    raise HistoricalPolicyBlockedError((PolicyBlockingReason(
                        BlockingCategory.SOURCE, "decision_rule.early_game", ResolutionState.BLOCKED,
                        "game commence falls inside the explicit slate early-game trigger margin",
                    ),))
                assert self.early_game_t_minus_minutes is not None
                decision = commence_utc - timedelta(minutes=self.early_game_t_minus_minutes)
        if decision.astimezone(UTC) >= commence_at.astimezone(UTC):
            raise HistoricalPolicyContractError("decision_at must be strictly before commence_at")
        return decision


@dataclass(frozen=True, slots=True)
class HistoricalPolicy:
    schema_version: str
    policy_name: str
    decision_rule: HistoricalDecisionRule
    cohort_split: RequiredPolicyValue
    injury: RequiredPolicyValue
    source: RequiredPolicyValue
    carryover: tuple[tuple[str, RequiredPolicyValue], ...]

    def __post_init__(self) -> None:
        if self.schema_version != HISTORICAL_POLICY_SCHEMA_VERSION:
            raise HistoricalPolicyContractError("unsupported historical policy schema")
        _identifier(self.policy_name, "policy_name")
        if not isinstance(self.decision_rule, HistoricalDecisionRule):
            raise HistoricalPolicyContractError("decision_rule must be a HistoricalDecisionRule")
        if not all(isinstance(item, RequiredPolicyValue) for item in (self.cohort_split, self.injury, self.source)):
            raise HistoricalPolicyContractError("required policy authorities must be RequiredPolicyValue records")
        if not isinstance(self.carryover, tuple):
            raise HistoricalPolicyContractError("carryover must be an immutable tuple")
        ids: list[str] = []
        for item in self.carryover:
            if not isinstance(item, tuple) or len(item) != 2 or not isinstance(item[1], RequiredPolicyValue):
                raise HistoricalPolicyContractError("carryover entries must be (family_id, RequiredPolicyValue)")
            ids.append(_identifier(item[0], "carryover family_id"))
        if len(set(ids)) != len(ids):
            raise HistoricalPolicyContractError("carryover family IDs must be unique")
        if self.cohort_split.state is ResolutionState.RESOLVED:
            _validate_cohort(self.cohort_split.value)
        if self.injury.state is ResolutionState.RESOLVED:
            _validate_injury(self.injury.value)
        if self.source.state is ResolutionState.RESOLVED:
            _validate_source(self.source.value)
        for family_id, value in self.carryover:
            if value.state is ResolutionState.RESOLVED:
                _validate_carryover(value.value, family_id)

    @property
    def content_id(self) -> str:
        return hashlib.sha256(canonical_policy_json(self).encode("utf-8")).hexdigest()

    @property
    def blocking_reasons(self) -> tuple[PolicyBlockingReason, ...]:
        entries = (
            (BlockingCategory.COHORT_SPLIT, "cohort_split", self.cohort_split),
            (BlockingCategory.INJURY, "injury", self.injury),
            (BlockingCategory.SOURCE, "source", self.source),
            *((BlockingCategory.CARRYOVER, family_id, value) for family_id, value in self.carryover),
        )
        return tuple(
            PolicyBlockingReason(category, setting_id, value.state, value.reason or "required value is absent")
            for category, setting_id, value in entries
            if value.state is not ResolutionState.RESOLVED
        )

    def require_execution_ready(self, *, required_families: tuple[str, ...]) -> None:
        if not isinstance(required_families, tuple) or not required_families:
            raise HistoricalPolicyContractError("required_families must be a nonempty immutable tuple")
        requested = tuple(_identifier(family_id, "required family_id") for family_id in required_families)
        if len(set(requested)) != len(requested):
            raise HistoricalPolicyContractError("required family IDs must be unique")
        configured = tuple(family_id for family_id, _ in self.carryover)
        if set(configured) != set(requested):
            raise HistoricalPolicyContractError("carryover family coverage must exactly match required_families")
        if self.blocking_reasons:
            raise HistoricalPolicyBlockedError(self.blocking_reasons)


@dataclass(frozen=True, slots=True, init=False)
class HistoricalGameContext:
    game_id: str
    game_date: date
    commence_at: datetime
    decision_at: datetime
    commence_provenance_ref: str
    policy_content_id: str

    def __init__(
        self,
        *,
        policy: HistoricalPolicy,
        required_families: tuple[str, ...],
        game_id: str,
        game_date: date,
        commence_at: datetime,
        commence_provenance_ref: str,
    ) -> None:
        if not isinstance(policy, HistoricalPolicy):
            raise HistoricalPolicyContractError("policy must be a HistoricalPolicy")
        policy.require_execution_ready(required_families=required_families)
        object.__setattr__(self, "game_id", _identifier(game_id, "game_id"))
        object.__setattr__(self, "game_date", game_date)
        object.__setattr__(self, "commence_at", commence_at)
        object.__setattr__(
            self,
            "commence_provenance_ref",
            _identifier(commence_provenance_ref, "commence_provenance_ref"),
        )
        object.__setattr__(self, "decision_at", policy.decision_rule.decision_at(game_date, commence_at))
        object.__setattr__(self, "policy_content_id", policy.content_id)
        if not isinstance(self.game_date, date) or isinstance(self.game_date, datetime):
            raise HistoricalPolicyContractError("game_date must be a date")
        for field, value in (("commence_at", self.commence_at), ("decision_at", self.decision_at)):
            if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
                raise HistoricalPolicyContractError(f"{field} must be timezone-aware")
        if self.decision_at.astimezone(UTC) >= self.commence_at.astimezone(UTC):
            raise HistoricalPolicyContractError("decision_at must be strictly before commence_at")



def build_game_context(
    policy: HistoricalPolicy,
    *,
    game_id: str,
    game_date: date,
    commence_at: datetime,
    commence_provenance_ref: str,
    required_families: tuple[str, ...],
) -> HistoricalGameContext:
    return HistoricalGameContext(
        policy=policy,
        required_families=required_families,
        game_id=game_id,
        game_date=game_date,
        commence_at=commence_at,
        commence_provenance_ref=commence_provenance_ref,
    )


def _value_payload(value: RequiredPolicyValue) -> dict[str, Any]:
    payload: dict[str, Any] = {"state": value.state.value}
    if value.state is ResolutionState.RESOLVED:
        payload["value"] = _thaw(value.value)
    elif value.state is ResolutionState.BLOCKED:
        payload["reason"] = value.reason
    return payload


def policy_payload(policy: HistoricalPolicy) -> dict[str, Any]:
    rule = policy.decision_rule
    rule_payload: dict[str, Any] = {"kind": rule.kind.value}
    if rule.kind is DecisionRuleKind.GAME_RELATIVE:
        rule_payload["t_minus_minutes"] = rule.t_minus_minutes
    else:
        assert rule.slate_clock is not None and rule.early_game_behavior is not None
        rule_payload.update({
            "slate_clock": rule.slate_clock.isoformat(),
            "early_game_behavior": rule.early_game_behavior.value,
            "early_game_trigger_margin_minutes": rule.early_game_trigger_margin_minutes,
        })
        if rule.early_game_t_minus_minutes is not None:
            rule_payload["early_game_t_minus_minutes"] = rule.early_game_t_minus_minutes
    return {
        "schema_version": policy.schema_version,
        "policy_name": policy.policy_name,
        "decision_rule": rule_payload,
        "authorities": {
            "cohort_split": _value_payload(policy.cohort_split),
            "injury": _value_payload(policy.injury),
            "source": _value_payload(policy.source),
            "carryover": [
                {"family_id": family_id, **_value_payload(value)}
                for family_id, value in sorted(policy.carryover, key=lambda item: item[0])
            ],
        },
    }


def canonical_policy_json(policy: HistoricalPolicy) -> str:
    return json.dumps(policy_payload(policy), sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def _pairs_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise HistoricalPolicyContractError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _required_value(payload: Any) -> RequiredPolicyValue:
    if not isinstance(payload, dict) or "state" not in payload:
        raise HistoricalPolicyContractError("required policy value must contain state")
    allowed = {"state", "value", "reason"}
    if set(payload) - allowed:
        raise HistoricalPolicyContractError("unknown required policy value keys")
    try:
        state = ResolutionState(payload["state"])
    except (TypeError, ValueError) as exc:
        raise HistoricalPolicyContractError("invalid required policy value state") from exc
    return RequiredPolicyValue(state, _freeze(payload["value"]) if "value" in payload else None, payload.get("reason"))


def policy_from_json(raw: str) -> HistoricalPolicy:
    if not isinstance(raw, str):
        raise HistoricalPolicyContractError("policy JSON must be text")
    try:
        payload = json.loads(raw, object_pairs_hook=_pairs_object, parse_constant=lambda value: (_ for _ in ()).throw(
            HistoricalPolicyContractError(f"nonfinite JSON number: {value}")
        ))
    except (json.JSONDecodeError, TypeError) as exc:
        raise HistoricalPolicyContractError("malformed policy JSON") from exc
    if not isinstance(payload, dict) or set(payload) != {"schema_version", "policy_name", "decision_rule", "authorities"}:
        raise HistoricalPolicyContractError("policy JSON has missing or unknown top-level keys")
    rule_data = payload["decision_rule"]
    authorities = payload["authorities"]
    if not isinstance(rule_data, dict) or not isinstance(authorities, dict):
        raise HistoricalPolicyContractError("decision_rule and authorities must be objects")
    try:
        kind = DecisionRuleKind(rule_data["kind"])
        if kind is DecisionRuleKind.GAME_RELATIVE:
            if set(rule_data) != {"kind", "t_minus_minutes"}:
                raise HistoricalPolicyContractError("invalid game-relative decision rule keys")
            rule = HistoricalDecisionRule(kind, t_minus_minutes=rule_data["t_minus_minutes"])
        else:
            allowed = {
                "kind", "slate_clock", "early_game_behavior", "early_game_t_minus_minutes",
                "early_game_trigger_margin_minutes",
            }
            required = {"kind", "slate_clock", "early_game_behavior", "early_game_trigger_margin_minutes"}
            if set(rule_data) - allowed or not required <= set(rule_data):
                raise HistoricalPolicyContractError("invalid slate-clock decision rule keys")
            wall_clock = time.fromisoformat(rule_data["slate_clock"])
            if wall_clock.isoformat() != rule_data["slate_clock"]:
                raise HistoricalPolicyContractError("slate_clock must use exact canonical ISO clock text")
            rule = HistoricalDecisionRule(
                kind, slate_clock=wall_clock, early_game_behavior=EarlyGameBehavior(rule_data["early_game_behavior"]),
                early_game_t_minus_minutes=rule_data.get("early_game_t_minus_minutes"),
                early_game_trigger_margin_minutes=rule_data["early_game_trigger_margin_minutes"],
            )
    except (KeyError, TypeError, ValueError) as exc:
        if isinstance(exc, HistoricalPolicyContractError):
            raise
        raise HistoricalPolicyContractError("malformed decision rule") from exc
    if set(authorities) != {"cohort_split", "injury", "source", "carryover"} or not isinstance(authorities["carryover"], list):
        raise HistoricalPolicyContractError("authorities object is malformed")
    carryover: list[tuple[str, RequiredPolicyValue]] = []
    for item in authorities["carryover"]:
        if not isinstance(item, dict) or "family_id" not in item:
            raise HistoricalPolicyContractError("carryover item is malformed")
        family_id = item["family_id"]
        carryover.append((family_id, _required_value({key: value for key, value in item.items() if key != "family_id"})))
    return HistoricalPolicy(
        payload["schema_version"], payload["policy_name"], rule,
        _required_value(authorities["cohort_split"]), _required_value(authorities["injury"]),
        _required_value(authorities["source"]), tuple(carryover),
    )


__all__ = [
    "HISTORICAL_POLICY_SCHEMA_VERSION", "BlockingCategory", "DecisionRuleKind", "EarlyGameBehavior",
    "HistoricalDecisionRule", "HistoricalGameContext", "HistoricalPolicy", "HistoricalPolicyBlockedError",
    "HistoricalPolicyContractError", "PolicyBlockingReason", "RequiredPolicyValue", "ResolutionState",
    "build_game_context", "canonical_policy_json", "policy_from_json", "policy_payload",
]
