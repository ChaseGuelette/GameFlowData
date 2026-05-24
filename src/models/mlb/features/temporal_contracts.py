"""Temporal contracts for MLB feature generation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum


class FeatureAsOfPolicy(str, Enum):
    """As-of policy labels for feature sources."""

    LEGACY_LATEST = "legacy_latest"
    AS_OF_DECISION_TIME = "as_of_decision_time"


@dataclass(frozen=True)
class FeatureTemporalContract:
    policy: FeatureAsOfPolicy
    as_of_time: datetime | None = None
    promotion_grade: bool = False

    def validate(self) -> None:
        if self.promotion_grade and self.as_of_time is None:
            raise ValueError("promotion-grade feature generation requires as_of_time")


def resolve_as_of_policy(as_of_time: datetime | None, *, promotion_grade: bool = False) -> FeatureTemporalContract:
    policy = FeatureAsOfPolicy.AS_OF_DECISION_TIME if as_of_time is not None else FeatureAsOfPolicy.LEGACY_LATEST
    contract = FeatureTemporalContract(policy=policy, as_of_time=as_of_time, promotion_grade=promotion_grade)
    contract.validate()
    return contract
