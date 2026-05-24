"""Tests for MLB feature temporal contracts."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.models.mlb.features.temporal_contracts import FeatureAsOfPolicy, resolve_as_of_policy


def test_resolve_as_of_policy_labels_legacy_latest_when_missing_as_of():
    contract = resolve_as_of_policy(None)
    assert contract.policy is FeatureAsOfPolicy.LEGACY_LATEST
    assert contract.as_of_time is None


def test_resolve_as_of_policy_labels_decision_time_when_as_of_present():
    as_of = datetime(2026, 5, 23, 17, 0, tzinfo=timezone.utc)
    contract = resolve_as_of_policy(as_of)
    assert contract.policy is FeatureAsOfPolicy.AS_OF_DECISION_TIME
    assert contract.as_of_time == as_of


def test_promotion_grade_requires_as_of_time():
    with pytest.raises(ValueError, match="requires as_of_time"):
        resolve_as_of_policy(None, promotion_grade=True)
