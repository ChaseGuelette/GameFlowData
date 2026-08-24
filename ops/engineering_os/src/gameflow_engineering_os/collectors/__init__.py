from __future__ import annotations

from collections.abc import Callable

from gameflow_engineering_os.config import EngineeringOSConfig
from gameflow_engineering_os.models import HealthCheckResult

Collector = Callable[[EngineeringOSConfig], HealthCheckResult]
