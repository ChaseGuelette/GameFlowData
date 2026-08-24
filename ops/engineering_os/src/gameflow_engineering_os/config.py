from __future__ import annotations

import os
import re
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

DEFAULT_CONFIG_PATH = "/etc/gameflow-engineering-os/engineering_os.yaml"


class DailyBriefConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    enabled: bool = False
    schedule: str = "08:00"
    retain_days: int = Field(default=90, ge=1)

    @field_validator("schedule")
    @classmethod
    def valid_schedule(cls, value: str) -> str:
        if not re.fullmatch(r"([01]\d|2[0-3]):[0-5]\d", value):
            raise ValueError("daily brief schedule must be HH:MM")
        return value


class EventsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    enabled: bool = True
    record_recovery: bool = True
    repeat_after_hours: int = Field(default=24, ge=1)


class WebConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    bind_host: str = "127.0.0.1"
    bind_port: int = Field(default=8765, ge=1, le=65535)
    public_base_url: str | None = None
    stale_after_minutes: int = Field(default=20, ge=1)
    manual_refresh_enabled: bool = False
    csrf_token: str | None = None


class PathsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kanban_db: Path
    kanban_backups: Path
    gameflow_repo: Path
    state_dir: Path
    log_dir: Path


class CommandsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    hermes: Path = Path("/home/chase/.local/bin/hermes")
    systemctl: Path = Path("/usr/bin/systemctl")
    tailscale: Path = Path("/usr/bin/tailscale")
    git: Path = Path("/usr/bin/git")
    df: Path = Path("/usr/bin/df")
    du: Path = Path("/usr/bin/du")


class ThresholdsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    backup_max_age_hours: int = Field(default=36, ge=1)
    stuck_task_minutes: int = Field(default=30, ge=1)
    disk_warning_percent: int | None = Field(default=None, ge=1, le=100)
    disk_critical_percent: int | None = Field(default=None, ge=1, le=100)
    artifact_growth_warning_bytes: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def disk_thresholds_ordered(self) -> ThresholdsConfig:
        if (
            self.disk_warning_percent is not None
            and self.disk_critical_percent is not None
            and self.disk_warning_percent >= self.disk_critical_percent
        ):
            raise ValueError("disk warning threshold must be below critical threshold")
        return self


class CollectorConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    timeout_seconds: float = Field(default=5.0, gt=0, le=120)


class SystemdConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    gateway_service: str = "hermes-gateway.service"
    gbrain_service: str = "gbrain-gameflow.service"
    expected_timers: list[str] = Field(default_factory=list)
    timer_services: dict[str, str] = Field(default_factory=dict)


class DiskConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    path: Path = Path("/")


class ArtifactsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    directories: list[Path] = Field(default_factory=list)


class EngineeringOSConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    timezone: str = "America/New_York"
    daily_brief: DailyBriefConfig
    events: EventsConfig
    web: WebConfig
    paths: PathsConfig
    commands: CommandsConfig
    collector: CollectorConfig = Field(default_factory=CollectorConfig)
    thresholds: ThresholdsConfig
    systemd: SystemdConfig = Field(default_factory=SystemdConfig)
    disk: DiskConfig = Field(default_factory=DiskConfig)
    artifacts: ArtifactsConfig = Field(default_factory=ArtifactsConfig)

    @field_validator("timezone")
    @classmethod
    def valid_timezone(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except ZoneInfoNotFoundError as exc:
            raise ValueError(f"unknown timezone: {value}") from exc
        return value

    def ensure_runtime_dirs(self) -> None:
        self.paths.state_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.paths.log_dir.mkdir(mode=0o700, parents=True, exist_ok=True)


def default_config_path() -> Path:
    return Path(os.environ.get("GFOS_CONFIG", DEFAULT_CONFIG_PATH))


def load_config(path: str | Path | None = None) -> EngineeringOSConfig:
    config_path = Path(path) if path else default_config_path()
    if not config_path.exists():
        raise FileNotFoundError(f"config not found: {config_path}")
    with config_path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    return EngineeringOSConfig.model_validate(data)
