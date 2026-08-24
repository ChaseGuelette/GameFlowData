from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from gameflow_engineering_os.config import load_config


def test_missing_config_fails(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        load_config(tmp_path / "missing.yaml")


def test_config_accepts_null_thresholds_without_creating_dirs(config_file: Path):
    cfg = load_config(config_file)
    assert cfg.thresholds.disk_warning_percent is None
    assert not cfg.paths.state_dir.exists()
    cfg.ensure_runtime_dirs()
    assert cfg.paths.state_dir.exists()


def test_config_rejects_bad_timezone(config_file: Path):
    data = yaml.safe_load(config_file.read_text(encoding="utf-8"))
    data["timezone"] = "Mars/Base"
    config_file.write_text(yaml.safe_dump(data), encoding="utf-8")
    with pytest.raises(ValueError):
        load_config(config_file)


def test_config_rejects_bad_schedule(config_file: Path):
    data = yaml.safe_load(config_file.read_text(encoding="utf-8"))
    data["daily_brief"]["schedule"] = "25:99"
    config_file.write_text(yaml.safe_dump(data), encoding="utf-8")
    with pytest.raises(ValueError):
        load_config(config_file)


def test_config_rejects_unknown_keys(config_file: Path):
    data = yaml.safe_load(config_file.read_text(encoding="utf-8"))
    data["unknown"] = True
    config_file.write_text(yaml.safe_dump(data), encoding="utf-8")
    with pytest.raises(ValueError):
        load_config(config_file)

