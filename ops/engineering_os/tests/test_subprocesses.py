from __future__ import annotations

import subprocess

import pytest
from gameflow_engineering_os.subprocesses import redact, run_command


def test_redacts_secret_material():
    text = (
        'Authorization: Basic shortsecret '
        'token=supersecretvalue api_key=anothersecretvalue '
        '{"access_token": "jsonsecret"} '
        'https://example.test/path?token=querysecret&ok=1'
    )
    cleaned = redact(text)
    assert "supersecretvalue" not in cleaned
    assert "shortsecret" not in cleaned
    assert "jsonsecret" not in cleaned
    assert "querysecret" not in cleaned
    assert "ok=1" in cleaned
    assert "[REDACTED]" in cleaned


def test_command_timeout(monkeypatch: pytest.MonkeyPatch):
    def fake_run(*args, **kwargs):
        raise subprocess.TimeoutExpired(cmd=["x"], timeout=0.01, output="token=supersecretvalue")

    monkeypatch.setattr(subprocess, "run", fake_run)
    result = run_command(["x"], 0.01)
    assert result.timed_out is True
    assert "supersecretvalue" not in result.stdout
