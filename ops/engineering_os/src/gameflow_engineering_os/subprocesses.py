from __future__ import annotations

import re
import subprocess
from dataclasses import dataclass

SECRET_PATTERNS = [
    re.compile(r"(?i)(authorization:\s*)\S+(?:\s+\S+)?"),
    re.compile(r'''(?i)((?:["']?(?:access_)?token["']?|["']?api[_-]?key["']?|["']?password["']?)\s*[:=]\s*["']?)[^"'&\s}]+'''),
    re.compile(r"//([^:/\s]+):([^@\s]+)@"),
    re.compile(r"\b[A-Za-z0-9_-]{32,}\.[A-Za-z0-9._-]{20,}\b"),
    re.compile(r"\b[A-Za-z0-9_-]{48,}\b"),
]


@dataclass(frozen=True)
class CommandResult:
    args: list[str]
    returncode: int | None
    stdout: str
    stderr: str
    timed_out: bool = False
    error: str | None = None


def redact(value: str, limit: int = 4000) -> str:
    redacted = value
    for pattern in SECRET_PATTERNS:
        if pattern.pattern.startswith("//"):
            redacted = pattern.sub("//***:***@", redacted)
        else:
            redacted = pattern.sub(lambda match: f"{match.group(1)}[REDACTED]" if match.groups() else "[REDACTED]", redacted)
    if len(redacted) > limit:
        return redacted[:limit] + "...[truncated]"
    return redacted


def run_command(args: list[str], timeout_seconds: float) -> CommandResult:
    try:
        completed = subprocess.run(
            args,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
        return CommandResult(
            args=args,
            returncode=completed.returncode,
            stdout=redact(completed.stdout),
            stderr=redact(completed.stderr),
        )
    except subprocess.TimeoutExpired as exc:
        return CommandResult(
            args=args,
            returncode=None,
            stdout=redact(exc.stdout or ""),
            stderr=redact(exc.stderr or ""),
            timed_out=True,
            error=f"command timed out after {timeout_seconds}s",
        )
    except OSError as exc:
        return CommandResult(args=args, returncode=None, stdout="", stderr="", error=str(exc))
