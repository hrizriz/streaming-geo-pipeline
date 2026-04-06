"""Remediasi terbatas: docker compose restart (whitelist)."""

from __future__ import annotations

import subprocess
import time
from collections import defaultdict
from pathlib import Path


class RestartBudget:
    """Batasi restart per servis per jendela waktu (anti-loop)."""

    def __init__(self, max_per_hour: int = 3, window_sec: int = 3600) -> None:
        self.max_per_hour = max_per_hour
        self.window_sec = window_sec
        self._times: dict[str, list[float]] = defaultdict(list)

    def allow(self, service: str) -> bool:
        now = time.monotonic()
        cutoff = now - self.window_sec
        self._times[service] = [t for t in self._times[service] if t > cutoff]
        return len(self._times[service]) < self.max_per_hour

    def record(self, service: str) -> None:
        self._times[service].append(time.monotonic())


def compose_restart(
    project_root: Path,
    service: str,
    timeout_sec: int = 180,
) -> tuple[bool, str]:
    try:
        r = subprocess.run(
            ["docker", "compose", "restart", service],
            cwd=str(project_root),
            capture_output=True,
            text=True,
            timeout=timeout_sec,
        )
        if r.returncode == 0:
            return True, (r.stdout or "").strip() or "ok"
        msg = (r.stderr or r.stdout or "").strip() or f"exit {r.returncode}"
        return False, msg
    except FileNotFoundError:
        return False, "docker tidak ditemukan di PATH"
    except subprocess.TimeoutExpired:
        return False, "timeout restart"
    except OSError as e:
        return False, str(e)
