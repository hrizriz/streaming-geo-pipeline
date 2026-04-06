"""Loop heartbeat + remediasi opsional."""

from __future__ import annotations

import json
import logging
import signal
import sys
import time
from pathlib import Path

from stack_health_agent.diagnose import build_snapshot, call_openrouter
from stack_health_agent.probes import default_probes, run_all
from stack_health_agent.remediate import RestartBudget, compose_restart

log = logging.getLogger(__name__)


def project_root_default() -> Path:
    return Path(__file__).resolve().parent.parent.parent


def run_daemon(
    *,
    project_root: Path,
    interval_sec: float,
    probe_timeout: float,
    remediate: bool,
    fail_threshold: int,
    restart_budget: RestartBudget,
    host: str,
) -> None:
    probes = default_probes(host)
    fail_streak: dict[str, int] = {p.id: 0 for p in probes}
    stop = False

    def _sig(_sig=None, _frame=None):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, _sig)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _sig)

    log.info(
        "stack_health_agent: heartbeat setiap %.1fs, remediate=%s, root=%s",
        interval_sec,
        remediate,
        project_root,
    )

    while not stop:
        results = run_all(probes, probe_timeout)
        for pid, (ok, detail) in results.items():
            if ok:
                fail_streak[pid] = 0
                log.debug("%s OK %s", pid, detail)
            else:
                fail_streak[pid] += 1
                log.warning("%s GAGAL (%s) streak=%d", pid, detail, fail_streak[pid])
                svc = next(p.compose_service for p in probes if p.id == pid)
                if remediate and fail_streak[pid] >= fail_threshold:
                    if restart_budget.allow(svc):
                        log.warning("remediate: docker compose restart %s", svc)
                        ok_r, msg = compose_restart(project_root, svc)
                        restart_budget.record(svc)
                        if ok_r:
                            fail_streak[pid] = 0
                            log.info("restart %s ok: %s", svc, msg)
                        else:
                            log.error("restart %s gagal: %s", svc, msg)
                    else:
                        log.error("restart %s dilewati (batas per jam)", svc)

        time.sleep(interval_sec)

    log.info("stack_health_agent: berhenti")


def run_once_json(*, project_root: Path, probe_timeout: float, host: str) -> dict:
    probes = default_probes(host)
    results = run_all(probes, probe_timeout)
    out = {pid: {"ok": ok, "detail": detail} for pid, (ok, detail) in results.items()}
    out["_snapshot"] = build_snapshot(project_root, results)
    return out


def run_diagnose(
    *,
    project_root: Path,
    probe_timeout: float,
    host: str,
    model: str,
) -> str:
    payload = run_once_json(project_root=project_root, probe_timeout=probe_timeout, host=host)
    results_plain = {k: (v["ok"], v["detail"]) for k, v in payload.items() if k != "_snapshot"}
    snap = build_snapshot(project_root, results_plain)
    key = __import__("os").environ.get("OPENROUTER_API_KEY")
    return call_openrouter(snap, api_key=key, model=model)
