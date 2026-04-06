"""Loop heartbeat + remediasi opsional + alert optional."""

from __future__ import annotations

import logging
import signal
import time
from pathlib import Path

from stack_health_agent.diagnose import build_snapshot, call_openrouter
from stack_health_agent.notify import any_channel_configured, send_probe_alerts
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
    alert_cooldown_sec: float,
    alert_on_recovery: bool,
    discord_url: str | None,
    slack_url: str | None,
    telegram_token: str | None,
    telegram_chat_id: str | None,
) -> None:
    probes = default_probes(host)
    fail_streak: dict[str, int] = {p.id: 0 for p in probes}
    prev_ok: dict[str, bool] = {}
    last_alert_mono: dict[str, float] = {}
    stop = False

    def _sig(_sig=None, _frame=None):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, _sig)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _sig)

    alert_enabled = any_channel_configured() or bool(
        discord_url or slack_url or (telegram_token and telegram_chat_id)
    )
    log.info(
        "stack_health_agent: heartbeat=%.1fs remediate=%s alert=%s root=%s",
        interval_sec,
        remediate,
        alert_enabled,
        project_root,
    )

    while not stop:
        now = time.monotonic()
        results = run_all(probes, probe_timeout)

        for pid, (ok, detail) in results.items():
            was_ok = prev_ok.get(pid, True)

            if ok:
                if not was_ok and alert_on_recovery and alert_enabled:
                    send_probe_alerts(
                        title=f"[pulih] {pid}",
                        body="Probe OK kembali.",
                        discord_url=discord_url,
                        slack_url=slack_url,
                        telegram_token=telegram_token,
                        telegram_chat_id=telegram_chat_id,
                    )
                fail_streak[pid] = 0
                log.debug("%s OK %s", pid, detail)
            else:
                fail_streak[pid] += 1
                log.warning("%s GAGAL (%s) streak=%d", pid, detail, fail_streak[pid])

                if alert_enabled:
                    should_alert = False
                    if was_ok:
                        should_alert = True
                    elif now - last_alert_mono.get(pid, 0.0) >= alert_cooldown_sec:
                        should_alert = True

                    if should_alert:
                        ch = send_probe_alerts(
                            title=f"[GAGAL] {pid}",
                            body=(
                                f"Detail: {detail}\n"
                                f"fail_streak={fail_streak[pid]} (threshold remediate={fail_threshold})"
                            ),
                            discord_url=discord_url,
                            slack_url=slack_url,
                            telegram_token=telegram_token,
                            telegram_chat_id=telegram_chat_id,
                        )
                        if ch:
                            last_alert_mono[pid] = now
                            log.info("alert terkirim %s via %s", pid, ch)

                svc = next(p.compose_service for p in probes if p.id == pid)
                if remediate and fail_streak[pid] >= fail_threshold:
                    if restart_budget.allow(svc):
                        log.warning("remediate: docker compose restart %s", svc)
                        ok_r, msg = compose_restart(project_root, svc)
                        restart_budget.record(svc)
                        if ok_r:
                            fail_streak[pid] = 0
                            log.info("restart %s ok: %s", svc, msg)
                            if alert_enabled:
                                send_probe_alerts(
                                    title=f"[remediate] {svc}",
                                    body=f"docker compose restart {svc}\n{msg[:500]}",
                                    discord_url=discord_url,
                                    slack_url=slack_url,
                                    telegram_token=telegram_token,
                                    telegram_chat_id=telegram_chat_id,
                                )
                        else:
                            log.error("restart %s gagal: %s", svc, msg)
                    else:
                        log.error("restart %s dilewati (batas per jam)", svc)

            prev_ok[pid] = ok

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
