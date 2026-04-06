"""CLI: python -m stack_health_agent"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )


def _load_credentials_env_defaults(project_root: Path) -> None:
    """Isi os.environ dari credentials.env hanya jika key belum ada (setdefault)."""
    path = project_root / "credentials.env"
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        key = k.strip()
        val = v.strip().strip('"').strip("'")
        if key:
            os.environ.setdefault(key, val)


def main() -> None:
    root_default = Path(__file__).resolve().parent.parent.parent
    # Agar defaults --host / env lain terbaca dari credentials.env sebelum parser dibangun
    _load_credentials_env_defaults(root_default)

    parser = argparse.ArgumentParser(description="Stack health agent (probe + remediate + OpenRouter)")
    parser.add_argument(
        "--project-root",
        type=Path,
        default=Path(os.environ.get("STACK_AGENT_ROOT", root_default)),
        help="Root repo (berisi docker-compose.yml)",
    )
    parser.add_argument("--host", default=os.environ.get("STACK_AGENT_HOST", "127.0.0.1"))
    parser.add_argument("--probe-timeout", type=float, default=5.0)
    parser.add_argument("-v", "--verbose", action="store_true")

    sub = parser.add_subparsers(dest="cmd", required=True)

    p_run = sub.add_parser("run", help="Daemon: heartbeat + remediate opsional")
    p_run.add_argument("--interval", type=float, default=3.0, help="Detik antar siklus probe")
    p_run.add_argument("--no-remediate", action="store_true", help="Matikan docker compose restart")
    p_run.add_argument(
        "--fail-threshold",
        type=int,
        default=2,
        help="Gagal beruntun sebanyak ini baru restart servis terkait",
    )
    p_run.add_argument("--max-restarts-per-hour", type=int, default=3)
    p_run.add_argument(
        "--alert-cooldown",
        type=float,
        default=float(os.environ.get("STACK_ALERT_COOLDOWN_SEC", "300")),
        help="Detik minimal antar alert berulang untuk probe yang sama (masih gagal)",
    )
    p_run.add_argument(
        "--no-alert-recovery",
        action="store_true",
        help="Jangan kirim notifikasi saat probe pulih",
    )
    p_run.add_argument("--discord-webhook", default=None)
    p_run.add_argument("--slack-webhook", default=None)
    p_run.add_argument("--telegram-token", default=None)
    p_run.add_argument("--telegram-chat-id", default=None)

    sub.add_parser("once", help="Satu siklus probe, keluarkan JSON ke stdout")

    p_diag = sub.add_parser("diagnose", help="Probe + ringkasan LLM via OpenRouter")
    p_diag.add_argument("--model", default=None)

    args = parser.parse_args()
    _load_credentials_env_defaults(args.project_root)
    _setup_logging(args.verbose)

    if args.cmd == "run":
        from stack_health_agent.agent import run_daemon
        from stack_health_agent.remediate import RestartBudget

        run_daemon(
            project_root=args.project_root,
            interval_sec=args.interval,
            probe_timeout=args.probe_timeout,
            remediate=not args.no_remediate,
            fail_threshold=args.fail_threshold,
            restart_budget=RestartBudget(max_per_hour=args.max_restarts_per_hour),
            host=args.host,
            alert_cooldown_sec=args.alert_cooldown,
            alert_on_recovery=not args.no_alert_recovery,
            discord_url=args.discord_webhook or os.environ.get("DISCORD_WEBHOOK_URL"),
            slack_url=args.slack_webhook or os.environ.get("SLACK_WEBHOOK_URL"),
            telegram_token=args.telegram_token or os.environ.get("TELEGRAM_BOT_TOKEN"),
            telegram_chat_id=args.telegram_chat_id or os.environ.get("TELEGRAM_CHAT_ID"),
        )
    elif args.cmd == "once":
        from stack_health_agent.agent import run_once_json

        out = run_once_json(
            project_root=args.project_root,
            probe_timeout=args.probe_timeout,
            host=args.host,
        )
        json.dump(out, sys.stdout, indent=2, ensure_ascii=False)
        sys.stdout.write("\n")
    elif args.cmd == "diagnose":
        from stack_health_agent.agent import run_diagnose

        text = run_diagnose(
            project_root=args.project_root,
            probe_timeout=args.probe_timeout,
            host=args.host,
            model=args.model or os.environ.get("OPENROUTER_MODEL", "openai/gpt-4o-mini"),
        )
        print(text)
    else:
        parser.error("perintah tidak dikenal")


if __name__ == "__main__":
    main()
