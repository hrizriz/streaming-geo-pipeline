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


def main() -> None:
    root_default = Path(__file__).resolve().parent.parent.parent
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

    sub.add_parser("once", help="Satu siklus probe, keluarkan JSON ke stdout")

    p_diag = sub.add_parser("diagnose", help="Probe + ringkasan LLM via OpenRouter")
    p_diag.add_argument(
        "--model",
        default=os.environ.get("OPENROUTER_MODEL", "openai/gpt-4o-mini"),
    )

    args = parser.parse_args()
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
            model=args.model,
        )
        print(text)
    else:
        parser.error("perintah tidak dikenal")


if __name__ == "__main__":
    main()
