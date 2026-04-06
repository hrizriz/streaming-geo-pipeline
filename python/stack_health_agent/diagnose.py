"""Snapshot + panggilan OpenRouter (opsional) untuk troubleshooting."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import requests

OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"


def compose_ps_json(project_root: Path, timeout: int = 30) -> str:
    try:
        r = subprocess.run(
            ["docker", "compose", "ps", "--format", "json"],
            cwd=str(project_root),
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if r.returncode != 0:
            return f"(compose ps gagal: {r.stderr or r.stdout})"
        # Bisa banyak baris JSON Lines
        lines = [ln for ln in r.stdout.strip().splitlines() if ln.strip()]
        if not lines:
            return "(kosong)"
        return "\n".join(lines[:40])  # batasi panjang
    except Exception as e:
        return f"(error: {e})"


def build_snapshot(
    project_root: Path,
    probe_results: dict[str, tuple[bool, str]],
) -> str:
    lines = ["## hasil probe terakhir"]
    for pid, (ok, detail) in sorted(probe_results.items()):
        lines.append(f"- {pid}: {'OK' if ok else 'GAGAL'} - {detail}")
    lines.append("\n## docker compose ps --format json (potongan)")
    lines.append(compose_ps_json(project_root))
    return "\n".join(lines)


def call_openrouter(
    snapshot: str,
    *,
    api_key: str | None,
    model: str,
    timeout: int = 120,
) -> str:
    if not api_key:
        return "OPENROUTER_API_KEY tidak set; lewati LLM."

    system = (
        "Kamu asisten troubleshooting DevOps untuk stack Docker lokal: "
        "Zookeeper, Kafka, MinIO, Flink (jobmanager/taskmanager), Postgres, Trino, Metabase. "
        "Jawab singkat Bahasa Indonesia: (1) ringkasan kemungkinan akar masalah, "
        "(2) langkah cek manual berurutan, (3) peringatan jika autoservice restart tidak cukup. "
        "Jangan mengarang port; gunakan hanya fakta dari snapshot."
    )
    user = f"Snapshot status:\n{snapshot[:12000]}"

    body = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": 0.2,
        "max_tokens": 1200,
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": os.environ.get("OPENROUTER_HTTP_REFERER", "https://github.com/hrizriz/streaming-geo-pipeline"),
        "X-Title": os.environ.get("OPENROUTER_X_TITLE", "stack-health-agent"),
    }
    r = requests.post(OPENROUTER_URL, headers=headers, json=body, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    try:
        return data["choices"][0]["message"]["content"].strip()
    except (KeyError, IndexError, TypeError):
        return json.dumps(data)[:2000]
