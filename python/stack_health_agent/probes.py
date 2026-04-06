"""Probe konkret ke stack lokal (TCP / HTTP singkat)."""

from __future__ import annotations

import socket
from dataclasses import dataclass
from typing import Callable

import requests


@dataclass(frozen=True)
class Probe:
    id: str
    description: str
    compose_service: str
    check: Callable[[float], tuple[bool, str]]


def _tcp(host: str, port: int, timeout: float) -> tuple[bool, str]:
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect((host, port))
        sock.close()
        return True, "ok"
    except OSError as e:
        return False, str(e)


def _http_get(url: str, timeout: float, ok_status: tuple[int, ...] = (200,)) -> tuple[bool, str]:
    try:
        r = requests.get(url, timeout=timeout)
        if r.status_code in ok_status:
            return True, f"http {r.status_code}"
        return False, f"http {r.status_code}"
    except requests.RequestException as e:
        return False, str(e)


def default_probes(host: str = "127.0.0.1") -> list[Probe]:
    """
    Host default localhost — layanan di-map ke port host dari docker-compose.
    """
    h = host
    return [
        Probe(
            "zookeeper",
            "Zookeeper 2181",
            "zookeeper",
            lambda t: _tcp(h, 2181, t),
        ),
        Probe(
            "kafka",
            "Kafka (host listener 9092)",
            "kafka",
            lambda t: _tcp(h, 9092, t),
        ),
        Probe(
            "minio",
            "MinIO API 9000",
            "minio",
            lambda t: _http_get(f"http://{h}:9000/minio/health/live", t),
        ),
        Probe(
            "flink",
            "Flink JobManager 8081",
            "jobmanager",
            lambda t: _http_get(f"http://{h}:8081/", t, (200,)),
        ),
        Probe(
            "postgres",
            "PostgreSQL 5432",
            "postgres",
            lambda t: _tcp(h, 5432, t),
        ),
        Probe(
            "trino",
            "Trino 8082 /v1/info",
            "trino",
            lambda t: _http_get(f"http://{h}:8082/v1/info", t),
        ),
        Probe(
            "metabase",
            "Metabase 3030 /api/health",
            "metabase",
            lambda t: _http_get(f"http://{h}:3030/api/health", t),
        ),
    ]


def run_all(probes: list[Probe], timeout: float) -> dict[str, tuple[bool, str]]:
    return {p.id: p.check(timeout) for p in probes}
