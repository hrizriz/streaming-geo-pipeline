"""
Baca Parquet curated di MinIO lewat DuckDB (S3-compatible).

Daftar file memakai boto3 (glob DuckDB + MinIO sering gagal untuk path hive dt=...).
Flink menulis part-* tanpa sufiks .parquet; read_parquet mendeteksi dari isi file.

Prasyarat: pip install -r requirements.txt, MinIO jalan, credentials.env di root project.

Contoh:
  python python/query_curated_parquet.py
  python python/query_curated_parquet.py --dt 2026-04-04 --limit 50
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path
from urllib.parse import urlparse

try:
    import boto3
    from botocore.config import Config as BotoConfig
except ImportError:
    print("Install dulu: pip install boto3", file=sys.stderr)
    raise SystemExit(1) from None

try:
    import duckdb
except ImportError as e:
    print("Install dulu: pip install duckdb", file=sys.stderr)
    raise SystemExit(1) from e


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _load_credentials_env() -> dict[str, str]:
    env_path = _project_root() / "credentials.env"
    out: dict[str, str] = {}
    if not env_path.is_file():
        return out
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            k, _, v = line.partition("=")
            out[k.strip()] = v.strip().strip('"').strip("'")
    return out


def _endpoint_host_port(api_url: str) -> str:
    if "://" not in api_url:
        return api_url.strip()
    p = urlparse(api_url)
    host = p.hostname or "localhost"
    port = p.port
    if port:
        return f"{host}:{port}"
    return host


def _list_part_parquet_urls(
    *,
    endpoint_url: str,
    access: str,
    secret: str,
    bucket: str,
    prefix: str,
) -> list[str]:
    """Kunci objek data Flink: part-* atau *.parquet di bawah prefix."""
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access,
        aws_secret_access_key=secret,
        config=BotoConfig(signature_version="s3v4"),
        region_name="us-east-1",
    )
    urls: list[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue
            base = key.rsplit("/", 1)[-1]
            if base.startswith("part-") or base.endswith(".parquet"):
                urls.append(f"s3://{bucket}/{key}")
    return sorted(urls)


def _build_sql_list(urls: list[str]) -> str:
    parts = ["'" + u.replace("'", "''") + "'" for u in urls]
    return "[" + ",".join(parts) + "]"


def _print_result(res: object) -> None:
    """Cetak hasil query tanpa pandas/numpy (fetchdf membutuhkan numpy)."""
    desc = getattr(res, "description", None)
    rows = res.fetchall()  # type: ignore[union-attr]
    if not desc:
        return
    cols = [d[0] for d in desc]
    widths = [len(c) for c in cols]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(str(cell)))
    sep = " | "
    header = sep.join(c.ljust(widths[i]) for i, c in enumerate(cols))
    print(header)
    print(sep.join("-" * widths[i] for i in range(len(cols))))
    for row in rows:
        print(sep.join(str(row[i]).ljust(widths[i]) for i in range(len(row))))


def main() -> None:
    parser = argparse.ArgumentParser(description="Query Parquet curated di MinIO (DuckDB + boto3)")
    parser.add_argument("--bucket", default="warehouse", help="Nama bucket MinIO")
    parser.add_argument(
        "--prefix",
        default="geo-events/curated/",
        help="Prefix di dalam bucket (default: semua curated)",
    )
    parser.add_argument(
        "--dt",
        default=None,
        metavar="YYYY-MM-DD",
        help="Pembatas satu partisi hive: set prefix ke .../curated/dt=YYYY-MM-DD/",
    )
    parser.add_argument("--limit", type=int, default=20, help="Baris maksimal")
    parser.add_argument(
        "--pattern",
        default=None,
        help="Opsional: s3://bucket/prefix/ untuk override bucket+prefix (tanpa glob)",
    )
    args = parser.parse_args()

    cred = _load_credentials_env()
    api_url = cred.get("MINIO_S3_API_URL", os.environ.get("MINIO_S3_API_URL", "http://localhost:9000"))
    if not api_url.startswith("http"):
        api_url = "http://" + api_url
    access = cred.get("MINIO_ROOT_USER", os.environ.get("MINIO_ROOT_USER", "minioadmin"))
    secret = cred.get("MINIO_ROOT_PASSWORD", os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin"))

    bucket = args.bucket
    prefix = args.prefix
    if args.dt:
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", args.dt):
            print("--dt harus YYYY-MM-DD", file=sys.stderr)
            raise SystemExit(2)
        prefix = f"geo-events/curated/dt={args.dt}/"

    if args.pattern:
        pat = args.pattern.strip().rstrip("*").rstrip("/") + "/"
        m = re.match(r"s3://([^/]+)/(.+)", pat)
        if not m:
            print(
                '--pattern harus seperti s3://warehouse/geo-events/curated/dt=2026-04-04/ (tanpa *)',
                file=sys.stderr,
            )
            raise SystemExit(2)
        bucket = m.group(1)
        prefix = m.group(2)
        if not prefix.endswith("/"):
            prefix += "/"

    endpoint = _endpoint_host_port(api_url)

    urls = _list_part_parquet_urls(
        endpoint_url=api_url,
        access=access,
        secret=secret,
        bucket=bucket,
        prefix=prefix,
    )
    if not urls:
        print(
            f"Tidak ada objek part-* / *.parquet di s3://{bucket}/{prefix}",
            file=sys.stderr,
        )
        raise SystemExit(3)

    con = duckdb.connect(database=":memory:")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET s3_endpoint='{endpoint}';")
    con.execute(f"SET s3_access_key_id='{access}';")
    con.execute(f"SET s3_secret_access_key='{secret}';")
    con.execute("SET s3_use_ssl=false;")
    con.execute("SET s3_url_style='path';")
    con.execute("SET s3_region='us-east-1';")

    lst = _build_sql_list(urls)
    sql = f"""
    SELECT * FROM read_parquet({lst})
    LIMIT {int(args.limit)}
    """
    print(f"(membaca {len(urls)} file)", file=sys.stderr)
    res = con.execute(sql)
    _print_result(res)


if __name__ == "__main__":
    main()
