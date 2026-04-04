"""
Baca Parquet curated di MinIO (part-* Flink) dan tulis ke tabel Apache Iceberg.

Metadata catalog memakai JDBC Postgres (sama dengan Trino). Data & metadata Iceberg
disimpan di s3://warehouse/iceberg/ (MinIO).

Prasyarat:
  - docker compose: postgres, minio, trino sudah jalan
  - pip install -r requirements.txt
  - Postgres port 5432 terbuka (script dijalankan dari host)

Contoh:
  python python/curated_to_iceberg.py --replace
  python python/curated_to_iceberg.py --dt 2026-04-04
"""

from __future__ import annotations

import argparse
import io
import os
import re
import sys
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from botocore.config import Config as BotoConfig
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import DoubleType, LongType, NestedField, StringType


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


def _list_part_parquet_urls(
    *,
    endpoint_url: str,
    access: str,
    secret: str,
    bucket: str,
    prefix: str,
) -> list[str]:
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


def _read_parquets_via_boto3(
    bucket: str,
    urls: list[str],
    endpoint_url: str,
    access: str,
    secret: str,
) -> pa.Table:
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access,
        aws_secret_access_key=secret,
        config=BotoConfig(signature_version="s3v4"),
        region_name="us-east-1",
    )
    tables: list[pa.Table] = []
    for u in urls:
        key = u.replace(f"s3://{bucket}/", "", 1)
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        tables.append(pq.read_table(io.BytesIO(body)))
    if not tables:
        raise SystemExit("Tidak ada baris untuk digabung.")
    return pa.concat_tables(tables)


def _iceberg_schema() -> Schema:
    return Schema(
        NestedField(1, "event_type", StringType(), required=False),
        NestedField(2, "source", StringType(), required=False),
        NestedField(3, "ts", LongType(), required=False),
        NestedField(4, "lat", DoubleType(), required=False),
        NestedField(5, "lon", DoubleType(), required=False),
        NestedField(6, "label", StringType(), required=False),
        NestedField(7, "mag", DoubleType(), required=False),
        NestedField(8, "temp_c", DoubleType(), required=False),
        NestedField(9, "humidity_pct", DoubleType(), required=False),
        NestedField(10, "detail", StringType(), required=False),
        NestedField(11, "dt", StringType(), required=False),
    )


def _align_arrow_to_schema(arrow_tbl: pa.Table, names: list[str]) -> pa.Table:
    missing = [n for n in names if n not in arrow_tbl.column_names]
    if missing:
        raise SystemExit(f"Kolom wajib tidak ada di Parquet: {missing}")
    arrow_tbl = arrow_tbl.select(names)
    casts = []
    for i, name in enumerate(names):
        col = arrow_tbl.column(i)
        if name == "ts":
            if pa.types.is_floating(col.type):
                col = pc.cast(col, pa.int64())
        elif name in ("lat", "lon", "mag", "temp_c", "humidity_pct"):
            if pa.types.is_integer(col.type):
                col = pc.cast(col, pa.float64())
        casts.append(col)
    return pa.table(dict(zip(names, casts, strict=True)))


def main() -> None:
    parser = argparse.ArgumentParser(description="Parquet curated MinIO -> Iceberg (JDBC Postgres + MinIO)")
    parser.add_argument("--bucket", default="warehouse", help="Bucket MinIO")
    parser.add_argument(
        "--dt",
        default=None,
        metavar="YYYY-MM-DD",
        help="Hanya partisi dt=...; default: semua curated",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Hapus tabel lalu buat ulang (tanpa ini: append ke tabel yang ada)",
    )
    parser.add_argument(
        "--jdbc-uri",
        default=os.environ.get(
            "ICEBERG_JDBC_URI",
            "postgresql+psycopg2://iceberg:iceberg@127.0.0.1:5432/iceberg",
        ),
        help="SQLAlchemy URI untuk catalog Iceberg (Postgres)",
    )
    args = parser.parse_args()

    cred = _load_credentials_env()
    api_url = cred.get("MINIO_S3_API_URL", os.environ.get("MINIO_S3_API_URL", "http://localhost:9000"))
    if not api_url.startswith("http"):
        api_url = "http://" + api_url
    access = cred.get("MINIO_ROOT_USER", os.environ.get("MINIO_ROOT_USER", "minioadmin"))
    secret = cred.get("MINIO_ROOT_PASSWORD", os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin"))

    prefix = (
        f"geo-events/curated/dt={args.dt}/"
        if args.dt
        else "geo-events/curated/"
    )
    if args.dt and not re.fullmatch(r"\d{4}-\d{2}-\d{2}", args.dt):
        print("--dt harus YYYY-MM-DD", file=sys.stderr)
        raise SystemExit(2)

    urls = _list_part_parquet_urls(
        endpoint_url=api_url,
        access=access,
        secret=secret,
        bucket=args.bucket,
        prefix=prefix,
    )
    if not urls:
        print(f"Tidak ada file part-* di s3://{args.bucket}/{prefix}", file=sys.stderr)
        raise SystemExit(3)

    print(f"(membaca {len(urls)} file Parquet)", file=sys.stderr)
    arrow_tbl = _read_parquets_via_boto3(args.bucket, urls, api_url, access, secret)
    col_order = [
        "event_type",
        "source",
        "ts",
        "lat",
        "lon",
        "label",
        "mag",
        "temp_c",
        "humidity_pct",
        "detail",
        "dt",
    ]
    arrow_tbl = _align_arrow_to_schema(arrow_tbl, col_order)

    schema = _iceberg_schema()
    spec = PartitionSpec(
        PartitionField(
            source_id=11,
            field_id=1000,
            transform=IdentityTransform(),
            name="dt",
        )
    )

    catalog = load_catalog(
        "warehouse",
        **{
            "type": "sql",
            "uri": args.jdbc_uri,
            "warehouse": "s3://warehouse/iceberg/",
            "s3.endpoint": api_url,
            "s3.access-key-id": access,
            "s3.secret-access-key": secret,
            "s3.path-style-access": "true",
            "s3.region": "us-east-1",
        },
    )

    ident = ("curated", "geo_curated")
    if catalog.table_exists(ident):
        if args.replace:
            catalog.drop_table(ident)
        else:
            tbl = catalog.load_table(ident)
            tbl.append(arrow_tbl)
            print("OK: append ke iceberg.curated.geo_curated", file=sys.stderr)
            return

    try:
        catalog.create_namespace(("curated",))
    except NamespaceAlreadyExistsError:
        pass

    catalog.create_table(ident, schema=schema, partition_spec=spec)
    tbl = catalog.load_table(ident)
    tbl.append(arrow_tbl)
    print("OK: tabel iceberg.curated.geo_curated dibuat dan diisi", file=sys.stderr)


if __name__ == "__main__":
    main()
