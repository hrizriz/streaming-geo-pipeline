#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"
docker compose exec jobmanager /opt/flink/bin/sql-client.sh embedded -f /opt/flink/jobs/indonesia_geo_curated_parquet.sql
