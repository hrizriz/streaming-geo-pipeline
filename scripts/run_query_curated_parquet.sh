#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
exec python python/query_curated_parquet.py "$@"
