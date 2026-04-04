#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python python/curated_to_iceberg.py "$@"
