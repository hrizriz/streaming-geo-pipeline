#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ ! -f credentials.env ]]; then
  cp credentials.env.example credentials.env
  echo "Dibuat credentials.env dari credentials.env.example."
fi
docker compose down --remove-orphans
docker compose up -d --build

echo ""
echo "=== Stack siap (setelah reset) ==="
echo "  Flink UI     http://localhost:8081"
echo "  MinIO        http://localhost:9001  (user/password: credentials.env)"
echo "  Kafka        localhost:9092"
echo "  Panduan alur  use_case/pipeline_flow.txt"
echo ""
