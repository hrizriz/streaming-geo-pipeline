#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ ! -f credentials.env ]]; then
  cp credentials.env.example credentials.env
  echo "Dibuat credentials.env dari credentials.env.example — sesuaikan password bila perlu."
fi
docker compose up -d --build

echo ""
echo "=== Stack siap ==="
echo "  Flink UI     http://localhost:8081"
echo "  MinIO        http://localhost:9001  (user/password: credentials.env)"
echo "  Kafka        localhost:9092"
echo "  Panduan alur  use_case/pipeline_flow.txt"
echo ""
echo "Berikutnya (producer Python):"
echo "  cd python"
echo "  source .venv/Scripts/activate"
echo "  pip install -r requirements.txt   # sekali"
echo "  python geo_id_stream.py"
echo "  # Sink MinIO (JSON): scripts/run_flink_minio_sql.sh   (dari root project)"
echo ""
