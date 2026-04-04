# Naikkan stack: Zookeeper, Kafka, MinIO, Flink (image + connector Kafka/S3).
Set-Location $PSScriptRoot\..
if (-not (Test-Path "credentials.env")) {
    Copy-Item "credentials.env.example" "credentials.env"
    Write-Host "Dibuat credentials.env dari credentials.env.example — sesuaikan password bila perlu."
}
docker compose up -d --build

Write-Host ""
Write-Host "=== Stack siap ===" -ForegroundColor Green
Write-Host "  Flink UI     http://localhost:8081"
Write-Host "  MinIO        http://localhost:9001  (user/password: credentials.env)"
Write-Host "  Kafka        localhost:9092"
Write-Host "  Panduan alur  use_case\pipeline_flow.txt"
Write-Host ""
Write-Host "Berikutnya (producer Python):" -ForegroundColor Cyan
Write-Host "  cd python"
Write-Host '  .\.venv\Scripts\Activate.ps1          # PowerShell'
Write-Host '  source .venv/Scripts/activate           # Git Bash'
Write-Host "  pip install -r requirements.txt       # sekali"
Write-Host "  python geo_id_stream.py"
Write-Host "  # Sink MinIO (JSON): .\scripts\run_flink_minio_sql.ps1   (dari root project)"
Write-Host ""
