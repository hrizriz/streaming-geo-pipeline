# Hentikan stack lalu naikkan lagi (mis. konflik nama container).
Set-Location $PSScriptRoot\..
if (-not (Test-Path "credentials.env")) {
    Copy-Item "credentials.env.example" "credentials.env"
    Write-Host "Dibuat credentials.env dari credentials.env.example."
}
docker compose down --remove-orphans
docker compose up -d --build

Write-Host ""
Write-Host "=== Stack siap (setelah reset) ===" -ForegroundColor Green
Write-Host "  Flink UI     http://localhost:8081"
Write-Host "  MinIO        http://localhost:9001  (user/password: credentials.env)"
Write-Host "  Kafka        localhost:9092"
Write-Host "  Panduan alur  use_case\pipeline_flow.txt"
Write-Host ""
