Set-Location $PSScriptRoot\..
docker compose exec jobmanager /opt/flink/bin/sql-client.sh embedded -f /opt/flink/jobs/indonesia_geo_to_minio.sql
