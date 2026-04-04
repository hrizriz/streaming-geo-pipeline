# Flink: baca topic indo_geo -> print (lihat use_case/pipeline_flow.txt). UI: http://localhost:8081
Set-Location $PSScriptRoot\..
docker compose exec jobmanager /opt/flink/bin/sql-client.sh embedded -f /opt/flink/jobs/indonesia_geo_print.sql
