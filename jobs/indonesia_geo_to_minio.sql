-- Kafka indo_geo -> file JSON di MinIO (s3a://warehouse/geo-events/)
-- Flink 1.18 menulis part-<uuid>-<subtask>-<roll> tanpa ekstensi (NDJSON); MinIO preview pakai Download atau buka file di editor.
-- Pastikan kredensial MinIO di docker-compose sama dengan credentials.env (default minioadmin)
-- Jalankan: scripts/run_flink_minio_sql.sh / .ps1
-- Lihat file: bucket warehouse di MinIO Console http://localhost:9001
--
-- Checkpoint wajib agar part-file di S3/MinIO di-finalize (tanpa ini bucket bisa tetap kosong).
SET 'execution.checkpointing.interval' = '30 s';

CREATE TABLE geo_kafka (
  event_type STRING,
  source STRING,
  ts BIGINT,
  lat DOUBLE,
  lon DOUBLE,
  label STRING,
  mag DOUBLE,
  temp_c DOUBLE,
  humidity_pct DOUBLE,
  detail STRING,
  proc_time AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'indo_geo',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.group.id' = 'flink-sql-indo-geo-minio',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.fail-on-missing-field' = 'false',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE geo_minio (
  event_type STRING,
  source STRING,
  ts BIGINT,
  lat DOUBLE,
  lon DOUBLE,
  label STRING,
  mag DOUBLE,
  temp_c DOUBLE,
  humidity_pct DOUBLE,
  detail STRING
) WITH (
  'connector' = 'filesystem',
  'path' = 's3a://warehouse/geo-events/',
  'format' = 'json',
  'sink.rolling-policy.file-size' = '8MB',
  'sink.rolling-policy.rollover-interval' = '1 min',
  'sink.rolling-policy.check-interval' = '30s'
);

INSERT INTO geo_minio
SELECT
  event_type,
  source,
  ts,
  lat,
  lon,
  label,
  mag,
  temp_c,
  humidity_pct,
  detail
FROM geo_kafka;
