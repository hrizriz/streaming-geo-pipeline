-- Pola A: Kafka indo_geo -> MinIO Parquet terpartisi harian (s3a://warehouse/geo-events/curated/dt=YYYY-MM-DD/)
-- File objek: part-* tanpa sufiks .parquet (perilaku Flink bulk); isi tetap Parquet.
-- Image: flink-sql-parquet + flink-shaded-hadoop-2-uber (Hadoop Configuration untuk writer Parquet). Lihat Dockerfile.flink.
-- Consumer group terpisah dari job JSON raw; boleh jalan bersamaan dengan indonesia_geo_to_minio.sql.
-- Jalankan: scripts/run_flink_curated_parquet.sh / .ps1
--
-- File kecil banyak: (1) throughput rendah — rolling 128MB jarang tercapai; (2) sink filesystem
-- menutup part-file saat checkpoint (exactly-once ke S3/MinIO), jadi seringnya file = "isi antar checkpoint";
-- (3) producer geo_id_stream.py default --interval 120s → batch data tipis per siklus.
-- Per pendekatan: perpanjang interval checkpoint di bawah (trade-off: recovery lebih lama) atau naikkan
-- volume stream / compaction downstream (Spark, Iceberg, dll).

SET 'execution.checkpointing.interval' = '10 min';

CREATE TABLE geo_kafka_curated (
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
  'properties.group.id' = 'flink-sql-indo-geo-curated-parquet',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.fail-on-missing-field' = 'false',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE geo_curated_parquet (
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
  dt STRING
) PARTITIONED BY (dt) WITH (
  'connector' = 'filesystem',
  'path' = 's3a://warehouse/geo-events/curated/',
  'format' = 'parquet',
  'parquet.compression' = 'SNAPPY',
  'sink.rolling-policy.file-size' = '128MB',
  'sink.rolling-policy.rollover-interval' = '1 h',
  'sink.rolling-policy.check-interval' = '5 min',
  'sink.partition-commit.trigger' = 'process-time',
  'sink.partition-commit.delay' = '1 min'
);

INSERT INTO geo_curated_parquet
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
  detail,
  DATE_FORMAT(TO_TIMESTAMP_LTZ(ts, 3), 'yyyy-MM-dd') AS dt
FROM geo_kafka_curated;
