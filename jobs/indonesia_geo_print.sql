-- Alur utama: topic indo_geo (JSON) -> sink print. Skema = keluaran geo_id_stream.py
-- Jalankan: scripts/run_flink_sql.sh / run_flink_sql.ps1

CREATE TABLE geo_events (
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
  'properties.group.id' = 'flink-sql-indo-geo',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.fail-on-missing-field' = 'false',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE geo_print (
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
  'connector' = 'print'
);

INSERT INTO geo_print
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
FROM geo_events;
