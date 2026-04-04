"""
Poll data internasional yang relevan wilayah Indonesia, lalu kirim ke Kafka.

- Gempa: USGS FDSN (format GeoJSON), bbox kira-kira wilayah Indonesia.
- Cuaca: Open-Meteo (gratis, tanpa API key), beberapa kota besar ID.

Bukan layanan resmi Indonesia; patuhi ToS & batas rate masing-masing.

Contoh:
  python geo_id_stream.py --interval 90
  KAFKA_TOPIC=indo_geo python geo_id_stream.py
"""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
from confluent_kafka import Producer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "indo_geo")

# Bounding box ~perairan & daratan Indonesia (kasar; sedikit overlap tetangga)
ID_MIN_LAT = -11.0
ID_MAX_LAT = 6.5
ID_MIN_LON = 95.0
ID_MAX_LON = 141.0

# Kota contoh (lat, lon, label)
ID_CITIES: list[tuple[float, float, str]] = [
    (-6.2088, 106.8456, "Jakarta"),
    (-7.2575, 112.7521, "Surabaya"),
    (-8.6705, 115.2126, "Denpasar"),
    (3.5952, 98.6722, "Medan"),
    (-0.9471, 100.4172, "Padang"),
    (-5.1354, 119.4236, "Makassar"),
]

USGS_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "modern_data_engineer/geo_id_stream (learning)"})

_seen_quake_ids: set[str] = set()


def envelope(
    event_type: str,
    source: str,
    lat: float,
    lon: float,
    label: str,
    ts_ms: int,
    mag: float | None = None,
    temp_c: float | None = None,
    humidity_pct: float | None = None,
    detail: str = "",
) -> dict[str, Any]:
    return {
        "event_type": event_type,
        "source": source,
        "ts": ts_ms,
        "lat": lat,
        "lon": lon,
        "label": label,
        "mag": mag,
        "temp_c": temp_c,
        "humidity_pct": humidity_pct,
        "detail": detail[:2000],
    }


def fetch_usgs_indonesia() -> list[dict[str, Any]]:
    start = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    params = {
        "format": "geojson",
        "starttime": start,
        "minlatitude": ID_MIN_LAT,
        "maxlatitude": ID_MAX_LAT,
        "minlongitude": ID_MIN_LON,
        "maxlongitude": ID_MAX_LON,
        "orderby": "time",
        "limit": "100",
    }
    r = SESSION.get(USGS_URL, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    out: list[dict[str, Any]] = []
    for f in data.get("features", []):
        eid = str(f.get("id", ""))
        if not eid or eid in _seen_quake_ids:
            continue
        props = f.get("properties") or {}
        geom = f.get("geometry") or {}
        coords = (geom.get("coordinates") or [0, 0, 0])[:3]
        lon, lat = float(coords[0]), float(coords[1])
        ms = int(props.get("time", 0))
        mag = props.get("mag")
        try:
            mag_f = float(mag) if mag is not None else None
        except (TypeError, ValueError):
            mag_f = None
        place = str(props.get("place") or "")
        out.append(
            envelope(
                "earthquake",
                "usgs",
                lat,
                lon,
                place[:200] or "unknown",
                ms,
                mag=mag_f,
                detail=f"usgs_id={eid}",
            )
        )
        _seen_quake_ids.add(eid)
        if len(_seen_quake_ids) > 2000:
            _seen_quake_ids.clear()
    return out


def fetch_open_meteo_city(lat: float, lon: float, label: str) -> dict[str, Any]:
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": "temperature_2m,relative_humidity_2m,weather_code,wind_speed_10m",
        "timezone": "Asia/Jakarta",
    }
    r = SESSION.get(OPEN_METEO_URL, params=params, timeout=30)
    r.raise_for_status()
    js = r.json()
    cur = js.get("current") or {}
    ts_s = cur.get("time")  # ISO string from API
    ts_ms = int(time.time() * 1000)
    if ts_s:
        try:
            dt = datetime.fromisoformat(str(ts_s).replace("Z", "+00:00"))
            ts_ms = int(dt.timestamp() * 1000)
        except ValueError:
            pass
    temp = cur.get("temperature_2m")
    hum = cur.get("relative_humidity_2m")
    try:
        tc = float(temp) if temp is not None else None
    except (TypeError, ValueError):
        tc = None
    try:
        hm = float(hum) if hum is not None else None
    except (TypeError, ValueError):
        hm = None
    wc = cur.get("weather_code")
    return envelope(
        "weather",
        "open_meteo",
        lat,
        lon,
        label,
        ts_ms,
        temp_c=tc,
        humidity_pct=hm,
        detail=f"weather_code={wc}",
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="USGS + Open-Meteo (ID) -> Kafka")
    p.add_argument(
        "--interval",
        type=float,
        default=120.0,
        help="Detik antar siklus poll (default 120)",
    )
    p.add_argument("--topic", "-t", default=TOPIC)
    p.add_argument("--bootstrap", default=BOOTSTRAP)
    p.add_argument(
        "--no-weather",
        action="store_true",
        help="Hanya gempa (USGS)",
    )
    p.add_argument(
        "--no-earthquake",
        action="store_true",
        help="Hanya cuaca (Open-Meteo)",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    prod = Producer({"bootstrap.servers": args.bootstrap})

    def delivery(err, msg) -> None:
        if err:
            print(f"Kafka error: {err}")

    print(f"Topic={args.topic} interval={args.interval}s USGS bbox ID | Open-Meteo cities={len(ID_CITIES)}")
    try:
        while True:
            n = 0
            if not args.no_earthquake:
                try:
                    for ev in fetch_usgs_indonesia():
                        prod.produce(
                            args.topic,
                            value=json.dumps(ev, ensure_ascii=False).encode("utf-8"),
                            callback=delivery,
                        )
                        prod.poll(0)
                        n += 1
                        print(f"[quake] {ev.get('label')} M={ev.get('mag')}")
                except requests.RequestException as e:
                    print(f"USGS gagal: {e}")

            if not args.no_weather:
                for lat, lon, label in ID_CITIES:
                    try:
                        ev = fetch_open_meteo_city(lat, lon, label)
                        prod.produce(
                            args.topic,
                            value=json.dumps(ev, ensure_ascii=False).encode("utf-8"),
                            callback=delivery,
                        )
                        prod.poll(0)
                        n += 1
                        print(f"[weather] {label} {ev.get('temp_c')}C")
                    except requests.RequestException as e:
                        print(f"Open-Meteo {label}: {e}")

            prod.flush(5)
            print(f"Siklus selesai, {n} pesan. Sleep {args.interval}s...")
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("Berhenti.")
    finally:
        prod.flush(10)


if __name__ == "__main__":
    main()
