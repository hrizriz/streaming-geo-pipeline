"""
Baca topic Kafka `indo_geo` (stream JSON dari geo_id_stream.py).
"""

import json
import os

from confluent_kafka import Consumer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "indo_geo")
GROUP = os.environ.get("KAFKA_GROUP_ID", "python-pipeline-consumer")


def main() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": GROUP,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe([TOPIC])
    print(f"Listening {BOOTSTRAP} topic={TOPIC} group={GROUP}")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
            try:
                data = json.loads(msg.value().decode("utf-8"))
                print(f"[{msg.partition()}@{msg.offset()}] {data}")
            except json.JSONDecodeError:
                print(f"Raw: {msg.value()!r}")
    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
