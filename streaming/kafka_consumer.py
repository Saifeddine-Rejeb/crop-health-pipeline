"""
streaming/kafka_consumer.py
-----------------------------
Consumes NDVI anomaly alerts from Kafka and appends them
to a local JSON-lines file that the Streamlit dashboard tail-reads.

Run this as a long-lived background process (or a Docker service).
"""

import json
import logging
import os
import signal
import sys
from datetime import datetime
from pathlib import Path

from confluent_kafka import Consumer, KafkaError

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC           = os.getenv("KAFKA_TOPIC", "crop-alerts")
ALERTS_FILE     = os.getenv("ALERTS_FILE", "data/alerts.jsonl")

_running = True


def handle_sigterm(*_):
    global _running
    logger.info("Received shutdown signal — stopping consumer.")
    _running = False


signal.signal(signal.SIGTERM, handle_sigterm)
signal.signal(signal.SIGINT,  handle_sigterm)


def run_consumer(
    bootstrap: str = KAFKA_BOOTSTRAP,
    topic: str = TOPIC,
    output_file: str = ALERTS_FILE,
    group_id: str = "dashboard-consumer",
) -> None:
    """
    Poll Kafka indefinitely and append each alert to a JSONL file.
    The dashboard reads this file to show real-time alerts.
    """
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)

    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id":          group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([topic])
    logger.info("Consumer subscribed to topic '%s'", topic)

    try:
        while _running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("Kafka error: %s", msg.error())
                continue

            try:
                payload = json.loads(msg.value().decode("utf-8"))
                payload["consumed_at"] = datetime.utcnow().isoformat() + "Z"

                with open(output_file, "a") as fh:
                    fh.write(json.dumps(payload) + "\n")

                logger.info(
                    "Alert consumed: lat=%.4f lon=%.4f ndvi=%.3f drop=%.1f%%",
                    payload.get("lat", 0),
                    payload.get("lon", 0),
                    payload.get("ndvi", 0),
                    payload.get("ndvi_drop_pct", 0) * 100,
                )
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                logger.warning("Could not decode message: %s", exc)

    finally:
        consumer.close()
        logger.info("Consumer closed.")


if __name__ == "__main__":
    run_consumer()
