"""
streaming/kafka_producer.py
-----------------------------
Reads the processed parquet (post-transformation) and publishes
anomaly alerts to the Kafka topic `crop-alerts` in real-time.

Called by the Airflow DAG after transformations complete.
"""

import json
import logging
import os
import time
from datetime import datetime

import pandas as pd
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC           = os.getenv("KAFKA_TOPIC", "crop-alerts")


def ensure_topic(bootstrap: str, topic: str, num_partitions: int = 1) -> None:
    """Create Kafka topic if it doesn't exist. Retries on broker not ready."""
    for attempt in range(1, 6):
        try:
            admin = AdminClient({
                "bootstrap.servers": bootstrap,
                "socket.timeout.ms": 10000,
                "metadata.request.timeout.ms": 10000,
            })
            existing = admin.list_topics(timeout=10).topics
            if topic not in existing:
                admin.create_topics([NewTopic(topic, num_partitions=num_partitions, replication_factor=1)])
                logger.info("Created Kafka topic: %s", topic)
            else:
                logger.info("Kafka topic already exists: %s", topic)
            return
        except Exception as exc:
            logger.warning("Kafka not ready (attempt %d/5): %s", attempt, exc)
            if attempt == 5:
                raise
            time.sleep(5 * attempt)


def delivery_report(err, msg) -> None:
    """Callback fired when a message is acknowledged by Kafka."""
    if err:
        logger.error("Delivery failed: %s", err)
    else:
        logger.debug("Delivered to %s [%s]", msg.topic(), msg.partition())


def publish_anomalies(
    parquet_path: str = "data/processed.parquet",
    bootstrap: str = KAFKA_BOOTSTRAP,
    topic: str = TOPIC,
    batch_delay_ms: int = 50,
) -> int:
    """
    Read processed parquet and publish each anomaly row as a JSON message.

    Returns:
        Number of messages published.
    """
    ensure_topic(bootstrap, topic)

    try:
        df = pd.read_parquet(parquet_path)
    except FileNotFoundError:
        logger.warning("Processed parquet not found at %s — skipping", parquet_path)
        return 0

    anomalies = df[df.get("is_anomaly", pd.Series(False, index=df.index)) == True]
    if anomalies.empty:
        logger.info("No anomalies to publish.")
        return 0

    producer = Producer({"bootstrap.servers": bootstrap})
    published = 0

    for _, row in anomalies.iterrows():
        message = {
            "event_type":        "NDVI_ANOMALY",
            "timestamp":         datetime.utcnow().isoformat() + "Z",
            "scene_date":        str(row.get("scene_date", "")),
            "lat":               float(row.get("lat", 0)),
            "lon":               float(row.get("lon", 0)),
            "ndvi":              float(row.get("ndvi", 0)),
            "rolling_mean_ndvi": float(row.get("rolling_mean_ndvi", 0)),
            "ndvi_drop_pct":     float(row.get("ndvi_drop_pct", 0)),
            "ndvi_class":        str(row.get("ndvi_class", "")),
            "aoi":               str(row.get("aoi_bbox", "")),
        }

        producer.produce(
            topic=topic,
            key=f"{row.get('lat')}_{row.get('lon')}",
            value=json.dumps(message).encode("utf-8"),
            callback=delivery_report,
        )
        published += 1

        # Throttle slightly to avoid overwhelming consumers
        if published % 100 == 0:
            producer.poll(0)
            time.sleep(batch_delay_ms / 1000)

    producer.flush()
    logger.info("Published %d anomaly alerts to topic '%s'", published, topic)
    return published


if __name__ == "__main__":
    publish_anomalies()