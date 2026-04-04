"""Publishes generated device telemetry to Kafka."""

import json
import logging
import os
import time
from typing import Any, Dict, Optional

from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic

from ingestion.device_data_generator import generate_device_stream


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class KafkaProducerService:
    """Encapsulated service responsible for topic prep and message publishing."""

    def __init__(self) -> None:
        self.kafka_broker = os.getenv("KAFKA_BROKER", "broker:9092")
        self.raw_topic = os.getenv("KAFKA_TOPIC_RAW", "raw_h2_data")
        self.producer: Optional[KafkaProducer] = None
        self.message_count = 0

    def print_configuration(self) -> None:
        print("Configuration:")
        print(f"  KAFKA_BROKER env: {os.getenv('KAFKA_BROKER', 'NOT SET')}")
        print(f"  Using broker: {self.kafka_broker}")
        print(f"  Topic: {self.raw_topic}")

    def validate_settings(self) -> None:
        if not self.kafka_broker:
            raise ValueError("KAFKA_BROKER is empty")
        if not self.raw_topic:
            raise ValueError("KAFKA_TOPIC_RAW is empty")

    def create_topic_if_not_exists(self) -> bool:
        admin_client = None
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=[self.kafka_broker],
                request_timeout_ms=30000,
            )
            existing_topics = admin_client.list_topics()
            logger.info("Existing topics: %s", list(existing_topics))

            if self.raw_topic not in existing_topics:
                logger.info("Creating topic: %s", self.raw_topic)
                admin_client.create_topics(
                    [
                        NewTopic(
                            name=self.raw_topic,
                            num_partitions=3,
                            replication_factor=1,
                        )
                    ]
                )
                logger.info("✅ Topic '%s' created successfully", self.raw_topic)
            else:
                logger.info("✅ Topic '%s' already exists", self.raw_topic)
            return True
        except Exception as exc:
            logger.warning("Could not create topic: %s", exc)
            return False
        finally:
            if admin_client:
                admin_client.close()

    def create_producer(self, max_retries: int = 15, retry_delay: int = 3) -> KafkaProducer:
        for attempt in range(max_retries):
            try:
                logger.info(
                    "Attempting to connect to Kafka at %s (attempt %d/%d)",
                    self.kafka_broker,
                    attempt + 1,
                    max_retries,
                )

                if attempt == 0:
                    self.create_topic_if_not_exists()

                producer = KafkaProducer(
                    bootstrap_servers=[self.kafka_broker],
                    value_serializer=lambda payload: json.dumps(payload).encode("utf-8"),
                    retries=5,
                    request_timeout_ms=30000,
                    metadata_max_age_ms=30000,
                    max_block_ms=60000,
                )

                time.sleep(2)
                partitions = producer.partitions_for(self.raw_topic) or set()
                logger.info(
                    "✅ Connected successfully! Topic '%s' has %d partitions",
                    self.raw_topic,
                    len(partitions),
                )
                self.producer = producer
                return producer
            except Exception as exc:
                logger.warning("⚠️ Connection error: %s", exc)
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    raise

    def publish_message(self, data: Dict[str, Any]) -> None:
        if not self.producer:
            raise RuntimeError("Producer is not initialized")

        future = self.producer.send(
            self.raw_topic,
            key=data.get("sensor_id", "unknown").encode(),
            value=data,
        )
        future.get(timeout=30)

        self.message_count += 1
        sensor_id = data.get("sensor_id", "unknown")
        value = data.get("value", 0)
        unit = data.get("unit", "N/A")

        if self.message_count % 5 == 0:
            logger.info("✅ Sent %d messages to Kafka", self.message_count)
        else:
            print(f" [{self.message_count:03d}] ✅ Sent: {sensor_id} = {value} {unit}")

    def run(self) -> None:
        self.print_configuration()
        self.validate_settings()

        logger.info("Waiting 5 seconds for services to initialize...")
        time.sleep(5)

        try:
            self.create_producer()
            print("\n 🚀 Starting authenticated data stream...")
            print("Press Ctrl+C to stop\n")

            while True:
                data = generate_device_stream()
                self.publish_message(data)
                time.sleep(2)

        except KeyboardInterrupt:
            print("\n\n🛑 Stopped by user")
            self.shutdown()
            print(f"📊 Total messages sent: {self.message_count}")
        except Exception as exc:
            logger.error("Producer failed: %s", exc)
            self.shutdown()
            raise

    def shutdown(self) -> None:
        if self.producer:
            self.producer.flush(timeout=10)
            self.producer.close()


def main() -> None:
    KafkaProducerService().run()


if __name__ == "__main__":
    main()
