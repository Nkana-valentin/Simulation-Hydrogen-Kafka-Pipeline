"""Kafka consumer pipeline with authentication and quality checks."""

import json
import logging
import os
import sys
import time
from typing import Dict, Optional, Tuple

from kafka import KafkaConsumer

from ingestion.json_tsdb_manager import Json2TsdbTransformer
from ingestion.quality_check import DataQualityChecker
from ingestion.questdbclient import QuestDBClient


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class KafkaToTsdbConsumerService:
    """Encapsulated ingestion pipeline from Kafka to QuestDB."""

    def __init__(self) -> None:
        self.kafka_broker = os.getenv("KAFKA_BROKER", "broker:9092")
        self.raw_topic = os.getenv("KAFKA_TOPIC_RAW", "raw_h2_data")
        self.questdb_client = QuestDBClient()
        self.quality = DataQualityChecker()
        self.schema = {
            "tags": ["lab", "sensor_id", "measurement_type", "unit", "qualityflag"],
            "fields": ["value"],
        }
        self.json2tsdb = Json2TsdbTransformer(
            table_name=self.raw_topic,
            tag_keys=self.schema.get("tags"),
            field_keys=self.schema.get("fields"),
        )
        self.consumer: Optional[KafkaConsumer] = None
        self.message_count = 0

    def print_banner(self) -> None:
        print("=" * 70)
        print("🔐 H2 SMART LAB INGESTION PIPELINE (Auth + Quality)")
        print("=" * 70)
        print(f"Kafka Broker: {self.kafka_broker}")
        print(f"Raw topic: {self.raw_topic}")
        print(f"QuestDB:  {self.questdb_client.QUESTDB_WRITE_URL}")
        print("=" * 70)

    def validate_settings(self) -> None:
        if not self.kafka_broker:
            raise ValueError("KAFKA_BROKER is empty")
        if not self.raw_topic:
            raise ValueError("KAFKA_TOPIC_RAW is empty")

    @staticmethod
    def is_authenticated(data: Dict) -> Tuple[bool, str]:
        if not isinstance(data, dict):
            return False, "invalid_payload_type"
        if "auth" not in data:
            return False, "missing_auth_section"

        auth = data["auth"]
        if not auth.get("authenticated", False):
            return False, "not_authenticated_flag"
        if "device_id" not in auth:
            return False, "missing_device_id"
        if "lab" not in auth:
            return False, "missing_lab"
        return True, "authenticated"

    def create_consumer(self) -> KafkaConsumer:
        max_retries = 30
        retry_delay = 5

        for attempt in range(max_retries):
            try:
                logger.info(
                    "Attempting to connect to Kafka at %s (attempt %d/%d)",
                    self.kafka_broker,
                    attempt + 1,
                    max_retries,
                )

                consumer = KafkaConsumer(
                    self.raw_topic,
                    bootstrap_servers=[self.kafka_broker],
                    value_deserializer=lambda payload: json.loads(payload.decode("utf-8")),
                    auto_offset_reset="earliest",
                    group_id="questdb_consumer",
                    enable_auto_commit=True,
                    consumer_timeout_ms=1000,
                    max_poll_records=100,
                    request_timeout_ms=40000,
                    session_timeout_ms=30000,
                    heartbeat_interval_ms=10000,
                    api_version_auto_timeout_ms=30000,
                )

                partitions = consumer.partitions_for_topic(self.raw_topic) or set()
                logger.info(
                    "✅ Connected to Kafka, topic '%s' has %d partitions",
                    self.raw_topic,
                    len(partitions),
                )
                self.consumer = consumer
                return consumer
            except Exception as exc:
                logger.warning("⚠️ Failed to connect to Kafka: %s", exc)
                if attempt < max_retries - 1:
                    logger.info("Waiting %d seconds...", retry_delay)
                    time.sleep(retry_delay)
                else:
                    logger.error("❌ Failed to connect to Kafka after all retries")
                    raise

    def setup_tsdb(self) -> None:
        if self.questdb_client.create_table(self.raw_topic, self.schema):
            logger.info("✅ Table '%s' is ready in QuestDB", self.raw_topic)

    def process_record(self, data: Dict) -> None:
        auth_ok, auth_reason = self.is_authenticated(data)

        if auth_ok:
            validation_record = dict(data)
            measurement_type = str(data.get("measurement_type", "")).lower()
            metric_map = {"flow": "flow_rate"}
            metric_field = metric_map.get(measurement_type, measurement_type)
            if metric_field:
                raw_value = data.get("value")
                try:
                    validation_record[metric_field] = float(raw_value)
                except (TypeError, ValueError):
                    validation_record[metric_field] = raw_value

            is_valid, errors = self.quality.validate_single_record(validation_record)
        else:
            is_valid = False
            errors = [f"auth_failed:{auth_reason}"]
            self.quality.stats["auth_failed"] += 1

        data["qualityflag"] = str(is_valid)
        if not is_valid and errors:
            data["qualityflag"] = ",".join(errors[:3])

        try:
            line_protocol = self.json2tsdb.transform(data)
            if self.questdb_client.insert_data(self.raw_topic, line_protocol):
                self.quality.update_stats(is_valid)
            else:
                logger.error("Failed to insert data into QuestDB")
        except Exception as exc:
            logger.error("Error inserting data: %s", exc)

    def run(self) -> None:
        self.print_banner()
        self.validate_settings()

        logger.info("Waiting 15 seconds for Kafka to be ready...")
        time.sleep(15)

        self.setup_tsdb()

        try:
            self.create_consumer()
        except Exception as exc:
            logger.error("Failed to create consumer: %s", exc)
            sys.exit(1)

        print("\n👂 Listening for sensor data...")
        print("-" * 70)

        try:
            while True:
                if not self.consumer:
                    raise RuntimeError("Consumer is not initialized")

                messages = self.consumer.poll(timeout_ms=1000)
                if not messages:
                    continue

                for _topic_partition, records in messages.items():
                    for message in records:
                        data = message.value
                        self.quality.stats["total_received"] += 1
                        self.message_count += 1

                        if self.message_count % 10 == 0:
                            logger.info("Received %d messages from Kafka", self.message_count)

                        self.process_record(data)

        except KeyboardInterrupt:
            print("\n\n🛑 Stopped by user")
        finally:
            if self.consumer:
                self.consumer.close()
            print("\n📊 FINAL STATISTICS:")
            self.quality.print_stats()
            print(f"Total messages consumed: {self.message_count}")


def main() -> None:
    KafkaToTsdbConsumerService().run()


if __name__ == "__main__":
    main()
