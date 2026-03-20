# ingestion/data_quality_consumer.py
"""
Kafka consumer performing data quality validation
using the advanced DataQualityChecker module.

Pipeline stage:
RAW topic -> validation -> VALIDATED / QUARANTINE topics
"""

import json
from kafka import KafkaConsumer, KafkaProducer
from typing import List, Dict
import structlog

from ingestion.quality_check import DataQualityChecker
#from pipeline_config import TOPICS, SETTINGS
from dataclasses import dataclass

logger = structlog.get_logger()


WINDOW_SIZE = 50


@dataclass(frozen=True)
class KafkaTopics:
    raw: str = "raw_h2_lab_data"
    validated: str = "validated_h2_lab_data"
    quarantine: str = "quarantine_h2_lab_data"
    tsdb_ready: str = "tsdb_ready_h2_lab_data"
    alerts: str = "alerts_h2_lab_data"


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str = "localhost:9092"
    questdb_host: str = "localhost"
    questdb_http_port: int = 9000


TOPICS = KafkaTopics()
SETTINGS = Settings()


class DataQualityConsumer:

    def __init__(self):

        self.checker = DataQualityChecker()

        self.consumer = KafkaConsumer(
            TOPICS.raw,
            bootstrap_servers=[SETTINGS.kafka_bootstrap],
            group_id="h2smartlab-quality-validator",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            key_deserializer=lambda x: x.decode("utf-8") if x else None,
        )

        self.producer = KafkaProducer(
            bootstrap_servers=[SETTINGS.kafka_bootstrap],
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
            key_serializer=lambda x: x.encode("utf-8") if x else None,
        )

        self.stream_window: List[Dict] = []

        logger.info("data_quality_consumer_initialized")


    def process_record(self, key: str, record: Dict):

        is_valid, errors = self.checker.validate_single_record(record)

        if is_valid:

            record["quality"] = {
                "valid": True,
                "validation_status": "validated"
            }

            self.producer.send(
                TOPICS.validated,
                key=key,
                value=record
            )

            logger.info(
                "record_validated",
                sensor=record.get("sensor_id"),
                topic=TOPICS.validated
            )

        else:

            record["quality"] = {
                "valid": False,
                "validation_status": "rejected",
                "errors": errors
            }

            self.producer.send(
                TOPICS.quarantine,
                key=key,
                value=record
            )

            logger.warning(
                "record_rejected",
                errors=errors,
                topic=TOPICS.quarantine
            )


    def update_stream_metrics(self):

        if len(self.stream_window) < 5:
            return

        metrics = self.checker.compute_stream_quality(self.stream_window)

        logger.info(
            "stream_quality_metrics",
            accuracy=f"{metrics['accuracy']:.2f}",
            completeness=f"{metrics['completeness']:.2f}",
            timeliness=f"{metrics['timeliness']:.2f}",
            wqs=f"{metrics['wqs']:.2f}"
        )


    def run(self):

        logger.info("data_quality_consumer_started", topic=TOPICS.raw)

        for msg in self.consumer:

            record = msg.value
            key = msg.key

            try:

                self.process_record(key, record)

                self.stream_window.append(record)

                if len(self.stream_window) > WINDOW_SIZE:
                    self.stream_window.pop(0)

                self.update_stream_metrics()

                self.producer.flush()

            except Exception as e:

                logger.error(
                    "processing_error",
                    error=str(e),
                    record=record
                )


def main():

    consumer = DataQualityConsumer()

    try:
        consumer.run()

    except KeyboardInterrupt:
        logger.info("consumer_stopped")


if __name__ == "__main__":
    main()