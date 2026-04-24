import json
import time
from typing import Any, Dict, Optional

import structlog
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic

logger = structlog.get_logger(__name__)


class KafkaProducerClient:
    def __init__(self, broker: str, topic: str) -> None:
        self.broker = broker
        self.topic = topic
        self._producer: Optional[KafkaProducer] = None

    def connect(self, max_retries: int = 15, retry_delay: int = 3) -> None:
        self._ensure_topic()
        for attempt in range(max_retries):
            try:
                logger.info("kafka_connecting", broker=self.broker, attempt=attempt + 1, max_retries=max_retries)
                self._producer = KafkaProducer(
                    bootstrap_servers=[self.broker],
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    retries=5,
                    request_timeout_ms=30_000,
                    metadata_max_age_ms=30_000,
                    max_block_ms=60_000,
                )
                time.sleep(2)
                partitions = self._producer.partitions_for(self.topic) or set()
                logger.info("kafka_connected", topic=self.topic, partitions=len(partitions))
                return
            except Exception as exc:
                logger.warning("kafka_connect_error", error=str(exc))
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
        raise RuntimeError(f"Could not connect to Kafka at {self.broker}")

    def publish(self, message: Dict[str, Any]) -> None:
        if not self._producer:
            raise RuntimeError("Producer not connected — call connect() first")
        future = self._producer.send(self.topic, value=message)
        future.get(timeout=30)

    def flush_and_close(self) -> None:
        if self._producer:
            self._producer.flush(timeout=10)
            self._producer.close()
            self._producer = None

    def _ensure_topic(self) -> None:
        try:
            admin = KafkaAdminClient(bootstrap_servers=[self.broker], request_timeout_ms=30_000)
            existing = admin.list_topics()
            if self.topic not in existing:
                admin.create_topics([NewTopic(name=self.topic, num_partitions=3, replication_factor=1)])
                logger.info("kafka_topic_created", topic=self.topic)
            admin.close()
        except Exception as exc:
            logger.warning("kafka_topic_check_failed", error=str(exc))
