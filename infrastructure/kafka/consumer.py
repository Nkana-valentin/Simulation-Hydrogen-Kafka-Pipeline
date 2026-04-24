import json
import time
from typing import Any, Dict, Iterable, Optional

import structlog
from kafka import KafkaConsumer

logger = structlog.get_logger(__name__)


class KafkaConsumerClient:
    def __init__(self, broker: str, topic: str, group_id: str = "questdb_consumer") -> None:
        self.broker = broker
        self.topic = topic
        self.group_id = group_id
        self._consumer: Optional[KafkaConsumer] = None

    def connect(self, max_retries: int = 30, retry_delay: int = 5) -> None:
        for attempt in range(max_retries):
            try:
                logger.info("kafka_connecting", broker=self.broker, attempt=attempt + 1, max_retries=max_retries)
                self._consumer = KafkaConsumer(
                    self.topic,
                    bootstrap_servers=[self.broker],
                    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                    auto_offset_reset="earliest",
                    group_id=self.group_id,
                    enable_auto_commit=True,
                    consumer_timeout_ms=1_000,
                    max_poll_records=100,
                    request_timeout_ms=40_000,
                    session_timeout_ms=30_000,
                    heartbeat_interval_ms=10_000,
                    api_version_auto_timeout_ms=30_000,
                )
                partitions = self._consumer.partitions_for_topic(self.topic) or set()
                logger.info("kafka_connected", topic=self.topic, partitions=len(partitions))
                return
            except Exception as exc:
                logger.warning("kafka_connect_error", error=str(exc))
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
        raise RuntimeError(f"Could not connect to Kafka at {self.broker}")

    def poll_messages(self, timeout_ms: int = 1_000) -> Iterable[Dict[str, Any]]:
        if not self._consumer:
            raise RuntimeError("Consumer not connected — call connect() first")
        batch = self._consumer.poll(timeout_ms=timeout_ms)
        for records in batch.values():
            for msg in records:
                yield msg.value

    def close(self) -> None:
        if self._consumer:
            self._consumer.close()
            self._consumer = None
