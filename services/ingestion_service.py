"""
Orchestrates: Kafka consume → auth check → quality validate → QuestDB insert.
Failed records (auth or validation) are routed to a dead-letter table instead
of being silently discarded.
"""
import json
import math
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import structlog

from domain import auth as domain_auth
from domain import quality as dq
from infrastructure.kafka.consumer import KafkaConsumerClient
from infrastructure.metrics import INGEST_MESSAGES, INGEST_QUALITY_YIELD
from infrastructure.questdb.client import QuestDBClient

logger = structlog.get_logger(__name__)

_STATS_EVERY_N = 100
_STATS_EVERY_SECS = 30

_DEAD_LETTER_SCHEMA = {
    "tags": ["failure_category", "failure_reason", "device_id", "raw_payload"],
    "fields": [],
    "tag_types": {
        "failure_category": "STRING",
        "failure_reason": "STRING",
        "device_id": "STRING",
        "raw_payload": "STRING",
    },
    "field_types": {},
}


class IngestionStats:
    def __init__(self) -> None:
        self.total_received = 0
        self.auth_failed = 0
        self.processed = 0
        self.valid = 0
        self.invalid = 0

    def log(self) -> None:
        total = self.total_received or 1
        yield_ratio = self.valid / total
        INGEST_QUALITY_YIELD.set(yield_ratio)
        logger.info(
            "ingestion_stats",
            total_received=self.total_received,
            auth_failed=self.auth_failed,
            valid=self.valid,
            invalid=self.invalid,
            yield_pct=round(yield_ratio * 100, 1),
        )


class IngestionService:
    def __init__(
        self,
        kafka: KafkaConsumerClient,
        questdb: QuestDBClient,
        table_name: str,
        schema: Dict[str, Any],
        validated_table: Optional[str] = None,
        jwt_secret: str = "",
        jwt_algorithm: str = "HS256",
    ) -> None:
        self._kafka = kafka
        self._db = questdb
        self._table = table_name
        self._dead_letter_table = f"{table_name}_dead_letter"
        self._validated_table = validated_table
        self._schema = schema
        self._schema_fields: List[str] = schema.get("fields", [])
        self._schema_tags: List[str] = schema.get("tags", [])
        self._stats = IngestionStats()
        self._field_history: Dict[str, List[float]] = {}
        self._wqs_history: List[float] = []
        self._processed = 0
        self._last_good_values: Dict[str, float] = {}
        self._jwt_secret = jwt_secret
        self._jwt_algorithm = jwt_algorithm

    def setup(self) -> None:
        self._db.create_table(self._table, self._schema)
        self._db.create_table(self._dead_letter_table, _DEAD_LETTER_SCHEMA)
        logger.info("dead_letter_table_ready", table=self._dead_letter_table)
        if self._validated_table:
            self._db.create_table(self._validated_table, self._schema)
            logger.info("validated_table_ready", table=self._validated_table)

    def run(self) -> None:
        logger.info("ingestion_start", broker=self._kafka.broker, topic=self._kafka.topic)
        self._kafka.connect()
        last_stats_time = time.time()
        try:
            while True:
                for record in self._kafka.poll_messages():
                    self._stats.total_received += 1
                    self._process(record)
                    self._processed += 1

                    if self._processed % _STATS_EVERY_N == 0:
                        self._stats.log()

                    now = time.time()
                    if now - last_stats_time >= _STATS_EVERY_SECS:
                        self._stats.log()
                        last_stats_time = now

        except KeyboardInterrupt:
            logger.info("ingestion_stopped_by_user")
        finally:
            self._kafka.close()
            self._stats.log()

    def _process(self, data: Dict[str, Any]) -> None:
        INGEST_MESSAGES.labels(result="received").inc()
        auth_ok, auth_reason = self._check_auth(data)
        if not auth_ok:
            self._stats.auth_failed += 1
            INGEST_MESSAGES.labels(result="auth_failed").inc()
            logger.warning("auth_failed", reason=auth_reason)
            self._write_dead_letter(data, "auth", auth_reason)
            return

        # Always store the raw record unchanged
        raw_row = self._build_row(data)
        if not self._db.insert_row(self._table, raw_row):
            logger.error("raw_insert_failed", row_keys=list(raw_row.keys()))

        dims = dq.evaluate_dimensions(
            data, self._field_history, self._wqs_history, self._processed
        )
        logger.debug("quality_dimensions", **{k: round(v, 3) for k, v in dims.items()})
        self._stats.processed += 1

        if self._validated_table:
            # Replace NaN/Inf with last known good values, then re-validate
            cleaned, imputed = dq.clean_record(data, self._last_good_values)
            if imputed:
                logger.debug("imputed_fields", fields=imputed)
            self._update_last_good(data)

            result = dq.validate_record(cleaned)
            if result.is_valid:
                self._stats.valid += 1
                INGEST_MESSAGES.labels(result="valid").inc()
                clean_row = self._build_row(cleaned)
                if not self._db.insert_row(self._validated_table, clean_row):
                    logger.error("validated_insert_failed", row_keys=list(clean_row.keys()))
            else:
                self._stats.invalid += 1
                INGEST_MESSAGES.labels(result="invalid").inc()
                reason = "; ".join(result.errors)
                logger.warning("dropped_after_clean", errors=result.errors)
                self._write_dead_letter(data, "validation", reason)
        else:
            result = dq.validate_record(data)
            if result.is_valid:
                self._stats.valid += 1
                INGEST_MESSAGES.labels(result="valid").inc()
            else:
                self._stats.invalid += 1
                INGEST_MESSAGES.labels(result="invalid").inc()
                reason = "; ".join(result.errors)
                logger.warning("validation_failed", errors=result.errors)
                self._write_dead_letter(data, "validation", reason)

    def _write_dead_letter(
        self, data: Dict[str, Any], category: str, reason: str
    ) -> None:
        # Strip JWT token before storing — never persist credentials
        sanitized = {k: v for k, v in data.items() if k != "auth"}
        auth = data.get("auth", {})
        if isinstance(auth, dict):
            sanitized["auth"] = {k: v for k, v in auth.items() if k != "token"}

        try:
            raw_payload = json.dumps(sanitized, default=str)
        except Exception:
            raw_payload = "{}"

        device_id = ""
        if isinstance(auth, dict):
            device_id = str(auth.get("device_id", ""))

        row = {
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z",
            "failure_category": category,
            "failure_reason": reason[:500],  # guard against runaway error strings
            "device_id": device_id,
            "raw_payload": raw_payload[:4000],  # QuestDB STRING practical limit
        }
        if not self._db.insert_row(self._dead_letter_table, row):
            logger.error("dead_letter_insert_failed", category=category, reason=reason)

    def _update_last_good(self, data: Dict[str, Any]) -> None:
        for key, value in data.items():
            if key == "timestamp" or not isinstance(value, float):
                continue
            if not math.isnan(value) and math.isfinite(value):
                self._last_good_values[key] = value

    def _check_auth(self, data: Dict[str, Any]) -> Tuple[bool, str]:
        auth = data.get("auth", {})
        if not isinstance(auth, dict):
            return False, "missing_auth_section"
        token = auth.get("token")
        if not token:
            return False, "missing_token"
        result = domain_auth.verify_token(token, self._jwt_secret, self._jwt_algorithm)
        if not result["valid"]:
            return False, result.get("reason", "invalid_token")
        payload = result["payload"]
        if "ingest_data" not in payload.get("permissions", []):
            return False, "insufficient_permissions"
        # Overwrite auth block with verified JWT claims (tamper-proof)
        auth["device_id"] = payload.get("device_id", "")
        auth["lab"] = payload.get("lab", "")
        return True, "ok"

    def _build_row(self, data: Dict[str, Any]) -> Dict[str, Any]:
        row = {k: data[k] for k in self._schema_fields if k in data}
        row["timestamp"] = data.get("timestamp")
        auth = data.get("auth", {})
        if isinstance(auth, dict):
            for tag in self._schema_tags:
                if tag in auth:
                    row[tag] = auth[tag]
        return row
