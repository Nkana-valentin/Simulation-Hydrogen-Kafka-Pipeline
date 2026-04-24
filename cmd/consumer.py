"""Consumer entry point — delegates to IngestionService."""
import logging
import time

from config.logging import configure_logging
from config.settings import get_settings
from infrastructure.kafka.consumer import KafkaConsumerClient
from infrastructure.questdb.client import QuestDBClient
from infrastructure.questdb.schema import load_schema
from services.ingestion_service import IngestionService

configure_logging()
logger = logging.getLogger(__name__)


def main() -> None:
    settings = get_settings()
    schema = load_schema(settings.tsdb_config_path)

    logger.info("Waiting 15 s for Kafka to be ready...")
    time.sleep(15)

    kafka = KafkaConsumerClient(broker=settings.kafka_broker, topic=settings.kafka_topic_raw)
    db = QuestDBClient(host=settings.questdb_host, port=settings.questdb_port)

    svc = IngestionService(
        kafka=kafka,
        questdb=db,
        table_name=settings.kafka_topic_raw,
        validated_table=settings.kafka_topic_validated,
        schema=schema,
        jwt_secret=settings.jwt_secret,
        jwt_algorithm=settings.jwt_algorithm,
    )
    svc.setup()
    svc.run()


if __name__ == "__main__":
    main()
