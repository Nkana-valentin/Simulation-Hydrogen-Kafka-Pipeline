"""Producer entry point — delegates to infrastructure and simulator."""
import logging
import sys
import time

from config.settings import get_settings
from infrastructure.kafka.producer import KafkaProducerClient
from simulator.physics_model import generate_physical_state, initial_state

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    settings = get_settings()
    client = KafkaProducerClient(broker=settings.kafka_broker, topic=settings.kafka_topic_raw)

    logger.info("Waiting 5 s for services to initialise...")
    time.sleep(5)

    client.connect()
    logger.info("Producer ready — streaming to topic '%s'", settings.kafka_topic_raw)

    state = initial_state()
    count = 0
    try:
        while True:
            state = generate_physical_state(state)
            state["auth"] = {
                "authenticated": True,
                "device_id": "simulation_device_01",
                "lab": "APSU",
                "device_type": "simulator",
                "auth_method": "simulation",
            }
            client.publish(state)
            count += 1
            if count % 5 == 0:
                logger.info("Sent %d messages", count)
            time.sleep(0.5)
    except KeyboardInterrupt:
        logger.info("Stopped by user — %d messages sent", count)
    finally:
        client.flush_and_close()


if __name__ == "__main__":
    main()
