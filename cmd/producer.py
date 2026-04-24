"""Producer entry point — authenticates as a device then streams telemetry."""
import logging
import time

import jwt
import requests

from config.logging import configure_logging
from config.settings import get_settings
from infrastructure.kafka.producer import KafkaProducerClient
from simulator.physics_model import generate_physical_state, initial_state

configure_logging()
logger = logging.getLogger(__name__)

_TOKEN_REFRESH_BUFFER_SECS = 300  # refresh 5 min before expiry


def _login(auth_url: str, device_id: str, device_secret: str) -> str:
    resp = requests.post(
        f"{auth_url}/device/login",
        json={"device_id": device_id, "device_secret": device_secret},
        timeout=10,
    )
    resp.raise_for_status()
    token = resp.json()["access_token"]
    logger.info("Device authenticated — token issued for '%s'", device_id)
    return token


def _needs_refresh(token: str) -> bool:
    try:
        payload = jwt.decode(token, options={"verify_signature": False}, algorithms=["HS256"])
        return (payload.get("exp", 0) - time.time()) < _TOKEN_REFRESH_BUFFER_SECS
    except Exception:
        return True


def _get_token(auth_url: str, device_id: str, device_secret: str, retries: int = 10) -> str:
    for attempt in range(1, retries + 1):
        try:
            return _login(auth_url, device_id, device_secret)
        except Exception as exc:
            logger.warning("Login attempt %d/%d failed: %s", attempt, retries, exc)
            time.sleep(5)
    raise RuntimeError(f"Could not authenticate after {retries} attempts")


def main() -> None:
    settings = get_settings()

    if not settings.device_secret:
        raise RuntimeError("DEVICE_SECRET is not set in environment")

    client = KafkaProducerClient(broker=settings.kafka_broker, topic=settings.kafka_topic_raw)

    logger.info("Waiting 10 s for services to initialise...")
    time.sleep(10)

    client.connect()

    token = _get_token(settings.auth_service_url, settings.device_id, settings.device_secret)

    state = initial_state()
    count = 0
    try:
        while True:
            if _needs_refresh(token):
                logger.info("Token near expiry — refreshing...")
                token = _get_token(
                    settings.auth_service_url, settings.device_id, settings.device_secret
                )

            state = generate_physical_state(state)
            state["auth"] = {
                "token": token,
                "device_id": settings.device_id,
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
