"""
Background asyncio worker — scheduling only, no business logic.
All data decisions delegate to SyncService and SSHTransferClient.
"""
import asyncio
import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

from config.settings import get_settings
from infrastructure.questdb.client import QuestDBClient
from infrastructure.questdb.schema import load_schema
from infrastructure.ssh.transfer import SSHTransferClient
from services.sync_service import SyncService

logger = logging.getLogger(__name__)

# Module-level constants resolved at import time so the router can reference them.
BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "50"))

_svc: Optional[SyncService] = None


def _get_service() -> SyncService:
    global _svc
    if _svc is None:
        settings = get_settings()
        schema = load_schema(settings.tsdb_config_path)
        db = QuestDBClient(host=settings.questdb_host, port=settings.questdb_port)
        _svc = SyncService(
            questdb=db,
            table_name=settings.kafka_topic_raw,
            sync_dir=Path(settings.local_sync_dir),
            schema_columns=schema["fields"],
        )
    return _svc


async def sync_new_data_batch() -> Dict[str, Any]:
    settings = get_settings()
    svc = _get_service()
    return svc.sync_batch(batch_size=settings.batch_size)


async def sync_to_remote() -> Dict[str, Any]:
    settings = get_settings()
    if not settings.remote_sync_enabled:
        return {"status": "skipped", "message": "REMOTE_SYNC_ENABLED=false"}

    svc = _get_service()
    sync_dirs = sorted(Path(settings.local_sync_dir).glob("sync_*"))
    if not sync_dirs:
        return {"status": "skipped", "message": "No sync directories"}

    latest = sync_dirs[-1]
    client = SSHTransferClient(
        host=settings.ssh_host,
        user=settings.ssh_user,
        key_path=settings.ssh_key_path,
        password=settings.ssh_password,
    )
    return client.upload_directory(latest, settings.ssh_remote_path)


async def sync_worker() -> None:
    settings = get_settings()
    interval = settings.sync_interval
    logger.info("Auto-sync worker started (interval=%ds, batch=%d)", interval, settings.batch_size)
    await asyncio.sleep(2)

    while True:
        try:
            logger.debug("Auto-sync check")
            result = await sync_new_data_batch()
            status = result.get("status")

            if status == "success":
                logger.info("Batch synced: %d records", result.get("records_synced", 0))
                if settings.remote_sync_enabled:
                    remote = await sync_to_remote()
                    logger.info("Remote sync: %s", remote.get("status"))
            elif status == "waiting":
                logger.debug("Waiting for batch threshold (%s/%s)", result.get("new_count"), settings.batch_size)
            elif status == "no_new_data":
                logger.debug("No new data")
            else:
                logger.error("Sync error: %s", result)

        except asyncio.CancelledError:
            logger.info("Auto-sync worker cancelled")
            break
        except Exception as exc:
            logger.error("Unexpected error in sync worker: %s", exc)
            await asyncio.sleep(60)
            continue

        await asyncio.sleep(interval)
