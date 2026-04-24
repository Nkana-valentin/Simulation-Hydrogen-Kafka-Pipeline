import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI
from prometheus_client import make_asgi_app

from api.routers import admin_sync, auth, researcher_sync
from config.logging import configure_logging
from workers.auto_sync_worker import sync_worker

configure_logging()
logger = logging.getLogger(__name__)

_sync_task: Optional[asyncio.Task] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _sync_task
    logger.info("Starting up — launching background sync worker")
    _sync_task = asyncio.create_task(sync_worker())
    yield
    if _sync_task:
        _sync_task.cancel()
        try:
            await _sync_task
        except asyncio.CancelledError:
            pass
    logger.info("Shutdown complete")


app = FastAPI(
    title="ORFEO-Hydor Authentication & Sync API",
    description="H2 laboratory data ingestion and synchronization",
    version="2.0.0",
    lifespan=lifespan,
)

app.include_router(auth.router)
app.include_router(researcher_sync.router)
app.include_router(admin_sync.router)

app.mount("/metrics", make_asgi_app())
