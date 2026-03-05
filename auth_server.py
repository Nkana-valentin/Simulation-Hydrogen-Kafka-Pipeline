#!/usr/bin/env python3
"""
Authentication Service for Hydrogen Research Pipeline
JWT Token Issuance and Verification
"""

from fastapi import FastAPI
from fastapi.security import HTTPBearer
from apps import researcher_sync_app, sissa_auto_sync_app, authentication
from contextlib import asynccontextmanager
import asyncio
import logging
from apps.sync_helpers import SyncHelpers

logger = logging.getLogger(__name__)
sync_helpers = SyncHelpers()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Start and stop background sync task with app lifecycle
    """
    global sync_task
    
    logger.info("🚀 Auto-sync app starting up...")
    
    # Load configuration
    BATCH_SIZE = sync_helpers.config.get('sissa', {}).get('batch_size', 100)
    
    # Start background sync worker
    sync_task = asyncio.create_task(sissa_auto_sync_app.sync_worker())
    logger.info(f"🔄 Background sync worker started (batch size: {BATCH_SIZE})")
    
    yield
    
    # Cleanup on shutdown
    if sync_task:
        sync_task.cancel()
        try:
            await sync_task
            logger.info("🛑 Background sync worker stopped")
        except asyncio.CancelledError:
            logger.info("Background sync worker cancelled")


# ===================================
# Main FastAPI App (Authentication)
# ===================================
app = FastAPI(
    title="ORFEO-Hydor Authentication API",
    description="Authentication service for H2 laboratory data synchronization",
    version="1.0.0",
    lifespan=lifespan
)


app.include_router(researcher_sync_app.router)
app.include_router(sissa_auto_sync_app.router)
app.include_router(authentication.router)

