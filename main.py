#main.py
"""
Authentication Service for Hydrogen Research Pipeline
JWT Token Issuance and Verification
"""
from fastapi import FastAPI
from fastapi.security import HTTPBearer
from synchronization import hydor_auto_sync_worker
from routers import authentication, hydor_auto_sync_api, researcher_sync_app

from contextlib import asynccontextmanager
import asyncio
import logging
from synchronization.sync_helpers import SyncHelpers

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
    sync_task = asyncio.create_task(hydor_auto_sync_worker.sync_worker())
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
    description="H2 laboratory data synchronization",
    version="1.0.0",
    lifespan=lifespan
)

app.include_router(authentication.router)
app.include_router(researcher_sync_app.router)
#app.include_router(sissa_auto_sync_app.router)
app.include_router(hydor_auto_sync_api.router)



# # In your lifespan function or startup
# @asynccontextmanager
# async def lifespan(app: FastAPI):
#     # Startup
#     logger.info("Starting SISSA Auto Sync App")
    
#     # Initialize table manager
#     table_manager = TableManager(sync_helpers.QUESTDB_QUERY_URL)
    
#     # Define required tables
#     required_tables = {
#         "sensor_readings": {
#             "timestamp": "TIMESTAMP",
#             "sensor_id": "SYMBOL",
#             "value": "DOUBLE",
#             "unit": "SYMBOL",
#             "location": "SYMBOL",
#             "quality": "INT"
#         }
#     }
    
#     # Initialize tables
#     results = table_manager.initialize_required_tables(required_tables)
    
#     if all(results.values()):
#         logger.info("✅ All required tables are ready")
#     else:
#         logger.error("❌ Some tables failed to initialize")
    
#     # Start background worker
#     task = asyncio.create_task(sync_worker())
    
#     # ... rest of lifespan ...


