# apps/researcher_sync_app.py
"""
Researcher Sync API - For researchers to manually sync their authorized data
"""
from fastapi import FastAPI, APIRouter, HTTPException, Depends, Query
from datetime import datetime
from typing import Optional, Dict, List, Any
import logging

# Import your existing sync_utils and helpers
from .sync_helpers import SyncHelpers
from auth_service.auth_token import verify_researcher_token
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

# Setup logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize helpers
sync_helpers = SyncHelpers()
security = HTTPBearer()


# ==============================
# Helper functions for this app
# ===============================
def get_current_researcher(
    credentials: HTTPAuthorizationCredentials = Depends(security)) -> Dict[str, Any]:
    """
    Get current researcher from token
    """
    token = credentials.credentials
    result = verify_researcher_token(token)
    if not result["valid"]:
        raise HTTPException(
            status_code=401,
            detail=result.get("reason", "Authentication failed")
        )
    return result["payload"]


# ================================
# Create the Researcher Sync App
# ================================
researcher_app = FastAPI(
    title="Researcher Data Sync API",
    description="API for researchers to manually sync their authorized H2 laboratory data",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Create router for endpoints
router = APIRouter(tags=['Researcher Sync'])


# ========================
# Researcher Sync Endpoints
# ========================

@router.post("/sync/trigger")
def trigger_sync(
    researcher: Dict = Depends(get_current_researcher),
    data_type: Optional[str] = Query(None, description="Specific data type to sync (optional)"),
    batch_size: int = Query(100, description="Number of records per batch", ge=1, le=1000),
):
    """
    Trigger a batch sync job-
    respects researcher permissions.
    """
    researcher_id = researcher.get("user_id")
    researcher_name = researcher.get("username")
    data_access = researcher.get("data_access", [])

    logger.info(f"🔐 Batch sync triggered by {researcher_name} ({researcher_id})")
    logger.info(f"📋 Data access: {data_access}")
    logger.info(f"📦 Batch size: {batch_size}")
    if data_type:
        logger.info(f"🎯 Specific data type: {data_type}")

    batch_result = sync_helpers.fetch_next_batch(
        researcher_id=researcher_id,
        data_access=data_access,
        data_type=data_type,
        batch_size=batch_size
    )

    if "error" in batch_result:
        raise HTTPException(status_code=500, detail=batch_result["error"])

    records = batch_result.get("records", [])

    if not records:
        return {
            "status": "success",
            "message": "No data available for your access permissions",
            "researcher": researcher_name,
            "institution": researcher.get("institution"),
            "data_access": data_access,
            "data_type": data_type or "all_authorized",
            "total_available_records": batch_result.get("total_count", 0),
            "records_synced_this_batch": 0,
            "progress": {
                "percent_complete": 100,
                "current_offset": batch_result.get("current_offset", 0),
                "total_records": batch_result.get("total_count", 0),
                "has_more": False,
            },
            "timestamp": datetime.now().isoformat(),
        }

    return sync_helpers.process_successful_batch(
        researcher=researcher,
        batch_result=batch_result,
        batch_size=batch_size,
        data_type=data_type
    )


@router.get("/sync/status")
async def get_sync_status(
    researcher: Dict = Depends(get_current_researcher),
    data_type: Optional[str] = None
):
    """
    Get detailed sync status for the authenticated researcher
    """
    
    researcher_id = researcher.get("user_id")
    researcher_name = researcher.get("username")
    data_access = researcher.get("data_access", [])
    
    state = sync_helpers.get_researcher_sync_state(researcher_id, data_type)
    
    # Get total available data counts per data type
    available_counts = {}
    for access in data_access:
        if access == "all":
            for dt in ["pressure", "flow", "temperature", "voltage"]:
                count_result = sync_helpers.query_researcher_data_batch(
                    researcher_id, [dt], data_type=dt, batch_size=1
                )
                available_counts[dt] = count_result.get("total_count", 0)
            break
        else:
            count_result = sync_helpers.query_researcher_data_batch(
                researcher_id, [access], data_type=access, batch_size=1
            )
            available_counts[access] = count_result.get("total_count", 0)
    
    return {
        "status": "active",
        "researcher": {
            "id": researcher_id,
            "name": researcher_name,
            "institution": researcher.get("institution"),
            "roles": researcher.get("roles"),
            "data_access": data_access
        },
        "sync_state": state,
        "available_data": available_counts,
        "sync_directory": str(sync_helpers.SYNC_DATA_DIR.absolute()),
        "timestamp": datetime.now().isoformat()
    }


@router.post("/sync/complete")
async def sync_all_batches(
    researcher: Dict = Depends(get_current_researcher),
    data_type: Optional[str] = None,
    batch_size: int = 100
):
    """
    Sync ALL available data for researcher
    Runs multiple batches until all data is synced
    """
    researcher_name = researcher.get("username")
    
    logger.info(f"🔄 Full sync initiated by {researcher_name}")
    
    results = []
    total_records = 0
    
    while True:
        batch_result = await trigger_sync(
            researcher=researcher,
            data_type=data_type,
            batch_size=batch_size
        )
        
        results.append(batch_result)
        total_records += batch_result.get("batch_info", {}).get("records_in_batch", 0)
        
        if not batch_result.get("progress", {}).get("has_more", False):
            break
    
    return {
        "status": "success",
        "message": f"Full sync completed in {len(results)} batches",
        "researcher": researcher_name,
        "total_records_synced": total_records,
        "batches": results,
        "timestamp": datetime.now().isoformat()
    }


@router.get("/sync/researcher/{researcher_id}/progress")
async def get_researcher_progress(
    researcher_id: str,
    current_researcher: Dict = Depends(get_current_researcher)
):
    """
    Get sync progress for a specific researcher
    Only accessible by the same researcher or admins
    """
    if (researcher_id != current_researcher.get("user_id") and 
        "admin" not in current_researcher.get("roles", [])):
        raise HTTPException(
            status_code=403,
            detail="You can only view your own sync progress"
        )
    
    state = sync_helpers.get_researcher_sync_state(researcher_id)
    
    data_access = current_researcher.get("data_access", [])
    total_available = 0
    
    for dt in data_access:
        if dt == "all":
            pass
        else:
            count_result = sync_helpers.query_researcher_data_batch(
                researcher_id, [dt], data_type=dt, batch_size=1
            )
            total_available += count_result.get("total_count", 0)
    
    return {
        "researcher_id": researcher_id,
        "sync_state": state,
        "progress": {
            "records_synced": state.get("total_records_synced", 0),
            "total_available": total_available,
            "percent_complete": round(
                (state.get("total_records_synced", 0) / total_available * 100), 2
            ) if total_available > 0 else 0
        }
    }


# Include router in the app
researcher_app.include_router(router, prefix="/api")


# ========================
# Debug Endpoints (Admin only)
# ========================
@researcher_app.get("/debug/token-info")
async def debug_token_info(
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Debug endpoint to check token info"""
    token = credentials.credentials
    result = verify_researcher_token(token)
    
    if result["valid"]:
        return {"valid": True, "payload": result["payload"]}
    else:
        return {"valid": False, "reason": result.get("reason")}


@researcher_app.get("/debug/researcher-state/{researcher_id}")
async def debug_researcher_state(
    researcher_id: str,
    current_researcher: Dict = Depends(get_current_researcher)
):
    """Debug endpoint to view researcher sync state (admin only)"""
    if "admin" not in current_researcher.get("roles", []):
        raise HTTPException(status_code=403, detail="Admin access required")
    
    state = sync_helpers.get_researcher_sync_state(researcher_id)
    state_file = sync_helpers.get_researcher_state_file(researcher_id)
    
    return {
        "researcher_id": researcher_id,
        "state_file_exists": state_file.exists(),
        "state_file_path": str(state_file),
        "sync_state": state
    }


# Health check for this app
@researcher_app.get("/health")
async def health_check():
    return {
        "app": "researcher-sync",
        "status": "running",
        "timestamp": datetime.now().isoformat()
    }