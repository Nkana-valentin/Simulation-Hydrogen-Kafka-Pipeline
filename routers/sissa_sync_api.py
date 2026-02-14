from fastapi import APIRouter, HTTPException, Depends, Query
from datetime import datetime
from typing import Optional, Dict, List
import math
from fastapi.security import HTTPAuthorizationCredentials
from auth_service.auth_token import verify_researcher_token

from . sync_utils import (
    get_current_researcher,
    get_researcher_sync_state,
    query_researcher_data_batch,
    save_batch_to_filesystem,
    update_researcher_sync_state,
    get_researcher_state_file,
    logger,
    SYNC_DATA_DIR,
    security
)

router = APIRouter(tags=['SISSA Sync API'])
# ========================
# Helper functions (add these above the router)
# ========================

def _fetch_next_batch(
    researcher_id: str,
    data_access: List[str],
    data_type: Optional[str],
    batch_size: int,) -> dict:
    """Fetch the next batch of data for the researcher (no side effects)."""
    # Always load full state for accurate batch counter later
    full_state = get_researcher_sync_state(researcher_id)  # no data_type → full state

    if data_type:
        dt_state = full_state.get("data_types", {}).get(data_type, {})
        current_offset = dt_state.get("last_offset", 0)
    else:
        current_offset = 0  # when syncing "all" we start from 0 (or you can implement global offset later)

    return query_researcher_data_batch(
        researcher_id=researcher_id,
        data_access=data_access,
        data_type=data_type,
        batch_size=batch_size,
        offset=current_offset,
    )


def _process_successful_batch(
    researcher: dict,
    batch_result: dict,
    batch_size: int,
    data_type: Optional[str],
) -> dict:
    """Save batch, update state, and return final response (only called on success)."""
    researcher_id = researcher["user_id"]
    researcher_name = researcher["username"]
    institution = researcher.get("institution")

    records = batch_result["records"]
    total_count = batch_result.get("total_count", 0)

    # === Proper batch numbering (based on successful batches only) ===
    full_state = get_researcher_sync_state(researcher_id)
    data_type_key = batch_result.get("data_type") or data_type or "all_authorized"
    dt_state = full_state.get("data_types", {}).get(data_type_key, {})
    batch_number = dt_state.get("batches_completed", 0) + 1
    total_batches = math.ceil(total_count / batch_size) if total_count > 0 else 0

    # Prepare batch info for saving
    batch_info = {
        "data_type": data_type_key,
        "start_offset": batch_result.get("current_offset", 0),
        "end_offset": batch_result.get("current_offset", 0) + len(records),
        "record_count": len(records),
        "latest_timestamp": batch_result.get("latest_timestamp"),
        "time_range": {
            "start": records[0].get("timestamp") if records else None,
            "end": records[-1].get("timestamp") if records else None,
        },
        "batch_number": batch_number,
        "total_batches": total_batches,
    }

    # 1. Save to filesystem
    save_result = save_batch_to_filesystem(records, researcher, batch_info)

    # 2. Update persistent state (this increments batches_completed)
    update_researcher_sync_state(
        researcher_id=researcher_id,
        researcher_name=researcher_name,
        institution=institution,
        data_type=data_type_key,
        batch_info=batch_info,
    )

    # 3. Build final response
    next_offset = batch_result.get("next_offset")
    has_more = batch_result.get("has_more", False)

    percent_complete = round((next_offset / total_count) * 100, 2) if next_offset and total_count > 0 else 100

    response = {
        "status": "success",
        "message": f"Batch {batch_number} synced successfully",
        "researcher": {
            "id": researcher_id,
            "name": researcher_name,
            "institution": institution,
            "roles": researcher.get("roles"),
        },
        "data_type": data_type_key,
        "batch_info": {
            "batch_number": batch_number,
            "total_batches": total_batches,
            "records_in_batch": len(records),
            "offset_range": f"{batch_info['start_offset']} - {batch_info['end_offset']}",
            "time_range": batch_info["time_range"],
        },
        "progress": {
            "percent_complete": percent_complete,
            "records_synced": next_offset,
            "total_available_records": total_count,
            "remaining_records": total_count - next_offset if next_offset else 0,
            "has_more": has_more,
        },
        "save_location": save_result.get("file"),
        "timestamp": datetime.now().isoformat(),
    }

    if has_more and next_offset:
        response["next_batch"] = {
            "message": "Call the same endpoint again to get the next batch",
            "next_offset": next_offset,
            "estimated_records": min(batch_size, total_count - next_offset),
        }

    return response

@router.post("/api/sync/trigger")
def trigger_sync(
    researcher: Dict = Depends(get_current_researcher),
    data_type: Optional[str] = Query(None, description="Specific data type to sync (optional)"),
    batch_size: int = Query(100, description="Number of records per batch", ge=1, le=1000),
):
    """Trigger a batch sync job – respects researcher permissions."""
    researcher_id = researcher.get("user_id")
    researcher_name = researcher.get("username")
    data_access = researcher.get("data_access", [])

    logger.info(f"🔐 Batch sync triggered by {researcher_name} ({researcher_id})")
    logger.info(f"📋 Data access: {data_access}")
    logger.info(f"📦 Batch size: {batch_size}")
    if data_type:
        logger.info(f"🎯 Specific data type: {data_type}")

    # === Step 1: Fetch data ===
    batch_result = _fetch_next_batch(
        researcher_id=researcher_id,
        data_access=data_access,
        data_type=data_type,
        batch_size=batch_size,
    )

    if "error" in batch_result:
        raise HTTPException(status_code=500, detail=batch_result["error"])

    records = batch_result.get("records", [])

    # === Step 2: No data case ===
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

    # === Step 3: Process successful batch ===
    return _process_successful_batch(
        researcher=researcher,
        batch_result=batch_result,
        batch_size=batch_size,
        data_type=data_type,
    )

@router.get("/api/sync/status")
async def get_sync_status(
    researcher: Dict = Depends(get_current_researcher),
    data_type: Optional[str] = None
):
    """Get detailed sync status for the authenticated researcher"""
    
    researcher_id = researcher.get("user_id")
    researcher_name = researcher.get("username")
    data_access = researcher.get("data_access", [])
    
    # Get researcher's sync state
    state = get_researcher_sync_state(researcher_id, data_type)
    
    # Get total available data counts per data type
    available_counts = {}
    for access in data_access:
        if access == "all":
            # Get counts for all data types
            for dt in ["pressure", "flow", "temperature", "voltage"]:
                count_result = query_researcher_data_batch(
                    researcher_id, [dt], data_type=dt, batch_size=1
                )
                available_counts[dt] = count_result.get("total_count", 0)
            break
        else:
            count_result = query_researcher_data_batch(
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
        "sync_directory": str(SYNC_DATA_DIR.absolute()),
        "timestamp": datetime.now().isoformat()
    }

@router.post("/api/sync/complete")
async def sync_all_batches(
    researcher: Dict = Depends(get_current_researcher),
    data_type: Optional[str] = None,
    batch_size: int = 100
):
    """
    Sync ALL available data for researcher
    Runs multiple batches until all data is synced
    """
    #researcher_id = researcher.get("user_id")
    researcher_name = researcher.get("username")
    #data_access = researcher.get("data_access", [])
    
    logger.info(f"🔄 Full sync initiated by {researcher_name}")
    
    results = []
    total_records = 0
    batch_number = 1
    
    while True:
        # Trigger one batch
        batch_result = await trigger_sync(
            researcher=researcher,
            data_type=data_type,
            batch_size=batch_size
        )
        
        results.append(batch_result)
        total_records += batch_result.get("batch_info", {}).get("records_in_batch", 0)
        
        # Check if there are more batches
        if not batch_result.get("progress", {}).get("has_more", False):
            break
        
        batch_number += 1
    
    return {
        "status": "success",
        "message": f"Full sync completed in {len(results)} batches",
        "researcher": researcher_name,
        "total_records_synced": total_records,
        "batches": results,
        "timestamp": datetime.now().isoformat()
    }

@router.get("/api/sync/researcher/{researcher_id}/progress")
async def get_researcher_progress(
    researcher_id: str,
    current_researcher: Dict = Depends(get_current_researcher)
):
    """
    Get sync progress for a specific researcher
    Only accessible by the same researcher or admins
    """
    # Authorization check - only self or admin
    if (researcher_id != current_researcher.get("user_id") and 
        "admin" not in current_researcher.get("roles", [])):
        raise HTTPException(
            status_code=403,
            detail="You can only view your own sync progress"
        )
    
    state = get_researcher_sync_state(researcher_id)
    
    # Get total available data
    data_access = current_researcher.get("data_access", [])
    total_available = 0
    
    for dt in data_access:
        if dt == "all":
            # Would need to query total count here
            pass
        else:
            count_result = query_researcher_data_batch(
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

# ========================
# Debug Endpoints
# ========================
@router.get("/api/debug/token-info")
async def debug_token_info(
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """
    Debug endpoint to check token info
    """
    get_token = credentials.credentials
    result = verify_researcher_token(get_token)
    
    if result["valid"]:
        return {
            "valid": True,
            "payload": result["payload"]
        }
    else:
        return {
            "valid": False,
            "reason": result.get("reason")
        }

@router.get("/api/debug/researcher-state/{researcher_id}")
async def debug_researcher_state(
    researcher_id: str,
    current_researcher: Dict = Depends(get_current_researcher)
):
    """
    Debug endpoint to view researcher sync state (admin only)
    """
    if "admin" not in current_researcher.get("roles", []):
        raise HTTPException(status_code=403, detail="Admin access required")
    
    state = get_researcher_sync_state(researcher_id)
    state_file = get_researcher_state_file(researcher_id)
    
    return {
        "researcher_id": researcher_id,
        "state_file_exists": state_file.exists(),
        "state_file_path": str(state_file),
        "sync_state": state
    }
