from datetime import datetime
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from api.dependencies import _sync_service, get_current_researcher
from services.sync_service import SyncService

router = APIRouter(prefix="/sync", tags=["Researcher Sync"])


@router.post("/trigger")
def trigger_sync(
    researcher: Dict[str, Any] = Depends(get_current_researcher),
    data_type: Optional[str] = Query(None),
    batch_size: int = Query(100, ge=1, le=1000),
    svc: SyncService = Depends(_sync_service),
):
    data_access = researcher.get("data_access", [])

    # Enforce access
    if data_type and data_type not in data_access and "all" not in data_access:
        raise HTTPException(status_code=403, detail=f"No access to data type: {data_type}")

    batch = svc.fetch_researcher_batch(
        researcher_id=researcher["user_id"],
        data_access=data_access,
        data_type=data_type,
        batch_size=batch_size,
    )
    if "error" in batch:
        raise HTTPException(status_code=500, detail=batch["error"])

    if not batch.get("records"):
        return {
            "status": "success",
            "message": "No data available for your access permissions",
            "researcher": researcher.get("username"),
            "data_type": data_type or "all_authorized",
            "records_synced": 0,
            "timestamp": datetime.now().isoformat(),
        }

    return svc.process_researcher_batch(
        researcher=researcher,
        batch_result=batch,
        batch_size=batch_size,
        data_type=data_type,
    )


@router.get("/status")
def sync_status(
    researcher: Dict[str, Any] = Depends(get_current_researcher),
    data_type: Optional[str] = Query(None),
    svc: SyncService = Depends(_sync_service),
):
    state = svc.get_researcher_state(researcher["user_id"], data_type)
    return {
        "status": "active",
        "researcher": {
            "id": researcher["user_id"],
            "name": researcher.get("username"),
            "institution": researcher.get("institution"),
            "data_access": researcher.get("data_access"),
        },
        "sync_state": state,
        "timestamp": datetime.now().isoformat(),
    }


@router.post("/complete")
def sync_all(
    researcher: Dict[str, Any] = Depends(get_current_researcher),
    data_type: Optional[str] = Query(None),
    batch_size: int = Query(100, ge=1, le=1000),
    svc: SyncService = Depends(_sync_service),
):
    results = []
    total = 0
    while True:
        batch = svc.fetch_researcher_batch(
            researcher_id=researcher["user_id"],
            data_access=researcher.get("data_access", []),
            data_type=data_type,
            batch_size=batch_size,
        )
        if not batch.get("records"):
            break
        result = svc.process_researcher_batch(
            researcher=researcher,
            batch_result=batch,
            batch_size=batch_size,
            data_type=data_type,
        )
        results.append(result)
        total += result.get("batch_info", {}).get("records_in_batch", 0)
        if not result.get("progress", {}).get("has_more", False):
            break

    return {
        "status": "success",
        "batches": len(results),
        "total_records_synced": total,
        "timestamp": datetime.now().isoformat(),
    }
