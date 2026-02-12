#!/usr/bin/env python3
"""
sissa_sync.py - Batch sync service with researcher authentication
- Syncs data in configurable batches (default: 100 records per batch)
- Tracks offset for each researcher independently
- Enforces data_access permissions per researcher
- Only researchers can access their authorized data types
"""
from fastapi import FastAPI, HTTPException, Depends, Header, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import requests
import yaml
import asyncio
from datetime import datetime, timedelta
import json
import csv
import os
from pathlib import Path
from typing import List, Dict, Any, Optional
import logging
import jwt
import math

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ========================
# Configuration
# ========================
AUTH_SERVICE_URL = "http://localhost:8001"
JWT_SECRET = "hydrogen-research-secret-key-2024"
JWT_ALGORITHM = "HS256"

# Load configuration
with open("TSDB.yml") as f:
    config = yaml.safe_load(f)

QUESTDB_QUERY_URL = f"http://{config['questdb']['host']}:{config['questdb']['port']}/exec"
SYNC_DATA_DIR = Path("./synced_data")
SYNC_DATA_DIR.mkdir(exist_ok=True)

# Sync state files - one per researcher
SYNC_STATE_DIR = SYNC_DATA_DIR / "sync_state"
SYNC_STATE_DIR.mkdir(exist_ok=True)

# ========================
# FastAPI App with Security
# ========================
app = FastAPI(title="Hydrogen Data Sync Service")
security = HTTPBearer()

# ========================
# JWT Verification
# ========================
async def verify_researcher_token(token: str) -> Dict[str, Any]:
    """Verify JWT token and ensure it's from a researcher"""
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        
        # Check if this is a researcher token
        if payload.get("identity_type") != "researcher":
            return {"valid": False, "reason": "Not a researcher token"}
        
        return {"valid": True, "payload": payload}
    
    except jwt.ExpiredSignatureError:
        return {"valid": False, "reason": "Token expired"}
    except jwt.InvalidTokenError:
        return {"valid": False, "reason": "Invalid token"}

async def get_current_researcher(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> Dict[str, Any]:
    """Dependency to get authenticated researcher"""
    token = credentials.credentials
    result = await verify_researcher_token(token)
    
    if not result["valid"]:
        raise HTTPException(
            status_code=401,
            detail=result.get("reason", "Authentication failed")
        )
    
    return result["payload"]

# ========================
# Researcher-Specific Sync State Management
# ========================
def get_researcher_state_file(researcher_id: str) -> Path:
    """Get the state file path for a specific researcher"""
    # Sanitize researcher ID for filename
    safe_id = researcher_id.replace("@", "_at_").replace(".", "_dot_")
    return SYNC_STATE_DIR / f"{safe_id}_sync_state.json"

def get_researcher_sync_state(researcher_id: str, data_type: Optional[str] = None) -> Dict[str, Any]:
    """
    Get sync state for a specific researcher
    Tracks offset and timestamp per data type
    """
    state_file = get_researcher_state_file(researcher_id)
    
    if not state_file.exists():
        logger.info(f"ℹ️ No sync state found for researcher {researcher_id}")
        return {
            "researcher_id": researcher_id,
            "data_types": {},
            "total_records_synced": 0,
            "last_sync_time": None,
            "batches_completed": 0
        }
    
    try:
        with open(state_file, 'r') as f:
            state = json.load(f)
            logger.info(f"📅 Researcher {researcher_id} last sync: {state.get('last_sync_time')}")
            
            if data_type:
                # Return state for specific data type
                data_type_state = state.get("data_types", {}).get(data_type, {})
                return {
                    "researcher_id": researcher_id,
                    "data_type": data_type,
                    "last_timestamp": data_type_state.get("last_timestamp"),
                    "last_offset": data_type_state.get("last_offset", 0),
                    "records_synced": data_type_state.get("records_synced", 0),
                    "total_researcher_records": state.get("total_records_synced", 0)
                }
            
            return state
            
    except Exception as e:
        logger.error(f"Error reading sync state for {researcher_id}: {e}")
        return {
            "researcher_id": researcher_id,
            "data_types": {},
            "total_records_synced": 0,
            "last_sync_time": None,
            "batches_completed": 0
        }

def update_researcher_sync_state(
    researcher_id: str,
    researcher_name: str,
    institution: str,
    data_type: str,
    batch_info: Dict[str, Any]
):
    """
    Update sync state for a researcher after a successful batch
    """
    state_file = get_researcher_state_file(researcher_id)
    
    # Load existing state or create new
    if state_file.exists():
        with open(state_file, 'r') as f:
            state = json.load(f)
    else:
        state = {
            "researcher_id": researcher_id,
            "researcher_name": researcher_name,
            "institution": institution,
            "first_sync_time": datetime.now().isoformat(),
            "data_types": {},
            "total_records_synced": 0,
            "batches": []
        }
    
    # Initialize data type state if not exists
    if data_type not in state["data_types"]:
        state["data_types"][data_type] = {
            "first_sync": datetime.now().isoformat(),
            "last_timestamp": None,
            "last_offset": 0,
            "records_synced": 0,
            "batches_completed": 0
        }
    
    # Update data type state
    dt_state = state["data_types"][data_type]
    dt_state["last_timestamp"] = batch_info.get("latest_timestamp")
    dt_state["last_offset"] = batch_info.get("end_offset", dt_state["last_offset"] + batch_info.get("record_count", 0))
    dt_state["records_synced"] += batch_info.get("record_count", 0)
    dt_state["batches_completed"] += 1
    
    # Update overall state
    state["total_records_synced"] += batch_info.get("record_count", 0)
    state["last_sync_time"] = datetime.now().isoformat()
    state["last_batch"] = {
        "timestamp": datetime.now().isoformat(),
        "data_type": data_type,
        "record_count": batch_info.get("record_count", 0),
        "start_offset": batch_info.get("start_offset", 0),
        "end_offset": batch_info.get("end_offset", 0),
        "time_range": batch_info.get("time_range")
    }
    
    # Keep last 10 batches for history
    if "batches" not in state:
        state["batches"] = []
    state["batches"].append(state["last_batch"])
    state["batches"] = state["batches"][-10:]  # Keep only last 10
    
    # Save state
    with open(state_file, 'w') as f:
        json.dump(state, f, indent=2, default=str)
    
    logger.info(f"✅ Updated sync state for {researcher_id} - {data_type}: offset {dt_state['last_offset']}")

# ========================
# QuestDB Helper Functions with Data Access Control
# ========================
def check_table_exists():
    """Check if the table exists"""
    try:
        query = "SELECT * FROM hydrogen_data LIMIT 1"
        response = requests.get(QUESTDB_QUERY_URL, params={"query": query})
        return response.status_code == 200
    except:
        return False

def build_data_access_filter(data_access: List[str]) -> str:
    """
    Build SQL filter for researcher's data access permissions
    """
    if not data_access:
        return "1=0"  # No access
    
    if "all" in data_access:
        return "1=1"  # Access to all data
    
    # Build filter for specific measurement types
    conditions = []
    for access in data_access:
        conditions.append(f"measurement_type = '{access}'")
    
    return "(" + " OR ".join(conditions) + ")"

def query_researcher_data_batch(
    researcher_id: str,
    data_access: List[str],
    data_type: Optional[str] = None,
    batch_size: int = 100,
    offset: int = 0
) -> Dict[str, Any]:
    """
    Query data for a specific researcher with access controls
    FIXED: QuestDB pagination syntax
    """
    if not check_table_exists():
        return {"records": [], "total_count": 0, "has_more": False, "offset": offset}
    
    try:
        # Get researcher's sync state for this data type
        state = get_researcher_sync_state(researcher_id, data_type)
        current_offset = state.get("last_offset", offset)
        
        # Build access filter
        if data_type:
            if data_type not in data_access and "all" not in data_access:
                logger.warning(f"⚠️ Researcher {researcher_id} does not have access to {data_type}")
                return {"records": [], "total_count": 0, "has_more": False, "offset": current_offset}
            access_filter = f"measurement_type = '{data_type}'"
        else:
            if "all" in data_access:
                access_filter = "1=1"
            else:
                conditions = [f"measurement_type = '{access}'" for access in data_access]
                access_filter = "(" + " OR ".join(conditions) + ")"
        
        # Get total count
        count_query = f"""
        SELECT count(*)
        FROM hydrogen_data
        WHERE {access_filter}
        """
        
        logger.info(f"📊 Count query: {count_query}")
        count_response = requests.get(QUESTDB_QUERY_URL, params={"query": count_query})
        
        total_count = 0
        if count_response.status_code == 200:
            count_result = count_response.json()
            if "dataset" in count_result and count_result["dataset"]:
                total_count = count_result["dataset"][0][0]
        
        logger.info(f"📊 Total accessible records for {researcher_id}: {total_count}")
        
        # ============= FIXED: QuestDB pagination syntax =============
        # OPTION 1: LIMIT {batch_size} OFFSET {offset} - Try this first
        query = f"""
        SELECT 
            timestamp,
            lab,
            sensor_id,
            measurement_type,
            unit,
            value
        FROM hydrogen_data
        WHERE {access_filter}
        ORDER BY timestamp ASC, sensor_id ASC
        LIMIT {batch_size} OFFSET {current_offset}
        """
        
        logger.info(f"🔍 Query: {query}")
        
        response = requests.get(QUESTDB_QUERY_URL, params={"query": query})
        
        # If OPTION 1 fails, try OPTION 2: LIMIT {offset},{batch_size}
        if response.status_code != 200:
            logger.warning("⚠️ Option 1 failed, trying LIMIT {offset},{batch_size} syntax")
            query = f"""
            SELECT 
                timestamp,
                lab,
                sensor_id,
                measurement_type,
                unit,
                value
            FROM hydrogen_data
            WHERE {access_filter}
            ORDER BY timestamp ASC, sensor_id ASC
            LIMIT {current_offset},{batch_size}
            """
            
            logger.info(f"🔍 Query (alternative): {query}")
            response = requests.get(QUESTDB_QUERY_URL, params={"query": query})
        # ===========================================================
        
        if response.status_code != 200:
            logger.error(f"❌ Query failed: {response.status_code} - {response.text}")
            return {"records": [], "total_count": total_count, "has_more": False, "offset": current_offset, "error": response.text}
        
        result = response.json()
        logger.info(f"📥 Query successful")
        
        records = []
        
        if "dataset" in result and isinstance(result["dataset"], list):
            for row in result["dataset"]:
                if len(row) >= 6:
                    record = {
                        "timestamp": row[0],
                        "lab": row[1],
                        "sensor_id": row[2],
                        "measurement_type": row[3],
                        "unit": row[4],
                        "value": row[5]
                    }
                    records.append(record)
        
        logger.info(f"📦 Retrieved {len(records)} records for {researcher_id}")
        
        # Determine if there are more records
        has_more = (current_offset + len(records)) < total_count
        next_offset = current_offset + len(records) if has_more else None
        
        # Get latest timestamp in this batch
        latest_timestamp = records[-1].get('timestamp') if records else None
        
        return {
            "records": records,
            "total_count": total_count,
            "has_more": has_more,
            "current_offset": current_offset,
            "next_offset": next_offset,
            "record_count": len(records),
            "latest_timestamp": latest_timestamp,
            "access_filter": access_filter,
            "data_type": data_type or "all_authorized"
        }
        
    except Exception as e:
        logger.error(f"❌ Error querying data: {e}", exc_info=True)
        return {
            "records": [],
            "total_count": 0,
            "has_more": False,
            "offset": offset,
            "error": str(e)
        }

# ========================
# File Export Functions
# ========================
def save_batch_to_filesystem(
    records: List[Dict],
    researcher: Dict,
    batch_info: Dict[str, Any]
) -> Dict:
    """Save a batch of synced data to local filesystem"""
    if not records:
        return {"status": "no_data", "record_count": 0}
    
    researcher_id = researcher.get("user_id")
    researcher_name = researcher.get("username")
    institution = researcher.get("institution")
    data_type = batch_info.get("data_type", "unknown")
    
    # Create researcher-specific directory structure
    researcher_dir = SYNC_DATA_DIR / f"{institution}_{researcher_name}"
    researcher_dir.mkdir(exist_ok=True)
    
    # Create data type subdirectory
    data_type_dir = researcher_dir / data_type
    data_type_dir.mkdir(exist_ok=True)
    
    # Batch filename with offset range
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    start_offset = batch_info.get("start_offset", 0)
    end_offset = batch_info.get("end_offset", 0)
    batch_file = data_type_dir / f"batch_{timestamp}_offsets_{start_offset}_{end_offset}.json"
    
    # Save batch data
    batch_data = {
        "batch_info": {
            "sync_timestamp": datetime.now().isoformat(),
            "researcher": {
                "id": researcher_id,
                "name": researcher_name,
                "institution": institution,
                "roles": researcher.get("roles", [])
            },
            "data_type": data_type,
            "offset_range": {
                "start": start_offset,
                "end": end_offset
            },
            "record_count": len(records),
            "time_range": {
                "start": records[0].get('timestamp') if records else None,
                "end": records[-1].get('timestamp') if records else None
            },
            "batch_number": batch_info.get("batch_number", 0),
            "total_batches": batch_info.get("total_batches", 0)
        },
        "records": records
    }
    
    with open(batch_file, 'w') as f:
        json.dump(batch_data, f, indent=2, default=str)
    
    # Also save as CSV for easy analysis
    if records:
        csv_file = batch_file.with_suffix('.csv')
        with open(csv_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=records[0].keys())
            writer.writeheader()
            writer.writerows(records)
    
    # Update researcher's manifest
    manifest_file = researcher_dir / "manifest.json"
    manifest = {
        "researcher": {
            "id": researcher_id,
            "name": researcher_name,
            "institution": institution
        },
        "data_types": {},
        "total_batches": 0,
        "total_records": 0,
        "last_update": datetime.now().isoformat()
    }
    
    if manifest_file.exists():
        with open(manifest_file, 'r') as f:
            manifest = json.load(f)
    
    # Update data type stats
    if data_type not in manifest["data_types"]:
        manifest["data_types"][data_type] = {
            "batches": 0,
            "records": 0,
            "first_sync": datetime.now().isoformat()
        }
    
    manifest["data_types"][data_type]["batches"] += 1
    manifest["data_types"][data_type]["records"] += len(records)
    manifest["data_types"][data_type]["last_sync"] = datetime.now().isoformat()
    manifest["total_batches"] += 1
    manifest["total_records"] += len(records)
    manifest["last_update"] = datetime.now().isoformat()
    
    with open(manifest_file, 'w') as f:
        json.dump(manifest, f, indent=2, default=str)
    
    logger.info(f"✅ Saved batch {start_offset}-{end_offset} ({len(records)} records) for {researcher_name}")
    
    return {
        "status": "success",
        "file": str(batch_file),
        "record_count": len(records),
        "offset_range": f"{start_offset}-{end_offset}"
    }

# ========================
# API Endpoints - Batch Sync with Researcher Access Control
# ========================
@app.post("/api/sync/trigger")
async def trigger_sync(
    researcher: Dict = Depends(get_current_researcher),
    data_type: Optional[str] = Query(None, description="Specific data type to sync (optional)"),
    batch_size: int = Query(100, description="Number of records per batch", ge=1, le=1000)
):
    """
    Trigger a batch sync job
    - Respects researcher's data_access permissions
    - Syncs in batches (default: 100 records)
    - Tracks offset per researcher per data type
    - Only syncs authorized data types
    """
    researcher_id = researcher.get("user_id")
    researcher_name = researcher.get("username")
    data_access = researcher.get("data_access", [])
    
    logger.info(f"🔐 Batch sync triggered by {researcher_name} ({researcher_id})")
    logger.info(f"📋 Data access: {data_access}")
    logger.info(f"📦 Batch size: {batch_size}")
    
    if data_type:
        logger.info(f"🎯 Specific data type requested: {data_type}")
    
    # Get researcher's current sync state
    state = get_researcher_sync_state(researcher_id, data_type)
    current_offset = state.get("last_offset", 0) if isinstance(state, dict) else 0
    
    # Query a batch of data with access controls
    batch_result = query_researcher_data_batch(
        researcher_id=researcher_id,
        data_access=data_access,
        data_type=data_type,
        batch_size=batch_size,
        offset=current_offset
    )
    
    if "error" in batch_result:
        raise HTTPException(status_code=500, detail=batch_result["error"])
    
    records = batch_result.get("records", [])
    total_count = batch_result.get("total_count", 0)
    has_more = batch_result.get("has_more", False)
    current_offset = batch_result.get("current_offset", 0)
    next_offset = batch_result.get("next_offset")
    
    if not records:
        return {
            "status": "success",
            "message": "No data available for your access permissions",
            "researcher": researcher_name,
            "institution": researcher.get("institution"),
            "data_access": data_access,
            "data_type": data_type or "all_authorized",
            "total_available_records": total_count,
            "records_synced_this_batch": 0,
            "progress": {
                "percent_complete": 100 if total_count == 0 else round((current_offset / total_count) * 100, 2),
                "current_offset": current_offset,
                "total_records": total_count,
                "has_more": False
            },
            "timestamp": datetime.now().isoformat()
        }
    
    # Calculate batch number
    batch_number = (current_offset // batch_size) + 1
    total_batches = math.ceil(total_count / batch_size) if total_count > 0 else 0
    
    # Prepare batch info
    batch_info = {
        "data_type": batch_result.get("data_type", data_type or "all_authorized"),
        "start_offset": current_offset,
        "end_offset": current_offset + len(records),
        "record_count": len(records),
        "latest_timestamp": batch_result.get("latest_timestamp"),
        "time_range": {
            "start": records[0].get('timestamp'),
            "end": records[-1].get('timestamp')
        },
        "batch_number": batch_number,
        "total_batches": total_batches
    }
    
    # Save batch to filesystem
    save_result = save_batch_to_filesystem(records, researcher, batch_info)
    
    # Update researcher's sync state
    update_researcher_sync_state(
        researcher_id=researcher_id,
        researcher_name=researcher_name,
        institution=researcher.get("institution"),
        data_type=batch_info["data_type"],
        batch_info=batch_info
    )
    
    # Calculate progress
    percent_complete = round((next_offset / total_count) * 100, 2) if next_offset and total_count > 0 else 100
    
    response = {
        "status": "success",
        "message": f"Batch {batch_number} synced successfully",
        "researcher": {
            "id": researcher_id,
            "name": researcher_name,
            "institution": researcher.get("institution"),
            "roles": researcher.get("roles")
        },
        "data_type": batch_info["data_type"],
        "batch_info": {
            "batch_number": batch_number,
            "total_batches": total_batches,
            "records_in_batch": len(records),
            "offset_range": f"{current_offset} - {current_offset + len(records)}",
            "time_range": batch_info["time_range"]
        },
        "progress": {
            "percent_complete": percent_complete,
            "records_synced": next_offset,
            "total_available_records": total_count,
            "remaining_records": total_count - next_offset if next_offset else 0,
            "has_more": has_more
        },
        "save_location": save_result.get("file"),
        "timestamp": datetime.now().isoformat()
    }
    
    # If there are more records, provide next batch info
    if has_more and next_offset:
        response["next_batch"] = {
            "message": "Use the same endpoint to sync next batch",
            "next_offset": next_offset,
            "estimated_records": min(batch_size, total_count - next_offset)
        }
    
    return response

@app.get("/api/sync/status")
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

@app.post("/api/sync/complete")
async def sync_all_batches(
    researcher: Dict = Depends(get_current_researcher),
    data_type: Optional[str] = None,
    batch_size: int = 100
):
    """
    Sync ALL available data for researcher
    Runs multiple batches until all data is synced
    """
    researcher_id = researcher.get("user_id")
    researcher_name = researcher.get("username")
    data_access = researcher.get("data_access", [])
    
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

@app.get("/api/sync/researcher/{researcher_id}/progress")
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
@app.get("/api/debug/token-info")
async def debug_token_info(
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Debug endpoint to check token info"""
    token = credentials.credentials
    result = await verify_researcher_token(token)
    
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

@app.get("/api/debug/researcher-state/{researcher_id}")
async def debug_researcher_state(
    researcher_id: str,
    current_researcher: Dict = Depends(get_current_researcher)
):
    """Debug endpoint to view researcher sync state (admin only)"""
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

if __name__ == "__main__":
    import uvicorn
    
    print("\n" + "=" * 70)
    print("🔐 HYDROGEN DATA SYNC SERVICE - BATCH PROCESSING WITH ACCESS CONTROL")
    print("=" * 70)
    print(f"📁 Sync directory: {SYNC_DATA_DIR.absolute()}")
    print(f"📊 Sync state directory: {SYNC_STATE_DIR.absolute()}")
    print(f"🔗 QuestDB URL: {QUESTDB_QUERY_URL}")
    print(f"🔐 Auth Service: {AUTH_SERVICE_URL}")
    print("\n📦 Features:")
    print("   ✅ Batch processing (default: 100 records/batch)")
    print("   ✅ Per-researcher sync state tracking")
    print("   ✅ Data access permissions enforced")
    print("   ✅ Offset-based pagination")
    print("   ✅ Resume from last offset")
    print("=" * 70 + "\n")
    
    uvicorn.run(app, host="0.0.0.0", port=8080, log_level="info")