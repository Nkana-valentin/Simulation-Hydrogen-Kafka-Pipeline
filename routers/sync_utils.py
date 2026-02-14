import logging
import json
import yaml
from pathlib import Path
import requests
from datetime import datetime
import csv
#import math
from typing import Dict, Any, Optional, List
from fastapi import HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from auth_service.auth_token import verify_researcher_token

# ========================
# Logging + Config
# ========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Load QuestDB config
with open("TSDB.yml") as f:
    config = yaml.safe_load(f)

QUESTDB_QUERY_URL = f"http://{config['questdb']['host']}:{config['questdb']['port']}/exec"

BASE_DIR = Path(__file__).resolve().parent.parent   # ← project root

SYNC_DATA_DIR = BASE_DIR / "synced_data"
SYNC_DATA_DIR.mkdir(parents=True, exist_ok=True)     # parents=True = create intermediate folders

SYNC_STATE_DIR = SYNC_DATA_DIR / "sync_state"
SYNC_STATE_DIR.mkdir(parents=True, exist_ok=True)

logger.info(f"✅ Sync directories ready → {SYNC_DATA_DIR.resolve()}")
logger.info(f"   State files → {SYNC_STATE_DIR.resolve()}")

# ========================
# Researcher Sync State
# ========================
def get_researcher_state_file(researcher_id: str) -> Path:
    safe_id = researcher_id.replace("@", "_at_").replace(".", "_dot_")
    return SYNC_STATE_DIR / f"{safe_id}_sync_state.json"


def get_researcher_sync_state(researcher_id: str, 
                            data_type: Optional[str] = None) -> Dict[str, Any]:
    state_file = get_researcher_state_file(researcher_id)
    if not state_file.exists():
        return {
            "researcher_id": researcher_id,
            "data_types": {},
            "total_records_synced": 0,
            "last_sync_time": None,
            "batches_completed": 0
        }

    try:
        with open(state_file) as f:
            state = json.load(f)
        if data_type:
            dt_state = state.get("data_types", {}).get(data_type, {})
            return {
                "researcher_id": researcher_id,
                "data_type": data_type,
                "last_timestamp": dt_state.get("last_timestamp"),
                "last_offset": dt_state.get("last_offset", 0),
                "records_synced": dt_state.get("records_synced", 0),
                "total_researcher_records": state.get("total_records_synced", 0)
            }
        return state
    except Exception as e:
        logger.error(f"Error reading sync state for {researcher_id}: {e}")
        return {"researcher_id": researcher_id, "data_types": {}, "total_records_synced": 0}


def update_researcher_sync_state(researcher_id: str,
    researcher_name: str,
    institution: str, 
    data_type: str, 
    batch_info: Dict[str, Any]):
    
    state_file = get_researcher_state_file(researcher_id)
    if state_file.exists():
        with open(state_file) as f:
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

    if data_type not in state["data_types"]:
        state["data_types"][data_type] = {
            "first_sync": datetime.now().isoformat(),
            "last_timestamp": None,
            "last_offset": 0,
            "records_synced": 0,
            "batches_completed": 0
        }

    dt_state = state["data_types"][data_type]
    dt_state["last_timestamp"] = batch_info.get("latest_timestamp")
    dt_state["last_offset"] = batch_info.get("end_offset", dt_state["last_offset"] + batch_info.get("record_count", 0))
    dt_state["records_synced"] += batch_info.get("record_count", 0)
    dt_state["batches_completed"] += 1

    state["total_records_synced"] += batch_info.get("record_count", 0)
    state["last_sync_time"] = datetime.now().isoformat()
    state["last_batch"] = {**batch_info, "timestamp": datetime.now().isoformat()}

    if "batches" not in state:
        state["batches"] = []
    state["batches"].append(state["last_batch"])
    state["batches"] = state["batches"][-10:]

    with open(state_file, "w") as f:
        json.dump(state, f, indent=2, default=str)

    logger.info(f"✅ Updated sync state for {researcher_id} - {data_type}")


# ========================
# QuestDB Helpers
# ========================
def check_table_exists() -> bool:
    try:
        response = requests.get(QUESTDB_QUERY_URL, params={"query": "SELECT * FROM hydrogen_data LIMIT 1"})
        return response.status_code == 200
    except:
        return False


def build_data_access_filter(data_access: List[str]) -> str:
    if not data_access:
        return "1=0"
    if "all" in data_access:
        return "1=1"
    conditions = [f"measurement_type = '{access}'" for access in data_access]
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


def save_batch_to_filesystem(
    records: List[Dict],
    researcher: Dict,
    batch_info: Dict[str, Any]
) -> Dict:
    """
    Save a batch of synced data to local filesystem
    """
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
# Auth Dependency (moved here)
# ========================
security = HTTPBearer()


def get_current_researcher(
    credentials: HTTPAuthorizationCredentials = Depends(security)) -> Dict[str, Any]:
    token = credentials.credentials
    result = verify_researcher_token(token)
    if not result["valid"]:
        raise HTTPException(
            status_code=401,
            detail=result.get("reason", 
                            "Authentication failed")
        )
    return result["payload"]