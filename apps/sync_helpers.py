# sync_helpers.py
import logging
import json
import yaml
import csv
import requests
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, List, Union
import math

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class SyncHelpers:
    """
    Unified helper class for SISSA synchronization system
    Combines researcher-specific sync utilities and system-wide sync functions
    """
    
    def __init__(self, config_path: str = "TSDB.yml"):
        """
        Initialize sync helpers with configuration
        
        Args:
            config_path: Path to YAML configuration file
        """
        self.config_path = config_path
        self.config = self._load_config()
        
        # QuestDB configuration
        self.QUESTDB_QUERY_URL = f"http://{self.config['questdb']['host']}:{self.config['questdb']['port']}/exec"
        
        # Directory structure
        self.BASE_DIR = Path(__file__).resolve().parent.parent
        self.SYNC_DATA_DIR = self.BASE_DIR / "synced_data"
        self.SYNC_STATE_DIR = self.SYNC_DATA_DIR / "sync_state"
        self.SYNC_STATE_FILE = self.SYNC_DATA_DIR / "last_sync.txt"
        
        # Create directories
        self._ensure_directories()
        
        logger.info(f"✅ Sync directories ready → {self.SYNC_DATA_DIR.resolve()}")
        logger.info(f"   State files → {self.SYNC_STATE_DIR.resolve()}")
    
    def _load_config(self) -> Dict:
        """Load configuration from YAML file"""
        try:
            with open(self.config_path) as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            return {"questdb": {"host": "localhost", "port": 9000}}
    
    def _ensure_directories(self):
        """
        Create required directories if they don't exist
        """
        self.SYNC_DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.SYNC_STATE_DIR.mkdir(parents=True, exist_ok=True)
    
    # ========================
    # Researcher Sync State Methods
    # ========================
    
    def get_researcher_state_file(self, researcher_id: str) -> Path:
        """
        Get path to researcher's state file
        """
        safe_id = researcher_id.replace("@", "_at_").replace(".", "_dot_")
        return self.SYNC_STATE_DIR / f"{safe_id}_sync_state.json"
    
    def get_researcher_sync_state(self, researcher_id: str, 
                    data_type: Optional[str] = None) -> Dict[str, Any]:
        """
        Get sync state for a researcher
        """
        state_file = self.get_researcher_state_file(researcher_id)
        
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
    
    def update_researcher_sync_state(self, researcher_id: str,
                                    researcher_name: str,
                                    institution: str, 
                                    data_type: str, 
                                    batch_info: Dict[str, Any]):
        """
        Update sync state for a researcher after successful batch
        """
        state_file = self.get_researcher_state_file(researcher_id)
        
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
        
        # Initialize data type if needed
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
        
        # Update global state
        state["total_records_synced"] += batch_info.get("record_count", 0)
        state["last_sync_time"] = datetime.now().isoformat()
        state["last_batch"] = {**batch_info, "timestamp": datetime.now().isoformat()}
        
        # Maintain batch history
        if "batches" not in state:
            state["batches"] = []
        state["batches"].append(state["last_batch"])
        state["batches"] = state["batches"][-10:]  # Keep last 10 batches
        
        # Save state
        with open(state_file, "w") as f:
            json.dump(state, f, indent=2, default=str)
        
        logger.info(f"✅ Updated sync state for {researcher_id} - {data_type}")
    
    # ========================
    # QuestDB Helper Methods
    # ========================
    
    def check_table_exists(self, 
                    table_name: str = "hydrogen_data") -> bool:
        """
        Check if a table exists in QuestDB
        """
        try:
            query = f"SELECT * FROM {table_name} LIMIT 1"
            response = requests.get(self.QUESTDB_QUERY_URL, 
                                params={"query": query})
            
            if response.status_code == 200:
                logger.info(f"Table '{table_name}' exists")
                return True
            elif response.status_code == 400:
                error_text = response.json().get('error', '')
                if 'table does not exist' in error_text.lower():
                    logger.warning(f"Table '{table_name}' does not exist")
                    return False
            return True
        except Exception as e:
            logger.error(f"Error checking table: {str(e)}")
            return False
    
    def build_data_access_filter(self, data_access: List[str]) -> str:
        """
        Build SQL filter from data access permissions
        """
        if not data_access:
            return "1=0"
        if "all" in data_access:
            return "1=1"
        conditions = [f"measurement_type = '{access}'" for access in data_access]
        return "(" + " OR ".join(conditions) + ")"
    
    def query_researcher_data_batch(self,
                                researcher_id: str,
                                data_access: List[str],
                                data_type: Optional[str] = None,
                                batch_size: int = 100,
                                offset: int = 0,
                                table_name: str = "hydrogen_data") -> Dict[str, Any]:
        """
        Query data for a specific researcher with access controls
        """
        if not self.check_table_exists(table_name):
            return {"records": [], "total_count": 0, "has_more": False, "offset": offset}
        
        try:
            # Get researcher's sync state for this data type
            state = self.get_researcher_sync_state(researcher_id, data_type)
            current_offset = state.get("last_offset", offset)
            
            # Build access filter
            if data_type:
                if data_type not in data_access and "all" not in data_access:
                    logger.warning(f"⚠️ Researcher {researcher_id} does not have access to {data_type}")
                    return {"records": [], "total_count": 0, "has_more": False, "offset": current_offset}
                access_filter = f"measurement_type = '{data_type}'"
            else:
                access_filter = self.build_data_access_filter(data_access)
            
            # Get total count
            count_query = f"""
            SELECT count(*)
            FROM {table_name}
            WHERE {access_filter}
            """
            
            logger.info(f"📊 Count query: {count_query}")
            count_response = requests.get(self.QUESTDB_QUERY_URL, params={"query": count_query})
            
            total_count = 0
            if count_response.status_code == 200:
                count_result = count_response.json()
                if "dataset" in count_result and count_result["dataset"]:
                    total_count = count_result["dataset"][0][0]
            
            logger.info(f"📊 Total accessible records for {researcher_id}: {total_count}")
            
            # Try different pagination syntaxes
            queries_to_try = [
                f"""
                SELECT timestamp, lab, sensor_id, measurement_type, unit, value
                FROM {table_name}
                WHERE {access_filter}
                ORDER BY timestamp ASC, sensor_id ASC
                LIMIT {batch_size} OFFSET {current_offset}
                """,
                f"""
                SELECT timestamp, lab, sensor_id, measurement_type, unit, value
                FROM {table_name}
                WHERE {access_filter}
                ORDER BY timestamp ASC, sensor_id ASC
                LIMIT {current_offset},{batch_size}
                """
            ]
            
            records = []
            successful_query = None
            
            for query in queries_to_try:
                logger.info(f"🔍 Trying query: {query}")
                response = requests.get(self.QUESTDB_QUERY_URL, params={"query": query})
                
                if response.status_code == 200:
                    successful_query = query
                    result = response.json()
                    records = self._parse_questdb_response(result)
                    break
                else:
                    logger.warning(f"Query failed: {response.status_code}")
            
            if not successful_query:
                logger.error(f"❌ All query attempts failed")
                return {"records": [], "total_count": total_count, "has_more": False, "offset": current_offset}
            
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
    
    def _parse_questdb_response(self, result: Dict) -> List[Dict]:
        """
        Parse QuestDB response into list of dictionaries
        """
        records = []
        
        try:
            if "dataset" not in result:
                logger.warning("No 'dataset' in response")
                return records
            
            dataset = result["dataset"]
            
            # Get column names
            columns = []
            if isinstance(result.get("columns"), list):
                for col_info in result["columns"]:
                    if isinstance(col_info, dict) and "name" in col_info:
                        columns.append(col_info["name"])
                    elif isinstance(col_info, str):
                        columns.append(col_info)
            
            if isinstance(dataset, list):
                for row in dataset:
                    if len(row) == len(columns):
                        record = {}
                        for j, col_name in enumerate(columns):
                            record[col_name] = row[j]
                        records.append(record)
                    else:
                        logger.warning(f"Row length mismatch: {row}")
            
            return records
            
        except Exception as e:
            logger.error(f"Error parsing QuestDB response: {str(e)}", exc_info=True)
            return records
    
    # ========================
    # Data Saving Methods
    # ========================
    
    def save_batch_to_filesystem(self,
                                records: List[Dict],
                                researcher: Dict,
                                batch_info: Dict[str, Any]) -> Dict:
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
        researcher_dir = self.SYNC_DATA_DIR / f"{institution}_{researcher_name}"
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
    
    def save_to_local_filesystem(self, records: List[Dict], sync_info: Dict) -> Dict:
        """
        Save synced data to local filesystem (system-wide sync)
        """
        if not records:
            logger.warning("No records to save")
            return {"status": "no_data"}
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create sync directory
        sync_dir = self.SYNC_DATA_DIR / f"sync_{timestamp}"
        sync_dir.mkdir(exist_ok=True)
        logger.info(f"Saving {len(records)} records to {sync_dir}")
        
        # Save as JSON
        json_path = sync_dir / "data.json"
        with open(json_path, 'w') as f:
            json.dump({
                "sync_time": datetime.now().isoformat(),
                "record_count": len(records),
                "time_range": sync_info.get("time_range"),
                "records": records
            }, f, indent=2, default=str)
        
        # Save as CSV
        csv_path = sync_dir / "data.csv"
        if records:
            fieldnames = records[0].keys()
            with open(csv_path, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(records)
        
        # Save summary
        summary_path = sync_dir / "summary.txt"
        with open(summary_path, 'w') as f:
            f.write(f"SYNC SUMMARY\n")
            f.write(f"============\n")
            f.write(f"Time: {datetime.now().isoformat()}\n")
            f.write(f"Records: {len(records)}\n")
            f.write(f"Time range: {sync_info.get('time_range', {}).get('start')} to {sync_info.get('time_range', {}).get('end')}\n")
            f.write(f"New data: {sync_info.get('new_data', False)}\n")
        
        return {
            "status": "success",
            "directory": str(sync_dir),
            "record_count": len(records)
        }
    
    # ========================
    # Sync State Management (System-wide)
    # ========================
    
    def get_last_sync_timestamp(self) -> Optional[str]:
        """
        Get the timestamp of the last synced record from file
        """
        if self.SYNC_STATE_FILE.exists():
            try:
                with open(self.SYNC_STATE_FILE, 'r') as f:
                    timestamp = f.read().strip()
                    if timestamp:
                        logger.info(f"Last sync timestamp: {timestamp}")
                        return timestamp
            except Exception as e:
                logger.error(f"Error reading last sync timestamp: {e}")
        
        logger.info("No previous sync timestamp found")
        return None
    
    def save_last_sync_timestamp(self, timestamp: str):
        """
        Save the timestamp of the last synced record
        """
        try:
            with open(self.SYNC_STATE_FILE, 'w') as f:
                f.write(timestamp)
            logger.info(f"Saved last sync timestamp: {timestamp}")
        except Exception as e:
            logger.error(f"Error saving last sync timestamp: {e}")
    
    # ========================
    # Batch Processing Helpers
    # ========================
    
    def fetch_next_batch(self,
                        researcher_id: str,
                        data_access: List[str],
                        data_type: Optional[str],
                        batch_size: int) -> dict:
        """
        Fetch the next batch of data for the researcher (no side effects)
        """
        full_state = self.get_researcher_sync_state(researcher_id)
        
        if data_type:
            dt_state = full_state.get("data_types", {}).get(data_type, {})
            current_offset = dt_state.get("last_offset", 0)
        else:
            current_offset = 0
        
        return self.query_researcher_data_batch(
            researcher_id=researcher_id,
            data_access=data_access,
            data_type=data_type,
            batch_size=batch_size,
            offset=current_offset,
        )
    
    def process_successful_batch(self,
                                researcher: dict,
                                batch_result: dict,
                                batch_size: int,
                                data_type: Optional[str]) -> dict:
        """
        Save batch, update state, and return final 
        response (only called on success)
        """
        researcher_id = researcher["user_id"]
        researcher_name = researcher["username"]
        institution = researcher.get("institution")
        
        records = batch_result["records"]
        total_count = batch_result.get("total_count", 0)
        
        full_state = self.get_researcher_sync_state(researcher_id)
        data_type_key = batch_result.get("data_type") or data_type or "all_authorized"
        dt_state = full_state.get("data_types", {}).get(data_type_key, {})
        batch_number = dt_state.get("batches_completed", 0) + 1
        total_batches = math.ceil(total_count / batch_size) if total_count > 0 else 0
        
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
        save_result = self.save_batch_to_filesystem(records, researcher, batch_info)
        
        # 2. Update persistent state
        self.update_researcher_sync_state(
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
    
    def check_for_new_data(self, table_name: str = "hydrogen_data") -> Dict[str, Any]:
        """
        Check if there's new data since last sync
        Returns info about new data including count
        """
        if not self.check_table_exists(table_name):
            return {"has_new_data": False, "message": "Table does not exist"}
        
        try:
            last_timestamp = self.get_last_sync_timestamp()
            
            if last_timestamp:
                query = f"""
                SELECT COUNT(*) as count
                FROM {table_name}
                WHERE timestamp > '{last_timestamp}'
                """
                logger.info(f"Checking for data newer than {last_timestamp}")
            else:
                query = f"SELECT COUNT(*) as count FROM {table_name}"
                logger.info("First sync - checking total records")
            
            response = requests.get(self.QUESTDB_QUERY_URL, params={"query": query})
            
            if response.status_code == 200:
                result = response.json()
                count = 0
                if "dataset" in result and result["dataset"]:
                    count = result["dataset"][0][0]
                
                has_new = count > 0
                
                if has_new:
                    logger.info(f"Found {count} new records available")
                    return {
                        "has_new_data": True,
                        "new_count": count,
                        "last_timestamp": last_timestamp,
                        "message": f"Found {count} new records"
                    }
                else:
                    logger.info("No new data available")
                    return {
                        "has_new_data": False,
                        "new_count": 0,
                        "last_timestamp": last_timestamp,
                        "message": "No new data"
                    }
            else:
                logger.error(f"Query failed: {response.text}")
                return {"has_new_data": False, "error": response.text}
                
        except Exception as e:
            logger.error(f"Error checking for new data: {str(e)}", exc_info=True)
            return {"has_new_data": False, "error": str(e)}