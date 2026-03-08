from fastapi import FastAPI, HTTPException, BackgroundTasks
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
from contextlib import asynccontextmanager
import httpx
import paramiko
from scp import SCPClient
import socket

from utility_functions import (
    check_table_exists,
    get_last_sync_timestamp,
    save_last_sync_timestamp,
    parse_questdb_response,
    save_to_local_filesystem
)


# Setup logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SyncManager:
    """
    SyncManager class to handle synchronization logic
    """
    def __init__(self, config_file: str = "TSDB.yml"):
        try:
            # Load yaml configuration file
            with open(config_file) as f:
                self.config = yaml.safe_load(f)
        except FileNotFoundError:
            print(f"Error: Configuration file '{config_file}' not found.")
            self.config = {}
        except yaml.YAMLError as e:
            print(f"Error parsing YAML file: {e}")
            self.config = {}
        self.QUESTDB_QUERY_URL = f"http://{self.config['questdb']['host']}:{self.config['questdb']['port']}/exec"  
        self.BATCH_SIZE = self.config.get('sissa', {}).get('batch_size', 50)  
        
    def sync_to_remote_ssh(self, local_dir: Path, sync_id: str):
        """
        Sync files to remote computer via SSH/SFTP with password authentication
        """
        try:
            import paramiko
        except ImportError:
            logger.error("paramiko not installed")
            return {"status": "error", "error": "paramiko not installed"}
        
        # Get SSH config
        ssh_config = self.config.get('remote_sync', {}).get('ssh', {})
        remote_host = ssh_config.get('host', '')
        remote_user = ssh_config.get('username', '')
        remote_path = ssh_config.get('path', '/tmp/synced_data')
        password = ssh_config.get('password', '')
        
        if not remote_host or not remote_user or not password:
            missing = []
            if not remote_host: missing.append("host")
            if not remote_user: missing.append("username") 
            if not password: missing.append("password")
            return {
                "status": "error", 
                "error": f"SSH configuration incomplete - missing: {', '.join(missing)}"
            }
        
        logger.info(f"Starting SSH sync to {remote_user}@{remote_host}:{remote_path}/{sync_id}")
        
        ssh = None
        sftp = None
        
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            # Connect with password only
            logger.info(f"Connecting to {remote_host} with password authentication...")
            ssh.connect(
                hostname=remote_host,
                username=remote_user,
                password=password,
                timeout=30,
                allow_agent=False,
                look_for_keys=False,
                auth_timeout=30
            )
            logger.info(f"SSH connection established to {remote_host}")
            
            # Open SFTP session
            sftp = ssh.open_sftp()
            
            # First, ensure the base directory exists
            try:
                sftp.stat(remote_path)
                logger.info(f"Base directory exists: {remote_path}")
            except FileNotFoundError:
                # Create base directory if it doesn't exist
                try:
                    sftp.mkdir(remote_path)
                    logger.info(f"Created base directory: {remote_path}")
                except Exception as e:
                    logger.error(f"Failed to create base directory: {e}")
                    return {
                        "status": "error",
                        "error": f"Failed to create base directory {remote_path}: {str(e)}"
                    }
            
            # Create sync-specific subdirectory
            remote_sync_path = f"{remote_path}/{sync_id}"
            
            # Try to create the sync directory
            try:
                sftp.mkdir(remote_sync_path)
                logger.info(f"Created sync directory: {remote_sync_path}")
            except IOError as e:
                # Directory might already exist (from a previous sync with same ID - unlikely)
                if "File exists" in str(e):
                    logger.info(f"Sync directory already exists (will use it): {remote_sync_path}")
                else:
                    logger.error(f"Failed to create sync directory: {e}")
                    return {
                        "status": "error",
                        "error": f"Failed to create sync directory: {str(e)}"
                    }
            
            # Transfer files
            transferred_files = []
            failed_files = []
            
            for file_path in local_dir.rglob("*"):
                if file_path.is_file():
                    relative_path = file_path.relative_to(local_dir)
                    remote_file = f"{remote_sync_path}/{relative_path}"
                    
                    # Create subdirectories if needed
                    remote_dir = str(Path(remote_file).parent)
                    if remote_dir != remote_sync_path:
                        try:
                            sftp.stat(remote_dir)
                        except FileNotFoundError:
                            # Create subdirectory
                            try:
                                sftp.mkdir(remote_dir)
                                logger.debug(f"Created subdirectory: {remote_dir}")
                            except Exception as e:
                                logger.error(f"Failed to create subdirectory {remote_dir}: {e}")
                                failed_files.append(str(relative_path))
                                continue
                    
                    try:
                        # Transfer file
                        logger.debug(f"Transferring: {relative_path} -> {remote_file}")
                        sftp.put(str(file_path), remote_file)
                        transferred_files.append(str(relative_path))
                        logger.debug(f"Transferred: {relative_path}")
                    except Exception as e:
                        logger.error(f"Failed to transfer {relative_path}: {e}")
                        failed_files.append(str(relative_path))
            
            logger.info(f"SFTP sync completed: {len(transferred_files)} files transferred")
            
            # Also create a manifest file with sync info
            manifest = {
                "sync_id": sync_id,
                "timestamp": datetime.now().isoformat(),
                "source_host": socket.gethostname(),
                "files": transferred_files,
                "total_files": len(transferred_files)
            }
            
            manifest_file = f"{remote_sync_path}/sync_manifest.json"
            try:
                with sftp.open(manifest_file, 'w') as f:
                    import json
                    f.write(json.dumps(manifest, indent=2))
                logger.info(f"Created manifest: {manifest_file}")
            except Exception as e:
                logger.warning(f"Could not create manifest: {e}")
            
            return {
                "status": "success" if not failed_files else "partial",
                "method": "ssh",
                "host": remote_host,
                "path": remote_sync_path,
                "files_transferred": len(transferred_files),
                "files_failed": len(failed_files),
                "transferred_files": transferred_files[:5]
            }
            
        except paramiko.AuthenticationException as e:
            logger.error(f"SSH authentication failed: {e}")
            return {
                "status": "error",
                "method": "ssh",
                "error": f"Authentication failed: {str(e)}. Check your password."
            }
        except paramiko.SSHException as e:
            logger.error(f"SSH connection failed: {e}")
            return {
                "status": "error",
                "method": "ssh",
                "error": f"SSH connection failed: {str(e)}"
            }
        except Exception as e:
            logger.error(f"SSH sync error: {e}", exc_info=True)
            return {
                "status": "error",
                "method": "ssh",
                "error": str(e)
            }
        finally:
            if sftp:
                sftp.close()
            if ssh:
                ssh.close()
                logger.info("SSH connection closed")  
    
    def load_remote_config(self):
        """
        Load remote sync configuration
        """
        #global REMOTE_SYNC_ENABLED, REMOTE_API_ENDPOINT, REMOTE_API_TOKEN, REMOTE_SYNC_METHOD
        
        try:
            if 'remote_sync' in self.config:
                remote_config = self.config['remote_sync']
                self.REMOTE_SYNC_ENABLED = remote_config.get('enabled', False)
                self.REMOTE_API_ENDPOINT = remote_config.get('api_endpoint', '')
                self.REMOTE_API_TOKEN = remote_config.get('api_token', '')
                self.REMOTE_SYNC_METHOD = remote_config.get('method', 'api')

                if self.REMOTE_SYNC_ENABLED:
                    logger.info(f"Remote sync enabled via {self.REMOTE_SYNC_METHOD} to {self.REMOTE_API_ENDPOINT}")
        except Exception as e:
            logger.error(f"Error loading remote config: {e}")                             



# Sync configuration
SYNC_DATA_DIR = Path("./synced_data")  # Directory to store synced data
SYNC_DATA_DIR.mkdir(exist_ok=True)


            

# Initialize FastAPI app
app = FastAPI()
sync_manager = SyncManager()

# Background sync task
sync_task = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Start and stop background sync task with app lifecycle
    """
    global sync_task
    
    # Load remote config
    sync_manager.load_remote_config()
    
    # Start sync task on startup
    sync_task = asyncio.create_task(sync_worker())
    logger.info(f"Background sync worker started")
    
    yield
    
    # Cleanup on shutdown
    if sync_task:
        sync_task.cancel()
        try:
            await sync_task
        except asyncio.CancelledError:
            logger.info("Background sync worker stopped")

# Initialize FastAPI with lifespan for background task management
app = FastAPI(lifespan=lifespan)



async def check_for_new_data() -> Dict[str, Any]:
    """
    Check if there's new data since last sync
    Returns info about new data including count
    """
    if not check_table_exists():
        return {"has_new_data": False, 
                "message": "Table does not exist"}
    
    try:
        # Get last sync timestamp
        last_timestamp = get_last_sync_timestamp()
        
        if last_timestamp:
            # Query to check for newer records
            query = f"""
            SELECT COUNT(*) as count
            FROM hydrogen_data
            WHERE timestamp > '{last_timestamp}'
            """
            logger.info(f"Checking for data newer than {last_timestamp}")
        else:
            # First sync - check total count
            query = "SELECT COUNT(*) as count FROM hydrogen_data"
            logger.info("First sync - checking total records")
        
        response = requests.get(sync_manager.QUESTDB_QUERY_URL, 
                                params={"query": query})
        
        if response.status_code == 200:
            result = response.json()
            
            # Parse count from response
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
    
    

async def sync_new_data() -> Dict[str, Any]:
    """
    Sync only new data since last sync with batch processing
    Waits until BATCH_SIZE records are available before syncing
    """
    logger.info("=" * 50)
    logger.info("STARTING SYNC")
    logger.info("=" * 50)
    
    try:
        # First check if there's new data
        check_result = await check_for_new_data()
        
        if not check_result.get("has_new_data"):
            logger.info("No new data to sync")
            return {
                "status": "no_new_data",
                "message": "No new data available",
                "timestamp": datetime.now().isoformat()
            }
        
        new_count = check_result.get("new_count", 0)
        last_timestamp = check_result.get("last_timestamp")
        
        if new_count < sync_manager.BATCH_SIZE:
            logger.info(f"⏳ Only {new_count} new records available (need {sync_manager.BATCH_SIZE} for batch sync). Waiting...")
            return {
                "status": "waiting",
                "message": f"Need {sync_manager.BATCH_SIZE} records for batch sync, only have {new_count}",
                "new_count": new_count,
                "batch_size": sync_manager.BATCH_SIZE,
                "timestamp": datetime.now().isoformat()
            }
        
        logger.info(f"✅ Batch threshold met: {new_count} new records available (threshold: {sync_manager.BATCH_SIZE})")
        logger.info(f"Found {new_count} new records to sync")
        
        # Generate sync ID
        sync_id = f"sync_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{os.urandom(4).hex()}"
        
        # Query the new data (limit to BATCH_SIZE)
        if last_timestamp:
            query = f"""
            SELECT 
                timestamp,
                lab,
                sensor_id,
                measurement_type,
                unit,
                value
            FROM hydrogen_data
            WHERE timestamp > '{last_timestamp}'
            ORDER BY timestamp ASC
            LIMIT {sync_manager.BATCH_SIZE}
            """
        else:
            # First sync - get all data up to BATCH_SIZE
            query = f"""
            SELECT 
                timestamp,
                lab,
                sensor_id,
                measurement_type,
                unit,
                value
            FROM hydrogen_data
            ORDER BY timestamp ASC
            LIMIT {sync_manager.BATCH_SIZE}
            """
        
        logger.info(f"Executing query to get up to {sync_manager.BATCH_SIZE} new records")
        response = requests.get(sync_manager.QUESTDB_QUERY_URL, params={"query": query})
        
        if response.status_code not in (200, 201, 204):
            logger.error(f"Query failed: {response.text}")
            return {"status": "error", "error": response.text}
        
        result = response.json()
        records = parse_questdb_response(result)
        
        logger.info(f"Retrieved {len(records)} records from database")
        
        if not records:
            logger.warning("No records returned despite count indicating new data")
            return {"status": "error", "message": "Count mismatch"}
        
        # Get time range of synced data
        time_range = {
            "start": records[0].get('timestamp') if records else None,
            "end": records[-1].get('timestamp') if records else None
        }
        
        # Save to filesystem
        sync_info = {
            "new_data": True,
            "time_range": time_range,
            "last_timestamp": last_timestamp,
            "batch_info": {
                "batch_size": sync_manager.BATCH_SIZE,
                "records_in_batch": len(records),
                "remaining": new_count - len(records)
            }
        }
        
        save_result = save_to_local_filesystem(records, sync_info)
        
        # Initialize result
        result_dict = {
            "status": "success" if save_result.get("status") == "success" else "partial",
            "records_synced": len(records),
            "batch_info": {
                "batch_size": sync_manager.BATCH_SIZE,
                "records_in_this_batch": len(records),
                "remaining_records": new_count - len(records),
                "threshold_met": True
            },
            "time_range": time_range,
            "local_save": save_result,
            "remote_sync": None,
            "timestamp": datetime.now().isoformat()
        }
        
        if save_result.get("status") == "success":
            # Save the timestamp of the last synced record
            if records:
                save_last_sync_timestamp(records[-1]['timestamp'])
            
            # Remote sync
            remote_sync_enabled = sync_manager.config.get('remote_sync', {}).get('enabled', False)
            
            if remote_sync_enabled:
                method = sync_manager.config.get('remote_sync', {}).get('method', 'ssh')
                logger.info(f"🚀 Starting REMOTE sync via {method}...")
                
                if method == "ssh":
                    # Get the directory that was just created
                    sync_dir = Path(save_result["directory"])
                    
                    # Call the SSH sync function
                    remote_result = sync_manager.sync_to_remote_ssh(sync_dir, sync_id)
                    
                    # Add to result
                    result_dict["remote_sync"] = remote_result
                    
                    if remote_result.get("status") == "success":
                        logger.info(f"✅ REMOTE sync successful: {remote_result.get('files_transferred', 0)} files transferred")
                    else:
                        logger.error(f"❌ REMOTE sync failed: {remote_result.get('error')}")
                
                # elif method == "api":
                #     # Handle API sync if needed
                #     remote_result = await sync_to_remote_api(records, sync_id)
                #     result_dict["remote_sync"] = remote_result
                    
                #     if remote_result.get("status") == "success":
                #         logger.info(f"✅ REMOTE API sync successful")
                #     else:
                #         logger.error(f"❌ REMOTE API sync failed: {remote_result.get('error')}")
            
            logger.info(f"✅ BATCH SYNC COMPLETE: {len(records)} records")
            logger.info(f"   Time range: {time_range['start']} to {time_range['end']}")
            logger.info(f"   Remaining records to sync: {new_count - len(records)}")
            logger.info(f"   Saved to: {save_result.get('directory')}")
            
            return result_dict
        else:
            logger.error("Failed to save records")
            return {"status": "error", "message": "Save failed"}
        
    except Exception as e:
        logger.error(f"Error during sync: {str(e)}", exc_info=True)
        return {"status": "error", "error": str(e)}
    

async def sync_worker():
    """
    Background worker that checks for new data periodically
    Implements batch processing - only syncs when threshold met
    """
    # Get sync interval from config (default 30 seconds)
    sync_interval = 30  # Check every 30 seconds
    if 'sissa' in sync_manager.config and 'sync_interval' in sync_manager.config['sissa']:
        sync_str = sync_manager.config['sissa']['sync_interval']
        sync_interval = int(sync_str.rstrip('s'))
    
    logger.info(f"🔄 Batch sync worker started - checking every {sync_interval} seconds")
    logger.info(f"📦 Batch size: {sync_manager.BATCH_SIZE} records")
    
    # Initial delay
    await asyncio.sleep(2)
    
    while True:
        try:
            logger.info("-" * 40)
            logger.info(f"Batch sync check at {datetime.now().isoformat()}")
            
            # Check for new data
            check_result = await check_for_new_data()
            
            if check_result.get("has_new_data"):
                new_count = check_result.get("new_count", 0)
                
                if new_count >= sync_manager.BATCH_SIZE:
                    logger.info(f"✅ BATCH THRESHOLD MET: {new_count} >= {sync_manager.BATCH_SIZE}")
                    
                    # Auto-sync if configured
                    if sync_manager.config.get('sissa', {}).get('auto_sync', True):
                        logger.info(f"Auto-sync enabled - syncing batch of up to {sync_manager.BATCH_SIZE} records...")
                        sync_result = await sync_new_data()
                        
                        if sync_result.get("status") == "success":
                            batch_info = sync_result.get("batch_info", {})
                            logger.info(f"✅ Batch sync completed: {batch_info.get('records_in_this_batch')} records synced")
                            if batch_info.get("remaining_records", 0) > 0:
                                logger.info(f"⏳ {batch_info.get('remaining_records')} records remain for next batch")
                        elif sync_result.get("status") == "waiting":
                            logger.info(f"⏳ Still waiting for batch threshold: {sync_result.get('message')}")
                        else:
                            logger.error(f"❌ Batch sync failed: {sync_result.get('error')}")
                    else:
                        logger.info("Auto-sync disabled - use POST /api/sync/trigger to sync")
                else:
                    logger.info(f"⏳ Waiting for batch threshold: {new_count}/{sync_manager.BATCH_SIZE} records")
            else:
                logger.info("✗ No new data")
            
            logger.info(f"Next check in {sync_interval} seconds")
            await asyncio.sleep(sync_interval)
            
        except asyncio.CancelledError:
            logger.info("Batch sync worker cancelled")
            break
        except Exception as e:
            logger.error(f"Error in batch sync worker: {str(e)}")
            await asyncio.sleep(60)

# async def sync_to_remote_api(records: List[Dict], sync_id: str):
#     """
#     Sync data to remote computer via REST API
#     """
#     if not REMOTE_SYNC_ENABLED or not REMOTE_API_ENDPOINT:
#         logger.warning("Remote API sync not configured")
#         return {"status": "disabled", "method": "api"}
    
#     payload = {
#         "sync_id": sync_id,
#         "timestamp": datetime.now().isoformat(),
#         "source": "hydrogen_data_sync",
#         "record_count": len(records),
#         "records": records
#     }
    
#     headers = {
#         "Content-Type": "application/json",
#         "User-Agent": "HydrogenDataSync/1.0"
#     }
    
#     if REMOTE_API_TOKEN:
#         headers["Authorization"] = f"Bearer {REMOTE_API_TOKEN}"
    
#     try:
#         timeout = httpx.Timeout(30.0, connect=10.0)
#         async with httpx.AsyncClient(timeout=timeout) as client:
#             response = await client.post(
#                 REMOTE_API_ENDPOINT,
#                 json=payload,
#                 headers=headers
#             )
            
#             if response.status_code in (200, 201):
#                 logger.info(f"Remote API sync successful: {response.json()}")
#                 return {
#                     "status": "success",
#                     "method": "api",
#                     "endpoint": REMOTE_API_ENDPOINT,
#                     "response": response.json()
#                 }
#             else:
#                 logger.error(f"Remote API sync failed: {response.status_code} - {response.text}")
#                 return {
#                     "status": "error",
#                     "method": "api",
#                     "endpoint": REMOTE_API_ENDPOINT,
#                     "error": f"{response.status_code}: {response.text}"
#                 }
                
#     except httpx.ConnectError:
#         logger.error(f"Cannot connect to remote API: {REMOTE_API_ENDPOINT}")
#         return {
#             "status": "error",
#             "method": "api",
#             "endpoint": REMOTE_API_ENDPOINT,
#             "error": "Connection failed"
#         }
#     except Exception as e:
#         logger.error(f"Remote API sync error: {e}")
#         return {
#             "status": "error",
#             "method": "api",
#             "endpoint": REMOTE_API_ENDPOINT,
#             "error": str(e)
#         }
        


# def sync_to_remote_ssh(local_dir: Path, sync_id: str):
#     """Sync files to remote computer via SSH/SFTP with password authentication"""
#     try:
#         import paramiko
#     except ImportError:
#         logger.error("paramiko not installed")
#         return {"status": "error", "error": "paramiko not installed"}
    
#     # Get SSH config
#     ssh_config = config.get('remote_sync', {}).get('ssh', {})
#     remote_host = ssh_config.get('host', '')
#     remote_user = ssh_config.get('username', '')
#     remote_path = ssh_config.get('path', '/tmp/synced_data')
#     password = ssh_config.get('password', '')
    
#     if not remote_host or not remote_user or not password:
#         missing = []
#         if not remote_host: missing.append("host")
#         if not remote_user: missing.append("username") 
#         if not password: missing.append("password")
#         return {
#             "status": "error", 
#             "error": f"SSH configuration incomplete - missing: {', '.join(missing)}"
#         }
    
#     logger.info(f"Starting SSH sync to {remote_user}@{remote_host}:{remote_path}/{sync_id}")
    
#     ssh = None
#     sftp = None
    
#     try:
#         ssh = paramiko.SSHClient()
#         ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
#         # Connect with password only - disable all other auth methods
#         logger.info(f"Connecting to {remote_host} with password authentication...")
#         ssh.connect(
#             hostname=remote_host,
#             username=remote_user,
#             password=password,
#             timeout=30,
#             allow_agent=False,  # Don't use SSH agent
#             look_for_keys=False,  # Don't look for keys
#             auth_timeout=30
#         )
#         logger.info(f"SSH connection established to {remote_host}")
        
#         # Open SFTP session
#         sftp = ssh.open_sftp()
        
#         # Create remote directory structure
#         remote_sync_path = f"{remote_path}/{sync_id}"
        
#         # Create directories recursively
#         path_parts = remote_sync_path.split('/')
#         current_path = ''
#         for part in path_parts:
#             if part:
#                 current_path += '/' + part
#                 try:
#                     sftp.stat(current_path)
#                     logger.debug(f"Directory exists: {current_path}")
#                 except FileNotFoundError:
#                     sftp.mkdir(current_path)
#                     logger.debug(f"Created directory: {current_path}")
        
#         logger.info(f"Created/verified remote directory: {remote_sync_path}")
        
#         # Transfer files
#         transferred_files = []
#         failed_files = []
        
#         for file_path in local_dir.rglob("*"):
#             if file_path.is_file():
#                 relative_path = file_path.relative_to(local_dir)
#                 remote_file = f"{remote_sync_path}/{relative_path}"
                
#                 # Create subdirectories if needed
#                 remote_dir = str(Path(remote_file).parent)
#                 if remote_dir != remote_sync_path:
#                     try:
#                         sftp.stat(remote_dir)
#                     except FileNotFoundError:
#                         # Create subdirectory
#                         sub_parts = Path(relative_path).parent.parts
#                         current_sub = remote_sync_path
#                         for part in sub_parts:
#                             current_sub += '/' + part
#                             try:
#                                 sftp.stat(current_sub)
#                             except FileNotFoundError:
#                                 sftp.mkdir(current_sub)
#                                 logger.debug(f"Created subdirectory: {current_sub}")
                
#                 try:
#                     # Transfer file
#                     logger.debug(f"Transferring: {relative_path} -> {remote_file}")
#                     sftp.put(str(file_path), remote_file)
#                     transferred_files.append(str(relative_path))
#                     logger.debug(f"Transferred: {relative_path}")
#                 except Exception as e:
#                     logger.error(f"Failed to transfer {relative_path}: {e}")
#                     failed_files.append(str(relative_path))
        
#         logger.info(f"SFTP sync completed: {len(transferred_files)} files transferred")
        
#         return {
#             "status": "success" if not failed_files else "partial",
#             "method": "ssh",
#             "host": remote_host,
#             "path": remote_sync_path,
#             "files_transferred": len(transferred_files),
#             "files_failed": len(failed_files),
#             "transferred_files": transferred_files[:5]
#         }
        
#     except paramiko.AuthenticationException as e:
#         logger.error(f"SSH authentication failed: {e}")
#         return {
#             "status": "error",
#             "method": "ssh",
#             "error": f"Authentication failed: {str(e)}. Check your password."
#         }
#     except paramiko.SSHException as e:
#         logger.error(f"SSH connection failed: {e}")
#         return {
#             "status": "error",
#             "method": "ssh",
#             "error": f"SSH connection failed: {str(e)}"
#         }
#     except Exception as e:
#         logger.error(f"SSH sync error: {e}", exc_info=True)
#         return {
#             "status": "error",
#             "method": "ssh",
#             "error": str(e)
#         }
#     finally:
#         if sftp:
#             sftp.close()
#         if ssh:
#             ssh.close()
#             logger.info("SSH connection closed")
            
            
            
async def sync_to_local():
    """Main sync function - saves data locally and optionally to remote"""
    try:
        logger.info("=" * 60)
        logger.info("STARTING SYNC")
        logger.info("=" * 60)
        
        sync_id = f"sync_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{os.urandom(4).hex()}"
        
        # Query data
        records = await query_simple_all()
        
        if not records:
            logger.info("No data found in table")
            return {
                "status": "no_data",
                "sync_id": sync_id
            }
        
        logger.info(f"Found {len(records)} records to sync")
        
        # Save to local filesystem
        local_result = save_to_local_filesystem(records, sync_id)
        
        result = {
            "status": "success",
            "record_count": len(records),
            "timestamp": datetime.now().isoformat(),
            "local_sync": local_result,
            "remote_sync": None  # Initialize
        }
        
        # Check if remote sync is enabled and call it!
        remote_sync_enabled = sync_manager.config.get('remote_sync', {}).get('enabled', False)
        
        if remote_sync_enabled:
            method = sync_manager.config.get('remote_sync', {}).get('method', 'ssh')
            logger.info(f"🚀 Starting REMOTE sync via {method}...")
            
            if method == "ssh":
                # Get the directory that was just created
                sync_dir = Path(local_result["directory"])
                
                # Call the SSH sync function
                remote_result = sync_manager.sync_to_remote_ssh(sync_dir, sync_id)
                
                # Add to result
                result["remote_sync"] = remote_result
                
                if remote_result.get("status") == "success":
                    logger.info(f"✅ REMOTE sync successful: {remote_result.get('files_transferred', 0)} files transferred")
                else:
                    logger.error(f"❌ REMOTE sync failed: {remote_result.get('error')}")
            
            elif method == "api":
                # Handle API sync if needed
                pass
        
        logger.info(f"Sync completed: {len(records)} records")
        logger.info("=" * 60)
        return result
        
    except Exception as e:
        logger.error(f"Error in sync_to_local: {str(e)}", exc_info=True)
        return {"status": "error", "error": str(e)}
        
        
# Add configuration endpoints
@app.post("/api/remote-sync/config")
async def configure_remote_sync(config_data: Dict[str, Any]):
    """
    Configure remote sync settings
    """
    #global REMOTE_SYNC_ENABLED, REMOTE_API_ENDPOINT, REMOTE_API_TOKEN, REMOTE_SYNC_METHOD
    
    sync_manager.REMOTE_SYNC_ENABLED = config_data.get("enabled", False)
    sync_manager.REMOTE_API_ENDPOINT = config_data.get("api_endpoint", "")
    sync_manager.REMOTE_API_TOKEN = config_data.get("api_token", "")
    sync_manager.REMOTE_SYNC_METHOD = config_data.get("method", "api")
    
    # Save to config file
    if 'remote_sync' not in sync_manager.config:
        sync_manager.config['remote_sync'] = {}
    
    sync_manager.config['remote_sync'].update({
        'enabled': sync_manager.REMOTE_SYNC_ENABLED,
        'api_endpoint': sync_manager.REMOTE_API_ENDPOINT,
        'api_token': sync_manager.REMOTE_API_TOKEN,
        'method': sync_manager.REMOTE_SYNC_METHOD
    })
    
    # with open("TSDB.yml", 'w') as f:
    #     yaml.dump(sync_manager.config, f)
    
    return {
        "status": "configured",
        "remote_sync": {
            "enabled": sync_manager.REMOTE_SYNC_ENABLED,
            "method": sync_manager.REMOTE_SYNC_METHOD,
            "api_endpoint": sync_manager.REMOTE_API_ENDPOINT[:20] + "..." if len(sync_manager.REMOTE_API_ENDPOINT) > 20 else sync_manager.REMOTE_API_ENDPOINT
        }
    }

# @app.get("/api/remote-sync/status")
# async def get_remote_sync_status():
#     """
#     Get remote sync configuration and status
#     """
#     return {
#         "enabled": REMOTE_SYNC_ENABLED,
#         "method": REMOTE_SYNC_METHOD,
#         "api_endpoint": REMOTE_API_ENDPOINT,
#         "configured": bool(REMOTE_API_ENDPOINT)
#     }

@app.post("/api/remote-sync/test")
async def test_remote_sync():
    """
    Test remote sync connection
    """
    if not sync_manager.REMOTE_SYNC_ENABLED:
        return {"status": "disabled", "message": "Remote sync not enabled"}
    
    # Test with minimal data
    test_records = [{
        "timestamp": datetime.now().isoformat(),
        "lab": "TEST",
        "sensor": "test_sensor",
        "value": 42.0,
        "measurement_type": "test",
        "unit": "unit"
    }]
    
    # if REMOTE_SYNC_METHOD == "api":
    #     result = await sync_to_remote_api(test_records, "test_sync")
    # else:
    #     return {"status": "unsupported", "method": REMOTE_SYNC_METHOD}
    
    return result


@app.post("/api/debug/test-ssh")
async def test_ssh_connection():
    """Test SSH connection to remote host with password authentication"""
    
    ssh_config = sync_manager.config.get('remote_sync', {}).get('ssh', {})
    remote_host = ssh_config.get('host', '')
    remote_user = ssh_config.get('username', '')
    remote_path = ssh_config.get('path', '')
    password = ssh_config.get('password', '')
    
    if not all([remote_host, remote_user, password]):
        return {
            "status": "error",
            "message": "SSH configuration incomplete",
            "required_fields": ["host", "username", "password"],
            "current_config": {
                "host": remote_host or "MISSING",
                "username": remote_user or "MISSING",
                "has_password": bool(password)
            }
        }
    
    # Test results
    results = {
        "host": remote_host,
        "username": remote_user,
        "path": remote_path,
        "authentication_method": "password",
        "tests": []
    }
    
    import socket
    
    # Test 1: DNS resolution
    try:
        socket.gethostbyname(remote_host)
        results["tests"].append({"name": "DNS resolution", "status": "OK"})
    except socket.gaierror:
        results["tests"].append({
            "name": "DNS resolution", 
            "status": "FAILED",
            "error": f"Cannot resolve hostname: {remote_host}"
        })
        return results
    
    # Test 2: Port 22 connectivity
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    try:
        result = sock.connect_ex((remote_host, 22))
        if result == 0:
            results["tests"].append({"name": "SSH port (22) connectivity", "status": "OK"})
        else:
            results["tests"].append({
                "name": "SSH port (22) connectivity",
                "status": "FAILED",
                "error": f"Port 22 is not reachable (error code: {result})"
            })
            sock.close()
            return results
        sock.close()
    except Exception as e:
        results["tests"].append({
            "name": "SSH port (22) connectivity",
            "status": "FAILED",
            "error": str(e)
        })
        return results
    
    # Test 3: SSH authentication with password
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        # Connect with password only
        ssh.connect(
            hostname=remote_host,
            username=remote_user,
            password=password,
            timeout=10,
            allow_agent=False,
            look_for_keys=False,
            auth_timeout=10
        )
        
        results["tests"].append({
            "name": "SSH authentication", 
            "status": "OK",
            "message": "Successfully connected with password"
        })
        
        # Test 4: Directory operations
        try:
            sftp = ssh.open_sftp()
            
            # Check if base directory exists
            try:
                sftp.stat(remote_path)
                results["tests"].append({
                    "name": "Base directory access", 
                    "status": "OK",
                    "message": f"Directory exists: {remote_path}"
                })
            except FileNotFoundError:
                # Try to create base directory
                try:
                    sftp.mkdir(remote_path)
                    results["tests"].append({
                        "name": "Base directory creation", 
                        "status": "OK",
                        "message": f"Created directory: {remote_path}"
                    })
                except Exception as e:
                    results["tests"].append({
                        "name": "Base directory creation",
                        "status": "FAILED",
                        "error": str(e)
                    })
            
            # Test 5: Write permissions with a test file
            test_dir = f"{remote_path}/test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            try:
                # Create test directory
                sftp.mkdir(test_dir)
                
                # Write test file
                test_file = f"{test_dir}/test_write.txt"
                with sftp.open(test_file, 'w') as f:
                    f.write("Test write permission\n")
                
                # Verify file exists
                sftp.stat(test_file)
                
                # Clean up
                sftp.remove(test_file)
                sftp.rmdir(test_dir)
                
                results["tests"].append({
                    "name": "Write permissions", 
                    "status": "OK",
                    "message": f"Successfully wrote and removed test file"
                })
            except Exception as e:
                results["tests"].append({
                    "name": "Write permissions",
                    "status": "FAILED",
                    "error": f"Cannot write to directory: {str(e)}"
                })
            
            sftp.close()
            
        except Exception as e:
            results["tests"].append({
                "name": "SFTP operations",
                "status": "FAILED",
                "error": str(e)
            })
        
        ssh.close()
        
    except paramiko.AuthenticationException as e:
        results["tests"].append({
            "name": "SSH authentication",
            "status": "FAILED",
            "error": f"Authentication failed: {str(e)}. Check your password."
        })
    except paramiko.SSHException as e:
        results["tests"].append({
            "name": "SSH connection",
            "status": "FAILED",
            "error": f"SSH protocol error: {str(e)}"
        })
    except Exception as e:
        results["tests"].append({
            "name": "SSH connection",
            "status": "FAILED",
            "error": str(e)
        })
    
    return results


# @app.post("/api/debug/test-ssh")
# async def test_ssh_connection():
#     """Test SSH connection to remote host with password authentication"""
    
#     ssh_config = config.get('remote_sync', {}).get('ssh', {})
#     remote_host = ssh_config.get('host', '')
#     remote_user = ssh_config.get('username', '')
#     remote_path = ssh_config.get('path', '')
#     password = ssh_config.get('password', '')
    
#     if not all([remote_host, remote_user, password]):
#         return {
#             "status": "error",
#             "message": "SSH configuration incomplete",
#             "required_fields": ["host", "username", "password"],
#             "current_config": {
#                 "host": remote_host or "MISSING",
#                 "username": remote_user or "MISSING",
#                 "has_password": bool(password)
#             }
#         }
    
#     # Test results
#     results = {
#         "host": remote_host,
#         "username": remote_user,
#         "path": remote_path,
#         "authentication_method": "password",
#         "tests": []
#     }
    
#     import socket
    
#     # Test 1: DNS resolution
#     try:
#         socket.gethostbyname(remote_host)
#         results["tests"].append({"name": "DNS resolution", "status": "OK"})
#     except socket.gaierror:
#         results["tests"].append({
#             "name": "DNS resolution", 
#             "status": "FAILED",
#             "error": f"Cannot resolve hostname: {remote_host}"
#         })
#         return results
    
#     # Test 2: Port 22 connectivity
#     sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#     sock.settimeout(5)
#     try:
#         result = sock.connect_ex((remote_host, 22))
#         if result == 0:
#             results["tests"].append({"name": "SSH port (22) connectivity", "status": "OK"})
#         else:
#             results["tests"].append({
#                 "name": "SSH port (22) connectivity",
#                 "status": "FAILED",
#                 "error": f"Port 22 is not reachable (error code: {result})"
#             })
#             sock.close()
#             return results
#         sock.close()
#     except Exception as e:
#         results["tests"].append({
#             "name": "SSH port (22) connectivity",
#             "status": "FAILED",
#             "error": str(e)
#         })
#         return results
    
#     # Test 3: SSH authentication with password
#     ssh = paramiko.SSHClient()
#     ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
#     try:
#         # Connect with password only - disable all other auth methods
#         ssh.connect(
#             hostname=remote_host,
#             username=remote_user,
#             password=password,
#             timeout=10,
#             allow_agent=False,  # Don't use SSH agent
#             look_for_keys=False,  # Don't look for keys
#             auth_timeout=10
#         )
        
#         results["tests"].append({
#             "name": "SSH authentication", 
#             "status": "OK",
#             "message": "Successfully connected with password"
#         })
        
#         # Test 4: Directory creation
#         try:
#             sftp = ssh.open_sftp()
            
#             # Try to create directory
#             try:
#                 sftp.mkdir(remote_path)
#                 results["tests"].append({
#                     "name": "Directory creation", 
#                     "status": "OK",
#                     "message": f"Created directory: {remote_path}"
#                 })
#             except IOError as e:
#                 if "File exists" in str(e):
#                     results["tests"].append({
#                         "name": "Directory creation", 
#                         "status": "OK",
#                         "message": f"Directory already exists: {remote_path}"
#                     })
#                 else:
#                     results["tests"].append({
#                         "name": "Directory creation",
#                         "status": "FAILED",
#                         "error": str(e)
#                     })
            
#             # Test 5: Write permissions
#             test_file = f"{remote_path}/test_write_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
#             try:
#                 with sftp.open(test_file, 'w') as f:
#                     f.write("Test write permission\n")
                
#                 # Verify file exists
#                 sftp.stat(test_file)
                
#                 # Clean up
#                 sftp.remove(test_file)
                
#                 results["tests"].append({
#                     "name": "Write permissions", 
#                     "status": "OK",
#                     "message": f"Successfully wrote and removed test file"
#                 })
#             except Exception as e:
#                 results["tests"].append({
#                     "name": "Write permissions",
#                     "status": "FAILED",
#                     "error": f"Cannot write to directory: {str(e)}"
#                 })
            
#             sftp.close()
            
#         except Exception as e:
#             results["tests"].append({
#                 "name": "SFTP operations",
#                 "status": "FAILED",
#                 "error": str(e)
#             })
        
#         ssh.close()
        
#     except paramiko.AuthenticationException as e:
#         results["tests"].append({
#             "name": "SSH authentication",
#             "status": "FAILED",
#             "error": f"Authentication failed: {str(e)}. Check your password."
#         })
#     except paramiko.SSHException as e:
#         results["tests"].append({
#             "name": "SSH connection",
#             "status": "FAILED",
#             "error": f"SSH protocol error: {str(e)}"
#         })
#     except Exception as e:
#         results["tests"].append({
#             "name": "SSH connection",
#             "status": "FAILED",
#             "error": str(e)
#         })
    
#     return results

@app.get("/api/sync/summary")
async def get_sync_summary():
    """Get summary of all syncs"""
    import glob
    
    sync_dirs = sorted(glob.glob("synced_data/sync_*"))
    
    summary = {
        "total_syncs": len(sync_dirs),
        "latest_sync": None,
        "syncs_by_date": {}
    }
    
    if sync_dirs:
        latest = sync_dirs[-1]
        summary["latest_sync"] = {
            "directory": latest,
            "files": os.listdir(latest)
        }
    
    return summary


# if __name__ == "__main__":
#     import uvicorn
    
#     print("\n" + "="*60)
#     print("HYDROGEN DATA SYNC SERVICE - WITH REMOTE SYNC")
#     print("="*60)
#     print(f"Local data directory: {SYNC_DATA_DIR.absolute()}")
#     print(f"QuestDB URL: {QUESTDB_QUERY_URL}")
    
#     if SyncConfig.REMOTE_SYNC_ENABLED:
#         print(f"Remote sync: ENABLED via {SyncConfig.REMOTE_SYNC_METHOD}")
#         if SyncConfig.REMOTE_API_ENDPOINT:
#             print(f"Remote endpoint: {SyncConfig.REMOTE_API_ENDPOINT}")
#     else:
#         print("Remote sync: DISABLED")
    
#     print(f"\nServer starting on: http://localhost:8080")
#     print("="*60 + "\n")
    
#     uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")