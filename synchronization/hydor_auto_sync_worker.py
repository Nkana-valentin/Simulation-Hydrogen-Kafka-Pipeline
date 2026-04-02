# apps/sissa_auto_sync_worker.py
"""
Automatic SISSA Sync Worker - Background sync worker logic
"""
from datetime import datetime
from typing import Dict, Any
import asyncio
import logging
import requests
import paramiko
from pathlib import Path
import glob
import os
import time

# Import helpers
from synchronization.sync_helpers import SyncHelpers
from ingestion.questdbclient import QuestDBClient

# Setup logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize helpers
sync_helpers = SyncHelpers()
#table_name = sync_helpers.config.get('sissa', {}).get('table_name', 'raw_hydrogen_data')
#table_name = "raw_h2_data"  # Default table name
#table_name = sync_helpers.config.get('sissa', {}).get('table_name', table_name)
questdbclient = QuestDBClient()

# Global variables
#BATCH_SIZE = 50  # Default batch size
BATCH_SIZE = int(os.getenv("BATCH_SIZE"))
table_name = os.getenv("KAFKA_TOPIC_RAW", "raw_h2_data")

# =============================
# Background Worker Functions
# =============================
async def sync_worker():
    """
    Background worker that checks for new data periodically
    Implements batch processing - only syncs when threshold met
    """
    # Get sync interval from config with fallback
    sync_interval = 30  # Default 30 seconds
    
    # Get BATCH_SIZE with fallback
    global BATCH_SIZE
    try:
        BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
    except (ValueError, TypeError):
        BATCH_SIZE = 50
        logger.warning(f"Invalid BATCH_SIZE, using default: {BATCH_SIZE}")
    
    # Get table_name with fallback
    global table_name
    table_name = os.getenv("KAFKA_TOPIC_RAW", "raw_h2_data")
    
    logger.info(f"🔄 Auto-sync worker started - checking every {sync_interval} seconds")
    logger.info(f"📦 Batch size: {BATCH_SIZE} records")
    logger.info(f"📊 Table name: {table_name}")
    
    # Initial delay
    await asyncio.sleep(2)
    
    while True:
        try:
            logger.info("-" * 50)
            logger.info(f"Auto-sync check at {datetime.now().isoformat()}")
            
            # Check for new data
            check_result = await check_for_new_data_async()
            
            if check_result.get("has_new_data"):
                new_count = check_result.get("new_count", 0)
                
                if new_count >= BATCH_SIZE:
                    logger.info(f"✅ BATCH THRESHOLD MET: {new_count} >= {BATCH_SIZE}")
                    
                    # Auto-sync if configured
                    if sync_helpers.config.get('sissa', {}).get('auto_sync', True):
                        logger.info(f"Auto-sync enabled - syncing batch...")
                        sync_result = await sync_new_data_batch()
                        
                        if sync_result.get("status") == "success":
                            batch_info = sync_result.get("batch_info", {})
                            logger.info(f"✅ Batch sync completed: {batch_info.get('records_in_this_batch')} records synced")
                            
                            # Check if remote sync is enabled
                            remote_enabled = os.getenv("REMOTE_SYNC_ENABLED", "false").lower() == "true"
                            if remote_enabled:
                                remote_result = await sync_to_remote()
                                logger.info(f"📤 Remote sync result: {remote_result.get('status')}")
                        else:
                            logger.error(f"❌ Batch sync failed: {sync_result.get('error')}")
                    else:
                        logger.info("Auto-sync disabled - use API to trigger sync")
                else:
                    logger.info(f"⏳ Waiting for batch threshold: {new_count}/{BATCH_SIZE} records")
            else:
                logger.info("✗ No new data")
            
            logger.info(f"Next check in {sync_interval} seconds")
            await asyncio.sleep(sync_interval)
            
        except asyncio.CancelledError:
            logger.info("Auto-sync worker cancelled")
            break
        except Exception as e:
            logger.error(f"Error in auto-sync worker: {str(e)}")
            await asyncio.sleep(60)


async def check_for_new_data_async() -> Dict[str, Any]:
    """
    Async wrapper for checking new data
    """
    return sync_helpers.check_for_new_data(table_name)


async def sync_new_data_batch() -> Dict[str, Any]:
    """
    Sync only new data since last sync with batch processing
    Waits until BATCH_SIZE records are available before syncing
    """
    logger.info("=" * 50)
    logger.info("STARTING AUTO SYNC BATCH")
    logger.info("=" * 50)
    
    try:
        # First check if there's new data
        check_result = sync_helpers.check_for_new_data(table_name)
        
        if not check_result.get("has_new_data"):
            logger.info("No new data to sync")
            return {
                "status": "no_new_data",
                "message": "No new data available",
                "timestamp": datetime.now().isoformat()
            }
        
        new_count = check_result.get("new_count", 0)
        last_timestamp = check_result.get("last_timestamp")
        
        if new_count < BATCH_SIZE:
            logger.info(f"⏳ Only {new_count} new records available (need {BATCH_SIZE} for batch sync). Waiting...")
            return {
                "status": "waiting",
                "message": f"Need {BATCH_SIZE} records for batch sync, only have {new_count}",
                "new_count": new_count,
                "batch_size": BATCH_SIZE,
                "timestamp": datetime.now().isoformat()
            }
        
        logger.info(f"✅ Batch threshold met: {new_count} new records available")
        
        # Generate sync ID
        sync_id = f"auto_sync_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Query the new data
        if last_timestamp:
            query = questdbclient.build_query(
                last_timestamp=last_timestamp, batch_size=BATCH_SIZE)
        else:
            # First sync - get all data up to BATCH_SIZE
            query = questdbclient.build_query(batch_size=BATCH_SIZE)
        
        logger.info(f"Executing query to get up to {BATCH_SIZE} new records")
        response = requests.get(sync_helpers.QUESTDB_QUERY_URL, 
                                params={"query": query})
        
        if response.status_code not in (200, 201, 204):
            logger.error(f"Query failed: {response.text}")
            return {"status": "error", "error": response.text}
        
        result = response.json()
        records = sync_helpers._parse_questdb_response(result)
        
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
                "batch_size": BATCH_SIZE,
                "records_in_batch": len(records),
                "remaining": new_count - len(records)
            }
        }
        
        save_result = sync_helpers.save_to_local_filesystem(records, sync_info)
        
        # Initialize result
        result_dict = {
            "status": "success" if save_result.get("status") == "success" else "partial",
            "sync_id": sync_id,
            "records_synced": len(records),
            "batch_info": {
                "batch_size": BATCH_SIZE,
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
                sync_helpers.save_last_sync_timestamp(records[-1]['timestamp'])
            
            logger.info(f"✅ AUTO SYNC COMPLETE: {len(records)} records")
            logger.info(f"   Time range: {time_range['start']} to {time_range['end']}")
            logger.info(f"   Saved to: {save_result.get('directory')}")
            
            return result_dict
        else:
            logger.error("Failed to save records")
            return {"status": "error", "message": "Save failed"}
        
    except Exception as e:
        logger.error(f"Error during auto sync: {str(e)}", exc_info=True)
        return {"status": "error", "error": str(e)}


# =========================
# Helpers
# =========================
def require_env(var_name: str) -> str:
    value = os.getenv(var_name)
    if not value:
        raise ValueError(f"❌ Missing environment variable: {var_name}")
    return value


def get_latest_sync_dir(base_dir: str) -> Path:
    all_matches = glob.glob(f"{base_dir}/sync_*")
    sync_dirs = sorted([
        d for d in all_matches
        if os.path.isdir(d) and not d.endswith("sync_state")
    ])

    if not sync_dirs:
        raise FileNotFoundError("❌ No sync directories found")

    return Path(sync_dirs[-1])


# =========================
# MAIN FUNCTION
# =========================

async def sync_to_remote() -> Dict[str, Any]:
    try:
        # ========================================
        # CONFIG - Check if remote sync is enabled
        # ========================================
        if os.getenv("REMOTE_SYNC_ENABLED", "false").lower() != "true":
            logger.info("ℹ️ Remote sync disabled (REMOTE_SYNC_ENABLED != true)")
            return {"status": "skipped", "message": "Remote sync disabled"}
        
        LOCAL_SYNC_DIR = os.getenv("LOCAL_SYNC_DIR", "./synced_data")
        
        # Check if all required SSH env vars exist
        required_ssh_vars = ["SSH_HOST", "SSH_USER", "SSH_REMOTE_PATH", "SSH_KEY_PATH"]
        missing_vars = [var for var in required_ssh_vars if not os.getenv(var)]
        
        if missing_vars:
            logger.warning(f"⚠️ Missing SSH environment variables: {missing_vars}")
            return {"status": "skipped", "message": f"Missing SSH config: {missing_vars}"}
        
        remote_host = require_env("SSH_HOST")
        remote_user = require_env("SSH_USER")
        remote_path = require_env("SSH_REMOTE_PATH")
        key_path = require_env("SSH_KEY_PATH")
        logger.info(f"🚀 Starting REMOTE sync to {remote_user}@{remote_host}:{remote_path}")
        logger.info(f"🔑 Using key: {key_path}")
        
        # Debug: Print all SSH config
        logger.info(f"🔧 SSH Configuration:")
        logger.info(f"   Host: {remote_host}")
        logger.info(f"   User: {remote_user}")
        logger.info(f"   Remote path: {remote_path}")
        logger.info(f"   Key path: {key_path}")
        
        # Check if key file exists
        logger.info(f"🔑 Checking SSH key at: {key_path}")
        if not os.path.exists(key_path):
            logger.error(f"❌ SSH key file not found: {key_path}")
            # List directory contents for debugging
            key_dir = os.path.dirname(key_path)
            if os.path.exists(key_dir):
                logger.info(f"📁 Contents of {key_dir}: {os.listdir(key_dir)}")
            return {"status": "error", "error": f"SSH key not found: {key_path}"}
        
        # Read and validate the key file
        try:
            with open(key_path, 'r') as f:
                key_content = f.read()
                if not key_content.startswith('-----BEGIN OPENSSH PRIVATE KEY-----') and \
                   not key_content.startswith('-----BEGIN RSA PRIVATE KEY-----'):
                    logger.warning(f"⚠️ SSH key doesn't look like a valid private key")
                    logger.info(f"First line: {key_content.split(chr(10))[0][:50]}")
                else:
                    logger.info(f"✅ SSH key appears valid (starts with {key_content.split(chr(10))[0][:30]}...)")
        except Exception as e:
            logger.error(f"❌ Could not read SSH key: {e}")
            return {"status": "error", "error": f"Cannot read key: {e}"}
        
        # Check key file permissions
        import stat
        key_stat = os.stat(key_path)
        key_permissions = oct(stat.S_IMODE(key_stat.st_mode))
        logger.info(f"🔐 SSH key permissions: {key_permissions}")
        
        if key_permissions not in ['0o600', '0o400']:
            logger.warning(f"⚠️ SSH key has incorrect permissions: {key_permissions}. Should be 600")
            # Try to fix permissions (if running as root)
            try:
                os.chmod(key_path, 0o600)
                logger.info("✅ Fixed SSH key permissions to 600")
            except Exception as e:
                logger.warning(f"Could not fix permissions: {e}")
        
        logger.info(f"🚀 Starting REMOTE sync (SSH)")
        logger.info(f"📂 Local sync dir: {LOCAL_SYNC_DIR}")
        
        # Check if sync directory exists
        if not os.path.exists(LOCAL_SYNC_DIR):
            logger.warning(f"❌ Local sync directory not found: {LOCAL_SYNC_DIR}")
            return {"status": "skipped", "message": "No sync directory found"}
        
        # =========================
        # FIND LATEST SYNC
        # =========================
        try:
            latest_dir = get_latest_sync_dir(LOCAL_SYNC_DIR)
        except FileNotFoundError as e:
            logger.warning(f"No sync directories found: {e}")
            return {"status": "skipped", "message": "No sync directories found"}
        
        sync_id = latest_dir.name
        logger.info(f"📦 Latest sync: {latest_dir}")
        
        files_to_transfer = list(latest_dir.glob("*"))
        files_to_transfer = [f for f in files_to_transfer if f.is_file()]
        
        if not files_to_transfer:
            logger.info("ℹ️ No files to transfer")
            return {"status": "success", "message": "No files to transfer"}
        
        logger.info(f"📄 Files to transfer: {[f.name for f in files_to_transfer]}")
        
        # =========================
        # SSH CONNECTION WITH MULTIPLE AUTHENTICATION METHODS
        # =========================
        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        connected = False
        auth_methods_tried = []
        
        # Try multiple authentication methods
        for attempt in range(3):
            try:
                logger.info(f"🔄 SSH connection attempt {attempt+1}/3")
                
                # Try with explicit key file first
                logger.info(f"   Trying key authentication with: {key_path}")
                ssh.connect(
                    hostname=remote_host,
                    username=remote_user,
                    key_filename=key_path,
                    timeout=30,
                    look_for_keys=False,
                    allow_agent=False
                )
                connected = True
                logger.info("✅ SSH connection established with key authentication")
                break
                
            except paramiko.AuthenticationException as e:
                logger.error(f"❌ Key authentication failed: {e}")
                auth_methods_tried.append("key")
                
                # Try with password if available (for testing)
                ssh_password = os.getenv("SSH_PASSWORD")
                if ssh_password and attempt == 1:  # Try password on second attempt
                    try:
                        logger.info("   Trying password authentication...")
                        ssh.connect(
                            hostname=remote_host,
                            username=remote_user,
                            password=ssh_password,
                            timeout=30,
                            look_for_keys=False,
                            allow_agent=False
                        )
                        connected = True
                        logger.info("✅ SSH connection established with password")
                        break
                    except Exception as pwd_e:
                        logger.error(f"❌ Password authentication failed: {pwd_e}")
                        auth_methods_tried.append("password")
                
                # Try with default SSH key locations on last attempt
                if attempt == 2:
                    try:
                        logger.info("   Trying default key locations...")
                        ssh.connect(
                            hostname=remote_host,
                            username=remote_user,
                            timeout=30,
                            look_for_keys=True,
                            allow_agent=False
                        )
                        connected = True
                        logger.info("✅ SSH connection established with default keys")
                        break
                    except Exception as default_e:
                        logger.error(f"❌ Default key authentication failed: {default_e}")
                        auth_methods_tried.append("default_keys")
                
                time.sleep(3)
                
            except Exception as e:
                logger.warning(f"⚠️ SSH attempt {attempt+1}/3 failed: {e}")
                time.sleep(3)
        
        if not connected:
            error_msg = f"SSH connection failed after trying: {', '.join(auth_methods_tried)}"
            logger.error(f"❌ {error_msg}")
            
            # Try to diagnose with manual key test if possible
            try:
                import subprocess
                # Test if key is valid with ssh-keygen
                test_cmd = f"ssh-keygen -y -f {key_path}"
                result = subprocess.run(test_cmd, shell=True, capture_output=True, text=True)
                if result.returncode == 0:
                    logger.info("✅ SSH key is valid (ssh-keygen test passed)")
                    logger.info(f"   Public key: {result.stdout[:50]}...")
                else:
                    logger.error(f"❌ SSH key validation failed: {result.stderr}")
            except Exception as sub_e:
                logger.warning(f"Could not run key validation: {sub_e}")
            
            raise Exception(error_msg)
        
        # =========================
        # CREATE REMOTE DIR
        # =========================
        remote_sync_path = f"{remote_path}/{sync_id}"
        
        try:
            stdin, stdout, stderr = ssh.exec_command(f"mkdir -p {remote_sync_path}")
            exit_status = stdout.channel.recv_exit_status()
            
            if exit_status == 0:
                logger.info(f"📁 Remote directory ready: {remote_sync_path}")
            else:
                error_msg = stderr.read().decode()
                logger.warning(f"mkdir warning: {error_msg}")
        except Exception as e:
            logger.error(f"❌ Failed to create remote directory: {e}")
            ssh.close()
            return {"status": "error", "error": f"Failed to create remote directory: {e}"}
        
        # =========================
        # SFTP TRANSFER
        # =========================
        try:
            sftp = ssh.open_sftp()
            files_transferred = 0
            
            for file_path in files_to_transfer:
                remote_file = f"{remote_sync_path}/{file_path.name}"
                logger.info(f"⬆️ Uploading {file_path.name} ({file_path.stat().st_size} bytes)")
                
                try:
                    sftp.put(str(file_path), remote_file)
                    
                    # Integrity check
                    local_size = file_path.stat().st_size
                    remote_size = sftp.stat(remote_file).st_size
                    
                    if local_size != remote_size:
                        logger.error(f"❌ Integrity check failed for {file_path.name}")
                        continue
                    
                    files_transferred += 1
                    logger.info(f"✅ Uploaded {file_path.name}")
                    
                except Exception as e:
                    logger.error(f"❌ Failed to upload {file_path.name}: {e}")
                    # Continue with other files
            
            sftp.close()
            
        except Exception as e:
            logger.error(f"❌ SFTP error: {e}")
            ssh.close()
            return {"status": "error", "error": f"SFTP failed: {e}"}
        
        ssh.close()
        
        logger.info(f"✅ Sync complete: {files_transferred}/{len(files_to_transfer)} files transferred")
        
        return {
            "status": "success",
            "files_transferred": files_transferred,
            "remote_path": remote_sync_path,
            "sync_id": sync_id
        }
        
    except Exception as e:
        logger.error(f"❌ Remote sync failed: {str(e)}")
        return {
            "status": "error",
            "error": str(e)
        }



# async def sync_to_remote() -> Dict[str, Any]:
#     try:
#         # =========================
#         # CONFIG - Check if remote sync is enabled
#         # =========================
#         if os.getenv("REMOTE_SYNC_ENABLED", "false").lower() != "true":
#             logger.info("ℹ️ Remote sync disabled (REMOTE_SYNC_ENABLED != true)")
#             return {"status": "skipped", "message": "Remote sync disabled"}
        
#         LOCAL_SYNC_DIR = os.getenv("LOCAL_SYNC_DIR", "./synced_data")
        
#         # Check if all required SSH env vars exist
#         required_ssh_vars = ["SSH_HOST", "SSH_USER", "SSH_REMOTE_PATH", "SSH_KEY_PATH"]
#         missing_vars = [var for var in required_ssh_vars if not os.getenv(var)]
        
#         if missing_vars:
#             logger.warning(f"⚠️ Missing SSH environment variables: {missing_vars}")
#             return {"status": "skipped", "message": f"Missing SSH config: {missing_vars}"}
        
#         remote_host = require_env("SSH_HOST")
#         remote_user = require_env("SSH_USER")
#         remote_path = require_env("SSH_REMOTE_PATH")
#         key_path = require_env("SSH_KEY_PATH")
        
#         # Debug: Check if key file exists and its permissions
#         logger.info(f"🔑 SSH Key path: {key_path}")
#         if not os.path.exists(key_path):
#             logger.error(f"❌ SSH key file not found: {key_path}")
#             # List directory contents for debugging
#             key_dir = os.path.dirname(key_path)
#             if os.path.exists(key_dir):
#                 logger.info(f"📁 Contents of {key_dir}: {os.listdir(key_dir)}")
#             return {"status": "error", "error": f"SSH key not found: {key_path}"}
        
#         # Check key file permissions
#         import stat
#         key_stat = os.stat(key_path)
#         key_permissions = oct(stat.S_IMODE(key_stat.st_mode))
#         logger.info(f"🔐 SSH key permissions: {key_permissions}")
        
#         if key_permissions not in ['0o600', '0o400']:
#             logger.warning(f"⚠️ SSH key has incorrect permissions: {key_permissions}. Should be 600")
#             # Try to fix permissions (if running as root)
#             try:
#                 os.chmod(key_path, 0o600)
#                 logger.info("✅ Fixed SSH key permissions to 600")
#             except Exception as e:
#                 logger.warning(f"Could not fix permissions: {e}")
        
#         logger.info(f"🚀 Starting REMOTE sync (SSH)")
#         logger.info(f"📂 Local sync dir: {LOCAL_SYNC_DIR}")
#         logger.info(f"🔗 Connecting to {remote_user}@{remote_host}")
        
#         # Check if sync directory exists
#         if not os.path.exists(LOCAL_SYNC_DIR):
#             logger.warning(f"❌ Local sync directory not found: {LOCAL_SYNC_DIR}")
#             return {"status": "skipped", "message": "No sync directory found"}
        
#         # =========================
#         # FIND LATEST SYNC
#         # =========================
#         try:
#             latest_dir = get_latest_sync_dir(LOCAL_SYNC_DIR)
#         except FileNotFoundError as e:
#             logger.warning(f"No sync directories found: {e}")
#             return {"status": "skipped", "message": "No sync directories found"}
        
#         sync_id = latest_dir.name
#         logger.info(f"📦 Latest sync: {latest_dir}")
        
#         files_to_transfer = list(latest_dir.glob("*"))
#         files_to_transfer = [f for f in files_to_transfer if f.is_file()]
        
#         if not files_to_transfer:
#             logger.info("ℹ️ No files to transfer")
#             return {"status": "success", "message": "No files to transfer"}
        
#         logger.info(f"📄 Files: {[f.name for f in files_to_transfer]}")
        
#         # =========================
#         # SSH CONNECTION (WITH RETRY AND DEBUG)
#         # =========================
#         ssh = paramiko.SSHClient()
#         ssh.load_system_host_keys()
#         ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
#         # Try to read the key file content for debugging (first few lines)
#         try:
#             with open(key_path, 'r') as f:
#                 first_line = f.readline().strip()
#                 logger.info(f"🔑 Key file starts with: {first_line[:20]}...")
#         except Exception as e:
#             logger.warning(f"Could not read key file: {e}")
        
#         connected = False
#         for attempt in range(3):
#             try:
#                 logger.info(f"🔄 SSH connection attempt {attempt+1}/3")
#                 ssh.connect(
#                     hostname=remote_host,
#                     username=remote_user,
#                     key_filename=key_path,
#                     timeout=30,
#                     look_for_keys=False,  # Don't look for keys in default locations
#                     allow_agent=False     # Don't use SSH agent
#                 )
#                 connected = True
#                 logger.info("✅ SSH connection established")
#                 break
#             except paramiko.AuthenticationException as e:
#                 logger.error(f"❌ Authentication failed on attempt {attempt+1}: {e}")
#                 # Try to diagnose the issue
#                 logger.error(f"   Username: {remote_user}")
#                 logger.error(f"   Key path: {key_path}")
#                 logger.error(f"   Key exists: {os.path.exists(key_path)}")
#                 if attempt == 2:  # Last attempt
#                     # Try to test with ssh command (if available)
#                     try:
#                         import subprocess
#                         test_cmd = f"ssh -i {key_path} -o ConnectTimeout=5 {remote_user}@{remote_host} echo 'Connection test'"
#                         logger.info(f"Testing with: {test_cmd}")
#                         result = subprocess.run(test_cmd, shell=True, capture_output=True, text=True)
#                         logger.info(f"SSH test result: {result.returncode}")
#                         if result.stderr:
#                             logger.error(f"SSH test error: {result.stderr}")
#                     except Exception as sub_e:
#                         logger.warning(f"Could not run SSH test: {sub_e}")
#                 time.sleep(5)
#             except Exception as e:
#                 logger.warning(f"⚠️ SSH attempt {attempt+1}/3 failed: {e}")
#                 time.sleep(5)
        
#         if not connected:
#             raise Exception("❌ SSH connection failed after retries")
        
#         # =========================
#         # CREATE REMOTE DIR
#         # =========================
#         remote_sync_path = f"{remote_path}/{sync_id}"
        
#         stdin, stdout, stderr = ssh.exec_command(f"mkdir -p {remote_sync_path}")
#         exit_status = stdout.channel.recv_exit_status()
        
#         if exit_status == 0:
#             logger.info(f"📁 Remote directory ready: {remote_sync_path}")
#         else:
#             error_msg = stderr.read().decode()
#             logger.warning(f"mkdir warning: {error_msg}")
        
#         # =========================
#         # SFTP TRANSFER
#         # =========================
#         sftp = ssh.open_sftp()
        
#         files_transferred = 0
#         for file_path in files_to_transfer:
#             remote_file = f"{remote_sync_path}/{file_path.name}"
#             logger.info(f"⬆️ Uploading {file_path.name} ({file_path.stat().st_size} bytes)")
            
#             try:
#                 sftp.put(str(file_path), remote_file)
                
#                 # Integrity check
#                 local_size = file_path.stat().st_size
#                 remote_size = sftp.stat(remote_file).st_size
                
#                 if local_size != remote_size:
#                     raise Exception(f"❌ Integrity check failed: {file_path.name}")
                
#                 files_transferred += 1
#                 logger.info(f"✅ Uploaded {file_path.name}")
                
#             except Exception as e:
#                 logger.error(f"❌ Failed to upload {file_path.name}: {e}")
#                 # Continue with other files
        
#         sftp.close()
#         ssh.close()
        
#         logger.info(f"✅ Sync complete: {files_transferred}/{len(files_to_transfer)} files transferred")
        
#         return {
#             "status": "success",
#             "files_transferred": files_transferred,
#             "remote_path": remote_sync_path,
#             "sync_id": sync_id
#         }
        
#     except Exception as e:
#         logger.error(f"❌ Remote sync failed: {str(e)}")
#         return {
#             "status": "error",
#             "error": str(e)
#         }