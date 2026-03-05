# apps/sissa_auto_sync_app.py
"""
Automatic SISSA Sync App - Background sync to SISSA Hydor system
"""
from fastapi import FastAPI, APIRouter, BackgroundTasks, HTTPException, Depends
from datetime import datetime
from typing import Optional, Dict, Any
import asyncio
import logging
from contextlib import asynccontextmanager
import requests
import paramiko
from pathlib import Path

# Import helpers
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

# Global variables
sync_task = None
BATCH_SIZE = 50  # Default batch size
# Create router
router = APIRouter(tags=['Auto Sync To Hydor'])
# ========================
# Admin Authentication Helper
# ========================
def get_admin_user(
    credentials: HTTPAuthorizationCredentials = Depends(security)) -> Dict[str, Any]:
    """
    Ensure user is admin
    """
    token = credentials.credentials
    result = verify_researcher_token(token)
    
    if not result["valid"]:
        raise HTTPException(
            status_code=401,
            detail=result.get("reason", "Authentication failed")
        )
    
    user = result["payload"]
    if "admin" not in user.get("roles", []):
        raise HTTPException(
            status_code=403,
            detail="Admin access required"
        )
    
    return user


# ========================
# Background Worker
# ========================
async def sync_worker():
    """
    Background worker that checks for new data periodically
    Implements batch processing - only syncs when threshold met
    """
    # Get sync interval from config
    sync_interval = 30  # Default 30 seconds
    if 'sissa' in sync_helpers.config and 'sync_interval' in sync_helpers.config['sissa']:
        sync_str = sync_helpers.config['sissa']['sync_interval']
        sync_interval = int(sync_str.rstrip('s'))
    
    logger.info(f"🔄 Auto-sync worker started - checking every {sync_interval} seconds")
    logger.info(f"📦 Batch size: {BATCH_SIZE} records")
    
    # Initial delay
    await asyncio.sleep(2)
    
    while True:
        try:
            logger.info("-" * 40)
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
                            
                            # Trigger remote sync if enabled
                            if sync_helpers.config.get('remote_sync', {}).get('enabled', False):
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
    return sync_helpers.check_for_new_data()


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
        check_result = sync_helpers.check_for_new_data()
        
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
            LIMIT {BATCH_SIZE}
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
            LIMIT {BATCH_SIZE}
            """
        
        logger.info(f"Executing query to get up to {BATCH_SIZE} new records")
        response = requests.get(sync_helpers.QUESTDB_QUERY_URL, params={"query": query})
        
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


async def sync_to_remote() -> Dict[str, Any]:
    """
    Sync the latest data to Hydor system
    """
    remote_sync_enabled = sync_helpers.config.get('remote_sync', {}).get('enabled', False)
    
    if not remote_sync_enabled:
        return {"status": "disabled", "message": "Remote sync not enabled"}
    
    method = sync_helpers.config.get('remote_sync', {}).get('method', 'ssh')
    logger.info(f"🚀 Starting REMOTE sync via {method}...")
    
    if method == "ssh":
        # Get latest sync directory
        import glob
        sync_dirs = sorted(glob.glob("synced_data/sync_*"))
        if not sync_dirs:
            return {"status": "error", "message": "No sync directories found"}
        
        latest_dir = Path(sync_dirs[-1])
        sync_id = latest_dir.name
        
        # SSH sync logic here
        ssh_config = sync_helpers.config.get('remote_sync', {}).get('ssh', {})
        remote_host = ssh_config.get('host', '')
        remote_user = ssh_config.get('username', '')
        remote_path = ssh_config.get('path', '')
        password = ssh_config.get('password', '')
        
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(
                hostname=remote_host,
                username=remote_user,
                password=password,
                timeout=30
            )
            
            # Use SFTP to transfer files
            sftp = ssh.open_sftp()
            
            # Create remote directory
            remote_sync_path = f"{remote_path}/{sync_id}"
            try:
                sftp.mkdir(remote_sync_path)
            except:
                pass  # Directory might already exist
            
            # Transfer all files
            files_transferred = 0
            for file_path in latest_dir.glob("*"):
                remote_file = f"{remote_sync_path}/{file_path.name}"
                sftp.put(str(file_path), remote_file)
                files_transferred += 1
            
            sftp.close()
            ssh.close()
            
            logger.info(f"✅ Remote sync successful: {files_transferred} files transferred")
            return {
                "status": "success",
                "method": "ssh",
                "files_transferred": files_transferred,
                "remote_path": remote_sync_path
            }
            
        except Exception as e:
            logger.error(f"❌ Remote sync failed: {str(e)}")
            return {"status": "error", "error": str(e)}
    
    return {"status": "unsupported", "method": method}


# ========================
# API Endpoints (Admin only)
# ========================

@router.get("/status")
async def get_auto_sync_status(admin: Dict = Depends(get_admin_user)):
    """Get status of auto-sync service"""
    last_timestamp = sync_helpers.get_last_sync_timestamp()
    check_result = sync_helpers.check_for_new_data()
    
    return {
        "service": {
            "status": "running",
            "batch_size": BATCH_SIZE,
            "sync_interval": sync_helpers.config.get('sissa', {}).get('sync_interval', '30s'),
            "auto_sync_enabled": sync_helpers.config.get('sissa', {}).get('auto_sync', True)
        },
        "last_sync": {
            "timestamp": last_timestamp,
            "has_data": last_timestamp is not None
        },
        "pending": {
            "has_new_data": check_result.get("has_new_data", False),
            "new_count": check_result.get("new_count", 0),
            "batches_pending": max(0, check_result.get("new_count", 0) // BATCH_SIZE)
        },
        "remote_sync": {
            "enabled": sync_helpers.config.get('remote_sync', {}).get('enabled', False),
            "method": sync_helpers.config.get('remote_sync', {}).get('method', 'ssh')
        },
        "timestamp": datetime.now().isoformat()
    }


@router.post("/trigger")
async def trigger_auto_sync(
    background_tasks: BackgroundTasks,
    admin: Dict = Depends(get_admin_user)
):
    """
    Manually trigger an auto-sync batch
    """
    background_tasks.add_task(sync_new_data_batch)
    
    return {
        "status": "triggered",
        "message": "Auto-sync started in background",
        "timestamp": datetime.now().isoformat()
    }


@router.post("/remote-sync/config")
async def configure_remote_sync(
    config_data: Dict[str, Any],
    admin: Dict = Depends(get_admin_user)
):
    """
    Configure remote sync settings
    """
    
    if 'remote_sync' not in sync_helpers.config:
        sync_helpers.config['remote_sync'] = {}
    
    sync_helpers.config['remote_sync'].update(config_data)
    
    # Save to config file
    import yaml
    with open(sync_helpers.config_path, 'w') as f:
        yaml.dump(sync_helpers.config, f)
    
    return {
        "status": "configured",
        "remote_sync": sync_helpers.config['remote_sync']
    }


@router.post("/remote-sync/test-ssh")
async def test_ssh_connection(
    admin: Dict = Depends(get_admin_user)):
    """
    Test SSH connection to remote host
    """
    
    ssh_config = sync_helpers.config.get('remote_sync', {}).get('ssh', {})
    remote_host = ssh_config.get('host', '')
    remote_user = ssh_config.get('username', '')
    remote_path = ssh_config.get('path', '')
    password = ssh_config.get('password', '')
    
    if not all([remote_host, remote_user, password]):
        return {
            "status": "error",
            "message": "SSH configuration incomplete",
            "required_fields": ["host", "username", "password"]
        }
    
    results = {
        "host": remote_host,
        "username": remote_user,
        "path": remote_path,
        "tests": []
    }
    
    import socket
    
    # Test DNS resolution
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
    
    # Test port connectivity
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
                "error": f"Port 22 is not reachable"
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
    
    # Test SSH authentication
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        ssh.connect(
            hostname=remote_host,
            username=remote_user,
            password=password,
            timeout=10
        )
        
        results["tests"].append({
            "name": "SSH authentication", 
            "status": "OK",
            "message": "Successfully connected"
        })
        
        # Test directory access
        try:
            sftp = ssh.open_sftp()
            try:
                sftp.stat(remote_path)
                results["tests"].append({
                    "name": "Directory access", 
                    "status": "OK"
                })
            except FileNotFoundError:
                results["tests"].append({
                    "name": "Directory access",
                    "status": "WARNING",
                    "message": f"Directory {remote_path} does not exist"
                })
            sftp.close()
        except Exception as e:
            results["tests"].append({
                "name": "SFTP access",
                "status": "FAILED",
                "error": str(e)
            })
        
        ssh.close()
        
    except Exception as e:
        results["tests"].append({
            "name": "SSH authentication",
            "status": "FAILED",
            "error": str(e)
        })
    
    return results


@router.get("/summary")
async def get_sync_summary(
    admin: Dict = Depends(get_admin_user)
):
    """Get summary of all auto-syncs"""
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
            "files": [f.name for f in Path(latest).glob("*")] if Path(latest).exists() else []
        }
    
    return summary


# Health check
@router.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "app": "auto-sync",
        "status": "running",
        "background_worker": "active" if sync_task and not sync_task.done() else "inactive",
        "timestamp": datetime.now().isoformat()
    }