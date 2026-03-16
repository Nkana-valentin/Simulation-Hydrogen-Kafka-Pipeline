# apps/sissa_auto_sync_worker.py
"""
Automatic SISSA Sync Worker - Background sync worker logic
"""
from datetime import datetime
from typing import Optional, Dict, Any
import asyncio
import logging
import requests
import paramiko
from pathlib import Path
import glob

# Import helpers
from .sync_helpers import SyncHelpers
from ingestion.query_builder import DataQueryBuilder

# Setup logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize helpers
sync_helpers = SyncHelpers()
data_query_builder = DataQueryBuilder()

# Global variables
BATCH_SIZE = 50  # Default batch size


# ========================
# Background Worker Functions
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
            query = data_query_builder.build_query(
                last_timestamp=last_timestamp, batch_size=BATCH_SIZE)
        else:
            # First sync - get all data up to BATCH_SIZE
            query = data_query_builder.build_query(batch_size=BATCH_SIZE)
        
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