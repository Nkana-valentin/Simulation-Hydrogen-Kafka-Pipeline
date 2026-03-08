#!/usr/bin/env python3
# sissa_sync.py - Simple sync that checks for new data
from fastapi import FastAPI, HTTPException, BackgroundTasks
import requests
import yaml
import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional
import logging
from contextlib import asynccontextmanager

from utility_functions import (
    check_table_exists,
    get_last_sync_timestamp,
    save_last_sync_timestamp,
    parse_questdb_response,
    save_to_local_filesystem
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load configuration
with open("TSDB.yml") as f:
    config = yaml.safe_load(f)

QUESTDB_QUERY_URL = f"http://{config['questdb']['host']}:{config['questdb']['port']}/exec"

app = FastAPI()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Start background sync worker
    """
    task = asyncio.create_task(sync_worker())
    logger.info("Sync worker started")
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        logger.info("Sync worker stopped")

app = FastAPI(lifespan=lifespan)

async def check_for_new_data() -> Dict[str, Any]:
    """
    Check if there's new data since last sync
    Returns info about new data if available
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
        
        response = requests.get(QUESTDB_QUERY_URL, params={"query": query})
        
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
    Sync only new data since last sync
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
        
        logger.info(f"Found {new_count} new records to sync")
        
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
            """
        else:
            # First sync - get all data
            query = """
            SELECT 
                timestamp,
                lab,
                sensor_id,
                measurement_type,
                unit,
                value
            FROM hydrogen_data
            ORDER BY timestamp ASC
            """
        
        logger.info(f"Executing query to get new data")
        response = requests.get(QUESTDB_QUERY_URL, params={"query": query})
        
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
            "last_timestamp": last_timestamp
        }
        
        save_result = save_to_local_filesystem(records, sync_info)
        
        if save_result.get("status") == "success":
            # Save the timestamp of the last synced record
            if records:
                save_last_sync_timestamp(records[-1]['timestamp'])
            
            result = {
                "status": "success",
                "records_synced": len(records),
                "time_range": time_range,
                "save_location": save_result.get("directory"),
                "timestamp": datetime.now().isoformat()
            }
            
            logger.info(f"SUCCESS: Synced {len(records)} records")
            logger.info(f"Time range: {time_range['start']} to {time_range['end']}")
            logger.info(f"Saved to: {save_result.get('directory')}")
            
            return result
        else:
            logger.error("Failed to save records")
            return {"status": "error", "message": "Save failed"}
        
    except Exception as e:
        logger.error(f"Error during sync: {str(e)}", exc_info=True)
        return {"status": "error", "error": str(e)}

async def sync_worker():
    """
    Background worker that checks for new data periodically
    """
    # Get sync interval from config (default 30 seconds)
    sync_interval = 60*5  # Default to 5 minutes for less frequent checks
    if 'sissa' in config and 'sync_interval' in config['sissa']:
        sync_str = config['sissa']['sync_interval']
        sync_interval = int(sync_str.rstrip('s'))
    
    logger.info(f"Sync worker started - checking every {sync_interval} seconds")
    
    # Initial delay
    await asyncio.sleep(2)
    
    while True:
        try:
            logger.info("-" * 40)
            logger.info(f"Sync check at {datetime.now().isoformat()}")
            
            # Check for new data (don't sync automatically, just check)
            check_result = await check_for_new_data()
            
            if check_result.get("has_new_data"):
                logger.info(f"✓ NEW DATA AVAILABLE: {check_result.get('new_count')} records")
                
                # Auto-sync if configured
                if config.get('sissa', {}).get('auto_sync', True):
                    logger.info("Auto-sync enabled - syncing new data...")
                    sync_result = await sync_new_data()
                    if sync_result.get("status") == "success":
                        logger.info(f"✓ Auto-sync completed: {sync_result.get('records_synced')} records")
                    else:
                        logger.error(f"✗ Auto-sync failed: {sync_result.get('error')}")
                else:
                    logger.info("Auto-sync disabled - use POST /api/sync/trigger to sync")
            else:
                logger.info("✗ No new data")
            
            logger.info(f"Next check in {sync_interval} seconds")
            await asyncio.sleep(sync_interval)
            
        except asyncio.CancelledError:
            logger.info("Sync worker cancelled")
            break
        except Exception as e:
            logger.error(f"Error in sync worker: {str(e)}")
            await asyncio.sleep(60)

# API Endpoints

@app.get("/")
async def root():
    """
    Root endpoint
    """
    table_exists = check_table_exists()
    last_sync = get_last_sync_timestamp()
    
    return {
        "name": "Simple Hydrogen Data Sync",
        "table_exists": table_exists,
        "last_sync": last_sync or "Never",
        "endpoints": {
            "check": "GET /api/sync/check - Check for new data",
            "trigger": "POST /api/sync/trigger - Manually trigger sync",
            "status": "GET /api/sync/status - Get sync status",
            "last": "GET /api/sync/last - Get last sync info"
        }
    }

@app.get("/api/sync/check")
async def check_sync():
    """
    Check if there's new data available
    """
    result = await check_for_new_data()
    
    # Add human-readable message
    if result.get("has_new_data"):
        result["message"] = f"✅ {result.get('new_count')} new records available"
    else:
        result["message"] = "❌ No new data available"
    
    return result

@app.post("/api/sync/trigger")
async def trigger_sync(background_tasks: BackgroundTasks):
    """
    Manually trigger a sync
    """
    background_tasks.add_task(sync_new_data)
    return {
        "status": "triggered",
        "message": "Sync started in background",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/api/sync/status")
async def get_status():
    """
    Get current sync status
    """
    table_exists = check_table_exists()
    last_timestamp = get_last_sync_timestamp()
    
    # Check current availability
    check_result = await check_for_new_data()
    
    return {
        "table_exists": table_exists,
        "last_sync": last_timestamp,
        "new_data_available": check_result.get("has_new_data", False),
        "new_records_count": check_result.get("new_count", 0) if check_result.get("has_new_data") else 0,
        "auto_sync": config.get('sissa', {}).get('auto_sync', True),
        "sync_interval": config.get('sissa', {}).get('sync_interval', '30s')
    }

@app.get("/api/sync/last")
async def get_last_sync():
    """
    Get information about the last sync
    """
    last_timestamp = get_last_sync_timestamp()
    
    if not last_timestamp:
        return {"message": "No sync has been performed yet"}
    
    return {
        "last_sync_timestamp": last_timestamp,
        "message": f"Last sync was at {last_timestamp}"
    }

# Debug endpoint
@app.get("/api/debug/check")
async def debug_check():
    """
    Debug endpoint with detailed information
    """
    table_exists = check_table_exists()
    last_timestamp = get_last_sync_timestamp()
    
    # Get total count
    total_count = 0
    if table_exists:
        try:
            response = requests.get(
                QUESTDB_QUERY_URL, 
                params={"query": "SELECT COUNT(*) FROM hydrogen_data"}
            )
            if response.status_code == 200:
                result = response.json()
                if result.get("dataset"):
                    total_count = result["dataset"][0][0]
        except:
            pass
    
    # Get new count if there's a last timestamp
    new_count = 0
    if last_timestamp and table_exists:
        try:
            query = f"SELECT COUNT(*) FROM hydrogen_data WHERE timestamp > '{last_timestamp}'"
            response = requests.get(QUESTDB_QUERY_URL, params={"query": query})
            if response.status_code == 200:
                result = response.json()
                if result.get("dataset"):
                    new_count = result["dataset"][0][0]
        except:
            pass
    
    return {
        "table_exists": table_exists,
        "total_records_in_db": total_count,
        "last_sync_timestamp": last_timestamp,
        "new_records_since_last_sync": new_count,
        "has_new_data": new_count > 0,
        "sync_state_file": str(Path("./synced_data/last_sync.txt").absolute()),
        "sync_state_file_exists": Path("./synced_data/last_sync.txt").exists()
    }