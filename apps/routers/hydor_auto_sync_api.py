# apps/hydor_auto_sync_api.py
"""
Automatic SISSA Sync API - FastAPI endpoints for controlling the sync service
"""
from fastapi import APIRouter, BackgroundTasks, HTTPException, Depends
from datetime import datetime
from typing import Optional, Dict, Any
import logging
import yaml
import glob
import socket
from pathlib import Path

# Import helpers and worker functions
from apps.sync_helpers import SyncHelpers
from apps.hydor_auto_sync_worker import (sync_worker, 
                                    sync_new_data_batch, 
                                    check_for_new_data_async,
                                    sync_to_remote, 
                                    BATCH_SIZE)
from auth_service.auth_token import get_admin_user
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

# Setup logging
logger = logging.getLogger(__name__)

# Initialize helpers
sync_helpers = SyncHelpers()
security = HTTPBearer()

# Global reference to sync task (to be set by the main app)
sync_task = None

# Create router
router = APIRouter(tags=['Auto Sync To Hydor'])

# ========================
# API Endpoints (Admin only)
# ========================

@router.get("/status")
async def get_auto_sync_status(admin: Dict = Depends(get_admin_user)):
    """
    Get status of auto-sync service
    """
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
    with open(sync_helpers.config_path, 'w') as f:
        yaml.dump(sync_helpers.config, f)
    
    return {
        "status": "configured",
        "remote_sync": sync_helpers.config['remote_sync']
    }


@router.post("/remote-sync/test-ssh")
async def test_ssh_connection(
    admin: Dict = Depends(get_admin_user)
):
    """
    Test SSH connection to remote host
    """
    import paramiko
    
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
async def health_check(admin: Dict = Depends(get_admin_user)):
    """Health check endpoint"""
    return {
        "app": "auto-sync",
        "status": "running",
        "background_worker": "active" if sync_task and not sync_task.done() else "inactive",
        "timestamp": datetime.now().isoformat()
    }