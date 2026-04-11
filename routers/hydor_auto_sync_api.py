# apps/hydor_auto_sync_api.py
"""
Automatic SISSA Sync API - FastAPI endpoints for controlling the sync service
"""
from fastapi import APIRouter, BackgroundTasks, HTTPException, Depends
from fastapi.security import HTTPBearer
from datetime import datetime
from typing import Dict, Any, List, Optional
import logging
import yaml
import glob
import socket
from pathlib import Path
import os
import asyncio
import paramiko
from pydantic import BaseModel, Field

# Import helpers and worker functions
from synchronization.sync_helpers import SyncHelpers
from synchronization.hydor_auto_sync_worker import (sync_worker, 
                                    sync_new_data_batch, 
                                    check_for_new_data_async,
                                    sync_to_remote, 
                                    BATCH_SIZE)
from auth_service.auth_token import get_admin_user

# Setup logging
logger = logging.getLogger(__name__)

# Initialize helpers
sync_helpers = SyncHelpers()
security = HTTPBearer()

# Global reference to sync task (to be set by the main app)
sync_task = None
AUTO_SYNC_ENABLED = True
MAX_LOG_ENTRIES = 500


class InMemoryLogHandler(logging.Handler):
    """Capture recent log messages for API consumption."""

    def __init__(self, storage: List[Dict[str, str]]):
        super().__init__()
        self.storage = storage

    def emit(self, record):
        self.storage.append({
            "timestamp": datetime.now().isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name
        })
        if len(self.storage) > MAX_LOG_ENTRIES:
            del self.storage[:-MAX_LOG_ENTRIES]


logs_buffer: List[Dict[str, str]] = []
if not any(getattr(handler, "_hydor_log_buffer", False) for handler in logger.handlers):
    memory_handler = InMemoryLogHandler(logs_buffer)
    memory_handler._hydor_log_buffer = True
    logger.addHandler(memory_handler)

sync_metrics: Dict[str, Any] = {
    "check_requests": 0,
    "trigger_requests": 0,
    "manual_sync_requests": 0,
    "remote_sync_requests": 0,
    "success_count": 0,
    "failure_count": 0,
    "cancel_count": 0,
    "pause_count": 0,
    "resume_count": 0,
    "last_success_at": None,
    "last_failure_at": None
}


class RemoteSyncConfigUpdate(BaseModel):
    enabled: Optional[bool] = None
    host: Optional[str] = None
    username: Optional[str] = None
    path: Optional[str] = None
    password: Optional[str] = None
    key_path: Optional[str] = None


class SyncNowRequest(BaseModel):
    background: bool = Field(default=False, description="Run sync in background task")
    include_remote: bool = Field(default=False, description="Run remote sync after local sync")
    force: bool = Field(default=False, description="Reserved for future behavior")


def _ensure_config_scaffold():
    if "sissa" not in sync_helpers.config:
        sync_helpers.config["sissa"] = {}
    if "remote_sync" not in sync_helpers.config:
        sync_helpers.config["remote_sync"] = {}
    if "ssh" not in sync_helpers.config["remote_sync"]:
        sync_helpers.config["remote_sync"]["ssh"] = {}


def _save_yaml_config():
    with open(sync_helpers.config_path, "w", encoding="utf-8") as config_file:
        yaml.dump(sync_helpers.config, config_file)


def _record_result(status: str):
    if status in {"success", "configured", "triggered"}:
        sync_metrics["success_count"] += 1
        sync_metrics["last_success_at"] = datetime.now().isoformat()
    elif status in {"error", "failed"}:
        sync_metrics["failure_count"] += 1
        sync_metrics["last_failure_at"] = datetime.now().isoformat()

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
            "auto_sync_enabled": AUTO_SYNC_ENABLED
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
    sync_metrics["trigger_requests"] += 1
    _record_result("triggered")
    
    return {
        "status": "triggered",
        "message": "Auto-sync started in background",
        "timestamp": datetime.now().isoformat()
    }


# @router.post("/remote-sync/config")
# async def configure_remote_sync(
#     config_data: Dict[str, Any],
#     admin: Dict = Depends(get_admin_user)
# ):
#     """
#     Configure remote sync settings
#     """
    
#     if 'remote_sync' not in sync_helpers.config:
#         sync_helpers.config['remote_sync'] = {}
    
#     sync_helpers.config['remote_sync'].update(config_data)
    
#     # Save to config file
#     with open(sync_helpers.config_path, 'w') as f:
#         yaml.dump(sync_helpers.config, f)
    
#     return {
#         "status": "configured",
#         "remote_sync": sync_helpers.config['remote_sync']
#     }


@router.post("/remote-sync/test-ssh")
async def test_ssh_connection(
    admin: Dict = Depends(get_admin_user)
):
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


@router.get("/health")
async def health_check(admin: Dict = Depends(get_admin_user)):
    """Health check endpoint"""
    return {
        "app": "auto-sync",
        "status": "running",
        "background_worker": "active" if sync_task and not sync_task.done() else "inactive",
        "auto_sync_enabled": AUTO_SYNC_ENABLED,
        "timestamp": datetime.now().isoformat()
    }


@router.get("/remote-sync/config")
async def get_remote_sync_config(admin: Dict = Depends(get_admin_user)):
    """Read current remote sync configuration."""
    ssh_config = sync_helpers.config.get("remote_sync", {}).get("ssh", {})
    return {
        "enabled": sync_helpers.config.get("remote_sync", {}).get("enabled", False),
        "method": sync_helpers.config.get("remote_sync", {}).get("method", "ssh"),
        "ssh": {
            "host": ssh_config.get("host") or os.getenv("SSH_HOST"),
            "username": ssh_config.get("username") or os.getenv("SSH_USER"),
            "path": ssh_config.get("path") or os.getenv("SSH_REMOTE_PATH"),
            "key_path": ssh_config.get("key_path") or os.getenv("SSH_KEY_PATH"),
            "password_set": bool(ssh_config.get("password"))
        },
        "timestamp": datetime.now().isoformat()
    }


@router.put("/remote-sync/config")
async def update_remote_sync_config(
    config_data: RemoteSyncConfigUpdate,
    admin: Dict = Depends(get_admin_user)
):
    """Update remote sync configuration and persist YAML config."""
    _ensure_config_scaffold()

    remote_sync_config = sync_helpers.config["remote_sync"]
    ssh_config = remote_sync_config["ssh"]
    updates = config_data.model_dump(exclude_none=True)

    if "enabled" in updates:
        remote_sync_config["enabled"] = updates["enabled"]

    for field, key in [
        ("host", "host"),
        ("username", "username"),
        ("path", "path"),
        ("password", "password"),
        ("key_path", "key_path")
    ]:
        if field in updates:
            ssh_config[key] = updates[field]

    _save_yaml_config()
    _record_result("configured")
    return {
        "status": "configured",
        "remote_sync": remote_sync_config,
        "timestamp": datetime.now().isoformat()
    }


@router.get("/pending")
async def get_pending_data(admin: Dict = Depends(get_admin_user)):
    """Return pending/new data information without triggering sync."""
    check_result = sync_helpers.check_for_new_data()
    sync_metrics["check_requests"] += 1
    return {
        "has_new_data": check_result.get("has_new_data", False),
        "new_count": check_result.get("new_count", 0),
        "batches_pending": max(0, check_result.get("new_count", 0) // BATCH_SIZE),
        "timestamp": datetime.now().isoformat()
    }


async def _sync_now_impl(include_remote: bool) -> Dict[str, Any]:
    sync_result = await sync_new_data_batch()
    if sync_result.get("status") == "success":
        _record_result("success")
        if include_remote:
            remote_result = await sync_to_remote()
            if remote_result.get("status") == "success":
                _record_result("success")
            elif remote_result.get("status") == "error":
                _record_result("error")
            sync_result["remote_sync"] = remote_result
    else:
        _record_result("error")
    return sync_result


@router.post("/sync-now")
async def sync_now(
    request: SyncNowRequest,
    background_tasks: BackgroundTasks,
    admin: Dict = Depends(get_admin_user)
):
    """Run sync immediately, with optional remote sync and background execution."""
    sync_metrics["manual_sync_requests"] += 1
    if request.background:
        background_tasks.add_task(_sync_now_impl, request.include_remote)
        return {
            "status": "triggered",
            "message": "Sync started in background",
            "include_remote": request.include_remote,
            "timestamp": datetime.now().isoformat()
        }

    result = await _sync_now_impl(request.include_remote)
    return result


@router.get("/history")
async def get_sync_history(limit: int = 20, admin: Dict = Depends(get_admin_user)):
    """Get recent sync directories as lightweight history."""
    sync_dirs = sorted(glob.glob("synced_data/sync_*"), reverse=True)
    items = []
    for directory in sync_dirs[: max(1, min(limit, 100))]:
        path_obj = Path(directory)
        stats = path_obj.stat() if path_obj.exists() else None
        items.append({
            "sync_id": path_obj.name,
            "directory": directory,
            "file_count": len(list(path_obj.glob("*"))) if path_obj.exists() else 0,
            "modified_at": datetime.fromtimestamp(stats.st_mtime).isoformat() if stats else None
        })

    return {"count": len(items), "items": items, "timestamp": datetime.now().isoformat()}


@router.get("/history/{sync_id}")
async def get_sync_history_detail(sync_id: str, admin: Dict = Depends(get_admin_user)):
    """Get detailed info about one sync directory."""
    sync_path = Path("synced_data") / sync_id
    if not sync_path.exists() or not sync_path.is_dir():
        raise HTTPException(status_code=404, detail=f"Sync ID not found: {sync_id}")

    files = []
    for file_path in sorted(sync_path.glob("*")):
        if file_path.is_file():
            files.append({
                "name": file_path.name,
                "size_bytes": file_path.stat().st_size
            })

    return {
        "sync_id": sync_id,
        "directory": str(sync_path),
        "file_count": len(files),
        "files": files,
        "timestamp": datetime.now().isoformat()
    }


@router.post("/remote-sync/run")
async def run_remote_sync(admin: Dict = Depends(get_admin_user)):
    """Run remote sync immediately."""
    sync_metrics["remote_sync_requests"] += 1
    result = await sync_to_remote()
    if result.get("status") == "success":
        _record_result("success")
    elif result.get("status") == "error":
        _record_result("error")
    return result


@router.post("/pause")
async def pause_auto_sync(admin: Dict = Depends(get_admin_user)):
    """Pause auto-sync worker behavior."""
    global AUTO_SYNC_ENABLED
    AUTO_SYNC_ENABLED = False
    sync_metrics["pause_count"] += 1
    sync_helpers.config.setdefault("sissa", {})["auto_sync"] = False
    return {"status": "paused", "timestamp": datetime.now().isoformat()}


@router.post("/resume")
async def resume_auto_sync(admin: Dict = Depends(get_admin_user)):
    """Resume auto-sync worker behavior."""
    global AUTO_SYNC_ENABLED
    AUTO_SYNC_ENABLED = True
    sync_metrics["resume_count"] += 1
    sync_helpers.config.setdefault("sissa", {})["auto_sync"] = True
    return {"status": "running", "timestamp": datetime.now().isoformat()}


@router.post("/cancel")
async def cancel_auto_sync(admin: Dict = Depends(get_admin_user)):
    """Cancel currently running sync task if present."""
    if sync_task and not sync_task.done():
        sync_task.cancel()
        sync_metrics["cancel_count"] += 1
        try:
            await asyncio.wait_for(sync_task, timeout=2)
        except Exception:
            pass
        return {"status": "cancelled", "timestamp": datetime.now().isoformat()}
    return {"status": "no_active_task", "timestamp": datetime.now().isoformat()}


@router.get("/metrics")
async def get_sync_metrics(admin: Dict = Depends(get_admin_user)):
    """Get in-memory API usage and sync metrics."""
    return {
        "metrics": sync_metrics,
        "timestamp": datetime.now().isoformat()
    }


@router.get("/logs")
async def get_sync_logs(limit: int = 100, admin: Dict = Depends(get_admin_user)):
    """Get recent in-memory logs for auto-sync API."""
    bounded_limit = max(1, min(limit, MAX_LOG_ENTRIES))
    return {
        "count": bounded_limit,
        "items": logs_buffer[-bounded_limit:],
        "timestamp": datetime.now().isoformat()
    }


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

