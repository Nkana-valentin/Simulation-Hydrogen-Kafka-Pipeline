import glob
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from pydantic import BaseModel

from api.dependencies import _sync_service, get_admin_user
from config.settings import Settings, get_settings
from infrastructure.ssh.transfer import SSHTransferClient
from services.sync_service import SyncService
from workers.auto_sync_worker import BATCH_SIZE, sync_new_data_batch, sync_to_remote

router = APIRouter(tags=["Auto Sync to Hydor"])

_sync_metrics: Dict[str, Any] = {
    "trigger_requests": 0,
    "manual_sync_requests": 0,
    "remote_sync_requests": 0,
    "success_count": 0,
    "failure_count": 0,
    "last_success_at": None,
    "last_failure_at": None,
}
_AUTO_SYNC_ENABLED = True


def _record(status: str) -> None:
    if status == "success":
        _sync_metrics["success_count"] += 1
        _sync_metrics["last_success_at"] = datetime.now().isoformat()
    elif status == "error":
        _sync_metrics["failure_count"] += 1
        _sync_metrics["last_failure_at"] = datetime.now().isoformat()


class SyncNowRequest(BaseModel):
    background: bool = False
    include_remote: bool = False


@router.get("/status")
async def get_status(
    admin: Dict = Depends(get_admin_user),
    svc: SyncService = Depends(_sync_service),
):
    check = svc.check_for_new_data()
    return {
        "service": {
            "status": "running",
            "batch_size": BATCH_SIZE,
            "auto_sync_enabled": _AUTO_SYNC_ENABLED,
        },
        "last_sync": {"timestamp": svc.get_last_sync_timestamp()},
        "pending": {
            "has_new_data": check.get("has_new_data", False),
            "new_count": check.get("new_count", 0),
        },
        "timestamp": datetime.now().isoformat(),
    }


@router.post("/trigger")
async def trigger(background_tasks: BackgroundTasks, admin: Dict = Depends(get_admin_user)):
    background_tasks.add_task(sync_new_data_batch)
    _sync_metrics["trigger_requests"] += 1
    return {"status": "triggered", "timestamp": datetime.now().isoformat()}


@router.post("/sync-now")
async def sync_now(
    request: SyncNowRequest,
    background_tasks: BackgroundTasks,
    admin: Dict = Depends(get_admin_user),
):
    _sync_metrics["manual_sync_requests"] += 1

    async def _run():
        result = await sync_new_data_batch()
        _record("success" if result.get("status") == "success" else "error")
        if request.include_remote and result.get("status") == "success":
            await sync_to_remote()
        return result

    if request.background:
        background_tasks.add_task(_run)
        return {"status": "triggered", "timestamp": datetime.now().isoformat()}
    return await _run()


@router.get("/pending")
async def get_pending(admin: Dict = Depends(get_admin_user), svc: SyncService = Depends(_sync_service)):
    check = svc.check_for_new_data()
    return {
        "has_new_data": check.get("has_new_data", False),
        "new_count": check.get("new_count", 0),
        "timestamp": datetime.now().isoformat(),
    }


@router.post("/remote-sync/run")
async def run_remote(admin: Dict = Depends(get_admin_user)):
    _sync_metrics["remote_sync_requests"] += 1
    result = await sync_to_remote()
    _record("success" if result.get("status") == "success" else "error")
    return result


@router.post("/remote-sync/test-ssh")
async def test_ssh(
    admin: Dict = Depends(get_admin_user),
    settings: Settings = Depends(get_settings),
):
    if not settings.ssh_host:
        raise HTTPException(status_code=400, detail="SSH not configured")
    client = SSHTransferClient(
        host=settings.ssh_host,
        user=settings.ssh_user,
        key_path=settings.ssh_key_path,
        password=settings.ssh_password.get_secret_value(),
    )
    return client.test_connectivity()


@router.get("/remote-sync/config")
async def get_remote_config(
    admin: Dict = Depends(get_admin_user),
    settings: Settings = Depends(get_settings),
):
    return {
        "enabled": settings.remote_sync_enabled,
        "ssh": {
            "host": settings.ssh_host,
            "user": settings.ssh_user,
            "remote_path": settings.ssh_remote_path,
            "key_path": settings.ssh_key_path,
            "password_set": bool(settings.ssh_password.get_secret_value()),
        },
    }


@router.get("/history")
async def get_history(limit: int = 20, admin: Dict = Depends(get_admin_user)):
    dirs = sorted(glob.glob("synced_data/sync_*"), reverse=True)
    items = []
    for d in dirs[: max(1, min(limit, 100))]:
        p = Path(d)
        stat = p.stat() if p.exists() else None
        items.append({
            "sync_id": p.name,
            "file_count": len(list(p.glob("*"))) if p.exists() else 0,
            "modified_at": datetime.fromtimestamp(stat.st_mtime).isoformat() if stat else None,
        })
    return {"count": len(items), "items": items}


@router.get("/history/{sync_id}")
async def get_history_detail(sync_id: str, admin: Dict = Depends(get_admin_user)):
    path = Path("synced_data") / sync_id
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Not found: {sync_id}")
    files = [{"name": f.name, "size_bytes": f.stat().st_size} for f in sorted(path.glob("*")) if f.is_file()]
    return {"sync_id": sync_id, "file_count": len(files), "files": files}


@router.get("/summary")
async def get_summary(admin: Dict = Depends(get_admin_user)):
    dirs = sorted(glob.glob("synced_data/sync_*"))
    latest = None
    if dirs:
        p = Path(dirs[-1])
        latest = {"directory": dirs[-1], "files": [f.name for f in p.glob("*") if f.is_file()]}
    return {"total_syncs": len(dirs), "latest_sync": latest}


@router.get("/metrics")
async def get_metrics(admin: Dict = Depends(get_admin_user)):
    return {"metrics": _sync_metrics, "timestamp": datetime.now().isoformat()}


@router.post("/pause")
async def pause(admin: Dict = Depends(get_admin_user)):
    global _AUTO_SYNC_ENABLED
    _AUTO_SYNC_ENABLED = False
    return {"status": "paused", "timestamp": datetime.now().isoformat()}


@router.post("/resume")
async def resume(admin: Dict = Depends(get_admin_user)):
    global _AUTO_SYNC_ENABLED
    _AUTO_SYNC_ENABLED = True
    return {"status": "running", "timestamp": datetime.now().isoformat()}
