"""
FastAPI dependencies — thin wiring layer between HTTP and services.
"""
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from config.settings import Settings, get_settings
from domain import auth as domain_auth
from infrastructure.questdb.client import QuestDBClient
from infrastructure.questdb.schema import load_schema
from infrastructure.registry.repository import default_device_repo, default_researcher_repo
from services.auth_service import AuthService
from services.sync_service import SyncService

_security = HTTPBearer()

# Module-level singletons — avoids the lru_cache + Depends(Settings) unhashable-type trap.
_auth_svc: Optional[AuthService] = None
_sync_svc: Optional[SyncService] = None


def _auth_service(settings: Settings = Depends(get_settings)) -> AuthService:
    global _auth_svc
    if _auth_svc is None:
        _auth_svc = AuthService(
            device_repo=default_device_repo(),
            researcher_repo=default_researcher_repo(),
            jwt_secret=settings.jwt_secret,
            jwt_algorithm=settings.jwt_algorithm,
            device_expiry_hours=settings.token_expiry_hours,
        )
    return _auth_svc


def _sync_service(settings: Settings = Depends(get_settings)) -> SyncService:
    global _sync_svc
    if _sync_svc is None:
        schema = load_schema(settings.tsdb_config_path)
        db = QuestDBClient(host=settings.questdb_host, port=settings.questdb_port)
        _sync_svc = SyncService(
            questdb=db,
            table_name=settings.kafka_topic_validated,
            sync_dir=Path(settings.local_sync_dir),
            schema_columns=schema["fields"],
        )
    return _sync_svc


# ------------------------------------------------------------------
# Per-request auth dependencies
# ------------------------------------------------------------------

def get_current_researcher(
    credentials: HTTPAuthorizationCredentials = Depends(_security),
    settings: Settings = Depends(get_settings),
) -> Dict[str, Any]:
    result = domain_auth.verify_researcher_token(
        credentials.credentials, settings.jwt_secret, settings.jwt_algorithm
    )
    if not result["valid"]:
        raise HTTPException(status_code=401, detail=result.get("reason", "Unauthorized"))
    return result["payload"]


def get_admin_user(
    credentials: HTTPAuthorizationCredentials = Depends(_security),
    settings: Settings = Depends(get_settings),
) -> Dict[str, Any]:
    result = domain_auth.verify_researcher_token(
        credentials.credentials, settings.jwt_secret, settings.jwt_algorithm
    )
    if not result["valid"]:
        raise HTTPException(status_code=401, detail=result.get("reason", "Unauthorized"))
    user = result["payload"]
    if "admin" not in user.get("roles", []):
        raise HTTPException(status_code=403, detail="Admin access required")
    return user
