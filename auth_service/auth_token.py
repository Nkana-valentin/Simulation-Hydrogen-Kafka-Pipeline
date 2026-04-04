import jwt
from datetime import datetime, timedelta, timezone
from .config import Config
from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import Dict, Any


# =================================
# Token Creation & Verification
# =================================

security = HTTPBearer()


def create_token(payload: dict,
                 expiry_hours: int = Config.TOKEN_EXPIRY_HOURS) -> str:
    """
    Create JWT token
    """
    issued_at = datetime.now(timezone.utc)
    expire = issued_at + timedelta(hours=expiry_hours)
    to_encode = payload.copy()
    to_encode.update({"exp": expire, "iat": issued_at})
    return jwt.encode(to_encode, Config.JWT_SECRET, algorithm=Config.JWT_ALGORITHM)



def verify_token(token: str) -> dict:
    """
    Verify any JWT token
    """
    try:
        payload = jwt.decode(token, Config.JWT_SECRET, algorithms=[Config.JWT_ALGORITHM])
        return {"valid": True, "payload": payload}
    except jwt.ExpiredSignatureError:
        return {"valid": False, "reason": "Token expired"}
    except jwt.InvalidTokenError:
        return {"valid": False, "reason": "Invalid token"}



def verify_researcher_token(token: str) -> dict:
    """
    Verify JWT token AND ensure it belongs to a researcher
    """
    try:
        payload = jwt.decode(token, Config.JWT_SECRET, algorithms=[Config.JWT_ALGORITHM])
        if payload.get("identity_type") != "researcher":
            return {"valid": False, "reason": "Not a researcher token"}
        return {"valid": True, "payload": payload}
    except jwt.ExpiredSignatureError:
        return {"valid": False, "reason": "Token expired"}
    except jwt.InvalidTokenError:
        return {"valid": False, "reason": "Invalid token"}



def get_current_researcher(
    credentials: HTTPAuthorizationCredentials = Depends(security)) -> Dict[str, Any]:
    token = credentials.credentials
    result = verify_researcher_token(token)
    if not result["valid"]:
        raise HTTPException(
            status_code=401,
            detail=result.get("reason",
                              "Authentication failed")
        )
    return result["payload"]


# =================================
# Admin Authentication Helper
# =================================
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
