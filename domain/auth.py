"""
Pure JWT functions.  No FastAPI, no config imports.
Callers pass the secret explicitly so these functions are trivially testable.
"""
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

import jwt


def create_token(
    payload: Dict[str, Any],
    secret: str,
    algorithm: str = "HS256",
    expiry_hours: int = 24,
) -> str:
    now = datetime.now(timezone.utc)
    to_encode = {
        **payload,
        "iat": now,
        "exp": now + timedelta(hours=expiry_hours),
    }
    return jwt.encode(to_encode, secret, algorithm=algorithm)


def decode_token(token: str, secret: str, algorithm: str = "HS256") -> Dict[str, Any]:
    """
    Decode and verify a JWT.
    Raises jwt.ExpiredSignatureError or jwt.InvalidTokenError on failure.
    """
    return jwt.decode(token, secret, algorithms=[algorithm])


def verify_token(token: str, secret: str, algorithm: str = "HS256") -> Dict[str, Any]:
    """
    Return {"valid": bool, "payload": dict | None, "reason": str | None}.
    """
    try:
        payload = decode_token(token, secret, algorithm)
        return {"valid": True, "payload": payload}
    except jwt.ExpiredSignatureError:
        return {"valid": False, "payload": None, "reason": "Token expired"}
    except jwt.InvalidTokenError:
        return {"valid": False, "payload": None, "reason": "Invalid token"}


def verify_researcher_token(
    token: str,
    secret: str,
    algorithm: str = "HS256") -> Dict[str, Any]:
    result = verify_token(token, secret, algorithm)
    if result["valid"] and result["payload"].get("identity_type") != "researcher":
        return {"valid": False, "payload": None, "reason": "Not a researcher token"}
    return result
