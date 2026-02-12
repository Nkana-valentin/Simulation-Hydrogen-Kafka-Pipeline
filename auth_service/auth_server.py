#!/usr/bin/env python3
"""
Authentication Service for Hydrogen Research Pipeline
JWT Token Issuance and Verification
"""
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, Any, Optional
import jwt
import time
import json
from datetime import datetime
import os

# ========================
# Configuration
# ========================
JWT_SECRET = "hydrogen-research-secret-key-2024"  # In production, use environment variable
JWT_ALGORITHM = "HS256"
TOKEN_EXPIRY_HOURS = 24

# Load device registry
with open(os.path.join(os.path.dirname(__file__), "device_registry.json"), "r") as f:
    REGISTRY = json.load(f)

# ========================
# Pydantic Models
# ========================
class DeviceLogin(BaseModel):
    device_id: str
    device_secret: str

class ResearcherLogin(BaseModel):
    username: str
    password: str
    institution: str

class TokenResponse(BaseModel):
    access_token: str
    token_type: str
    expires_in: int
    identity: Dict[str, Any]

class TokenVerifyRequest(BaseModel):
    token: str

class TokenVerifyResponse(BaseModel):
    valid: bool
    payload: Optional[Dict[str, Any]] = None
    reason: Optional[str] = None

# ========================
# FastAPI App
# ========================
app = FastAPI(title="Hydrogen Lab Authentication Service")

# ========================
# Helper Functions
# ========================
def create_token(payload: Dict[str, Any], expiry_hours: int = TOKEN_EXPIRY_HOURS) -> str:
    """Create JWT token"""
    payload_with_exp = {
        **payload,
        "exp": time.time() + (expiry_hours * 3600),
        "iat": time.time()
    }
    return jwt.encode(payload_with_exp, JWT_SECRET, algorithm=JWT_ALGORITHM)

def verify_token(token: str) -> Dict[str, Any]:
    """Verify JWT token"""
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return {"valid": True, "payload": payload}
    except jwt.ExpiredSignatureError:
        return {"valid": False, "reason": "Token expired"}
    except jwt.InvalidTokenError:
        return {"valid": False, "reason": "Invalid token"}

# ========================
# Authentication Endpoints
# ========================
@app.post("/auth/device/login", response_model=TokenResponse)
async def device_login(login: DeviceLogin):
    """Authenticate a lab device (sensor, analyzer, etc.)"""
    
    # Find device in registry
    device = None
    for d in REGISTRY["devices"]:
        if d["device_id"] == login.device_id and d["device_secret"] == login.device_secret:
            device = d
            break
    
    if not device:
        raise HTTPException(status_code=401, detail="Invalid device credentials")
    
    # Create token payload
    payload = {
        "identity_type": "device",
        "device_id": device["device_id"],
        "lab": device["lab"],
        "device_type": device["type"],
        "permissions": device["permissions"]
    }
    
    token = create_token(payload)
    
    return TokenResponse(
        access_token=token,
        token_type="bearer",
        expires_in=TOKEN_EXPIRY_HOURS * 3600,
        identity=payload
    )

@app.post("/auth/researcher/login", response_model=TokenResponse)
async def researcher_login(login: ResearcherLogin):
    """Authenticate a researcher"""
    
    # Find researcher in registry
    researcher = None
    for r in REGISTRY["researchers"]:
        if (r["username"] == login.username and 
            r["password"] == login.password and 
            r["institution"] == login.institution):
            researcher = r
            break
    
    if not researcher:
        raise HTTPException(status_code=401, detail="Invalid researcher credentials")
    
    # Create token payload
    payload = {
        "identity_type": "researcher",
        "user_id": f"{researcher['institution']}_{researcher['username']}",
        "username": researcher["username"],
        "institution": researcher["institution"],
        "roles": researcher["roles"],
        "data_access": researcher["data_access"]
    }
    
    token = create_token(payload, expiry_hours=8)  # Shorter expiry for humans
    
    return TokenResponse(
        access_token=token,
        token_type="bearer",
        expires_in=8 * 3600,
        identity=payload
    )

@app.post("/auth/verify", response_model=TokenVerifyResponse)
async def verify_token_endpoint(request: TokenVerifyRequest):
    """Verify a token (for other services)"""
    return verify_token(request.token)

@app.get("/auth/health")
async def health():
    """Health check"""
    return {
        "status": "healthy",
        "service": "Authentication Service",
        "timestamp": datetime.now().isoformat(),
        "devices_registered": len(REGISTRY["devices"]),
        "researchers_registered": len(REGISTRY["researchers"])
    }

if __name__ == "__main__":
    import uvicorn
    print("\n" + "=" * 60)
    print("🔐 HYDROGEN LAB AUTHENTICATION SERVICE")
    print("=" * 60)
    print("Starting on http://localhost:8001")
    print("\n📚 API Documentation: http://localhost:8001/docs")
    print("\n✅ Test credentials:")
    print("   Device:     pressure_sensor_01 / sensor123")
    print("   Device:     flow_sensor_02 / flow456")
    print("   Researcher: dr_smith / research2024 (APSU)")
    print("   Researcher: dr_jones / hydrogen2024 (MFI)")
    print("=" * 60 + "\n")
    
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="info")
