from fastapi import APIRouter, HTTPException, status
from  auth_service import schemas 
from  auth_service import auth_token
from  auth_service import registry
from  auth_service.config import Config
from datetime import datetime


# Optional: create a fast lookup dict at startup (highly recommended)
DEVICE_MAP: dict = {
    d["device_id"]: d
    for d in registry.REGISTRY["devices"]
}

RESEARCHER_MAP: dict = {
    (r["institution"], r["username"]): r
    for r in registry.REGISTRY["researchers"]
}
# Create router
router = APIRouter(tags=['Authentication'])


@router.post("/device/login", 
            response_model=schemas.TokenResponse, 
            status_code=status.HTTP_201_CREATED)
async def device_login(login: schemas.DeviceLogin):
    """
    Authenticate a lab device (sensor, analyzer, etc.)
    """
    device = DEVICE_MAP.get(login.device_id)

    if not device or device["device_secret"] != login.device_secret:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid device credentials"
        )

    # Build JWT payload
    payload = {
        "identity_type": "device",
        "device_id": device["device_id"],
        "lab": device["lab"],
        "device_type": device["type"],
        "permissions": device["permissions"],
    }

    token = auth_token.create_token(payload)

    return schemas.TokenResponse(
        access_token=token,
        token_type="bearer",
        expires_in=Config.TOKEN_EXPIRY_HOURS * 3600,
        identity=payload)
    
    
@router.post("/researcher/login", 
            response_model=schemas.TokenResponse,
            status_code=status.HTTP_201_CREATED)
def researcher_login(login: schemas.ResearcherLogin):
    """
    Authenticate a researcher
    """
    
    # Find researcher in registry
    key = (login.institution, login.username)
    researcher = RESEARCHER_MAP.get(key)

    if not researcher or researcher["password"] != login.password:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid researcher credentials"
        )
    
    # Create token payload
    payload = {
        "identity_type": "researcher",
        "user_id": f"{researcher['institution']}_{researcher['username']}",
        "username": researcher["username"],
        "institution": researcher["institution"],
        "roles": researcher["roles"],
        "data_access": researcher["data_access"]
    }
    
    token = auth_token.create_token(payload, expiry_hours=8)  # Shorter expiry for humans
    
    return schemas.TokenResponse(
        access_token=token,
        token_type="bearer",
        expires_in=8 * 3600,
        identity=payload
    )

@router.post("/verify", response_model=schemas.TokenVerifyResponse)
def verify_token_endpoint(request: schemas.TokenVerifyRequest):
    """
    Verify a token (for other services)
    """
    return auth_token.verify_token(request.token)


@router.get("/health")
def health():
    """
    Health check
    """
    return {
        "status": "healthy",
        "service": "Authentication Service",
        "timestamp": datetime.now().isoformat(),
        "devices_registered": len(registry.REGISTRY["devices"]),
        "researchers_registered": len(registry.REGISTRY["researchers"])
    }