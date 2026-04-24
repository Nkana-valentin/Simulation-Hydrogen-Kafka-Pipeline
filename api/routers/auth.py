from datetime import datetime
from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from api.dependencies import _auth_service
from services.auth_service import AuthService


class DeviceLogin(BaseModel):
    device_id: str = Field(..., example="pressure_sensor_01")
    device_secret: str = Field(..., example="sensor123")


class ResearcherLogin(BaseModel):
    username: str = Field(..., example="dr_smith")
    password: str = Field(..., example="research2024")
    institution: str = Field(..., example="APSU")


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int
    identity: Dict[str, Any]


class VerifyRequest(BaseModel):
    token: str


router = APIRouter(tags=["Authentication"])


@router.post("/device/login", response_model=TokenResponse, status_code=status.HTTP_201_CREATED)
def device_login(body: DeviceLogin, svc: AuthService = Depends(_auth_service)):
    result = svc.login_device(body.device_id, body.device_secret)
    if not result:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid device credentials")
    return result


@router.post("/researcher/login", response_model=TokenResponse, status_code=status.HTTP_201_CREATED)
def researcher_login(body: ResearcherLogin, svc: AuthService = Depends(_auth_service)):
    result = svc.login_researcher(body.institution, body.username, body.password)
    if not result:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid researcher credentials")
    return result


@router.post("/verify")
def verify_token(body: VerifyRequest, svc: AuthService = Depends(_auth_service)):
    return svc.verify(body.token)


@router.get("/health")
def health(svc: AuthService = Depends(_auth_service)):
    return {
        "status": "healthy",
        "service": "Authentication",
        "timestamp": datetime.now().isoformat(),
        "devices_registered": svc.device_count(),
        "researchers_registered": svc.researcher_count(),
    }
