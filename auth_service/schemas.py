from pydantic import BaseModel, Field
from typing import Dict, Any, Optional


# ========================
# Pydantic Models
# ========================

class DeviceLogin(BaseModel):
    """Credentials for device authentication."""
    device_id: str = Field(
        ...,
        description="Unique device identifier (e.g. sensor-lab1-001)",
        example="sensor-lab1-001",
    )
    device_secret: str = Field(
        ...,
        description="Secret key configured for this device",
        example="supersecret123",
    )


class ResearcherLogin(BaseModel):
    """Credentials for researcher authentication."""
    username: str = Field(..., description="Researcher username", example="jane_doe")
    password: str = Field(..., description="Researcher password", example="MyPassw0rd!")
    institution: str = Field(..., description="Institution / lab name", example="ETH Zurich")


class TokenResponse(BaseModel):
    """JWT token response returned after successful login."""
    access_token: str = Field(
        ...,
        description="The signed JWT access token",
        example="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZGVudGl0eV90eXBlIjoiZGV2aWNlIiwiZGV2aWNlX2lkIjoic2Vuc29yLWxhYjEtMDAxIn0.signature",
    )
    token_type: str = Field("bearer", description="Token type (always 'bearer')")
    expires_in: int = Field(
        ...,
        description="Lifetime of the token in seconds",
        example=28800,
    )
    identity: Dict[str, Any] = Field(
        ...,
        description="Identity claims (device or researcher info)",
        example={
            "identity_type": "device",
            "device_id": "sensor-lab1-001",
            "lab": "Lab A",
            "device_type": "spectrometer",
            "permissions": ["read", "write"],
        },
    )


class TokenVerifyRequest(BaseModel):
    """Request body to verify a token."""
    token: str = Field(
        ...,
        description="JWT token to validate",
        example="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
    )


class TokenVerifyResponse(BaseModel):
    """Result of token verification (used by other microservices)."""
    valid: bool = Field(..., description="True if the token is valid")
    payload: Optional[Dict[str, Any]] = Field(
        None,
        description="Decoded JWT payload (only present when valid=True)",
        example={
            "identity_type": "researcher",
            "user_id": "ETH_Zurich_jane_doe",
            "username": "jane_doe",
            "institution": "ETH Zurich",
            "roles": ["admin"],
            "data_access": ["hydrogen_data"],
            "exp": 1740000000,
            "iat": 1739996400,
        },
    )
    reason: Optional[str] = Field(
        None,
        description="Reason why the token is invalid (only present when valid=False)",
        example="Token expired",
    )