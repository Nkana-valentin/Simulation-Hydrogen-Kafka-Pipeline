from typing import Any, Dict, Optional
from pydantic import BaseModel


class AuthBlock(BaseModel):
    authenticated: bool
    device_id: str
    lab: str
    device_type: Optional[str] = None
    auth_method: Optional[str] = None


class TelemetryRecord(BaseModel):
    timestamp: str
    auth: AuthBlock

    # Sensor fields — all optional because incoming records may be partial
    FC_STATE:   Optional[float] = None
    H2_001FT:   Optional[float] = None
    H2_001PT:   Optional[float] = None
    H2_002PT:   Optional[float] = None
    H2_003PT:   Optional[float] = None
    H2_005PT:   Optional[float] = None
    CA_001FC:   Optional[float] = None
    H2_001TT:   Optional[float] = None
    H2_002TT:   Optional[float] = None
    H2_003TT:   Optional[float] = None
    H2_005TT:   Optional[float] = None
    FC_STACK_V: Optional[float] = None
    FC_STACK_i: Optional[float] = None

    model_config = {"extra": "allow"}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TelemetryRecord":
        return cls(**data)

    def is_authenticated(self) -> bool:
        return self.auth.authenticated

    def to_storage_dict(self, schema_fields: list[str]) -> Dict[str, Any]:
        """
        Return only the columns present in schema_fields plus timestamp.
        """
        raw = self.model_dump(exclude={"auth"})
        payload = {k: v for k, v in raw.items() if k in schema_fields and v is not None}
        payload["timestamp"] = self.timestamp
        return payload
