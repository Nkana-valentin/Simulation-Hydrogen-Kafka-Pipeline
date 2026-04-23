"""
Token issuance — bridges domain/auth.py and the registry repositories.
"""
from typing import Any, Dict, Optional

import bcrypt

from domain import auth as domain_auth
from infrastructure.registry.repository import DeviceRepository, ResearcherRepository


def _verify_hash(plaintext: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(plaintext.encode(), hashed.encode())
    except Exception:
        return False


class AuthService:
    def __init__(
        self,
        device_repo: DeviceRepository,
        researcher_repo: ResearcherRepository,
        jwt_secret: str,
        jwt_algorithm: str = "HS256",
        device_expiry_hours: int = 24,
        researcher_expiry_hours: int = 8,
    ) -> None:
        self._devices = device_repo
        self._researchers = researcher_repo
        self._secret = jwt_secret
        self._algorithm = jwt_algorithm
        self._device_expiry = device_expiry_hours
        self._researcher_expiry = researcher_expiry_hours

    def login_device(self, device_id: str, device_secret: str) -> Optional[Dict[str, Any]]:
        device = self._devices.find_by_id(device_id)
        if not device:
            return None
        if not _verify_hash(device_secret, device.get("device_secret_hash", "")):
            return None

        payload = {
            "identity_type": "device",
            "device_id": device["device_id"],
            "lab": device["lab"],
            "device_type": device.get("type"),
            "permissions": device.get("permissions", []),
        }
        token = domain_auth.create_token(
            payload, self._secret, self._algorithm, self._device_expiry
        )
        return {
            "access_token": token,
            "token_type": "bearer",
            "expires_in": self._device_expiry * 3600,
            "identity": payload,
        }

    def login_researcher(
        self, institution: str, username: str, password: str
    ) -> Optional[Dict[str, Any]]:
        researcher = self._researchers.find(institution, username)
        if not researcher:
            return None
        if not _verify_hash(password, researcher.get("password_hash", "")):
            return None

        payload = {
            "identity_type": "researcher",
            "user_id": f"{institution}_{username}",
            "username": username,
            "institution": institution,
            "roles": researcher.get("roles", []),
            "data_access": researcher.get("data_access", []),
        }
        token = domain_auth.create_token(
            payload, self._secret, self._algorithm, self._researcher_expiry
        )
        return {
            "access_token": token,
            "token_type": "bearer",
            "expires_in": self._researcher_expiry * 3600,
            "identity": payload,
        }

    def verify(self, token: str) -> Dict[str, Any]:
        return domain_auth.verify_token(token, self._secret, self._algorithm)

    def device_count(self) -> int:
        return len(self._devices.all())

    def researcher_count(self) -> int:
        return len(self._researchers.all())
