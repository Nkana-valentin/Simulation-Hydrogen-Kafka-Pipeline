"""
Abstract registry interface + JSON-file implementation.
Swap JsonFile* for a database-backed version without touching any router or service.
"""
import json
import os
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class DeviceRepository(ABC):
    @abstractmethod
    def find_by_id(self, device_id: str) -> Optional[Dict[str, Any]]:
        ...

    @abstractmethod
    def all(self) -> List[Dict[str, Any]]:
        ...


class ResearcherRepository(ABC):
    @abstractmethod
    def find(self, institution: str, username: str) -> Optional[Dict[str, Any]]:
        ...

    @abstractmethod
    def all(self) -> List[Dict[str, Any]]:
        ...


class JsonFileDeviceRepository(DeviceRepository):
    def __init__(self, registry_path: str) -> None:
        with open(registry_path) as f:
            data = json.load(f)
        self._devices: Dict[str, Dict[str, Any]] = {
            d["device_id"]: d for d in data.get("devices", [])
        }

    def find_by_id(self, device_id: str) -> Optional[Dict[str, Any]]:
        return self._devices.get(device_id)

    def all(self) -> List[Dict[str, Any]]:
        return list(self._devices.values())


class JsonFileResearcherRepository(ResearcherRepository):
    def __init__(self, registry_path: str) -> None:
        with open(registry_path) as f:
            data = json.load(f)
        self._researchers: Dict[tuple, Dict[str, Any]] = {
            (r["institution"], r["username"]): r
            for r in data.get("researchers", [])
        }

    def find(self, institution: str, username: str) -> Optional[Dict[str, Any]]:
        return self._researchers.get((institution, username))

    def all(self) -> List[Dict[str, Any]]:
        return list(self._researchers.values())


_DEFAULT_REGISTRY = os.path.join(
    os.path.dirname(__file__), "../../auth_service/device_registry.json"
)


def default_device_repo() -> JsonFileDeviceRepository:
    return JsonFileDeviceRepository(os.path.normpath(_DEFAULT_REGISTRY))


def default_researcher_repo() -> JsonFileResearcherRepository:
    return JsonFileResearcherRepository(os.path.normpath(_DEFAULT_REGISTRY))
