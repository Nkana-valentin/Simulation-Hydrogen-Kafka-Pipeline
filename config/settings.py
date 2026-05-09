from functools import lru_cache
from typing import Any

from pydantic import SecretStr, field_validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Kafka
    kafka_broker: str = "broker:9092"
    kafka_topic_raw: str = "raw_h2_data"
    validated_table: str = "validated_h2_data"

    # QuestDB
    questdb_host: str = "questdb"
    questdb_port: int = 9000

    # JWT — no default; must be set in environment; minimum 32 characters
    jwt_secret: str
    jwt_algorithm: str = "HS256"
    token_expiry_hours: int = 24

    @field_validator("jwt_secret")
    @classmethod
    def _validate_jwt_secret(cls, v: str) -> str:
        if len(v) < 32:
            raise ValueError("JWT_SECRET must be at least 32 characters")
        return v

    # Sync worker
    batch_size: int = 50
    sync_interval: int = 30          # accepts plain int, "30s", or "2m"
    local_sync_dir: str = "./synced_data"
    remote_sync_enabled: bool = False

    @field_validator("sync_interval", mode="before")
    @classmethod
    def _parse_duration(cls, v: Any) -> int:
        if isinstance(v, int):
            return v
        s = str(v).strip()
        if s.endswith("s"):
            return int(s[:-1])
        if s.endswith("m"):
            return int(s[:-1]) * 60
        return int(s)

    # SSH (only validated when remote_sync_enabled=True)
    ssh_host: str = ""
    ssh_user: str = ""
    ssh_remote_path: str = "/tmp"
    ssh_key_path: str = ""
    ssh_password: SecretStr = SecretStr("")

    # Producer → auth service
    auth_service_url: str = "http://fastapi_app:8000"

    # Device identity (used by producer to authenticate against auth service)
    device_id: str = "simulation_device_01"
    device_secret: SecretStr = SecretStr("")

    # TSDB schema config path
    tsdb_config_path: str = "TSDB.yml"

    model_config = {
        "env_file": ".env",
        "case_sensitive": False,
        "extra": "ignore",
    }

    def validate_remote_sync(self) -> None:
        if not self.remote_sync_enabled:
            return
        missing = [
            name
            for name, val in [
                ("SSH_HOST", self.ssh_host),
                ("SSH_USER", self.ssh_user),
                ("SSH_KEY_PATH", self.ssh_key_path),
                ("SSH_REMOTE_PATH", self.ssh_remote_path),
            ]
            if not val
        ]
        if missing:
            raise ValueError(f"REMOTE_SYNC_ENABLED=true but missing: {missing}")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    settings.validate_remote_sync()
    return settings
