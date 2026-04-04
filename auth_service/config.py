# ========================
# Configuration
# ========================
import os


class Config:
    # JWT settings
    # NOTE: hard-coded fallback retained for local/dev compatibility.
    # Set JWT_SECRET in environment for real deployments.
    JWT_SECRET = os.getenv("JWT_SECRET", "hydrogen-research-secret-key-2024")
    JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
    TOKEN_EXPIRY_HOURS = int(os.getenv("TOKEN_EXPIRY_HOURS", "24"))
