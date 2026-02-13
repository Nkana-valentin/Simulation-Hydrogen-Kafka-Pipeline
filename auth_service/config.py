# ========================
# Configuration
# ========================

class Config:
    # JWT settings
    JWT_SECRET = "hydrogen-research-secret-key-2024"  # In production, use environment variable
    JWT_ALGORITHM = "HS256"
    TOKEN_EXPIRY_HOURS = 24