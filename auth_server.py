#!/usr/bin/env python3
"""
Authentication Service for Hydrogen Research Pipeline
JWT Token Issuance and Verification
"""
from turtle import title

from fastapi import FastAPI
#from fastapi.security import HTTPBearer
from routers import authentication, sissa_sync_api


# ========================
# FastAPI App
# ========================
app = FastAPI(title="ORFEO-SISSA Synchronization API",
    description="API for synchronizing H2 laboratory data between ORFEO and SISSA Hydor",
    version="1.0.0",
    docs_url="/docs")

app.include_router(authentication.router)
app.include_router(sissa_sync_api.router)


# if __name__ == "__main__":
#     import uvicorn
#     print("\n" + "=" * 60)
#     print("🔐 HYDROGEN LAB AUTHENTICATION SERVICE")
#     print("=" * 60)
#     print("Starting on http://localhost:8001")
#     print("\n📚 API Documentation: http://localhost:8001/docs")
#     print("\n✅ Test credentials:")
#     print("   Device:     pressure_sensor_01 / sensor123")
#     print("   Device:     flow_sensor_02 / flow456")
#     print("   Researcher: dr_smith / research2024 (APSU)")
#     print("   Researcher: dr_jones / hydrogen2024 (MFI)")
#     print("=" * 60 + "\n")
#     uvicorn.run(app, host="0.0.0.0", port=8001, log_level="info")
