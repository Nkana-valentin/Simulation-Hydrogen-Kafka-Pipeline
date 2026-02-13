#!/usr/bin/env python3
"""
Authentication Service for Hydrogen Research Pipeline
JWT Token Issuance and Verification
"""
from fastapi import FastAPI
from . import authentication


# ========================
# FastAPI App
# ========================
app = FastAPI(title="Hydrogen Lab Authentication Service")
app.include_router(authentication.router)


if __name__ == "__main__":
    import uvicorn
    print("\n" + "=" * 60)
    print("🔐 HYDROGEN LAB AUTHENTICATION SERVICE")
    print("=" * 60)
    print("Starting on http://localhost:8001")
    print("\n📚 API Documentation: http://localhost:8001/docs")
    print("\n✅ Test credentials:")
    print("   Device:     pressure_sensor_01 / sensor123")
    print("   Device:     flow_sensor_02 / flow456")
    print("   Researcher: dr_smith / research2024 (APSU)")
    print("   Researcher: dr_jones / hydrogen2024 (MFI)")
    print("=" * 60 + "\n")
    
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="info")
