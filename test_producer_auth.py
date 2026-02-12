#!/usr/bin/env python3
"""
Authenticated Test Producer for Hydrogen Lab
- Gets JWT token from auth service
- Sends authenticated data to Kafka
"""
import json
import time
import datetime as dt
import random
import requests
from kafka import KafkaProducer
import sys

# Configuration
AUTH_URL = "http://localhost:8001"
KAFKA_BOOTSTRAP = "localhost:9092"

def get_device_token(device_id, device_secret):
    """Get JWT token from auth service"""
    print(f"🔑 Getting token for device: {device_id}")
    
    response = requests.post(
        f"{AUTH_URL}/auth/device/login",
        json={"device_id": device_id, "device_secret": device_secret}
    )
    
    if response.status_code == 200:
        token_data = response.json()
        print(f"✅ Token received (expires in {token_data['expires_in']}s)")
        return token_data["access_token"], token_data["identity"]
    else:
        print(f"❌ Authentication failed: {response.text}")
        sys.exit(1)

# Device credentials (from device_registry.json)
DEVICES = [
    {"id": "pressure_sensor_01", "secret": "sensor123", "lab": "APSU", "type": "pressure", "unit": "bar"},
    {"id": "flow_sensor_02", "secret": "flow456", "lab": "APSU", "type": "flow", "unit": "L/min"},
    {"id": "temp_sensor_03", "secret": "temp789", "lab": "MFI", "type": "temperature", "unit": "°C"},
    {"id": "voltage_sensor_04", "secret": "volt012", "lab": "MFI", "type": "voltage", "unit": "V"}
]

# Authenticate each device and store tokens
authenticated_devices = []
for device in DEVICES:
    token, identity = get_device_token(device["id"], device["secret"])
    authenticated_devices.append({
        **device,
        "token": token,
        "identity": identity
    })

print(f"\n✅ Authenticated {len(authenticated_devices)} devices")

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8')
)
print(f"✅ Connected to Kafka at {KAFKA_BOOTSTRAP}")

print("\n🚀 Starting authenticated data stream...")
print("Press Ctrl+C to stop\n")

try:
    message_count = 0
    while True:
        # Pick a random authenticated device
        device = random.choice(authenticated_devices)
        
        # Generate sensor data
        if device["type"] == "pressure":
            value = round(random.uniform(50, 150), 2)
        elif device["type"] == "flow":
            value = round(random.uniform(10, 100), 2)
        elif device["type"] == "temperature":
            value = round(random.uniform(20, 80), 2)
        else:  # voltage
            value = round(random.uniform(12, 48), 2)
        
        # Create authenticated data packet
        data = {
            # Original sensor data
            "timestamp": dt.datetime.utcnow().isoformat() + "Z",
            "sensor_id": device["id"],
            "measurement_type": device["type"],
            "value": value,
            "unit": device["unit"],
            
            # Authentication metadata
            "auth": {
                "device_id": device["identity"]["device_id"],
                "lab": device["identity"]["lab"],
                "device_type": device["identity"]["device_type"],
                "authenticated": True,
                "auth_method": "jwt",
                "token": device["token"][:20] + "..."  # Truncated for logging
            },
            
            # Data quality
            "quality": {
                "valid": True,
                "source": "authenticated_sensor",
                "ingestion_time": dt.datetime.utcnow().isoformat() + "Z"
            }
        }
        
        # Send to Kafka
        producer.send(topic='h2_lab_raw', key=device["id"].encode(), value=data)
        producer.send(topic='h2_lab_data', key=device["id"].encode(), value=data)
        producer.flush()
        
        message_count += 1
        print(f"[{message_count:03d}] ✅ {device['lab']} | {device['id']} | {device['type']} = {value} {device['unit']}")
        
        # Wait 2 seconds
        time.sleep(2)

except KeyboardInterrupt:
    print("\n\n🛑 Stopped by user")
    producer.close()
    print(f"📊 Total messages sent: {message_count}")
