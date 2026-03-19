# ingestion/device_data_generator.py
"""
device_data_generator.py
Simulates hydrogen lab devices and generates sensor measurements
WITH DATA QUALITY ISSUES for testing the pipeline
"""

import random
import datetime as dt
import requests
import sys
import math
from fastapi import status


# Device credentials (from device_registry.json)
DEVICES = [
    {"id": "pressure_sensor_01", "secret": "sensor123", "lab": "APSU", "type": "pressure", "unit": "bar"},
    {"id": "flow_sensor_02", "secret": "flow456", "lab": "APSU", "type": "flow", "unit": "L/min"},
    {"id": "temp_sensor_03", "secret": "temp789", "lab": "MFI", "type": "temperature", "unit": "°C"},
    {"id": "voltage_sensor_04", "secret": "volt012", "lab": "MFI", "type": "voltage", "unit": "V"}
]

AUTH_URL = "http://localhost:8000"

# Configuration for data quality issues
QUALITY_ISSUE_PROBABILITY = 0.195  # 19.5% chance of some quality issue
MISSING_FIELD_PROBABILITY = 0.055   # 5.5% chance of missing a required field
NAN_VALUE_PROBABILITY = 0.035       # 3.5% chance of NaN value
INF_VALUE_PROBABILITY = 0.025       # 2.5% chance of infinity value
OUT_OF_RANGE_PROBABILITY = 0.15    # 15% chance of out-of-range value
TIMESTAMP_ERROR_PROBABILITY = 0.07 # 7% chance of timestamp issues
AUTH_FAILURE_PROBABILITY = 0.035    # 3.5% chance of authentication failure
DUPLICATE_TIMESTAMP_PROBABILITY = 0.028  # 2.8% chance of duplicate timestamp
EMPTY_STRING_PROBABILITY = 0.025    # 2.5% chance of empty string in text fields

# Track last timestamps for each device to detect duplicates
last_timestamps = {device["id"]: None for device in DEVICES}

def get_device_token(device_id, device_secret):
    """
    Get JWT token from auth service
    """
    print(f"🔑 Getting token for device: {device_id}")
    
    response = requests.post(
        f"{AUTH_URL}/device/login",
        json={"device_id": device_id, "device_secret": device_secret}
    )
    
    if response.status_code == status.HTTP_201_CREATED:
        token_data = response.json()
        print(f"✅ Token received (expires in {token_data['expires_in']}s)")
        return token_data["access_token"], token_data["identity"]
    else:
        print(f"❌ Authentication failed: {response.text}")
        sys.exit(1)
        
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
        

def generate_value(sensor_type, introduce_issues=True):
    """
    Generate a value for a sensor, optionally with data quality issues
    """
    # Normal value generation
    normal_value = None
    
    if sensor_type == "pressure":
        normal_value = round(random.uniform(50, 150), 2)
    elif sensor_type == "flow":
        normal_value = round(random.uniform(10, 100), 2)
    elif sensor_type == "temperature":
        normal_value = round(random.uniform(20, 80), 2)
    elif sensor_type == "voltage":
        normal_value = round(random.uniform(12, 48), 2)
    else:
        normal_value = round(random.uniform(0, 10), 2)
    
    if not introduce_issues:
        return normal_value
    
    # Introduce value issues based on probabilities
    rand = random.random()
    
    # NaN value
    if rand < NAN_VALUE_PROBABILITY:
        print(f"   ⚠️ Introducing NaN value for {sensor_type}")
        return float('nan')
    
    # Infinity value
    elif rand < NAN_VALUE_PROBABILITY + INF_VALUE_PROBABILITY:
        print(f"   ⚠️ Introducing Infinity value for {sensor_type}")
        return float('inf')
    
    # Out of range values (extreme values)
    elif rand < NAN_VALUE_PROBABILITY + INF_VALUE_PROBABILITY + OUT_OF_RANGE_PROBABILITY:
        if sensor_type == "pressure":
            extreme = random.choice([-100, 1000])
            print(f"   ⚠️ Introducing out-of-range pressure: {extreme}")
            return float(extreme)
        elif sensor_type == "temperature":
            extreme = random.choice([-273, 1000])
            print(f"   ⚠️ Introducing out-of-range temperature: {extreme}")
            return float(extreme)
        elif sensor_type == "flow":
            extreme = random.choice([-50, 500])
            print(f"   ⚠️ Introducing out-of-range flow: {extreme}")
            return float(extreme)
        elif sensor_type == "voltage":
            extreme = random.choice([-100, 1000])
            print(f"   ⚠️ Introducing out-of-range voltage: {extreme}")
            return float(extreme)
        else:
            extreme = random.choice([-9999, 9999])
            print(f"   ⚠️ Introducing out-of-range value: {extreme}")
            return float(extreme)
    
    # Return normal value
    return normal_value


def generate_timestamp(device_id, introduce_issues=True):
    """
    Generate timestamp, optionally with issues
    """
    now = dt.datetime.utcnow()
    
    if not introduce_issues:
        return now.isoformat() + "Z"
    
    global last_timestamps
    
    rand = random.random()
    
    # Future timestamp (clock skew / wrong time)
    if rand < TIMESTAMP_ERROR_PROBABILITY * 0.3:
        future_hours = random.randint(1, 24)
        future_time = now + dt.timedelta(hours=future_hours)
        print(f"   ⚠️ Introducing future timestamp (+{future_hours}h)")
        return future_time.isoformat() + "Z"
    
    # Old timestamp (stale data)
    elif rand < TIMESTAMP_ERROR_PROBABILITY * 0.6:
        past_days = random.randint(1, 7)
        past_time = now - dt.timedelta(days=past_days)
        print(f"   ⚠️ Introducing old timestamp (-{past_days}d)")
        return past_time.isoformat() + "Z"
    
    # Duplicate timestamp (same as last for this device)
    elif rand < TIMESTAMP_ERROR_PROBABILITY * 0.8 and last_timestamps[device_id]:
        print(f"   ⚠️ Introducing duplicate timestamp")
        return last_timestamps[device_id]
    
    # Invalid format (missing Z, wrong format)
    elif rand < TIMESTAMP_ERROR_PROBABILITY:
        print(f"   ⚠️ Introducing invalid timestamp format")
        # Return without Z suffix or in wrong format
        return now.strftime("%Y-%m-%d %H:%M:%S")
    
    # Normal timestamp
    timestamp = now.isoformat() + "Z"
    last_timestamps[device_id] = timestamp
    return timestamp


def generate_record(device, force_issues=True):
    """
    Generate a record, optionally with data quality issues
    """
    introduce_issues = force_issues or (random.random() < QUALITY_ISSUE_PROBABILITY)
    
    # Basic record structure
    record = {}
    
    # Decide which fields to include (maybe missing fields)
    include_timestamp = True
    include_sensor_id = True
    include_measurement_type = True
    include_value = True
    include_unit = True
    include_auth = True
    
    if introduce_issues:
        rand = random.random()
        
        # Missing timestamp
        if rand < MISSING_FIELD_PROBABILITY * 0.2:
            include_timestamp = False
            print(f"   ⚠️ Missing timestamp field")
        
        # Missing sensor_id
        elif rand < MISSING_FIELD_PROBABILITY * 0.4:
            include_sensor_id = False
            print(f"   ⚠️ Missing sensor_id field")
        
        # Missing measurement_type
        elif rand < MISSING_FIELD_PROBABILITY * 0.6:
            include_measurement_type = False
            print(f"   ⚠️ Missing measurement_type field")
        
        # Missing value
        elif rand < MISSING_FIELD_PROBABILITY * 0.8:
            include_value = False
            print(f"   ⚠️ Missing value field")
        
        # Missing unit
        elif rand < MISSING_FIELD_PROBABILITY:
            include_unit = False
            print(f"   ⚠️ Missing unit field")
    
    # Add fields based on inclusion flags
    if include_timestamp:
        record["timestamp"] = generate_timestamp(device["id"], introduce_issues)
    
    if include_sensor_id:
        # Possibly empty string
        if introduce_issues and random.random() < EMPTY_STRING_PROBABILITY:
            record["sensor_id"] = ""
            print(f"   ⚠️ Empty sensor_id")
        else:
            record["sensor_id"] = device["id"]
    
    if include_measurement_type:
        # Possibly wrong measurement type
        if introduce_issues and random.random() < EMPTY_STRING_PROBABILITY * 0.5:
            wrong_types = ["invalid", "unknown", "garbage", "test"]
            record["measurement_type"] = random.choice(wrong_types)
            print(f"   ⚠️ Wrong measurement type: {record['measurement_type']}")
        else:
            record["measurement_type"] = device["type"]
    
    if include_value:
        record["value"] = generate_value(device["type"], introduce_issues)
    
    if include_unit:
        # Possibly empty or wrong unit
        if introduce_issues:
            unit_rand = random.random()
            if unit_rand < EMPTY_STRING_PROBABILITY * 0.3:
                record["unit"] = ""
                print(f"   ⚠️ Empty unit")
            elif unit_rand < EMPTY_STRING_PROBABILITY * 0.6:
                record["unit"] = "invalid"
                print(f"   ⚠️ Invalid unit: invalid")
            else:
                record["unit"] = device["unit"]
        else:
            record["unit"] = device["unit"]
    
    # Auth section
    if include_auth:
        # Possibly auth failure
        auth_failure = introduce_issues and random.random() < AUTH_FAILURE_PROBABILITY
        
        if auth_failure:
            print(f"   ⚠️ Authentication failure")
            record["auth"] = {
                "device_id": device["id"],
                "lab": device["lab"],
                "device_type": device["type"],
                "authenticated": False,
                "auth_method": "none"
            }
        else:
            record["auth"] = {
                "device_id": device["id"],
                "lab": device["lab"],
                "device_type": device["type"],
                "authenticated": True,
                "auth_method": "jwt"
            }
    else:
        # Missing auth section entirely
        print(f"   ⚠️ Missing auth section")
    
    # Add quality flag (null or with issues)
    record["qualityflag"] = None
    
    return record


def generate_device_stream(force_issues=False):
    """
    Generate a stream of device data with optional forced issues
    """
    device = random.choice(DEVICES)
    return generate_record(device, force_issues)


# For testing: generate a batch of records with various issues
def generate_test_batch(num_records=20, issue_rate=0.3):
    """
    Generate a batch of test records with specified issue rate
    """
    print(f"\n🧪 Generating test batch with {issue_rate*100:.0f}% issue rate")
    print("-" * 50)
    
    records = []
    for i in range(num_records):
        force = random.random() < issue_rate
        record = generate_record(random.choice(DEVICES), force)
        records.append(record)
        
        # Print summary
        has_issues = False
        if 'value' in record and (isinstance(record['value'], float) and 
                                   (math.isnan(record['value']) or math.isinf(record['value']))):
            has_issues = True
        elif len(record) < 7:  # Missing fields
            has_issues = True
            
        status = "⚠️ ISSUE" if has_issues else "✅ OK"
        print(f"   Record {i+1}: {status}")
    
    print("-" * 50)
    print(f"📊 Generated {len(records)} records")
    return records


# if __name__ == "__main__":
#     # Test the generator
#     print("🔧 Testing Device Data Generator with Quality Issues")
#     print("=" * 60)
    
#     # Generate a few test records
#     for i in range(5):
#         record = generate_device_stream()
#         print(f"\n📤 Record {i+1}:")
#         for key, value in record.items():
#             if key == 'value' and isinstance(value, float):
#                 if math.isnan(value):
#                     print(f"   {key}: NaN")
#                 elif math.isinf(value):
#                     print(f"   {key}: Infinity")
#                 else:
#                     print(f"   {key}: {value}")
#             else:
#                 print(f"   {key}: {value}")
    
#     print("\n" + "=" * 60)
#     print("✅ Generator ready with data quality issues")