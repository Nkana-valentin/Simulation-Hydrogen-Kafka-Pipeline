# ingestion/json_to_TSDB.py
"""
json_to_TSDB.py - Kafka Consumer with JWT verification
Only processes messages that are properly authenticated
"""
import json
import requests
import time
from kafka import KafkaConsumer, KafkaAdminClient
import logging
from datetime import datetime
import sys

# Configuration
KAFKA_BOOTSTRAP = ['localhost:9092']
QUESTDB_URL = "http://localhost:9000/write"
TOPIC = 'raw_h2_data'

BATCH_SIZE = 10
FLUSH_INTERVAL = 5
logger = logging.getLogger(__name__)

def create_consumer():
    """
    Create Kafka consumer
    """
    #print(f" 🔌 Connecting to Kafka at {KAFKA_BOOTSTRAP}...")
    try:
        #admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)
        #topics = admin_client.list_topics()
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest',
            group_id='questdb-consumer',
            enable_auto_commit=True
        )
        print(f" 🔌 Connected to Kafka topic: {TOPIC}")
        return consumer
    except Exception as e:
        print(f"❌ Kafka connection failed: {e}")
        sys.exit(1)

def is_authenticated(data):
    """
    Check if data is properly authenticated
    """
    # Check if auth section exists
    if 'auth' not in data:
        print("   ❌ No auth section")
        return False
    
    auth = data['auth']
    
    # Check required auth fields
    if not auth.get('authenticated', False):
        print("   ❌ Not authenticated")
        return False
    
    if 'device_id' not in auth:
        print("   ❌ No device_id")
        return False
    
    if 'lab' not in auth:
        print("   ❌ No lab")
        return False
    
    # All checks passed
    return True

def transform_to_influx_line(data):
    """
    Convert authenticated JSON to InfluxDB line protocol
    """
    try:
        auth = data['auth']
        
        # Parse timestamp
        if 'timestamp' in data:
            dt = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
            timestamp_ns = int(dt.timestamp() * 1_000_000_000)
        else:
            timestamp_ns = int(time.time() * 1_000_000_000)
        
        # Tags (indexed fields)
        tags = (
            f"lab={auth.get('lab', 'unknown')},"
            f"sensor_id={data.get('sensor_id', 'unknown')},"
            f"measurement_type={data.get('measurement_type', 'unknown')},"
            f"unit={data.get('unit', 'unknown')},"
        )
        
        # Fields (values)
        fields = f"value={float(data.get('value', 0))}"
        
        return f"hydrogen_data,{tags} {fields} {timestamp_ns}"
    
    except Exception as e:
        print(f"   ❌ Transform error: {e}")
        return None

def send_to_questdb(lines):
    """
    Send batch to QuestDB
    """
    if not lines:
        return
    
    payload = "\n".join(lines)
    try:
        response = requests.post(
            QUESTDB_URL,
            data=payload,
            params={'precision': 'n'},
            timeout=5
        )
        
        if response.status_code in (200, 201, 204):
            print(f"   ✅ Written to QuestDB: {len(lines)} points")
        else:
            print(f"   ❌ QuestDB error: {response.status_code}")
    
    except Exception as e:
        print(f"   ❌ HTTP error: {e}")

def main():
    print("=" * 60)
    print("🔐 HYDROGEN DATA TSDB INGESTOR (AUTHENTICATED ONLY)")
    print("=" * 60)
    
    consumer = create_consumer()
    batch = []
    last_flush = time.time()
    
    print("\n👂 Listening for authenticated sensor data...")
    print("   Only messages with valid authentication will be processed")
    print("-" * 60)
    
    try:
        for message in consumer:
            data = message.value
            
            # Check authentication FIRST
            if not is_authenticated(data):
                print(f"📥 Skipping unauthenticated message from {message.partition}:{message.offset}")
                continue
            
            # Only authenticated data reaches here
            print(f"\n📥 Authenticated: {data['auth']['lab']} | {data['sensor_id']} = {data['value']} {data['unit']}")
            
            # Transform to InfluxDB format
            line = transform_to_influx_line(data)
            if line:
                batch.append(line)
            
            # Flush batch if conditions met
            if len(batch) >= BATCH_SIZE or (time.time() - last_flush) >= FLUSH_INTERVAL:
                send_to_questdb(batch)
                batch.clear()
                last_flush = time.time()
    
    except KeyboardInterrupt:
        print("\n\n🛑 Stopped by user")
        if batch:
            print(f"💾 Flushing final batch ({len(batch)} points)...")
            send_to_questdb(batch)
    
    finally:
        consumer.close()
        print("👋 Consumer stopped")

if __name__ == "__main__":
    main()
