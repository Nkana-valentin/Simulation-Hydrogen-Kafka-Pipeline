# kafka_consumer/kafka_consumer_to_tsdb.py

"""
kafka_consumer_to_tsdb.py - 
Kafka Consumer with JWT verification and Data Quality Validation
"""

import json
import os
import time
import logging
from kafka import KafkaConsumer
from datetime import datetime
from typing import Dict, List, Tuple
import sys
from ingestion.questdbclient import QuestDBClient
from ingestion.json_tsdb_manager import Json2TsdbTransformer

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ================================
# Configuration
# ================================
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'broker:9092')
RAW_TOPIC  = os.getenv("KAFKA_TOPIC_RAW") #os.getenv('KAFKA_TOPIC', 'raw_h2_data')


print("=" * 70)
print("🔐 H2 SMART LAB INGESTION PIPELINE (Auth + Quality)")
print("=" * 70)
print(f"Kafka Broker: {KAFKA_BROKER}")
print(f"Raw topic: {RAW_TOPIC}")

questdb_client = QuestDBClient()
QUESTDB_WRITE_URL = questdb_client.QUESTDB_WRITE_URL
print(f"QuestDB:  {QUESTDB_WRITE_URL}")
print("=" * 70)

# =============================================================
# Simple Data Quality Checker: 
# will be replaced by ingestion/quality_check.py in the future
# =============================================================

class SimpleQualityChecker:
    def __init__(self):
        self.rules = {
            "temperature": {"min": -50, "max": 500},
            "pressure": {"min": 0, "max": 1000},
            "flow": {"min": 0, "max": 100},
            "voltage": {"min": 0, "max": 1000},
            "current": {"min": 0, "max": 100},
            "efficiency": {"min": 0, "max": 100}
        }
        
        self.stats = {
            "total_received": 0,
            "auth_failed": 0,
            "processed": 0,
            "valid": 0,
            "invalid": 0
        }
        self.required_fields = ['timestamp', 'sensor_id', 'measurement_type', 'value', 'unit']
    
    def validate_record(self, record: Dict) -> Tuple[bool, List[str]]:
        errors = []
        
        for field in self.required_fields:
            if field not in record:
                errors.append(f"missing_field:{field}")
        
        if errors:
            return False, errors
        
        try:
            ts_str = record['timestamp'].replace('Z', '+00:00')
            dt = datetime.fromisoformat(ts_str)
        except Exception as e:
            errors.append(f"invalid_timestamp:{str(e)}")
        
        try:
            value = float(record['value'])
            measurement_type = record.get('measurement_type', '').lower()
            if measurement_type in self.rules:
                rule = self.rules[measurement_type]
                if value < rule['min']:
                    errors.append(f"value_below_min:{value}<{rule['min']}")
                if value > rule['max']:
                    errors.append(f"value_above_max:{value}>{rule['max']}")
        except (ValueError, TypeError):
            errors.append(f"value_not_numeric:{record.get('value')}")
        
        return len(errors) == 0, errors
    
    def update_stats(self, is_valid: bool, auth_success: bool = True):
        self.stats["processed"] += 1
        if is_valid:
            self.stats["valid"] += 1
        else:
            self.stats["invalid"] += 1
    
    def print_stats(self):
        print("\n" + "-" * 50)
        print(f"📊 QUALITY STATS:")
        print(f"   📥 Total Received: {self.stats['total_received']}")
        print(f"   🔒 Auth Failed: {self.stats['auth_failed']}")
        print(f"   ⚙️  Processed: {self.stats['processed']}")
        print(f"   ├─ ✅ Valid: {self.stats['valid']}")
        print(f"   └─ ❌ Invalid: {self.stats['invalid']}")
        print("-" * 50)

# ============================================================================
# Authentication Checker
# ============================================================================

def is_authenticated(data: Dict) -> Tuple[bool, str]:
    if 'auth' not in data:
        return False, "missing_auth_section"
    
    auth = data['auth']
    if not auth.get('authenticated', False):
        return False, "not_authenticated_flag"
    if 'device_id' not in auth:
        return False, "missing_device_id"
    if 'lab' not in auth:
        return False, "missing_lab"
    
    return True, "authenticated"

# ============================================================================
# Kafka Helpers
# ============================================================================

def create_consumer():
    """Create Kafka consumer with retry logic"""
    max_retries = 30
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Attempting to connect to Kafka at {KAFKA_BROKER} (attempt {attempt + 1}/{max_retries})")
            
            # Create consumer with correct timeout settings
            consumer = KafkaConsumer(
                RAW_TOPIC,
                bootstrap_servers=[KAFKA_BROKER],
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                auto_offset_reset='earliest',
                group_id='questdb_consumer',
                enable_auto_commit=True,
                consumer_timeout_ms=1000,
                max_poll_records=100,
                # FIX: request_timeout_ms must be > session_timeout_ms
                request_timeout_ms=40000,  # Increased to 40 seconds
                session_timeout_ms=30000,   # Keep at 30 seconds
                heartbeat_interval_ms=10000,
                api_version_auto_timeout_ms=30000
            )
            
            # Get partition info to verify connection
            partitions = consumer.partitions_for_topic(RAW_TOPIC)
            logger.info(f"✅ Connected to Kafka, topic '{RAW_TOPIC}' has {len(partitions)} partitions")
            return consumer
            
        except Exception as e:
            logger.warning(f"⚠️ Failed to connect to Kafka: {e}")
            if attempt < max_retries - 1:
                logger.info(f"Waiting {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error("❌ Failed to connect to Kafka after all retries")
                raise

# ====================
# Main Pipeline
# ====================

def main():
    # Wait for Kafka to be ready
    logger.info("Waiting 15 seconds for Kafka to be ready...")
    time.sleep(15)
    
    # Initialize quality checker
    quality = SimpleQualityChecker()
    
    # Define schema for QuestDB table
    schema = {
        'tags': ['lab', 'sensor_id', 'measurement_type', 'unit', 'qualityflag'],     
        'fields': ['value']
    }
    
    # Create table in QuestDB
    if questdb_client.create_table(RAW_TOPIC, schema):
        logger.info(f"✅ Table '{RAW_TOPIC}' is ready in QuestDB")
    
    # Create transformer
    json2tsdb = Json2TsdbTransformer(
        table_name=RAW_TOPIC, 
        tag_keys=schema.get('tags'), 
        field_keys=schema.get('fields')
    )
    
    # Create Kafka consumer
    try:
        consumer = create_consumer()
    except Exception as e:
        logger.error(f"Failed to create consumer: {e}")
        sys.exit(1)
    
    print("\n👂 Listening for sensor data...")
    print("-" * 70)
    
    message_count = 0
    
    try:
        # Main consumption loop
        while True:
            # Poll for messages with timeout
            messages = consumer.poll(timeout_ms=1000)
            
            if not messages:
                # No messages received, continue polling
                continue
            
            # Process messages
            for topic_partition, records in messages.items():
                for message in records:
                    data = message.value
                    
                    # Count received messages
                    quality.stats["total_received"] += 1
                    message_count += 1
                    
                    if message_count % 10 == 0:
                        logger.info(f"Received {message_count} messages from Kafka")
                    
                    # Authentication check
                    auth_ok, auth_reason = is_authenticated(data)
                    
                    # Quality validation
                    if auth_ok:
                        is_valid, errors = quality.validate_record(data)
                    else:
                        is_valid = False
                        errors = [f"auth_failed:{auth_reason}"]
                        quality.stats["auth_failed"] += 1
                    
                    # Add quality metadata
                    data["qualityflag"] = str(is_valid)
                    if not is_valid and errors:
                        data["qualityflag"] = ",".join(errors[:3])
                    
                    # Insert into QuestDB
                    try:
                        line_protocol = json2tsdb.transform(data)
                        if questdb_client.insert_data(RAW_TOPIC, line_protocol):
                            quality.update_stats(is_valid, auth_ok)
                        else:
                            logger.error(f"Failed to insert data into QuestDB")
                    except Exception as e:
                        logger.error(f"Error inserting data: {e}")
    
    except KeyboardInterrupt:
        print("\n\n🛑 Stopped by user")
    
    finally:
        consumer.close()
        print("\n📊 FINAL STATISTICS:")
        quality.print_stats()
        print(f"Total messages consumed: {message_count}")

if __name__ == "__main__":
    main()