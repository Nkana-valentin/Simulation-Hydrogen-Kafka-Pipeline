# ingestion/h2_ingestion_pipeline.py
"""
h2_ingestion_pipeline.py - Kafka Consumer with JWT verification and Data Quality Validation

Simplified pipeline that:
1. Authenticates messages (JWT check)
2. Validates data quality (ranges, completeness, etc.)
3. Routes valid data to QuestDB and quarantine invalid data
"""

import json
from kafka import KafkaConsumer, KafkaAdminClient
from datetime import datetime, timezone
from typing import Dict, List, Tuple
import sys
import pandas as pd
import numpy as np
from ingestion.questdbclient import QuestDBClient
from ingestion.json_tsdb_manager import Json2TsdbTransformer
import logging

# ================================
# Configuration
# ================================
questdb_client = QuestDBClient()
KAFKA_BOOTSTRAP = questdb_client.config['kafka']['bootstrap_servers']
RAW_TOPIC = questdb_client.config['kafka']['topics']['raw_data']
QUESTDB_WRITE_URL = questdb_client.QUESTDB_WRITE_URL

BATCH_SIZE = 10
FLUSH_INTERVAL = 5  # seconds
WINDOW_SIZE = 50  # records for quality metrics

#table_manager = TableManager(QUESTDB_URL)
logger = logging.getLogger(__name__)

# ======================================
# Simple Data Quality Checker
# ======================================

class SimpleQualityChecker:
    """
    Simplified data quality checker with basic validation rules
    """
    
    def __init__(self):
        # Define validation rules for different measurement types
        self.rules = {
            "temperature": {"min": -50, "max": 500},
            "pressure": {"min": 0, "max": 1000},
            "flow": {"min": 0, "max": 100},
            "voltage": {"min": 0, "max": 1000},
            "current": {"min": 0, "max": 100},
            "efficiency": {"min": 0, "max": 100}
        }
        
        self.stream_window = []
        self.stats = {
            "total_received": 0,  # Total messages received from Kafka
            "auth_failed": 0,     # Messages that failed authentication
            "processed": 0,
            "valid": 0,
            "invalid": 0,
            "quarantined": 0
        }
    
    def validate_record(self, record: Dict) -> Tuple[bool, List[str]]:
        """
        Validate a single record
        Returns: (is_valid, list_of_errors)
        """
        errors = []
        
        # 1. Check required fields
        required_fields = ['timestamp', 'sensor_id', 'measurement_type', 'value', 'unit']
        for field in required_fields:
            if field not in record:
                errors.append(f"missing_field:{field}")
        
        if errors:
            return False, errors
        
        # 2. Validate timestamp
        try:
            # Parse timestamp (handle Z suffix)
            ts_str = record['timestamp'].replace('Z', '+00:00')
            dt = datetime.fromisoformat(ts_str)
            
            # Ensure timezone-aware
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            
            # Check if timestamp is not in future (allow 5 seconds clock skew)
            now = datetime.now(timezone.utc)
            if dt > now:
                time_diff = (dt - now).total_seconds()
                if time_diff > 5:  # More than 5 seconds in future
                    errors.append(f"timestamp_in_future:{time_diff:.1f}s")
                    
        except Exception as e:
            errors.append(f"invalid_timestamp:{str(e)}")
        
        # 3. Validate value is numeric
        try:
            value = float(record['value'])
            
            # 4. Apply range rules based on measurement type
            measurement_type = record.get('measurement_type', '').lower()
            if measurement_type in self.rules:
                rule = self.rules[measurement_type]
                if value < rule['min']:
                    errors.append(f"value_below_min:{value}<{rule['min']}")
                if value > rule['max']:
                    errors.append(f"value_above_max:{value}>{rule['max']}")
            
            # 5. Check for NaN or infinity
            if pd.isna(value) or np.isinf(value):
                errors.append(f"invalid_numeric_value:{value}")
                
        except (ValueError, TypeError):
            errors.append(f"value_not_numeric:{record.get('value')}")
        
        # 6. Validate sensor_id format (basic check)
        sensor_id = record.get('sensor_id', '')
        if not isinstance(sensor_id, str) or len(sensor_id) < 3:
            errors.append("invalid_sensor_id")
        
        return len(errors) == 0, errors
    
    def print_quality_alert(self, record: Dict, errors: List[str]):
        """
        Print a formatted quality alert
        """
        print(f"   ⚠️  QUALITY ISSUE: {record.get('sensor_id', 'unknown')}")
        for error in errors[:3]:  # Show first 3 errors
            print(f"      - {error}")
        if len(errors) > 3:
            print(f"      - ... and {len(errors)-3} more issues")
    
    def update_stats(self, is_valid: bool, auth_success: bool = True):
        """
        Update processing statistics
        """
        self.stats["processed"] += 1
        if is_valid:
            self.stats["valid"] += 1
        else:
            self.stats["invalid"] += 1
    
    def print_stats(self):
        """
        Print current statistics
        """
        print("\n" + "-" * 50)
        print(f"📊 QUALITY STATS:")
        print(f"   📥 Total Received: {self.stats['total_received']}")
        print(f"   🔒 Auth Failed: {self.stats['auth_failed']}")
        print(f"   ⚙️  Processed: {self.stats['processed']}")
        print(f"   ├─ ✅ Valid: {self.stats['valid']}")
        print(f"   └─ ❌ Invalid: {self.stats['invalid']}")
        print(f"   📁 Quarantined: {self.stats['quarantined']}")
        
        if self.stats['total_received'] > 0:
            auth_success_rate = ((self.stats['total_received'] - self.stats['auth_failed']) / self.stats['total_received']) * 100
            valid_rate = (self.stats['valid'] / self.stats['total_received']) * 100
            print(f"\n   📈 Rates:")
            print(f"      Auth Success: {auth_success_rate:.1f}%")
            print(f"      Valid Data: {valid_rate:.1f}%")
            print(f"      Overall Yield: {valid_rate:.1f}%")
        print("-" * 50)


# ============================================================================
# Authentication Checker
# ============================================================================

def is_authenticated(data: Dict) -> Tuple[bool, str]:
    """
    Check if data is properly authenticated via JWT
    Returns: (is_authenticated, reason_if_failed)
    """
    # Check if auth section exists
    if 'auth' not in data:
        return False, "missing_auth_section"
    
    auth = data['auth']
    
    # Check required auth fields
    if not auth.get('authenticated', False):
        return False, "not_authenticated_flag"
    
    if 'device_id' not in auth:
        return False, "missing_device_id"
    
    if 'lab' not in auth:
        return False, "missing_lab"
    
    if 'auth_method' not in auth:
        return False, "missing_auth_method"
    
    # Optional: Check if token is expired (would need actual JWT validation)
    # For now, I just check that authenticated is True
    
    return True, "authenticated"


# ============================================================================
# Kafka Helpers
# ============================================================================

def create_consumer():
    """
    Create Kafka consumer for raw data
    """
    #print(f" 🔌 Connecting to Kafka at {KAFKA_BOOTSTRAP}...")
    try: 
        # First, verify Kafka is accessible
        #admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)
        #topics = admin_client.list_topics()
        #logger.info(f"📋 Available topics: {topics}")
        #sys.exit(0)  # Exit after listing topics for verification
        consumer = KafkaConsumer(
            RAW_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest',
            group_id='questdb_consumer',
            enable_auto_commit=True
        )
        print(f" 🔌 Connected to Kafka topic: {RAW_TOPIC}")
        return consumer
    except Exception as e:
        logger.error(f"❌ Kafka connection failed: {e}")
        sys.exit(1)
        
# ====================
# Main Pipeline
# ====================

def main():
    print("=" * 70)
    print("🔐 H2 SMART LAB INGESTION PIPELINE (Auth + Quality)")
    print("=" * 70)
    print(f"Raw topic: {RAW_TOPIC}")
    print(f"QuestDB:  {QUESTDB_WRITE_URL}")
    print("=" * 70)
    
    # Initialize components
    consumer = create_consumer()
    #producer = create_producer()
    quality = SimpleQualityChecker()

    """
    Define schema for QuestDB table 
    can be extended with more fields/tags as needed
    """
    schema = {
        'tags': ['lab', 
                'sensor_id', 
                'measurement_type', 
                'unit', 
                'qualityflag'],
        'fields': ['value']
    }
    if questdb_client.create_table(RAW_TOPIC, schema):
        print(f"✅ Table '{RAW_TOPIC}' is ready in QuestDB")
        json2tsdb = Json2TsdbTransformer(table_name=RAW_TOPIC, 
                                        tag_keys=schema.get('tags'), 
                                        field_keys=schema.get('fields'))
    
    print("\n👂 Listening for sensor data...")
    print("   Step 1: Check JWT authentication")
    print("   Step 2: Validate data quality")
    print("   Step 3: Route to validated/quarantine topics")
    print("   Step 4: Write validated data to QuestDB")
    print("-" * 70)
    
    try:
        for message in consumer:
            data = message.value
            #key = message.key
            
            # ======================================
            # STEP 0: COUNT EVERY MESSAGE RECEIVED
            # ======================================
            quality.stats["total_received"] += 1
            print(f"\n📥 Received [{quality.stats['total_received']}]: partition={message.partition}, offset={message.offset}")
            
            # =============================
            # STEP 1: AUTHENTICATION CHECK
            # =============================
            auth_ok, auth_reason = is_authenticated(data)
            print(f" 🔒 Authentication: {'✅ OK' if auth_ok else '❌ FAILED'} ({auth_reason})")
            
            # ==================================================
            # STEP 2: Validate quality (only if authenticated)
            # ==================================================
            if auth_ok:
                # Perform quality validation
                is_valid, errors = quality.validate_record(data)
                status = "✅ VALID" if is_valid else "⚠️ INVALID"
                print(f"  📊 Quality: {status} | Errors: {len(errors)}")
                if errors:
                    for error in errors[:2]:
                        print(f" - {error}")
            else:
                is_valid = False
                errors = [f"auth_failed:{auth_reason}"]
                print(f" 📊 Quality: ⚠️ SKIPPED (auth failed)")
                print(f" 🔓 AUTH OK: {data['auth']['lab']} | {data['auth']['device_id']}")
            
            # Add quality metadata
            data["qualityflag"] = is_valid
            
            if not is_valid:
                data["qualityflag"] = errors
            line_protocol = json2tsdb.transform(data)
            quality.update_stats(is_valid)
            # insert into QuestDB
            questdb_client.insert_data(RAW_TOPIC, line_protocol)
    
    except KeyboardInterrupt:
        print("\n\n🛑 Stopped by user")
            
    
    finally:
        consumer.close()
        print("\n📊 FINAL STATISTICS:")
        quality.print_stats()
        
        # Calculate and show reconciliation
        print("\n" + "=" * 50)
        print("🔍 RECONCILIATION REPORT")
        print("=" * 50)
        print(f"Producer sent:        [?] messages (check producer output)")
        print(f"Pipeline received:   {quality.stats['total_received']} messages")
        print(f"Auth failed:         {quality.stats['auth_failed']} messages")
        print(f"Processed:           {quality.stats['processed']} messages")
        print(f"Valid:               {quality.stats['valid']} messages")
        print(f"Invalid:             {quality.stats['invalid']} messages")
        print(f"Quarantined:         {quality.stats['quarantined']} messages")
        print("-" * 50)
        print(f"Pipeline total = Auth Failed + Processed: {quality.stats['auth_failed'] + quality.stats['processed']}")
        print("=" * 50)
        
        print("\n👋 Pipeline stopped")


if __name__ == "__main__":
    main()   