#ingestion/json2tsdb.py
"""
json_to_TSDB.py

Consumes VALIDATED telemetry messages from Kafka
Transforms them to QuestDB ILP format
Writes to QuestDB
"""

import json
import requests
import time
from kafka import KafkaConsumer
from datetime import datetime
import uuid
import sys


KAFKA_BOOTSTRAP = ['localhost:9092']

VALIDATED_TOPIC = "validated_h2_lab_data"

QUESTDB_URL = "http://localhost:9000/write"

BATCH_SIZE = 20
FLUSH_INTERVAL = 5


def create_consumer():

    try:
        consumer = KafkaConsumer(
            VALIDATED_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            auto_offset_reset="earliest",
            group_id="questdb-writer",
            enable_auto_commit=True
        )

        print(f"✅ Connected to Kafka topic: {VALIDATED_TOPIC}")
        return consumer

    except Exception as e:

        print(f"❌ Kafka connection failed: {e}")
        sys.exit(1)


def transform_to_ilp(data):

    try:

        auth = data["auth"]

        if "timestamp" in data:
            dt = datetime.fromisoformat(data["timestamp"].replace("Z", "+00:00"))
            timestamp_ns = int(dt.timestamp() * 1_000_000_000)
        else:
            timestamp_ns = int(time.time() * 1_000_000_000)

        trace_id = str(uuid.uuid4())

        tags = (
            f"lab={auth.get('lab','unknown')},"
            f"sensor_id={data.get('sensor_id','unknown')},"
            f"measurement_type={data.get('measurement_type','unknown')},"
            f"unit={data.get('unit','unknown')},"
            f"quality_flag=OK"
        )

        fields = (
            f"value={float(data.get('value',0))},"
            f"trace_id=\"{trace_id}\""
        )

        return f"hydrogen_data,{tags} {fields} {timestamp_ns}"

    except Exception as e:

        print(f"❌ Transform error: {e}")
        return None


def send_to_questdb(lines):

    if not lines:
        return

    payload = "\n".join(lines)

    try:

        response = requests.post(
            QUESTDB_URL,
            data=payload,
            params={"precision": "n"},
            timeout=5
        )

        if response.status_code in (200,201,204):

            print(f"✅ Written to QuestDB: {len(lines)} points")

        else:

            print(f"❌ QuestDB error: {response.status_code}")

    except Exception as e:

        print(f"❌ HTTP error: {e}")


def main():

    print("="*60)
    print("QUESTDB INGESTION SERVICE")
    print("="*60)

    consumer = create_consumer()

    batch = []
    last_flush = time.time()

    print("\n👂 Listening for VALIDATED telemetry...\n")

    try:

        for message in consumer:

            data = message.value

            line = transform_to_ilp(data)

            if line:
                batch.append(line)

            if len(batch) >= BATCH_SIZE or (time.time() - last_flush) >= FLUSH_INTERVAL:

                send_to_questdb(batch)
                batch.clear()
                last_flush = time.time()

    except KeyboardInterrupt:

        print("\nStopped by user")

        if batch:
            send_to_questdb(batch)

    finally:

        consumer.close()
        print("Consumer stopped")


if __name__ == "__main__":
    main()