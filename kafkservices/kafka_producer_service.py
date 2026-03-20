# ingestion/kafka_producer_service.py
"""
kafka_producer_service.py
Publishes device telemetry to Kafka
This acts like an edge gateway that publishes device telemetry.
"""

import json
import time
from kafka import KafkaProducer
from ingestion.device_data_generator import generate_device_stream


KAFKA_BOOTSTRAP = "localhost:9092"
RAW_TOPIC = "raw_h2_data"


def create_producer():

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        value_serializer=lambda x: json.dumps(x).encode("utf-8")
    )

    print(f"✅ Connected to Kafka: {KAFKA_BOOTSTRAP}")
    print("\n🚀 Starting authenticated data stream...")
    print("Press Ctrl+C to stop\n")
    return producer


def main():

    print("=" * 60)
    print("H2SMARTLAB DATA PRODUCER")
    print("=" * 60)

    producer = create_producer()
    message_count = 0

    try:

        while True:

            data = generate_device_stream()
            #print(data)
            producer.send(RAW_TOPIC,
                key=data["sensor_id"].encode(),
                value=data
            )
            producer.flush()

            message_count += 1
            print(f" [{message_count:03d}] ✅ Sent to Kafka: {data}")
            #print(f"[{message_count:03d}] ✅ {data['auth']['lab']} | {data['sensor_id']} | {data['measurement_type']} = {data['value']} {data['unit']}")

            time.sleep(2)

    except KeyboardInterrupt:

            print("\n\n🛑 Stopped by user")
            producer.close()
            print(f"📊 Total messages sent: {message_count}")


if __name__ == "__main__":
    main()