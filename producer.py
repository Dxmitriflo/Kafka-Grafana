
import csv
import json
import time
import sys
from kafka import KafkaProducer
from kafka.errors import KafkaError

KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "weather_stream"
CSV_FILE = "weatherHistory.csv"
PUBLISH_RATE = 0.1  


def parse_row(row):
    
    try:
        return {
            "formatted_date": row["Formatted Date"].strip(),
            "summary": row["Summary"].strip(),
            "precip_type": row["Precip Type"].strip() if row["Precip Type"].strip() else "unknown",
            "temperature_c": float(row["Temperature (C)"]),
            "apparent_temperature_c": float(row["Apparent Temperature (C)"]),
            "humidity": float(row["Humidity"]),
            "wind_speed_kmh": float(row["Wind Speed (km/h)"]),
            "wind_bearing_degrees": float(row["Wind Bearing (degrees)"]),
            "visibility_km": float(row["Visibility (km)"]),
            "pressure_millibars": float(row["Pressure (millibars)"]),
        }
    except (ValueError, KeyError) as e:
        print(f"[WARN] Skipping malformed row: {e} — {row}")
        return None


def on_send_success(record_metadata):
    print(
        f"[OK] topic={record_metadata.topic} "
        f"partition={record_metadata.partition} "
        f"offset={record_metadata.offset}"
    )


def on_send_error(exc):
    print(f"[ERROR] Failed to send message: {exc}", file=sys.stderr)


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
    )

    print(f"[INFO] Connected to Kafka broker at {KAFKA_BROKER}")
    print(f"[INFO] Publishing to topic: {TOPIC_NAME}")
    print(f"[INFO] Reading from: {CSV_FILE}\n")

    sent = 0
    skipped = 0

    with open(CSV_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            record = parse_row(row)
            if record is None:
                skipped += 1
                continue

            producer.send(TOPIC_NAME, value=record).add_callback(on_send_success).add_errback(on_send_error)
            sent += 1

            if sent % 100 == 0:
                print(f"[INFO] Sent {sent} records so far (skipped {skipped})...")

            time.sleep(PUBLISH_RATE)

    producer.flush()
    print(f"\n[DONE] Finished. Sent: {sent} | Skipped (malformed): {skipped}")


if __name__ == "__main__":
    main()
