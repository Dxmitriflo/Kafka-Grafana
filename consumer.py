
import json
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "weather_stream"
GROUP_ID = "weather_consumer_group"

INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "my-super-secret-token"
INFLUX_ORG = "weather_org"
INFLUX_BUCKET = "weather_bucket"


def compute_wind_chill(temp_c, wind_speed_kmh):
    if temp_c <= 10.0 and wind_speed_kmh > 4.8:
        v016 = wind_speed_kmh ** 0.16
        wind_chill = 13.12 + 0.6215 * temp_c - 11.37 * v016 + 0.3965 * temp_c * v016
        return round(wind_chill, 4)
    return round(temp_c, 4)


def classify_wind_chill(wind_chill):
    if wind_chill >= 20:
        return "warm"
    elif wind_chill >= 10:
        return "mild"
    elif wind_chill >= 0:
        return "cool"
    elif wind_chill >= -10:
        return "cold"
    else:
        return "very_cold"


def enrich(record):
    wind_chill = compute_wind_chill(record["temperature_c"], record["wind_speed_kmh"])
    record["wind_chill_c"] = wind_chill
    record["comfort_band"] = classify_wind_chill(wind_chill)
    return record


def print_summary(record):
    print(
        f"  {record['formatted_date']}"
        f"  |  {record['summary']:<20}"
        f"  |  Temp: {record['temperature_c']:>6.2f}C"
        f"  |  Wind: {record['wind_speed_kmh']:>6.2f} km/h"
        f"  |  Humidity: {record['humidity']:.0%}"
        f"  |  Wind Chill: {record['wind_chill_c']:>6.2f}C  [{record['comfort_band']}]"
    )


def record_to_influx_point(record):
    point = (
        Point("weather")
        .tag("summary", record["summary"])
        .tag("precip_type", record["precip_type"])
        .tag("comfort_band", record["comfort_band"])
        .field("temperature_c", float(record["temperature_c"]))
        .field("apparent_temperature_c", float(record["apparent_temperature_c"]))
        .field("humidity", float(record["humidity"]))
        .field("wind_speed_kmh", float(record["wind_speed_kmh"]))
        .field("wind_bearing_degrees", float(record["wind_bearing_degrees"]))
        .field("visibility_km", float(record["visibility_km"]))
        .field("pressure_millibars", float(record["pressure_millibars"]))
        .field("wind_chill_c", float(record["wind_chill_c"]))
    )
    return point


def main():
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    print(f"[INFO] Consuming from topic: {TOPIC_NAME}")

    influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = influx_client.write_api(write_options=SYNCHRONOUS)
    print(f"[INFO] Connected to InfluxDB at {INFLUX_URL} -> bucket: {INFLUX_BUCKET}\n")

    consumed = 0

    for message in consumer:
        record = message.value
        record = enrich(record)
        print_summary(record)
        consumed += 1

        try:
            point = record_to_influx_point(record)
            write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
        except Exception as e:
            print(f"  [ERROR] InfluxDB write failed: {e}")

        if consumed % 100 == 0:
            print(f"  [INFO] Total consumed & written: {consumed}")


if __name__ == "__main__":
    main()