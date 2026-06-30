# Assignment 1 — Apache Kafka + Grafana
## Weather History Streaming Pipeline

---

## File Structure
```
assignment1/
├── docker-compose.yml   # All infrastructure
├── producer.py          # Task 1 — Kafka Producer
├── consumer.py          # Task 2 & 3 — Consumer + InfluxDB sink
├── requirements.txt     # Python dependencies
└── weatherHistory.csv   # ← put your dataset here
```

---

## Step 1 — Start Infrastructure

```bash
docker-compose up -d
```

Verify all 4 containers are running (screenshot this):
```bash
docker ps
```
You should see: `zookeeper`, `kafka`, `influxdb`, `grafana`

---

## Step 2 — Install Python dependencies

```bash
pip install -r requirements.txt
```

---

## Step 3 — Run the Producer (Task 1)

Place `weatherHistory.csv` in the same folder, then:

```bash
python producer.py
```

You'll see output like:
```
[OK] topic=weather_stream partition=0 offset=0
[OK] topic=weather_stream partition=0 offset=1
...
```
**Screenshot this terminal** for Task 1.

---

## Step 4 — Run the Consumer (Task 2 & 3)

Open a **second terminal** and run:

```bash
python consumer.py
```

You'll see enriched output including the derived **Wind Chill** field and **comfort band**:
```
📅 04-01 00:00:00 | 🌤 Partly Cloudy | 🌡 Temp: 9.47°C | 💨 Wind: 14.12 km/h | 💧 Humidity: 89% | 🥶 Wind Chill: 5.23°C [cool]
```
**Screenshot this terminal** for Task 2.

---

## Step 5 — Verify InfluxDB (Task 3)

1. Open `http://localhost:8086` in your browser
2. Login: `admin` / `adminpassword`
3. Go to **Data Explorer**
4. Select bucket: `weather_bucket`, measurement: `weather`
5. You should see incoming data points

**Screenshot the Data Explorer** for Task 3.

---

## Step 6 — Grafana Dashboard (Task 4)

1. Open `http://localhost:3000` — login: `admin` / `admin`
2. Go to **Connections → Data Sources → Add data source → InfluxDB**
3. Set:
   - Query language: **Flux**
   - URL: `http://influxdb:8086`
   - Organisation: `weather_org`
   - Token: `my-super-secret-token`
   - Default bucket: `weather_bucket`
4. Click **Save & Test**

### Panel 1 — Temperature Over Time (Time Series)
```flux
from(bucket: "weather_bucket")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "weather" and r._field == "temperature_c")
```

### Panel 2 — Wind Chill vs Actual Temp (Time Series, 2 lines)
```flux
from(bucket: "weather_bucket")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "weather")
  |> filter(fn: (r) => r._field == "temperature_c" or r._field == "wind_chill_c")
```

### Panel 3 — Humidity Over Time (Gauge or Time Series)
```flux
from(bucket: "weather_bucket")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "weather" and r._field == "humidity")
```

### Panel 4 — Records by Comfort Band (Bar Chart)
```flux
from(bucket: "weather_bucket")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "weather" and r._field == "temperature_c")
  |> group(columns: ["comfort_band"])
  |> count()
```

5. Set dashboard **auto-refresh** to `5s` (top-right refresh dropdown)
6. Give your dashboard a title (e.g. "Weather Stream Dashboard")

**Screenshot the full dashboard** for Task 4.

---

## Derived Field Explanation (for your report)

**Wind Chill Index** — Measures how cold it actually feels based on temperature and wind speed.

Formula (Environment Canada / NOAA standard):
```
Wind Chill = 13.12 + 0.6215·T - 11.37·V^0.16 + 0.3965·T·V^0.16
```
Where T = temperature in °C, V = wind speed in km/h.
Applied only when T ≤ 10°C and V > 4.8 km/h; otherwise actual temperature is used.

We also derive a **comfort_band** tag: `warm / mild / cool / cold / very_cold`

---

## Analysis Questions (Task 5)

**Q1 — Patterns observed:**
Discuss temperature drops overnight, humidity spikes with rain precip type, and how wind chill diverges from actual temperature during high-wind periods.

**Q2 — Why InfluxDB over CSV/relational DB:**
InfluxDB is a time-series database optimised for high-frequency timestamped writes. It compresses time-series data efficiently, supports fast range queries by time, and integrates natively with Grafana. A CSV cannot be queried in real-time, and a relational DB like MySQL lacks time-series indexing, making it orders of magnitude slower for continuous streaming writes.

**Q3 — Kafka resilience to broker failure:**
Configure Kafka with replication factor ≥ 2 across multiple brokers. Use `acks=all` in the producer to ensure all replicas confirm writes. Consumers use committed offsets so they can resume from the last confirmed offset after a failure. A multi-broker cluster means if one broker goes down, a replica leader is elected automatically.
