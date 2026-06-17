# Real-Time IoT Anomaly Detection Pipeline

Detects equipment anomalies in real time from streaming sensor data, before they become costly failures.

## Architecture

```
Sensor Simulator → Kafka (partitioned by device) → Spark Structured Streaming → Delta Lake (bronze/silver/gold) → Alert Dashboard
```

| Stage | Component | Purpose |
|---|---|---|
| Ingest | `producer/sensor_simulator.py` | Simulates multi-device sensor telemetry, publishes to Kafka |
| Transport | Kafka (`sensor-readings` topic, partitioned by `device_id`) | Ordered per-device delivery, replay for backtesting |
| Process | `spark/jobs/stream_to_bronze.py`, `bronze_to_silver.py`, `silver_to_gold.py` | Structured Streaming jobs, sub-minute latency |
| Storage | Delta Lake bronze/silver/gold | Medallion architecture for raw/clean/aggregated data |
| Serve | `dashboard/app.py` | Live alert dashboard |

## Key Decisions

- **Kafka over a simpler queue**: ordered delivery per device + replay capability for backtesting alert thresholds.
- **Structured Streaming over batch**: sub-minute detection latency is the point; batch windows defeat it.
- **Medallion architecture**: bronze preserves raw events untouched (replay/debugging), silver cleans/dedupes, gold aggregates anomaly alerts.

See [docs/DECISIONS.md](docs/DECISIONS.md) for full rationale.

## Prerequisites

| Tool | Minimum version | Check |
|---|---|---|
| Docker + Compose | 24.x / v2 | `docker compose version` |
| Python | 3.11+ | `python3 --version` |
| uv | 0.4+ | `uv --version` |
| Apache Spark | 3.5 or 4.x | `spark-submit --version` |
| Java | 17+ | `java -version` |

## Setup

```bash
# 1. Create virtual environment & install all deps
uv venv .venv
uv pip install --python .venv/bin/python -r producer/requirements.txt
uv pip install --python .venv/bin/python -r dashboard/requirements.txt
uv pip install --python .venv/bin/python -r tests/requirements.txt

# 2. Ensure local data directories exist
mkdir -p data/checkpoints data/delta/bronze data/delta/silver data/delta/gold
```

## Quickstart

Run each command in a **separate terminal** (producer & Spark jobs are long-running):

```bash
# Terminal 1 — Infrastructure
docker compose -f docker/docker-compose.yml up -d
./scripts/wait-for-kafka.sh
./scripts/create-topics.sh

# Terminal 2 — Producer (streams indefinitely, Ctrl-C to stop)
.venv/bin/python producer/sensor_simulator.py --devices 20 --rate 5

# Terminal 3 — Spark Streaming (one per job, or run all three)
./scripts/spark-submit-with-packages.sh spark/jobs/stream_to_bronze.py
./scripts/spark-submit-with-packages.sh spark/jobs/bronze_to_silver.py
./scripts/spark-submit-with-packages.sh spark/jobs/silver_to_gold.py

# Terminal 4 — Dashboard (opens at http://localhost:8501)
.venv/bin/streamlit run dashboard/app.py
```

## Useful Commands

### Infrastructure

```bash
# Start only Kafka (skip Spark/dashboard containers for local dev)
docker compose -f docker/docker-compose.yml up -d zookeeper kafka

# View Kafka container logs
docker compose -f docker/docker-compose.yml logs -f kafka

# List Kafka topics
docker exec iot-kafka kafka-topics --bootstrap-server localhost:9092 --list

# Describe a topic
docker exec iot-kafka kafka-topics --bootstrap-server localhost:9092 \
  --describe --topic sensor-readings

# Consume messages from topic (debug)
docker exec iot-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic sensor-readings --from-beginning --max-messages 5

# Tear everything down (removes volumes)
docker compose -f docker/docker-compose.yml down -v
```

### Producer

```bash
# Quick test (3 devices, low rate)
.venv/bin/python producer/sensor_simulator.py --devices 3 --rate 1

# High anomaly injection for testing alerts
.venv/bin/python producer/sensor_simulator.py --devices 20 --rate 5 --anomaly-prob 0.1
```

### Spark Jobs

```bash
# Run a single job with explicit packages (auto-detects Spark 3.5 vs 4.x)
./scripts/spark-submit-with-packages.sh spark/jobs/stream_to_bronze.py

# Override Kafka bootstrap for remote broker
KAFKA_BOOTSTRAP_SERVERS=broker.example.com:9092 \
  ./scripts/spark-submit-with-packages.sh spark/jobs/stream_to_bronze.py

# Reset a streaming job (clear its checkpoint to reprocess from scratch)
rm -rf data/checkpoints/bronze_sensor_readings
rm -rf data/checkpoints/silver_sensor_readings
rm -rf data/checkpoints/gold_anomaly_alerts
```

### Dashboard

```bash
# Default launch (http://localhost:8501)
.venv/bin/streamlit run dashboard/app.py

# Custom port
.venv/bin/streamlit run dashboard/app.py --server.port 9000
```

### Tests

```bash
# Unit tests
.venv/bin/python -m pytest tests/ -v
```

## Environment Variables

| Variable | Default | Purpose |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:29092` | Kafka broker address for Spark jobs |
| `BRONZE_PATH` | `data/delta/bronze/sensor_readings` | Delta table path for bronze |
| `SILVER_PATH` | `data/delta/silver/sensor_readings` | Delta table path for silver |
| `GOLD_PATH` | `data/delta/gold/anomaly_alerts` | Delta table path for gold |
| `BRONZE_CHECKPOINT` | `data/checkpoints/bronze_sensor_readings` | Streaming checkpoint for bronze |
| `SILVER_CHECKPOINT` | `data/checkpoints/silver_sensor_readings` | Streaming checkpoint for silver |
| `GOLD_CHECKPOINT` | `data/checkpoints/gold_anomaly_alerts` | Streaming checkpoint for gold |
| `Z_THRESHOLD` | `3.0` | Anomaly z-score threshold |
| `SPARK_EXTRA_PACKAGES` | _(auto)_ | Override Maven package coordinates |

## Layout

```
producer/          Kafka producer: device telemetry simulator
spark/jobs/        Structured Streaming jobs (bronze, silver, gold)
spark/conf/        Spark/Delta packages & config
data/delta/        Local Delta Lake tables (bronze/silver/gold)
data/checkpoints/  Structured Streaming checkpoints (per job)
dashboard/         Lightweight alert dashboard reading gold table
docker/            Docker Compose stack: Kafka, Zookeeper, Spark, dashboard
tests/             Unit tests for transforms & anomaly logic
scripts/           Helper scripts (topic creation, health checks, spark wrapper)
docs/              Architecture decisions & troubleshooting
```

## Status

`in-progress` — local end-to-end pipeline runs; CI and production hardening pending.
