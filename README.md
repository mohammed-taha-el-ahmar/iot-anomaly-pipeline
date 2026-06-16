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

## Quickstart

```bash
docker compose -f docker/docker-compose.yml up -d
./scripts/wait-for-kafka.sh
python producer/sensor_simulator.py --devices 20 --rate 5
spark-submit spark/jobs/stream_to_bronze.py
spark-submit spark/jobs/bronze_to_silver.py
spark-submit spark/jobs/silver_to_gold.py
python dashboard/app.py
```

## Layout

```
producer/        Kafka producer: device telemetry simulator
spark/jobs/       Structured Streaming jobs (bronze, silver, gold)
spark/conf/       Spark/Delta packages & config
data/delta/       Local Delta Lake tables (bronze/silver/gold)
data/checkpoints/ Structured Streaming checkpoints (per job)
dashboard/        Lightweight alert dashboard reading gold table
docker/           Docker Compose stack: Kafka, Zookeeper, Spark, dashboard
tests/            Unit tests for transforms & anomaly logic
scripts/          Helper scripts (topic creation, health checks)
.github/workflows/CI: lint, unit tests, compose config validation
```

## Status
`planned` — scaffold only, implementation in progress.
