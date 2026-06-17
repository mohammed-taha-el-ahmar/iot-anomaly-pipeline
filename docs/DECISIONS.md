# Architecture Decisions

## Kafka over a simpler queue (SQS/RabbitMQ)
Need ordered delivery per device (partition key = `device_id`) and the
ability to replay historical streams from any offset for backtesting
alert thresholds. Simpler queues either lack ordering guarantees per key
or lack replay/retention semantics.

## Spark Structured Streaming over micro-batch/batch
Sub-minute detection latency is the entire point of the system; a
15-minute batch window would defeat the purpose. Structured Streaming
gives incremental, checkpointed processing with the same DataFrame API
as batch, so the silver/gold logic is testable with static DataFrames.

## Medallion architecture (bronze/silver/gold) in Delta Lake
- **Bronze**: raw Kafka payload + metadata, untouched — enables replay
  and debugging without re-consuming from Kafka (which has finite
  retention).
- **Silver**: parsed, type-cast, deduplicated (watermark + dropDuplicates
  on `device_id` + `event_time`) to handle Kafka's at-least-once
  delivery semantics.
- **Gold**: windowed aggregates with anomaly flags — what the dashboard
  reads. Kept separate from silver so the detection rule can be changed
  and backfilled without touching upstream layers.

## Anomaly rule: rolling z-score, not a fixed threshold
Per-device baselines vary (different equipment, different normal
operating ranges), so a static threshold across all devices would
either over- or under-fire. Z-score against the device's own rolling
mean/stddev adapts automatically. The threshold (`Z_THRESHOLD`) is
parameterized via env var specifically so it can be tuned by replaying
bronze data once labeled incidents are available.

## `uv` for Python dependency management
Chose `uv` over pip/virtualenv for fast, reproducible installs. Single
command creates the `.venv` and installs all requirement files. Works
well in CI and locally with no extra setup.

## `spark-submit-with-packages.sh` wrapper
`spark.jars.packages` set inside Python code via SparkSession.builder
is resolved *after* the session is already constructing — too late for
data source class loading. Passing `--packages` on the spark-submit
command line ensures Maven JARs are downloaded and on the classpath
before Spark initializes any session extensions or data sources. The
wrapper auto-detects Spark 3.5 vs 4.x by parsing `spark-submit --version`.

## Repo-local data paths (not container-only `/opt/data`)
Default paths for Delta tables and checkpoints resolve relative to
`Path(__file__).parents[2]` (project root). This lets developers
`spark-submit` from the host without Docker while keeping env-var
overrides for containerized deployments (`/opt/data/...` via compose
environment section).

## Apache Spark image over Bitnami
The `bitnami/spark:3.5` tag was removed from Docker Hub. Switched to
the official `apache/spark:3.5.1` image which is maintained by the
Spark project itself and supports explicit master/worker commands.

## Open items / next steps
- Backfill/replay job from bronze to validate `Z_THRESHOLD` against
  historical incidents once real labeled data exists.
- Add a dead-letter sink for malformed bronze payloads that fail
  silver parsing (currently silently dropped).
- Compaction/OPTIMIZE schedule for the Delta tables (small-file problem
  under high-frequency triggers).
- CI pipeline: lint (`ruff`), unit tests, `docker compose config`
  validation.
- Proper `pyproject.toml` with all dependency groups for a single
  `uv sync` command.
