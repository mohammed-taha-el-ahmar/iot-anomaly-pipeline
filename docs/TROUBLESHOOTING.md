# Troubleshooting

Common issues encountered when running the pipeline locally, and how to fix them.

---

## Docker / Infrastructure

### `version` attribute is obsolete warning

```
WARN: the attribute `version` is obsolete, it will be ignored
```

**Fix**: Remove the `version: "3.8"` line from `docker/docker-compose.yml`. Modern Docker Compose (v2+) no longer requires it.

---

### Spark image not found (`bitnami/spark:3.5`)

```
Error response from daemon: failed to resolve reference "docker.io/bitnami/spark:3.5": not found
```

**Cause**: The `bitnami/spark:3.5` tag was removed from Docker Hub.

**Fix**: Use the official Apache Spark image instead:

```yaml
image: apache/spark:3.5.1
```

---

### Dashboard Dockerfile fails — `openjdk-17-jre-headless` not available

```
E: Package 'openjdk-17-jre-headless' has no installation candidate
```

**Cause**: The `python:3.11-slim` base now uses Debian Trixie which only ships OpenJDK 21.

**Fix**: In `dashboard/Dockerfile`, change to:

```dockerfile
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/*
```

---

### Kafka not ready (wait-for-kafka.sh times out)

```
[wait-for-kafka] Kafka did not become ready in time.
```

**Possible causes**:
1. Docker services aren't running — check `docker compose -f docker/docker-compose.yml ps`
2. Another compose issue prevented Kafka from starting (e.g. dashboard build failure blocks the whole stack)

**Fix**: Start only what you need:

```bash
docker compose -f docker/docker-compose.yml up -d zookeeper kafka
./scripts/wait-for-kafka.sh
```

---

## Python / Dependencies

### `ModuleNotFoundError: No module named 'numpy'` (or any missing module)

**Cause**: Dependencies not installed in the active environment.

**Fix** (using `uv`):

```bash
uv venv .venv
uv pip install --python .venv/bin/python -r producer/requirements.txt
uv pip install --python .venv/bin/python -r dashboard/requirements.txt
```

Always run scripts with `.venv/bin/python` to ensure the correct env is used.

---

### Streamlit shows "run it with `streamlit run`" warning

```
Warning: to view this Streamlit app on a browser, run it with the following command:
    streamlit run dashboard/app.py
```

**Cause**: Running with `python dashboard/app.py` starts the Spark session but not the Streamlit web server.

**Fix**: Use the Streamlit CLI:

```bash
.venv/bin/streamlit run dashboard/app.py
```

Then open **http://localhost:8501** in your browser.

---

## Spark Jobs

### `Failed to find data source: kafka`

```
pyspark.errors.exceptions.captured.AnalysisException: Failed to find data source: kafka.
```

**Cause**: Kafka connector JARs weren't on the classpath. The `spark.jars.packages` config set inside Python code isn't applied early enough by `spark-submit`.

**Fix**: Use the wrapper script that passes `--packages` on the command line:

```bash
./scripts/spark-submit-with-packages.sh spark/jobs/stream_to_bronze.py
```

The wrapper auto-detects Spark 3.5 vs 4.x and injects the correct Maven coordinates.

---

### `ClassNotFoundException: io.delta.sql.DeltaSparkSessionExtension`

**Cause**: Same as above — Delta JARs not present at session creation time.

**Fix**: Same as above — use `./scripts/spark-submit-with-packages.sh`.

---

### Checkpoint directory creation fails (`mkdir` error at `/opt/data/...`)

```
org.apache.hadoop.fs.FileSystem.primitiveMkdir ... /opt/data/checkpoints/...
```

**Cause**: Spark jobs defaulted to container paths (`/opt/data/...`) which don't exist on the host.

**Fix**: The jobs now default to repo-local `data/` paths. If you see this, ensure directories exist:

```bash
mkdir -p data/checkpoints data/delta/bronze data/delta/silver data/delta/gold
```

Or override via environment variables (see README for the full list).

---

### `UnknownTopicOrPartitionException`

```
org.apache.kafka.common.errors.UnknownTopicOrPartitionException: This server does not host this topic-partition.
```

**Cause**: The `sensor-readings` topic hasn't been created before the Spark job subscribes.

**Fix**: Create topics before launching Spark:

```bash
./scripts/create-topics.sh
```

If you hit this after a stale run, also clear the checkpoint:

```bash
rm -rf data/checkpoints/bronze_sensor_readings
```

---

### SparkUI port conflict (`Service 'SparkUI' could not bind on port 4040`)

**Cause**: Multiple Spark sessions running simultaneously (e.g. bronze + silver + gold).

**Impact**: Harmless — Spark picks the next available port (4041, 4042, ...). No action needed.

---

## Spark Version Compatibility

The project supports both **Spark 3.5.x** and **Spark 4.x**. Key differences:

| Concern | Spark 3.5 | Spark 4.x |
|---|---|---|
| Scala version | 2.12 | 2.13 |
| Delta Lake | `delta-spark_2.12:3.1.0` | `delta-spark_2.13:4.0.0` |
| Kafka connector | `spark-sql-kafka-0-10_2.12:3.5.0` | `spark-sql-kafka-0-10_2.13:4.0.1` |

The `scripts/spark-submit-with-packages.sh` wrapper and `spark/conf/spark_session.py` detect the version automatically.

---

## Full Reset

To tear everything down and start fresh:

```bash
# Stop all containers
docker compose -f docker/docker-compose.yml down -v

# Clear local state
rm -rf data/checkpoints/* data/delta/bronze/* data/delta/silver/* data/delta/gold/*
rm -rf spark-warehouse/

# Restart
docker compose -f docker/docker-compose.yml up -d zookeeper kafka
./scripts/wait-for-kafka.sh
./scripts/create-topics.sh
```
