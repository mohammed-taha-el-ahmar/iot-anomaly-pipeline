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

## Open items / next steps
- Backfill/replay job from bronze to validate `Z_THRESHOLD` against
  historical incidents once real labeled data exists.
- Add a dead-letter sink for malformed bronze payloads that fail
  silver parsing (currently silently dropped).
- Compaction/OPTIMIZE schedule for the Delta tables (small-file problem
  under high-frequency triggers).
