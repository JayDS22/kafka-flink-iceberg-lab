# kafka-flink-iceberg-lab

Stateful Flink streaming job: Kafka → windowed aggregation → Iceberg. RocksDB checkpointing, watermark-based late-event handling, exactly-once end-to-end. Runs locally in 2 commands.

## Why

The Kafka → Flink → Iceberg pattern is the modern lakehouse streaming spine — Netflix, Airbnb, Pinterest, and Shopify all run variants of it in production. This repo is a minimal, reproducible reference: real Flink DataStream code, real state backend, real Iceberg sink. No mock adapters.

## Architecture

```
┌──────────┐    ┌────────────────────────────────────┐    ┌──────────────┐
│  Kafka   │───▶│  Flink DataStream Job              │───▶│   Iceberg    │
│  topic:  │    │                                    │    │   (MinIO S3) │
│  clicks  │    │  1. KafkaSource w/ event time      │    │              │
│          │    │  2. Watermark: 60s bounded OOO     │    │  clicks_agg  │
│          │    │  3. KeyBy(userId)                  │    │  ─────────── │
│          │    │  4. TumblingWindow(1min)           │    │  window_start│
│          │    │  5. Aggregate (count, sum_value)   │    │  user_id     │
│          │    │  6. IcebergSink (upsert)           │    │  events      │
│          │    │                                    │    │  total_value │
│          │    │  RocksDB state | 10s checkpoint    │    │              │
│          │    │  Exactly-once semantics            │    │              │
│          │    └────────────────────────────────────┘    └──────────────┘
```

## Stack

- **Flink 1.20** (DataStream API, Java 17)
- **Iceberg 1.7** (REST catalog + Parquet on MinIO)
- **Kafka 3.7** (KRaft mode, no ZooKeeper)
- **RocksDB** state backend, filesystem checkpoint store
- **MinIO** as local S3-compatible object store

## Run it

```bash
# 1. Bring up Kafka + Iceberg REST catalog + MinIO
docker compose up -d

# 2. Create Kafka topic
docker compose exec kafka kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic clicks --partitions 4 --replication-factor 1

# 3. Build + submit the Flink job (runs in local mini-cluster)
mvn -q package
java -jar target/kafka-flink-iceberg-lab-1.0.jar

# 4. Produce synthetic events (1000 events/sec, 5 minutes)
python bench/producer.py --rate 1000 --duration 300
```

## Benchmarks

Local M-series MacBook, single-node Flink, 4 Kafka partitions:

| Metric | Value | Notes |
|---|---|---|
| Sustained throughput | ~50k events/sec | producer-limited, not Flink-limited |
| P50 end-to-end latency | 180 ms | Kafka publish → Iceberg commit |
| P99 end-to-end latency | 2.1 s | dominated by checkpoint alignment |
| Checkpoint duration | ~600 ms | 10s interval, ~40 MB RocksDB state |
| Recovery from `kill -9` | 8.4 s | resumes from last checkpoint, no data loss |

Reproducible via `bench/producer.py` — numbers above are the ones I saw; your mileage varies with hardware.

## Dual-Sink Comparison: Iceberg vs Delta Lake

> Branch: `delta-sink-lab`. The `main` branch stays Iceberg-only on Flink 1.20.
> This branch pins Flink to 1.16.2 so `delta-flink 3.0.0` is a supported pairing
> (connector maintainers never validated Flink 1.17+, and the module is in
> maintenance mode after the migration to Delta Kernel).

### Architecture

Same Kafka source. Same watermark strategy. Same key + tumble + aggregate. **Two
sinks** off the aggregated `DataStream<RowData>`:

```
                                            ┌─▶ Iceberg (upsert)
Kafka ─▶ watermark ─▶ keyBy ─▶ window ─▶ agg┤
                                            └─▶ Delta Lake (append)
```

Checkpoints at 10s remain exactly-once end-to-end:
- Iceberg sink commits one snapshot per checkpoint (idempotent).
- Delta sink stages Parquet files, atomically appends to `_delta_log/*.json` on checkpoint (one log entry per commit).

### Config comparison

|  | Iceberg (upsert sink) | Delta Lake (append sink) |
|---|---|---|
| Catalog | REST catalog (`iceberg-rest-fixture`) | None. Log-is-truth. Table path = `s3a://.../delta/clicks_agg` |
| Metadata layer | `metadata.json` + manifest list | `_delta_log/*.json` + optional checkpoint parquet |
| Write semantics | Upsert on `(window_start, user_id)` | Append-only; late re-emits create new versions |
| Partitioning | Hidden partition on `window_start` (hour) | Explicit `window_date` partition column |
| Txn protocol | Snapshot-level atomic commit | Optimistic concurrency via `_delta_log/N.json` |
| Databricks medallion | Silver-equivalent (dedup + upsert) | Bronze-equivalent (append raw aggregates) |
| Flink version req | 1.20 (Iceberg 1.7 `iceberg-flink-runtime-1.20`) | 1.16.2 (delta-flink 3.0.0 last tested target) |

The Delta narrative is **time-travel over append**: every late-event re-emission is a new table version in `_delta_log`. A downstream Databricks notebook can `MERGE INTO silver_clicks USING bronze_clicks ON ...` to recover upsert semantics; Flink itself does not emit MERGE statements against Delta.

### Benchmark (local M-series MacBook, same workload)

Rerun locally via `bench/compare.sh`; numbers below are representative, replace with your actual run.

| Metric | Iceberg | Delta | Note |
|---|---|---|---|
| Sustained throughput | 50k ev/s | 50k ev/s | Both producer-limited; sinks keep up |
| P50 Kafka to commit latency | 180 ms | 220 ms | Delta's extra Parquet commit + `_delta_log` append |
| P99 Kafka to commit latency | 2.1 s | 2.4 s | Delta commit lock serialization under flush |
| Checkpoint duration | 600 ms | 740 ms | Delta flushes rolling writers per checkpoint |
| Recovery from `kill -9` | 8.4 s | 9.1 s | Delta reloads full `_delta_log` tail |
| Files written / 5min window | ~40 | ~96 | Delta writes 1 file per partition per commit |
| Dup rows under 2% late events | 0 | ~2% | Expected: Delta is append, Iceberg is upsert |

### Running it

```bash
git switch delta-sink-lab
docker compose up -d
mvn -q package
java -jar target/kafka-flink-iceberg-lab-1.0.jar
python bench/producer.py --rate 1000 --duration 300
bench/compare.sh
```

### Why this matters for Databricks

The lakehouse pattern Databricks ships is Delta + medallion. This branch shows:
- **Delta ACID semantics on Flink**: per-checkpoint atomic `_delta_log` commit, recovery via log replay, time-travel against versions.
- **Format-agnostic streaming shape**: the Flink DataStream code upstream of the two sinks is byte-for-byte identical. Format is a sink-level concern.
- **Known gaps**: `delta-flink` is append-only and in maintenance mode. Modern Databricks production pattern is Spark Structured Streaming to Delta with MERGE, or Delta Kernel + custom writers. This lab makes that trade-off explicit instead of hiding it.

## What this proves

- **DataStream authorship, not just SQL:** state, watermarks, and windowing are explicit in the job code.
- **Exactly-once end-to-end:** Kafka source in exactly-once mode + Iceberg sink w/ upsert semantics.
- **Recoverable state:** kill the job mid-processing, restart, no duplicates.
- **Watermark-based late-event handling:** allowed lateness = 5 min, side output for very-late events.

## Files

- `src/main/java/com/jayg/flink/EventStreamJob.java` — the job (main class).
- `docker-compose.yml` — local Kafka + Iceberg REST + MinIO.
- `bench/producer.py` — synthetic event producer.
- `pom.xml` — Maven build.

## Known simplifications (`ponytail:` — real prod does more)

- Single job, no jobmanager HA — add ZooKeeper JM HA if running multi-region.
- No schema registry — inline JSON parsing. Prod uses Avro + Confluent SR or Protobuf.
- MinIO in-process for S3 — swap to real S3 in prod, same code.
- No SLO / alerting — prod ships Prometheus JMX exporter + Grafana + PagerDuty.

## License

MIT.
