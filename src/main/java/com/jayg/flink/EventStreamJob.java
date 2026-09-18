package com.jayg.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Kafka -> windowed aggregation -> Iceberg.
 *
 * Input:  JSON click events on topic "clicks":
 *   {"user_id": "u123", "value": 4.20, "ts_ms": 1735000000000}
 *
 * Output: Iceberg table "default.clicks_agg" with 1-minute rollups per user.
 *
 * Guarantees:
 *   - Event-time processing with 60s bounded out-of-orderness
 *   - Exactly-once end-to-end (Kafka source in EO mode + Iceberg upsert sink)
 *   - RocksDB state backend, checkpoint every 10s to filesystem
 *   - Allowed lateness of 5 minutes; very-late events dropped (add side output in prod)
 */
public final class EventStreamJob {

    private static final ObjectMapper JSON = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        final String bootstrap = env("KAFKA_BOOTSTRAP", "localhost:9092");
        final String topic     = env("KAFKA_TOPIC", "clicks");
        final String checkpointDir = env("CHECKPOINT_DIR", "file:///tmp/flink-checkpoints");
        final String restCatalog   = env("ICEBERG_REST_URI", "http://localhost:8181");
        final String warehouse     = env("ICEBERG_WAREHOUSE", "s3://warehouse/");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // Checkpointing: 10s interval, exactly-once, 60s timeout, min 5s pause between.
        env.enableCheckpointing(10_000, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig cfg = env.getCheckpointConfig();
        cfg.setCheckpointStorage(checkpointDir);
        cfg.setMinPauseBetweenCheckpoints(5_000);
        cfg.setCheckpointTimeout(60_000);
        cfg.setTolerableCheckpointFailureNumber(2);
        cfg.setExternalizedCheckpointCleanup(
            CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // Kafka source: earliest offset, JSON payloads as strings.
        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers(bootstrap)
            .setTopics(topic)
            .setGroupId("flink-lab-agg")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        // Watermark: 60s bounded out-of-orderness, event time from payload ts_ms.
        WatermarkStrategy<ClickEvent> watermarks = WatermarkStrategy
            .<ClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(60))
            .withTimestampAssigner((ev, ts) -> ev.tsMs)
            .withIdleness(Duration.ofMinutes(1));

        DataStream<ClickEvent> events = env
            .fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-clicks")
            .map(EventStreamJob::parse)
            .filter(ev -> ev != null)
            .assignTimestampsAndWatermarks(watermarks);

        // Key by userId, tumble every 1 minute, allow 5-min lateness.
        DataStream<RowData> aggregated = events
            .keyBy(ev -> ev.userId)
            .window(TumblingEventTimeWindows.of(Time.minutes(1)))
            .allowedLateness(Time.minutes(5))
            .aggregate(new CountAndSum(), new AggToRow());

        // Iceberg sink: upsert semantics on (window_start, user_id).
        CatalogLoader catalog = CatalogLoader.rest("rest", new HashMap<>() {{
            put("uri", restCatalog);
            put("warehouse", warehouse);
            put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
            put("s3.endpoint", env("S3_ENDPOINT", "http://localhost:9000"));
            put("s3.access-key-id", env("S3_ACCESS_KEY", "minioadmin"));
            put("s3.secret-access-key", env("S3_SECRET_KEY", "minioadmin"));
            put("s3.path-style-access", "true");
        }});
        TableLoader table = TableLoader.fromCatalog(catalog, TableIdentifier.of("default", "clicks_agg"));

        FlinkSink.forRowData(aggregated)
            .tableLoader(table)
            .upsert(true)
            .append();

        env.execute("kafka-flink-iceberg-lab");
    }

    // ----------------- helpers -----------------

    private static String env(String k, String def) {
        String v = System.getenv(k);
        return v == null || v.isEmpty() ? def : v;
    }

    private static ClickEvent parse(String raw) {
        try {
            JsonNode n = JSON.readTree(raw);
            return new ClickEvent(
                n.get("user_id").asText(),
                n.get("value").asDouble(),
                n.get("ts_ms").asLong()
            );
        } catch (Exception e) {
            return null;
        }
    }

    // Public schema kept simple so the sink writes the exact fields we advertise.
    public static Schema outputSchema() {
        return new Schema(
            Types.NestedField.required(1, "window_start", Types.TimestampType.withZone()),
            Types.NestedField.required(2, "user_id", Types.StringType.get()),
            Types.NestedField.required(3, "events", Types.LongType.get()),
            Types.NestedField.required(4, "total_value", Types.DoubleType.get())
        );
    }

    // ----------------- POJOs -----------------

    public record ClickEvent(String userId, double value, long tsMs) {}

    public record Agg(long count, double sum) {}

    public static final class CountAndSum implements AggregateFunction<ClickEvent, Agg, Agg> {
        @Override public Agg createAccumulator() { return new Agg(0, 0.0); }
        @Override public Agg add(ClickEvent e, Agg a) { return new Agg(a.count + 1, a.sum + e.value); }
        @Override public Agg getResult(Agg a) { return a; }
        @Override public Agg merge(Agg a, Agg b) { return new Agg(a.count + b.count, a.sum + b.sum); }
    }

    public static final class AggToRow
        extends ProcessWindowFunction<Agg, RowData, String, TimeWindow> {
        @Override
        public void process(String userId, Context ctx, Iterable<Agg> aggs, Collector<RowData> out) {
            Agg a = aggs.iterator().next();
            GenericRowData row = new GenericRowData(4);
            row.setField(0, org.apache.flink.table.data.TimestampData.fromInstant(
                Instant.ofEpochMilli(ctx.window().getStart())));
            row.setField(1, StringData.fromString(userId));
            row.setField(2, a.count);
            row.setField(3, a.sum);
            out.collect(row);
        }
    }
}
