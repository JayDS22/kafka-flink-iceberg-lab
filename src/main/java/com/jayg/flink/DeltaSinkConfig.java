package com.jayg.flink;

import io.delta.flink.sink.DeltaSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.hadoop.conf.Configuration;

/**
 * Delta Lake sink wiring for the same aggregated stream the Iceberg sink consumes.
 *
 * Table layout under MinIO:
 *   s3a://warehouse/delta/clicks_agg/_delta_log/
 *   s3a://warehouse/delta/clicks_agg/window_date=YYYY-MM-DD/part-*.parquet
 *
 * Delta sink is APPEND-ONLY. The Iceberg sibling runs upsert on
 * (window_start, user_id). For the comparison, we accept duplicate rows on late
 * events here: the Delta story is "every window emission is a new version in the
 * log, time-travel to any prior state". That is Databricks's medallion-bronze
 * contract. If upsert parity is needed, run a downstream Delta MERGE (Spark/DBX
 * notebook) - not possible from Flink today.
 *
 * ponytail: static partitioning on window_date keeps file counts manageable.
 * Upgrade path: add hour sub-partition when > 10M rows/day/user.
 */
public final class DeltaSinkConfig {

    private DeltaSinkConfig() {}

    // Row schema MUST match EventStreamJob.AggToRow output exactly, plus derived
    // window_date STRING for partitioning.
    public static RowType rowType() {
        return RowType.of(
            new LocalZonedTimestampType(3),
            new VarCharType(VarCharType.MAX_LENGTH),
            new BigIntType(),
            new DoubleType(),
            new VarCharType(10)                 // window_date, yyyy-MM-dd
        );
    }

    public static Configuration hadoopConf(String s3Endpoint,
                                           String accessKey,
                                           String secretKey) {
        Configuration c = new Configuration();
        c.set("fs.s3a.endpoint", s3Endpoint);
        c.set("fs.s3a.access.key", accessKey);
        c.set("fs.s3a.secret.key", secretKey);
        c.set("fs.s3a.path.style.access", "true");
        c.set("fs.s3a.connection.ssl.enabled", "false");
        // Delta's single-driver log store is fine for one Flink JM.
        c.set("io.delta.storage.S3SingleDriverLogStore.class",
              "io.delta.storage.S3SingleDriverLogStore");
        return c;
    }

    public static DeltaSink<RowData> build(String tablePath, Configuration hadoopConf) {
        return DeltaSink
            .forRowData(new Path(tablePath), hadoopConf, rowType())
            .withPartitionColumns("window_date")
            .build();
    }

    /** Attach the sink to the aggregated stream (same stream the Iceberg sink reads). */
    public static void attach(DataStream<RowData> aggregated,
                              String tablePath,
                              String s3Endpoint,
                              String accessKey,
                              String secretKey) {
        DeltaSink<RowData> sink = build(tablePath, hadoopConf(s3Endpoint, accessKey, secretKey));
        aggregated
            .map(DeltaSinkConfig::withWindowDate)
            .name("delta-add-partition-col")
            .sinkTo(sink)
            .name("delta-sink")
            .uid("delta-sink-v1");                // stable UID for state compat
    }

    // Append the window_date partition column derived from window_start.
    static RowData withWindowDate(RowData in) {
        org.apache.flink.table.data.TimestampData ts = in.getTimestamp(0, 3);
        String date = java.time.Instant
            .ofEpochMilli(ts.getMillisecond())
            .atZone(java.time.ZoneOffset.UTC)
            .toLocalDate()
            .toString();                           // yyyy-MM-dd
        org.apache.flink.table.data.GenericRowData out =
            new org.apache.flink.table.data.GenericRowData(5);
        out.setField(0, ts);
        out.setField(1, in.getString(1));
        out.setField(2, in.getLong(2));
        out.setField(3, in.getDouble(3));
        out.setField(4, org.apache.flink.table.data.StringData.fromString(date));
        return out;
    }

    // ponytail: single runnable check per the testing rule.
    // Run with: `mvn -q exec:java -Dexec.mainClass=com.jayg.flink.DeltaSinkConfig`
    public static void main(String[] args) {
        RowType t = rowType();
        if (t.getFieldCount() != 5) {
            throw new AssertionError("schema arity drift: " + t.getFieldCount());
        }
        org.apache.flink.table.data.GenericRowData in =
            new org.apache.flink.table.data.GenericRowData(4);
        in.setField(0, org.apache.flink.table.data.TimestampData.fromEpochMillis(1_735_000_000_000L));
        in.setField(1, org.apache.flink.table.data.StringData.fromString("u0001"));
        in.setField(2, 42L);
        in.setField(3, 123.45);
        RowData out = withWindowDate(in);
        if (out.getArity() != 5) {
            throw new AssertionError("output arity: " + out.getArity());
        }
        String date = out.getString(4).toString();
        if (!date.equals("2024-12-23")) {
            throw new AssertionError("unexpected date: " + date);
        }
        System.out.println("OK");
    }
}
