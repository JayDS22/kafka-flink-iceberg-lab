#!/usr/bin/env bash
# Compare Iceberg vs Delta commits over a 60s window.
# ponytail: polls MinIO file counts + mtimes instead of hooking into Flink metrics.
# Upgrade path: swap for Prometheus JMX exporter when running against real cluster.
set -euo pipefail

ENDPOINT="${S3_ENDPOINT:-http://localhost:9000}"
ACCESS="${S3_ACCESS_KEY:-minioadmin}"
SECRET="${S3_SECRET_KEY:-minioadmin}"
DURATION="${DURATION:-60}"

export AWS_ACCESS_KEY_ID="$ACCESS"
export AWS_SECRET_ACCESS_KEY="$SECRET"
AWS=(aws --endpoint-url="$ENDPOINT" --no-cli-pager)

snapshot () {
    local prefix="$1"
    local n ts
    n=$("${AWS[@]}" s3 ls --recursive "s3://warehouse/${prefix}/" 2>/dev/null | wc -l | tr -d ' ')
    ts=$(date +%s)
    printf "%s %s\n" "$ts" "$n"
}

echo "[0s] baseline"
BASE_IB=$(snapshot iceberg)
BASE_DT=$(snapshot delta)

sleep "$DURATION"

echo "[${DURATION}s] delta"
NOW_IB=$(snapshot iceberg)
NOW_DT=$(snapshot delta)

iceberg_commits=$(( $(echo "$NOW_IB" | awk '{print $2}') - $(echo "$BASE_IB" | awk '{print $2}') ))
delta_commits=$((  $(echo "$NOW_DT" | awk '{print $2}') - $(echo "$BASE_DT" | awk '{print $2}') ))

delta_logs=$("${AWS[@]}" s3 ls --recursive "s3://warehouse/delta/clicks_agg/_delta_log/" 2>/dev/null | wc -l | tr -d ' ')
iceberg_meta=$("${AWS[@]}" s3 ls --recursive "s3://warehouse/iceberg/" 2>/dev/null | grep -E '\.metadata\.json$' | wc -l | tr -d ' ')

printf "\n%-20s %10s %10s\n" "" "Iceberg" "Delta"
printf "%-20s %10d %10d\n" "files added (${DURATION}s)" "$iceberg_commits" "$delta_commits"
printf "%-20s %10d %10s\n" "metadata snapshots"       "$iceberg_meta"      "-"
printf "%-20s %10s %10d\n" "_delta_log entries"       "-"                  "$delta_logs"

if command -v duckdb >/dev/null 2>&1; then
    echo ""
    echo "Row counts (DuckDB over MinIO):"
    duckdb -c "
      INSTALL httpfs; LOAD httpfs;
      SET s3_endpoint='localhost:9000';
      SET s3_access_key_id='${ACCESS}';
      SET s3_secret_access_key='${SECRET}';
      SET s3_use_ssl=false;
      SET s3_url_style='path';
      SELECT 'iceberg' AS fmt, COUNT(*) AS rows FROM read_parquet('s3://warehouse/iceberg/**/*.parquet')
      UNION ALL
      SELECT 'delta',   COUNT(*)         FROM read_parquet('s3://warehouse/delta/clicks_agg/**/*.parquet');
    "
fi
