#!/bin/bash

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_DIR="$BASE_DIR/scripts/pids"
LOG_DIR="$BASE_DIR/logs"

mkdir -p "$PID_DIR" "$LOG_DIR"

if ! hdfs dfsadmin -report > /dev/null 2>&1; then
    exit 1
fi

if ! nc -z localhost 9092; then
    exit 1
fi

kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic topic1_logs --partitions 2 --replication-factor 1

hdfs dfs -mkdir -p /user/hive/warehouse/ecommerce_dw.db/streaming_orders
hdfs dfs -mkdir -p /user/spark/checkpoints/streaming_orders

hive -f "$BASE_DIR/hive/create_table.sql" > "$LOG_DIR/hive_ddl.log" 2>&1

python3 "$BASE_DIR/producer/generate_orders.py" > "$LOG_DIR/producer.log" 2>&1 &
PRODUCER_PID=$!
echo $PRODUCER_PID > "$PID_DIR/producer.pid"

spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
  "$BASE_DIR/spark/spark_streaming.py" > "$LOG_DIR/spark.log" 2>&1 &
SPARK_PID=$!
echo $SPARK_PID > "$PID_DIR/spark.pid"
