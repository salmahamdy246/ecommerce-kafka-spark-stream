# Stream Ingestion & Analytics Pipeline

## Project Overview

Processes thousands of customer order transactions every minute across multiple regions. This project replaces an outdated nightly batch reporting process with an automated, fault-tolerant real-time streaming pipeline.

The system continuously generates simulated live transaction records, buffers events in Apache Kafka, processes micro-batches using PySpark Structured Streaming, stores optimized Parquet data in HDFS, exposes tables via an external Hive metastore, and automates control operations using bash orchestration scripts.

---

## Architecture & Data Flow

```text
+-------------------+        +-------------------+        +------------------------+        +-------------------+        +-------------------+
|  Python Producer  | -----> |    Apache Kafka   | -----> |   Spark Streaming      | -----> |     HDFS Storage  | <----> |  Hive External    |
| (generate_orders) | JSON   |   (topic1_logs)   | Stream | (spark_streaming.py)   | Parquet|   (ecommerce_dw)  | Table  |  Metastore & OLAP |
+-------------------+        +-------------------+        +------------------------+        +-------------------+        +-------------------+

```

### Data Flow Steps

1. Event Generation & Kafka Producer (`generate_orders.py`): Generates live synthetic transaction records (JSON format) containing fields like `order_id`, `customer_id`, `product_id`, `quantity`, `price`, and `order_time`. Messages are published asynchronously to the Kafka topic `topic1_logs` at 1–2 second intervals.
2. Real-time Ingestion & Processing (`spark_streaming.py`): Spark Structured Streaming consumes events from `topic1_logs`. It parses the binary JSON, casts data types, computes the derived metric `total_amount = quantity * price`, converts timestamps, and writes 10-second micro-batches into HDFS.
3. Data Persistence (HDFS Parquet): Data is written in columnar Parquet format to HDFS at `/user/hive/warehouse/ecommerce_dw.db/streaming_orders`. Engine state is saved at `/user/spark/checkpoints/streaming_orders` for fault-tolerant state recovery.
4. Lakehouse Analytics (`create_table.sql`): An external Hive table (`streaming_orders`) is mapped directly to the Parquet directory on HDFS, allowing instant analytical queries without data movement.

---

## Prerequisites & Dependencies

### Required Services

* Hadoop (HDFS & YARN): Version 3.x+ running on `localhost`
* Apache Kafka & Zookeeper: Running on `localhost:9092`
* Apache Hive: Hive metastore service active
* Apache Spark: Version 3.x+ with PySpark support

### Python Packages

```bash
pip3 install kafka-python

```

---

## Directory Topology

```text
kafka_spark_pipeline/
├── producer/
│   └── generate_orders.py
├── spark/
│   └── spark_streaming.py
├── hive/
│   └── create_table.sql
├── scripts/
│   ├── start_pipeline.sh
│   └── stop_pipeline.sh
└── README.md

```

---

## Deployment & Execution Guide

### 1. Make Scripts Executable

```bash
chmod +x kafka_spark_pipeline/scripts/*.sh

```

### 2. Start the Complete Pipeline

Run the initialization script:

```bash
./kafka_spark_pipeline/scripts/start_pipeline.sh

```

This script automatically performs the following tasks:

* Verifies HDFS and Kafka health.
* Ensures the `topic1_logs` Kafka topic exists.
* Provisions required HDFS storage and checkpoint directories.
* Executes the Hive SQL DDL setup script.
* Launches `generate_orders.py` and `spark_streaming.py` as background processes.

### 3. Stop the Pipeline Safely

Run the shutdown script:

```bash
./kafka_spark_pipeline/scripts/stop_pipeline.sh

```

This script gracefully stops the Python producer and Spark streaming job using PID tracking while preserving all checkpoint files and HDFS Parquet data.

---

## Verification & Testing Commands

* **Kafka Message Stream:**
```bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic topic1_logs --from-beginning

```


* **HDFS Parquet Files:**
```bash
hdfs dfs -ls /user/hive/warehouse/ecommerce_dw.db/streaming_orders

```


* **Execute Hive Analytics Queries:**
```bash
hive -f kafka_spark_pipeline/hive/create_table.sql

```
