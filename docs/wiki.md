# Repository Wiki

---

## Overview
`realtime-transaction-pipeline` is a Docker-based demonstration of a real-time analytics workflow. It simulates incoming purchase transactions, aggregates them with Apache Spark, and stores the results in ClickHouse. Kafka serves as the messaging backbone.

Key components:

1. **Producer** – Generates random transactions and publishes them to Kafka.
2. **Spark streaming job** – Reads raw transactions from Kafka, filters invalid records, and computes minute-level aggregates.
3. **ClickHouse consumer** – Reads the aggregated stream from Kafka and writes it to ClickHouse.
4. **Infrastructure** – Docker Compose orchestrates Kafka, Zookeeper, Spark master/worker, ClickHouse, and the custom services.

---

## Directory Layout
```
.
├── clickhouse-consumer/          # Kafka-to-ClickHouse ingestion service
├── clickhouse-init/              # SQL scripts executed when ClickHouse starts
├── producer/                     # Demo transaction generator
├── spark-job/                    # Spark structured streaming job
└── docker-compose.yml            # Service orchestration
```

---

## Running the Stack
1. **Prerequisites**
   - Docker and Docker Compose installed.

2. **Start services**
   ```bash
   docker-compose up --build
   ```
   This command launches Kafka, Zookeeper, ClickHouse, Spark master and worker, the streaming job, and the consumer.

3. **Send sample data**
   Run the producer locally:
   ```bash
   python producer/producer.py
   ```
   The script continuously sends randomized transactions to the `transactions_raw` Kafka topic.

4. **Monitor ClickHouse**
   Connect to ClickHouse (port `9000` or HTTP `8123`) to inspect the `aggregated_analytics` table defined in `clickhouse-init/init.sql`.

5. **Verify Kafka streams**
   ```bash
   # Read a few raw events
   docker-compose exec kafka kafka-console-consumer \
     --bootstrap-server kafka:29092 \
     --topic transactions_raw --from-beginning --max-messages 5

   # Read a few aggregated events
   docker-compose exec kafka kafka-console-consumer \
     --bootstrap-server kafka:29092 \
     --topic transactions_agg --from-beginning --max-messages 5
   ```
   These commands confirm data is flowing through both topics.

6. **Query stored data**
   ```bash
   docker-compose exec clickhouse-server clickhouse-client \
     --user consumer --password MySecret \
     --query "SELECT * FROM aggregated_analytics LIMIT 5;"
   ```
   Replace `aggregated_analytics` with any table you want to inspect.

---

## Producer
The producer creates synthetic purchase events using `faker` and `random` libraries. It publishes JSON to Kafka with fields such as `order_id`, `user_id`, `product_id`, `total_amount`, and `payment_type`. Some records deliberately contain invalid or missing `payment_type` values to demonstrate filtering logic.

---

## Spark Streaming Job
`spark-job/stream_job.py` consumes raw events from Kafka and performs the following steps:

1. Create the Spark session with the Kafka connector package.
2. Define the schema for incoming JSON records.
3. Read from Kafka topic `transactions_raw` and parse JSON.
4. Filter invalid records (non-negative amounts and valid payment types).
5. Aggregate over 1-minute tumbling windows: total sales, total orders, average order value, and a placeholder for top product.
6. Write aggregated output back to Kafka (`transactions_agg`) with checkpointing.

---

## ClickHouse Consumer
`clickhouse-consumer/consumer.py` subscribes to the aggregated Kafka topic and inserts batches into ClickHouse. It uses environment variables for configuration, retries Kafka connections, and parses timestamps flexibly before insertion.

---

## Database Initialization
`clickhouse-init/` contains scripts executed automatically when the ClickHouse container starts. `init.sql` creates the `aggregated_analytics` table and `01-create-user.sql` defines a user with permissions.

## Access Credentials
The default ClickHouse user created by the initialization scripts is:

- **Username:** `consumer`
- **Password:** `MySecret`

Use these credentials when connecting via `clickhouse-client` or any ClickHouse GUI.

---

## Data Flow
1. **Producer** pushes transactions to `transactions_raw`.
2. **Spark job** reads `transactions_raw`, filters and aggregates into 1-minute windows, then writes to `transactions_agg`.
3. **ClickHouse consumer** ingests `transactions_agg` and stores results in `aggregated_analytics`.

---

## Customization Tips
- Modify `producer/producer.py` to generate different payloads or adjust `PAYMENT_TYPES`.
- In `spark-job/stream_job.py`, update the aggregation logic (e.g., implement real top-product ranking).
- Adjust ClickHouse connection settings using environment variables in `docker-compose.yml`.
- Checkpoint location for Spark can be changed from `/tmp/checkpoints`.

---

## Troubleshooting
- Check container logs with `docker-compose logs <service>` if a component fails to start.
- Kafka connection errors trigger retries so initial startup delays may occur.
- Ensure ports (`9092`, `8123`, `9000`, `7077`, etc.) are not in use by other processes.

