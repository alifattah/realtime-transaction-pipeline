# import os
# import json
# import time
# from kafka import KafkaConsumer
# from clickhouse_driver import Client
# from datetime import datetime

# # تنظیمات از environment
# KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
# CH_HOST = os.getenv("CLICKHOUSE_HOST")
# CH_PORT = int(os.getenv("CLICKHOUSE_PORT", 9000))
# CH_DB = os.getenv("CLICKHOUSE_DB", "default")
# CH_TABLE = os.getenv("CLICKHOUSE_TABLE", "aggregated_analytics")

# user = os.getenv("CLICKHOUSE_USER", "default")
# password = os.getenv("CLICKHOUSE_PASSWORD", "")
# host = os.getenv("CLICKHOUSE_HOST", "clickhouse-server")
# port = int(os.getenv("CLICKHOUSE_PORT", 9000))

# client = Client(host=host, port=port, user=user, password=password,
#                 database=os.getenv("CLICKHOUSE_DB", "default"))

# # راه‌اندازی Consumer Kafka
# consumer = KafkaConsumer(
#     'transactions_agg',
#     bootstrap_servers=[KAFKA_SERVERS],
#     auto_offset_reset='earliest',
#     value_deserializer=lambda m: json.loads(m.decode('utf-8')),
#     consumer_timeout_ms=1000
# )

# # اتصال به ClickHouse with user and password
# client = Client(
#     host=CH_HOST,
#     port=CH_PORT,
#     database=CH_DB,
#     user=user,
#     password=password
# )

# print("🚀 Consumer to ClickHouse is up and running…")

# while True:
#     batch = []
#     for msg in consumer:
#         data = msg.value
#         try:
#             # Handle ISO 8601 format: '2025-07-20T19:27:00.000Z'
#             order_date = datetime.strptime(data['window_start'], '%Y-%m-%dT%H:%M:%S.%fZ')
#             total_sales = float(data['total_sales'])
#             total_orders = int(data['total_orders'])
#             avg_order_value = float(data['avg_order_value'])
#             top_product = str(data['top_product'])
#         except (KeyError, ValueError, TypeError) as e:
#             print(f"Error parsing message: {e} - Skipping: {data}")
#             continue

#         batch.append([
#             order_date,
#             total_sales,
#             total_orders,
#             avg_order_value,
#             top_product
#         ])

#     if batch:
#         client.execute(
#             f'INSERT INTO {CH_DB}.{CH_TABLE} '
#             '(order_date, total_sales, total_orders, avg_order_value, top_product) VALUES',
#             batch
#         )
#         print(f"Inserted {len(batch)} rows into ClickHouse")
#     time.sleep(5)


import os
import json
import time
from kafka import KafkaConsumer
from clickhouse_driver import Client
from datetime import datetime
from kafka.errors import NoBrokersAvailable

# --- HELPER FUNCTION: This is the key to solving the format issue ---
def parse_flexible_datetime(datetime_str):
    """
    Tries to parse a datetime string by trying a list of possible formats.
    """
    # Add any other timestamp formats you encounter to this list.
    formats_to_try = [
        '%Y-%m-%dT%H:%M:%S.%fZ',  # Format 1: ISO 8601 with Z
        '%Y-%m-%d %H:%M:%S'      # Format 2: Simple with space
    ]
    for fmt in formats_to_try:
        try:
            return datetime.strptime(datetime_str, fmt)
        except ValueError:
            continue
    # If no format matched, raise an error.
    raise ValueError(f"Time data '{datetime_str}' does not match any known format.")

# --- CONFIGURATION ---
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse-server")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", 9000))
CH_DB = os.getenv("CLICKHOUSE_DB", "default")
CH_TABLE = os.getenv("CLICKHOUSE_TABLE", "aggregated_analytics")
CH_USER = os.getenv("CLICKHOUSE_USER", "default")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions_agg")

# --- KAFKA CONNECTION WITH RETRY LOGIC ---
def create_kafka_consumer():
    for attempt in range(10):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=[KAFKA_SERVERS],
                auto_offset_reset='earliest',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                consumer_timeout_ms=5000  # Increased timeout
            )
            print("✅ Successfully connected to Kafka.", flush=True)
            return consumer
        except NoBrokersAvailable:
            print(f"⚠️ Kafka not available, retrying in 5s... (Attempt {attempt + 1})", flush=True)
            time.sleep(5)
    raise Exception("Could not connect to Kafka after multiple retries.")

# --- CLICKHOUSE CONNECTION ---
# We only need to create the client once.
client = Client(
    host=CH_HOST,
    port=CH_PORT,
    database=CH_DB,
    user=CH_USER,
    password=CH_PASSWORD,
    send_receive_timeout=60 # Added timeout for resilience
)
print("✅ ClickHouse client created.")

consumer = create_kafka_consumer()
print(f"🚀 Consumer to ClickHouse is up and running, listening to topic '{KAFKA_TOPIC}'...")

# --- MAIN PROCESSING LOOP ---
while True:
    batch = []
    print("---------------------------------", flush=True)
    print(f"[{datetime.now()}] Waiting for new messages...", flush=True)
    for msg in consumer:
        data = msg.value
        try:
            # ### CHANGE: Use the new flexible parsing function ###
            order_date = parse_flexible_datetime(data['window_start'])

            total_sales = float(data['total_sales'])
            total_orders = int(data['total_orders'])
            avg_order_value = float(data['avg_order_value'])
            top_product = str(data['top_product'])
            
            batch.append([
                order_date, total_sales, total_orders, avg_order_value, top_product
            ])
        except (KeyError, ValueError, TypeError) as e:
            print(f"❗️ Error processing message: {e} - Skipping: {data}", flush=True)
            continue
    
    if batch:
        try:
            client.execute(
                f'INSERT INTO {CH_DB}.{CH_TABLE} VALUES',
                batch
            )
            print(f"✅ Successfully inserted {len(batch)} rows into ClickHouse.", flush=True)
        except Exception as e:
            print(f"❌ Failed to insert batch into ClickHouse: {e}", flush=True)
    else:
        print("No new messages received in the last 5 seconds.", flush=True)

    time.sleep(5)