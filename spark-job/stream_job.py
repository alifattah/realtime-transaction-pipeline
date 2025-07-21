from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, to_json, to_timestamp,
    col, window, struct, expr,
    sum as _sum, count as _count, avg as _avg
)
from pyspark.sql.types import (
    StructType, StructField, LongType, IntegerType,
    DoubleType, StringType, TimestampType
)

# --- Initialize SparkSession ---
# Sets up the entry point for Spark functionality, configuring the application name,
# master URL, and necessary Kafka package for stream processing.
spark = SparkSession.builder \
    .appName("RealTimeAggregations") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

# --- Define the Schema for Incoming Data ---
# Specifies the structure and data types of the JSON messages expected from the raw Kafka topic.
# This ensures data is parsed correctly and consistently.
schema = StructType([
    StructField("order_id", LongType()),
    StructField("user_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("quantity", IntegerType()),
    StructField("price", DoubleType()),
    StructField("total_amount", DoubleType()),
    StructField("payment_type", StringType()),
    StructField("transaction_time", StringType())
])

# --- Read Data Stream from Kafka ---
# Subscribes to the 'transactions_raw' Kafka topic to create a streaming DataFrame.
# 'startingOffsets': 'latest' means it will only process new messages.
df_raw = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "transactions_raw") \
    .option("startingOffsets", "latest") \
    .load()

# --- Parse JSON and Prepare DataFrame ---
# Deserializes the JSON string from the 'value' column into a structured format using the defined schema.
# Also, converts the string-based transaction time into a proper timestamp format for windowing operations.
df = df_raw.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*") \
    .withColumn("transaction_time", to_timestamp(col("transaction_time")))

# --- Filter Invalid or Unwanted Records ---
# Cleans the data by removing records with negative monetary values or payment types
# that are not in the accepted list ('Credit Card', 'Internet', 'POS').
df_clean = df.filter(
    (col("price") >= 0) &
    (col("total_amount") >= 0) &
    col("payment_type").isin("کارت اعتباری", "اینترنت", "POS")
)

# 6. Define Aggregations over a Tumbling Window
# Groups data into 1-minute, non-overlapping (tumbling) windows based on the transaction time.
# For each window, it calculates the total sales, total order count, and average order value.
agg = df_clean \
    .groupBy(window(col("transaction_time"), "1 minute")) \
    .agg(
        _sum("total_amount").alias("total_sales"),
        _count("*").alias("total_orders"),
        _avg("total_amount").alias("avg_order_value"),
        expr("first(product_id)").alias("top_product_id")
    )

# --- Format the Output for Kafka ---
# Prepares the aggregated data to be sent to a new Kafka topic.
# All relevant columns are packed into a single JSON string in the 'value' column.
output = agg.selectExpr(
    "window.start as window_start",
    "total_sales", "total_orders", "avg_order_value", "top_product_id"
).withColumn(
    "value", to_json(struct("*"))
).select("value")

# --- Write the Aggregated Stream to Kafka ---
# Starts the streaming query to write the processed data to the 'transactions_agg' topic.
# - outputMode("update"): Sends only the updated window results for each batch.
# - checkpointLocation: Saves the stream's progress to handle failures and ensure exactly-once semantics.
query = output \
    .writeStream \
    .format("kafka") \
    .outputMode("update") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("topic", "transactions_agg") \
    .option("checkpointLocation", "/tmp/checkpoints") \
    .start()

# Wait for the streaming query to terminate (e.g., by manual interruption or an error).
query.awaitTermination()
