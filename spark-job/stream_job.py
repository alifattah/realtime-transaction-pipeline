from pyspark.sql.functions import to_json, struct
from pyspark.sql.functions import from_json, to_timestamp
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, window, sum as _sum, count as _count, avg as _avg, first
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, DoubleType, StringType, TimestampType

# 1. ساخت SparkSession
spark = SparkSession.builder \
    .appName("RealTimeAggregations") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

# 2. تعریف schema داده ورودی
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

# 3. خواندن استریم از Kafka
df_raw = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "transactions_raw") \
    .option("startingOffsets", "latest") \
    .load()

# 4. پارس کردن value JSON با schema

df = df_raw.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*") \
 .withColumn("transaction_time", to_timestamp(col("transaction_time")))

# 5. فیلتر رکوردهای نامعتبر
df_clean = df.filter(
    (col("price") >= 0) &
    (col("total_amount") >= 0) &
    col("payment_type").isin("کارت اعتباری", "اینترنت", "POS")
)

# 6. تعریف aggregation در پنجره ۱ دقیقه‌ای (tumbling window)
agg = df_clean \
    .groupBy(window(col("transaction_time"), "1 minute")) \
    .agg(
        _sum("total_amount").alias("total_sales"),
        _count("*").alias("total_orders"),
        _avg("total_amount").alias("avg_order_value"),
        # جایگزین این با logic پر فروش‌ترین
        expr("first(product_id)").alias("top_product")
    )

# 7. فرمت خروجی برای Kafka

output = agg.selectExpr(
    "window.start as window_start",
    "total_sales", "total_orders", "avg_order_value", "top_product"
).withColumn(
    "value", to_json(struct("*"))
).select("value")

# 8. نوشتن استریم به Kafka
query = output \
    .writeStream \
    .format("kafka") \
    .outputMode("update") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("topic", "transactions_agg") \
    .option("checkpointLocation", "/tmp/checkpoints") \
    .start()

query.awaitTermination()
