# spark/etl.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

# 1. Spark session with Kafka support
spark = SparkSession.builder \
    .appName("CryptoPriceAnomalyDetector") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Read from Kafka topic
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "crypto_prices") \
    .option("startingOffsets", "latest") \
    .load()

# 3. Define schema for parsing messages
schema = StructType() \
    .add("symbol", StringType()) \
    .add("price_usd", DoubleType()) \
    .add("timestamp", StringType())  # ISO string

# 4. Parse and convert Kafka value
parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp("timestamp"))

# 5. Detect anomalies using price jumps in a 10-second window
windowed_df = parsed_df \
    .withWatermark("event_time", "30 seconds") \
    .groupBy(window("event_time", "10 seconds"), "symbol") \
    .agg(
        avg("price_usd").alias("avg_price"),
        stddev("price_usd").alias("std_dev"),
        max("price_usd").alias("max_price"),
        min("price_usd").alias("min_price")
    ) \
    .withColumn("spike", expr("max_price - min_price > std_dev * 3"))

# 6. Output to console
query = windowed_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
