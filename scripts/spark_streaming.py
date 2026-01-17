from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Configuration
KAFKA_BOOTSTRAP_SERVERS = 'redpanda:29092' # Internal Docker networking
TOPIC_NAME = 'crypto_market_data'

def create_spark_session():
    return SparkSession.builder \
        .appName("CryptoStreamProcessor") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

def process_stream():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Define schema matching producer
    schema = StructType([
        StructField("event_time", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("volume", DoubleType(), True),
        StructField("source", StringType(), True)
    ])

    # Read from Redpanda (Kafka)
    # Note: We use the INTERNAL port 29092 because Spark is running inside Docker
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", TOPIC_NAME) \
        .option("startingOffsets", "latest") \
        .load()

    # Parse JSON
    parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    
    # Cast event_time to Timestamp
    parsed_df = parsed_df.withColumn("timestamp", col("event_time").cast(TimestampType()))

    # Basic Transformation: Calculate 30-second moving average per symbol
    windowed_counts = parsed_df \
        .withWatermark("timestamp", "1 minute") \
        .groupBy(
            window(col("timestamp"), "30 seconds"),
            col("symbol")
        ) \
        .agg(avg("price").alias("avg_price"))

    # Write to Console (for debugging)
    query = windowed_counts.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    process_stream()
