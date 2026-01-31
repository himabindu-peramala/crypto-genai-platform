from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Configuration
KAFKA_BOOTSTRAP_SERVERS = 'redpanda:29092'
TOPIC_NAME = 'crypto_market_data'
ICEBERG_WAREHOUSE = 's3a://lakehouse/'
CHECKPOINT_LOCATION = 's3a://checkpoint/crypto_prices_v2'

def create_spark_session():
    # We will pass packages via spark-submit CLI to keep Python script clean
    # But we define the S3/Iceberg configs here or in CLI. 
    # For simplicity, we assume they are passed or configured.
    # Actually, defining them here ensures they are applied.
    
    return SparkSession.builder \
        .appName("CryptoIcebergIngestion") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.demo.type", "hadoop") \
        .config("spark.sql.catalog.demo.warehouse", ICEBERG_WAREHOUSE) \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minio") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

def process_stream():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Schema
    schema = StructType([
        StructField("event_time", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("volume", DoubleType(), True),
        StructField("source", StringType(), True)
    ])

    # 1. Read Stream
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", TOPIC_NAME) \
        .option("startingOffsets", "latest") \
        .load()

    # 2. Transform
    parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    parsed_df = parsed_df.withColumn("timestamp", col("event_time").cast(TimestampType()))

    # 2.5 Ensure Table Exists
    # We use the catalog 'demo' to create the table.
    spark.sql("""
        CREATE TABLE IF NOT EXISTS demo.crypto_prices_v2 (
            event_time STRING,
            symbol STRING,
            price DOUBLE,
            volume DOUBLE,
            source STRING,
            timestamp TIMESTAMP
        )
        USING iceberg
    """)

    # 3. Write Stream to Iceberg
    # We write to the 'demo' catalog, database 'db', table 'crypto_prices'
    query = parsed_df.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .trigger(processingTime="1 minute") \
        .option("path", "demo.crypto_prices_v2") \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .start()

    print("Streaming to Iceberg... (Check MinIO 'lakehouse' bucket)")
    query.awaitTermination()

if __name__ == "__main__":
    process_stream()
