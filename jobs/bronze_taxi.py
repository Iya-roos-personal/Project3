import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = "taxi-trips"

S3_ENDPOINT = "http://minio:9000"
S3_BUCKET = "s3a://warehouse"


def create_spark():
    return (
        SparkSession.builder
        .appName("project3_bronze_taxi")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "rest")
        .config("spark.sql.catalog.lakehouse.uri", "http://iceberg-rest:8181")
        .config("spark.sql.catalog.lakehouse.warehouse", S3_BUCKET)
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.lakehouse.s3.endpoint", S3_ENDPOINT)
        .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
        .config("spark.sql.catalog.lakehouse.s3.access-key-id", os.environ["AWS_ACCESS_KEY_ID"])
        .config("spark.sql.catalog.lakehouse.s3.secret-access-key", os.environ["AWS_SECRET_ACCESS_KEY"])
        .config("spark.sql.catalog.lakehouse.s3.region", "us-east-1")
        .getOrCreate()
    )


spark = create_spark()
spark.sparkContext.setLogLevel("WARN")

spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse.bronze")

spark.sql("""
CREATE TABLE IF NOT EXISTS lakehouse.bronze.stg_taxi (
    kafka_time TIMESTAMP,
    key STRING,
    offset BIGINT,
    partition INT,
    value STRING
) USING iceberg
""")

raw_df = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
)

batch_df = raw_df.select(
    F.col("timestamp").alias("kafka_time"),
    F.col("key").cast("string").alias("key"),
    F.col("offset").cast("long").alias("offset"),
    F.col("partition").cast("int").alias("partition"),
    F.col("value").cast("string").alias("value"),
)

batch_df.createOrReplaceTempView("taxi_bronze_batch")

spark.sql("""
MERGE INTO lakehouse.bronze.stg_taxi t
USING taxi_bronze_batch s
ON t.partition = s.partition
AND t.offset = s.offset
WHEN NOT MATCHED THEN INSERT *
""")

count = spark.sql("SELECT COUNT(*) FROM lakehouse.bronze.stg_taxi").collect()[0][0]
print(f"Bronze taxi complete. lakehouse.bronze.stg_taxi rows: {count}")

spark.stop()