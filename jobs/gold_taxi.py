import os
from pyspark.sql import SparkSession

S3_ENDPOINT = "http://minio:9000"
S3_BUCKET = "s3a://warehouse"


def create_spark():
    return (
        SparkSession.builder
        .appName("project3_gold_taxi")
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

spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse.gold")

spark.sql("""
CREATE OR REPLACE TABLE lakehouse.gold.analytical_taxi_trips
USING ICEBERG
PARTITIONED BY (months(tpep_pickup_datetime), bucket(16, PU_Zone))
AS
SELECT
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    PU_Zone,
    PU_Borough,
    PU_service_zone,
    total_amount,
    CAST(
        ROUND(
            (unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 60.0
        ) AS INT
    ) AS trip_duration_minutes
FROM lakehouse.silver.fct_taxi_trip
WHERE tpep_pickup_datetime IS NOT NULL
  AND tpep_dropoff_datetime IS NOT NULL
  AND tpep_dropoff_datetime > tpep_pickup_datetime
  AND total_amount >= 0
""")

spark.sql("""
CREATE OR REPLACE TABLE lakehouse.gold.taxi_zone_hourly_metrics
USING ICEBERG
PARTITIONED BY (pickup_date)
AS
SELECT
    DATE(tpep_pickup_datetime) AS pickup_date,
    date_trunc('hour', tpep_pickup_datetime) AS pickup_hour,
    PU_Zone,
    PU_Borough,
    COUNT(*) AS trip_count,
    ROUND(AVG(total_amount), 2) AS avg_total_amount,
    ROUND(AVG(trip_duration_minutes), 2) AS avg_trip_duration_minutes,
    ROUND(AVG(total_amount / NULLIF(trip_duration_minutes, 0)), 2) AS avg_amount_per_minute
FROM lakehouse.gold.analytical_taxi_trips
WHERE trip_duration_minutes > 0
GROUP BY
    DATE(tpep_pickup_datetime),
    date_trunc('hour', tpep_pickup_datetime),
    PU_Zone,
    PU_Borough
""")

count_main = spark.sql("SELECT COUNT(*) FROM lakehouse.gold.analytical_taxi_trips").collect()[0][0]
count_metrics = spark.sql("SELECT COUNT(*) FROM lakehouse.gold.taxi_zone_hourly_metrics").collect()[0][0]

print(f"Gold taxi complete.")
print(f"lakehouse.gold.analytical_taxi_trips rows: {count_main}")
print(f"lakehouse.gold.taxi_zone_hourly_metrics rows: {count_metrics}")

spark.stop()