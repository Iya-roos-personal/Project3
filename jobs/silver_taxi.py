import os
from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    TimestampType,
    DoubleType,
    StringType,
)

S3_ENDPOINT = "http://minio:9000"
S3_BUCKET = "s3a://warehouse"


def create_spark():
    return (
        SparkSession.builder
        .appName("project3_silver_taxi")
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

spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse.silver")

taxi_schema = StructType([
    StructField("VendorID", IntegerType()),
    StructField("tpep_pickup_datetime", TimestampType()),
    StructField("tpep_dropoff_datetime", TimestampType()),
    StructField("passenger_count", IntegerType()),
    StructField("trip_distance", DoubleType()),
    StructField("RatecodeID", IntegerType()),
    StructField("store_and_fwd_flag", StringType()),
    StructField("PULocationID", IntegerType()),
    StructField("DOLocationID", IntegerType()),
    StructField("payment_type", IntegerType()),
    StructField("fare_amount", DoubleType()),
    StructField("extra", DoubleType()),
    StructField("mta_tax", DoubleType()),
    StructField("tip_amount", DoubleType()),
    StructField("tolls_amount", DoubleType()),
    StructField("improvement_surcharge", DoubleType()),
    StructField("total_amount", DoubleType()),
    StructField("congestion_surcharge", DoubleType()),
    StructField("Airport_fee", DoubleType()),
    StructField("cbd_congestion_fee", DoubleType()),
])

bronze_df = spark.table("lakehouse.bronze.stg_taxi")

silver_df = (
    bronze_df
    .select(F.from_json("value", taxi_schema).alias("d"))
    .select(
        F.col("d.VendorID").cast("int").alias("VendorID"),
        F.col("d.RatecodeID").cast("int").alias("RatecodeID"),
        F.col("d.PULocationID").cast("int").alias("PULocationID"),
        F.col("d.DOLocationID").cast("int").alias("DOLocationID"),
        F.to_timestamp("d.tpep_pickup_datetime").alias("tpep_pickup_datetime"),
        F.to_timestamp("d.tpep_dropoff_datetime").alias("tpep_dropoff_datetime"),
        F.col("d.passenger_count").cast("int").alias("passenger_count"),
        F.col("d.trip_distance").cast("double").alias("trip_distance"),
        F.col("d.store_and_fwd_flag").cast("string").alias("store_and_fwd_flag"),
        F.col("d.payment_type").cast("int").alias("payment_type"),
        F.col("d.fare_amount").cast("double").alias("fare_amount"),
        F.col("d.extra").cast("double").alias("extra"),
        F.col("d.mta_tax").cast("double").alias("mta_tax"),
        F.col("d.tip_amount").cast("double").alias("tip_amount"),
        F.col("d.tolls_amount").cast("double").alias("tolls_amount"),
        F.col("d.improvement_surcharge").cast("double").alias("improvement_surcharge"),
        F.col("d.congestion_surcharge").cast("double").alias("congestion_surcharge"),
        F.col("d.Airport_fee").cast("double").alias("Airport_fee"),
        F.col("d.cbd_congestion_fee").cast("double").alias("cbd_congestion_fee"),
        F.col("d.total_amount").cast("double").alias("total_amount"),
    )
    .filter(F.col("tpep_pickup_datetime").isNotNull())
    .filter(F.col("tpep_dropoff_datetime").isNotNull())
    .filter(F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
    .filter(F.col("PULocationID").isNotNull())
    .filter(F.col("DOLocationID").isNotNull())
    .filter(F.col("trip_distance") >= 0)
    .filter(F.col("fare_amount") >= 0)
    .na.fill({
        "passenger_count": 1,
        "trip_distance": 0.0,
        "store_and_fwd_flag": "N",
        "payment_type": 0,
        "fare_amount": 0.0,
        "extra": 0.0,
        "mta_tax": 0.0,
        "tip_amount": 0.0,
        "tolls_amount": 0.0,
        "improvement_surcharge": 0.0,
        "congestion_surcharge": 0.0,
        "Airport_fee": 0.0,
        "cbd_congestion_fee": 0.0,
    })
)

fee_components = (
    F.col("fare_amount")
    + F.col("extra")
    + F.col("mta_tax")
    + F.col("tip_amount")
    + F.col("tolls_amount")
    + F.col("improvement_surcharge")
    + F.col("congestion_surcharge")
    + F.col("Airport_fee")
    + F.col("cbd_congestion_fee")
)

silver_df = silver_df.withColumn(
    "total_amount",
    F.coalesce(F.col("total_amount"), fee_components),
)

silver_df = silver_df.withColumn(
    "tripID",
    F.sha2(
        F.concat_ws(
            "|",
            F.col("VendorID").cast("string"),
            F.col("tpep_pickup_datetime").cast("string"),
            F.col("tpep_dropoff_datetime").cast("string"),
            F.col("PULocationID").cast("string"),
            F.col("DOLocationID").cast("string"),
            F.col("total_amount").cast("string"),
        ),
        256,
    ),
)

window = Window.partitionBy("tripID").orderBy(F.col("tpep_pickup_datetime").desc())

silver_df = (
    silver_df
    .withColumn("rn", F.row_number().over(window))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

zones = spark.read.parquet("/home/jovyan/project/data/taxi_zone_lookup.parquet").select(
    F.col("LocationID").cast("int").alias("LocationID"),
    F.col("Zone"),
    F.col("Borough"),
    F.col("service_zone"),
)

z_pu = zones.alias("z_pu")
z_do = zones.alias("z_do")
s = silver_df.alias("s")

silver_enriched = (
    s
    .join(F.broadcast(z_pu), F.col("s.PULocationID") == F.col("z_pu.LocationID"), "left")
    .join(F.broadcast(z_do), F.col("s.DOLocationID") == F.col("z_do.LocationID"), "left")
    .select(
        F.col("s.tripID"),
        F.col("s.VendorID"),
        F.col("s.RatecodeID"),
        F.col("s.PULocationID"),
        F.col("s.DOLocationID"),
        F.col("s.tpep_pickup_datetime"),
        F.col("s.tpep_dropoff_datetime"),
        F.col("s.passenger_count"),
        F.col("s.trip_distance"),
        F.col("s.store_and_fwd_flag"),
        F.col("s.payment_type"),
        F.col("s.fare_amount"),
        F.col("s.extra"),
        F.col("s.mta_tax"),
        F.col("s.tip_amount"),
        F.col("s.tolls_amount"),
        F.col("s.improvement_surcharge"),
        F.col("s.congestion_surcharge"),
        F.col("s.Airport_fee"),
        F.col("s.cbd_congestion_fee"),
        F.col("s.total_amount"),
        F.col("z_pu.Zone").alias("PU_Zone"),
        F.col("z_pu.Borough").alias("PU_Borough"),
        F.col("z_pu.service_zone").alias("PU_service_zone"),
        F.col("z_do.Zone").alias("DO_Zone"),
        F.col("z_do.Borough").alias("DO_Borough"),
        F.col("z_do.service_zone").alias("DO_service_zone"),
    )
)

# Deterministic full rebuild from Bronze.
# This avoids Spark/Iceberg MERGE planning errors and remains idempotent because
# Bronze taxi is deduplicated by Kafka partition + offset.
spark.sql("DROP TABLE IF EXISTS lakehouse.silver.fct_taxi_trip")

(
    silver_enriched.writeTo("lakehouse.silver.fct_taxi_trip")
    .using("iceberg")
    .tableProperty("write.identifier-columns", "tripID")
    .create()
)

count = spark.sql("SELECT COUNT(*) FROM lakehouse.silver.fct_taxi_trip").collect()[0][0]
print(f"Silver taxi complete. lakehouse.silver.fct_taxi_trip rows: {count}")

spark.stop()