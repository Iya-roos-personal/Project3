import os

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    BooleanType,
)


S3_ENDPOINT = "http://minio:9000"
S3_BUCKET = "s3://warehouse/"


def create_spark():
    return (
        SparkSession.builder
        .appName("silver-cdc")
        .config(
            "spark.jars.packages",
            ",".join(
                [
                    "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0",
                    "org.apache.iceberg:iceberg-aws-bundle:1.10.0",
                ]
            ),
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.demo.type", "rest")
        .config("spark.sql.catalog.demo.uri", "http://iceberg-rest:8181")
        .config("spark.sql.catalog.demo.warehouse", S3_BUCKET)
        .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.demo.s3.endpoint", S3_ENDPOINT)
        .config("spark.sql.catalog.demo.s3.access-key-id", os.getenv("AWS_ACCESS_KEY_ID", "admin"))
        .config("spark.sql.catalog.demo.s3.secret-access-key", os.getenv("AWS_SECRET_ACCESS_KEY", "admin123"))
        .config("spark.sql.catalog.demo.s3.path-style-access", "true")
        .config("spark.sql.catalog.demo.s3.region", "us-east-1")
        .config("spark.sql.catalog.demo.client.region", "us-east-1")
        .getOrCreate()
    )


customer_schema = StructType(
    [
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("email", StringType()),
        StructField("country", StringType()),
        StructField("created_at", StringType()),
    ]
)

driver_schema = StructType(
    [
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("license_number", StringType()),
        StructField("rating", DoubleType()),
        StructField("city", StringType()),
        StructField("active", BooleanType()),
        StructField("created_at", StringType()),
    ]
)


def parse_created_at(col):
    return F.when(
        col.rlike("^[0-9]+$"),
        F.timestamp_micros(col.cast("long")),
    ).otherwise(F.to_timestamp(col))


def ensure_silver_tables(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.silver")

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS demo.silver.customers (
            id INT,
            name STRING,
            email STRING,
            country STRING,
            created_at TIMESTAMP,
            cdc_event_ts_ms BIGINT,
            cdc_lsn BIGINT,
            cdc_kafka_offset BIGINT
        )
        USING iceberg
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS demo.silver.drivers (
            id INT,
            name STRING,
            license_number STRING,
            rating DOUBLE,
            city STRING,
            active BOOLEAN,
            created_at TIMESTAMP,
            cdc_event_ts_ms BIGINT,
            cdc_lsn BIGINT,
            cdc_kafka_offset BIGINT
        )
        USING iceberg
        """
    )


def merge_customers(spark):
    bronze = spark.table("demo.bronze.customers_cdc")

    source = (
        bronze.filter(F.col("op").isin("c", "u", "r", "d"))
        .withColumn("after", F.from_json(F.col("after_json"), customer_schema))
        .select(
            F.col("pk_id").cast("int").alias("id"),
            F.col("op"),
            F.col("event_ts_ms"),
            F.col("lsn"),
            F.col("kafka_offset"),
            F.col("after.name").alias("name"),
            F.col("after.email").alias("email"),
            F.col("after.country").alias("country"),
            parse_created_at(F.col("after.created_at")).alias("created_at"),
        )
    )

    source.createOrReplaceTempView("customers_events")

    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW customers_latest AS
        SELECT id, op, name, email, country, created_at, event_ts_ms, lsn, kafka_offset
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY id
                       ORDER BY event_ts_ms DESC NULLS LAST,
                                lsn DESC NULLS LAST,
                                kafka_offset DESC
                   ) AS rn
            FROM customers_events
            WHERE id IS NOT NULL
        )
        WHERE rn = 1
        """
    )

    spark.sql(
        """
        MERGE INTO demo.silver.customers t
        USING customers_latest s
        ON t.id = s.id

        WHEN MATCHED AND s.op = 'd' THEN DELETE

        WHEN MATCHED AND s.op IN ('c', 'u', 'r') THEN UPDATE SET
            name = s.name,
            email = s.email,
            country = s.country,
            created_at = s.created_at,
            cdc_event_ts_ms = s.event_ts_ms,
            cdc_lsn = s.lsn,
            cdc_kafka_offset = s.kafka_offset

        WHEN NOT MATCHED AND s.op IN ('c', 'u', 'r') THEN INSERT (
            id,
            name,
            email,
            country,
            created_at,
            cdc_event_ts_ms,
            cdc_lsn,
            cdc_kafka_offset
        ) VALUES (
            s.id,
            s.name,
            s.email,
            s.country,
            s.created_at,
            s.event_ts_ms,
            s.lsn,
            s.kafka_offset
        )
        """
    )

    count = spark.sql("SELECT COUNT(*) AS n FROM demo.silver.customers").collect()[0]["n"]
    print(f"Silver customers rows={count}")


def merge_drivers(spark):
    bronze = spark.table("demo.bronze.drivers_cdc")

    source = (
        bronze.filter(F.col("op").isin("c", "u", "r", "d"))
        .withColumn("after", F.from_json(F.col("after_json"), driver_schema))
        .select(
            F.col("pk_id").cast("int").alias("id"),
            F.col("op"),
            F.col("event_ts_ms"),
            F.col("lsn"),
            F.col("kafka_offset"),
            F.col("after.name").alias("name"),
            F.col("after.license_number").alias("license_number"),
            F.col("after.rating").alias("rating"),
            F.col("after.city").alias("city"),
            F.col("after.active").alias("active"),
            parse_created_at(F.col("after.created_at")).alias("created_at"),
        )
    )

    source.createOrReplaceTempView("drivers_events")

    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW drivers_latest AS
        SELECT id, op, name, license_number, rating, city, active,
               created_at, event_ts_ms, lsn, kafka_offset
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY id
                       ORDER BY event_ts_ms DESC NULLS LAST,
                                lsn DESC NULLS LAST,
                                kafka_offset DESC
                   ) AS rn
            FROM drivers_events
            WHERE id IS NOT NULL
        )
        WHERE rn = 1
        """
    )

    spark.sql(
        """
        MERGE INTO demo.silver.drivers t
        USING drivers_latest s
        ON t.id = s.id

        WHEN MATCHED AND s.op = 'd' THEN DELETE

        WHEN MATCHED AND s.op IN ('c', 'u', 'r') THEN UPDATE SET
            name = s.name,
            license_number = s.license_number,
            rating = s.rating,
            city = s.city,
            active = s.active,
            created_at = s.created_at,
            cdc_event_ts_ms = s.event_ts_ms,
            cdc_lsn = s.lsn,
            cdc_kafka_offset = s.kafka_offset

        WHEN NOT MATCHED AND s.op IN ('c', 'u', 'r') THEN INSERT (
            id,
            name,
            license_number,
            rating,
            city,
            active,
            created_at,
            cdc_event_ts_ms,
            cdc_lsn,
            cdc_kafka_offset
        ) VALUES (
            s.id,
            s.name,
            s.license_number,
            s.rating,
            s.city,
            s.active,
            s.created_at,
            s.event_ts_ms,
            s.lsn,
            s.kafka_offset
        )
        """
    )

    count = spark.sql("SELECT COUNT(*) AS n FROM demo.silver.drivers").collect()[0]["n"]
    print(f"Silver drivers rows={count}")


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    ensure_silver_tables(spark)
    merge_customers(spark)
    merge_drivers(spark)

    print("Silver CDC MERGE complete.")
    spark.stop()


if __name__ == "__main__":
    main()