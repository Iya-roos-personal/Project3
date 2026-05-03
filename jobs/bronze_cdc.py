import os

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    IntegerType,
    DoubleType,
    BooleanType,
)


BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
S3_ENDPOINT = "http://minio:9000"
S3_BUCKET = "s3://warehouse/"


def create_spark():
    return (
        SparkSession.builder
        .appName("bronze-cdc")
        .config(
            "spark.jars.packages",
            ",".join(
                [
                    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0",
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


customer_row_schema = StructType(
    [
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("email", StringType()),
        StructField("country", StringType()),
        StructField("created_at", StringType()),
    ]
)

driver_row_schema = StructType(
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

source_schema = StructType(
    [
        StructField("lsn", LongType()),
        StructField("ts_ms", LongType()),
    ]
)

key_schema = StructType(
    [
        StructField("schema", StringType()),
        StructField(
            "payload",
            StructType(
                [
                    StructField("id", IntegerType()),
                ]
            ),
        ),
    ]
)


def debezium_schema(row_schema):
    return StructType(
        [
            StructField("schema", StringType()),
            StructField(
                "payload",
                StructType(
                    [
                        StructField("before", row_schema),
                        StructField("after", row_schema),
                        StructField("source", source_schema),
                        StructField("op", StringType()),
                        StructField("ts_ms", LongType()),
                    ]
                ),
            ),
        ]
    )


def ensure_bronze_table(spark, table_name):
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS demo.bronze.{table_name} (
            pk_id BIGINT,
            topic STRING,
            kafka_partition INT,
            kafka_offset BIGINT,
            kafka_timestamp TIMESTAMP,
            is_tombstone BOOLEAN,
            op STRING,
            lsn BIGINT,
            event_ts_ms BIGINT,
            key_json STRING,
            before_json STRING,
            after_json STRING,
            value_json STRING
        )
        USING iceberg
        """
    )


def load_topic(spark, topic_name, target_table, row_schema):
    ensure_bronze_table(spark, target_table)

    raw = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", topic_name)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )

    parsed = (
        raw.select(
            F.col("topic"),
            F.col("partition").cast("int").alias("kafka_partition"),
            F.col("offset").cast("long").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("key").cast("string").alias("key_json"),
            F.col("value").cast("string").alias("value_json"),
            F.from_json(F.col("key").cast("string"), key_schema).alias("key_msg"),
            F.col("value").isNull().alias("is_tombstone"),
            F.from_json(F.col("value").cast("string"), debezium_schema(row_schema)).alias("msg"),
        )
        .select(
            F.coalesce(
                F.col("msg.payload.after.id"),
                F.col("msg.payload.before.id"),
                F.col("key_msg.payload.id"),
            ).cast("long").alias("pk_id"),
            F.col("topic"),
            F.col("kafka_partition"),
            F.col("kafka_offset"),
            F.col("kafka_timestamp"),
            F.col("is_tombstone"),
            F.when(F.col("is_tombstone"), F.lit("t")).otherwise(F.col("msg.payload.op")).alias("op"),
            F.col("msg.payload.source.lsn").cast("long").alias("lsn"),
            F.coalesce(F.col("msg.payload.ts_ms"), F.col("msg.payload.source.ts_ms"))
            .cast("long")
            .alias("event_ts_ms"),
            F.col("key_json"),
            F.to_json(F.col("msg.payload.before")).alias("before_json"),
            F.to_json(F.col("msg.payload.after")).alias("after_json"),
            F.col("value_json"),
        )
        .filter(F.col("pk_id").isNotNull())
    )

    parsed.createOrReplaceTempView("new_cdc_events")

    spark.sql(
        f"""
        SELECT topic, kafka_partition, kafka_offset
        FROM demo.bronze.{target_table}
        """
    ).createOrReplaceTempView("existing_cdc_offsets")

    to_insert = spark.sql(
        """
        SELECT n.*
        FROM new_cdc_events n
        LEFT ANTI JOIN existing_cdc_offsets e
          ON n.topic = e.topic
         AND n.kafka_partition = e.kafka_partition
         AND n.kafka_offset = e.kafka_offset
        """
    )

    inserted = to_insert.count()

    if inserted > 0:
        to_insert.writeTo(f"demo.bronze.{target_table}").append()

    total = spark.sql(f"SELECT COUNT(*) AS n FROM demo.bronze.{target_table}").collect()[0]["n"]
    print(f"{target_table}: inserted={inserted}, total={total}")


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.bronze")

    load_topic(spark, "dbserver1.public.customers", "customers_cdc", customer_row_schema)
    load_topic(spark, "dbserver1.public.drivers", "drivers_cdc", driver_row_schema)

    spark.stop()


if __name__ == "__main__":
    main()