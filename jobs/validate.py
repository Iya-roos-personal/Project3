import os
from decimal import Decimal

import psycopg2
from pyspark.sql import SparkSession


S3_ENDPOINT = "http://minio:9000"
S3_BUCKET = "s3://warehouse/"


def create_spark():
    return (
        SparkSession.builder
        .appName("validate-cdc")
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


def pg_conn():
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "postgres"),
        port=int(os.getenv("PG_PORT", "5432")),
        dbname=os.getenv("PG_DB", "sourcedb"),
        user=os.getenv("PG_USER", "cdc_user"),
        password=os.getenv("PG_PASSWORD", "cdc_pass"),
    )


def pg_count(table):
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM public.{table};")
            return cur.fetchone()[0]


def pg_sample(table, limit=3):
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM public.{table} ORDER BY id LIMIT %s;", (limit,))
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            return [dict(zip(columns, row)) for row in rows]


def normalize(value):
    if value is None:
        return None

    if isinstance(value, Decimal):
        return round(float(value), 2)

    if isinstance(value, float):
        return round(value, 2)

    return str(value)


def compare_values(column, pg_value, silver_value):
    if column == "created_at":
        return str(pg_value)[:19] == str(silver_value)[:19]

    return normalize(pg_value) == normalize(silver_value)


def validate_count(spark, table):
    pg_n = pg_count(table)
    silver_n = spark.sql(f"SELECT COUNT(*) AS n FROM demo.silver.{table}").collect()[0]["n"]

    print(f"{table}: postgres={pg_n}, silver={silver_n}")

    if pg_n != silver_n:
        raise AssertionError(f"{table} count mismatch: postgres={pg_n}, silver={silver_n}")


def validate_spot_check(spark, table):
    samples = pg_sample(table, limit=3)

    for pg_row in samples:
        row_id = int(pg_row["id"])

        silver_rows = spark.sql(
            f"""
            SELECT *
            FROM demo.silver.{table}
            WHERE id = {row_id}
            """
        ).collect()

        if len(silver_rows) != 1:
            raise AssertionError(f"{table} id={row_id} missing from Silver")

        silver_row = silver_rows[0].asDict()

        for column, pg_value in pg_row.items():
            if column not in silver_row:
                raise AssertionError(f"{table} column {column} missing from Silver")

            silver_value = silver_row[column]

            if not compare_values(column, pg_value, silver_value):
                raise AssertionError(
                    f"{table} id={row_id} column={column} mismatch: "
                    f"postgres={pg_value}, silver={silver_value}"
                )

        print(f"{table}: spot-check passed for id={row_id}")


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    for table in ["customers", "drivers"]:
        validate_count(spark, table)
        validate_spot_check(spark, table)

    print("Validation passed: Silver CDC matches PostgreSQL counts and sampled rows.")
    spark.stop()


if __name__ == "__main__":
    main()