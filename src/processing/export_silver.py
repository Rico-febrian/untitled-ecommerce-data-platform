"""
Export Silver data dari MinIO (Delta Lake) ke local Parquet files.
File ini yang nanti di-upload ke Databricks.

Cara jalanin:
  docker exec ecommerce-spark-master spark-submit \
    --master spark://spark-master:7077 \
    --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 \
    /opt/spark-jobs/processing/export_silver.py
"""

import os

from pyspark.sql import SparkSession


def create_spark_session():
    return (
        SparkSession.builder
        .appName("ExportSilver")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


TABLES = ["clickstream", "users", "products", "orders", "order_items", "payments"]
OUTPUT_DIR = "/opt/spark-jobs/exports"


def main():
    spark = create_spark_session()

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    for table in TABLES:
        print(f"Exporting: silver/{table}")
        df = spark.read.format("delta").load(f"s3a://silver/{table}")

        # Export sebagai 1 file Parquet (coalesce(1) = gabungin jadi 1 file)
        output_path = f"{OUTPUT_DIR}/{table}"
        df.coalesce(1).write.mode("overwrite").parquet(output_path)
        print(f"  → {df.count()} rows exported to {output_path}")

    print("\nExport complete! Files ada di src/exports/")
    spark.stop()


if __name__ == "__main__":
    main()
