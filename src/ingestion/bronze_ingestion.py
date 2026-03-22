"""
Spark Structured Streaming job: Baca dari Redpanda → Tulis ke MinIO (Bronze layer) sebagai Delta Lake.

Cara jalanin (dari docker exec):
  docker exec ecommerce-spark-master spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 \
    /opt/spark-jobs/ingestion/bronze_ingestion.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StringType


def create_spark_session():
    """Bikin Spark session dengan config Delta Lake dan MinIO (S3)."""
    import os

    return (
        SparkSession.builder
        .appName("BronzeIngestion")
        # Delta Lake config
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # MinIO (S3-compatible) config — credentials dari environment variables
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")  # MinIO butuh path-style (bukan virtual-hosted)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def stream_topic_to_bronze(spark, topic, bronze_path):
    """
    Baca dari satu Kafka/Redpanda topic secara streaming, tulis ke Bronze layer.

    Data disimpen as-is (raw) — belum ada transformasi.
    Kolom yang disimpen:
      - key: message key (biasanya primary key dari CDC)
      - value: isi message (JSON string)
      - topic: nama topic asal
      - partition: partition number
      - offset: offset di partition
      - timestamp: waktu message diterima Kafka
      - ingested_at: waktu data masuk ke Bronze (ditambahin Spark)
    """
    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "redpanda:9092")
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")  # Baca dari awal (biar dapet semua data termasuk snapshot)
        .load()
    )

    # Cast key dan value dari bytes ke string biar readable
    # Tambahin kolom ingested_at buat tau kapan data masuk Bronze
    df_transformed = (
        df.select(
            col("key").cast(StringType()).alias("key"),
            col("value").cast(StringType()).alias("value"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp"),
        )
        .withColumn("ingested_at", current_timestamp())
    )

    # Tulis ke MinIO sebagai Delta Lake
    # checkpointLocation = Spark nyimpen progress streaming di sini
    #   (mirip offset di Kafka — biar kalo restart, lanjut bukan ulang)
    # trigger(availableNow=True) = proses semua data yang ada, lalu stop otomatis
    #   (batch-friendly buat Airflow, tapi tetep pake streaming engine)
    query = (
        df_transformed.writeStream
        .format("delta")
        .outputMode("append")  # Append only, nggak overwrite data lama
        .option("checkpointLocation", f"s3a://bronze/_checkpoints/{topic}")
        .trigger(availableNow=True)
        .start(f"s3a://bronze/{topic}")
    )

    return query


def main():
    spark = create_spark_session()

    # Topics yang mau di-ingest ke Bronze
    topics = [
        "clickstream-events",         # Clickstream dari producer
        "ecommerce.public.users",     # CDC dari tabel users
        "ecommerce.public.products",  # CDC dari tabel products
        "ecommerce.public.orders",    # CDC dari tabel orders
        "ecommerce.public.order_items",  # CDC dari tabel order_items
        "ecommerce.public.payments",  # CDC dari tabel payments
    ]

    # Start streaming query per topic
    queries = []
    for topic in topics:
        print(f"Starting stream for topic: {topic}")
        q = stream_topic_to_bronze(spark, topic, f"s3a://bronze/{topic}")
        queries.append(q)

    # Tunggu semua streaming query selesai (stop otomatis setelah data habis)
    for q in queries:
        q.awaitTermination()


if __name__ == "__main__":
    main()
