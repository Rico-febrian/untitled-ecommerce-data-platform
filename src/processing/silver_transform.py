"""
Spark batch job: Baca dari Bronze (MinIO) → Parse & clean → Tulis ke Silver (MinIO).

Bronze = data mentah (kolom value masih JSON string)
Silver = data bersih (JSON udah di-parse jadi kolom terpisah, tipe data bener)

Approach:
  - Clickstream: incremental append (track offset terakhir, cuma proses data baru)
  - CDC tables: merge/upsert (update kalo ada, insert kalo baru, delete kalo di-delete)

Cara jalanin:
  docker exec ecommerce-spark-master spark-submit \
    --master spark://spark-master:7077 \
    --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 \
    /opt/spark-jobs/processing/silver_transform.py
"""

import os

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, row_number, get_json_object, max as spark_max, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    TimestampType, DoubleType, DecimalType
)
from pyspark.sql.window import Window


def create_spark_session():
    """Bikin Spark session dengan config Delta Lake dan MinIO."""
    return (
        SparkSession.builder
        .appName("SilverTransform")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


# === SCHEMA DEFINITIONS ===
# Blueprint buat Spark tau cara parse JSON dari tiap data source

CLICKSTREAM_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", IntegerType()),
    StructField("event_type", StringType()),
    StructField("product_id", IntegerType()),
    StructField("timestamp", StringType()),
    StructField("device", StringType()),
    StructField("session_id", StringType()),
])

USERS_SCHEMA = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("email", StringType()),
    StructField("city", StringType()),
    StructField("registered_at", StringType()),
])

PRODUCTS_SCHEMA = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("category", StringType()),
    StructField("price", StringType()),
    StructField("stock", IntegerType()),
])

ORDERS_SCHEMA = StructType([
    StructField("id", IntegerType()),
    StructField("user_id", IntegerType()),
    StructField("order_date", StringType()),
    StructField("status", StringType()),
    StructField("total_amount", StringType()),
])

ORDER_ITEMS_SCHEMA = StructType([
    StructField("id", IntegerType()),
    StructField("order_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("quantity", IntegerType()),
    StructField("price", StringType()),
])

PAYMENTS_SCHEMA = StructType([
    StructField("id", IntegerType()),
    StructField("order_id", IntegerType()),
    StructField("payment_method", StringType()),
    StructField("amount", StringType()),
    StructField("status", StringType()),
    StructField("paid_at", StringType()),
])


# === TYPE CONVERSION ===
# Debezium encode data dengan format khusus:
#   - Timestamp → microseconds since epoch (int64), contoh: 1773794542598663
#   - Decimal   → Base64-encoded bytes, contoh: "J4urFA=="
# Kita perlu decode format ini ke tipe yang bener di Silver.

from pyspark.sql.functions import udf, expr
import base64


def _decode_debezium_decimal(b64_str, scale=2):
    """Decode Debezium Base64 Decimal → Python float.
    Debezium encode NUMERIC/DECIMAL sebagai Base64(BigInteger.toByteArray()).
    Kita decode: Base64 → bytes → big-endian signed int → bagi 10^scale.
    """
    if b64_str is None:
        return None
    try:
        raw = base64.b64decode(b64_str)
        # bytes → signed big-endian integer
        unscaled = int.from_bytes(raw, byteorder='big', signed=True)
        return float(unscaled) / (10 ** scale)
    except Exception:
        return None


def _decode_debezium_microtimestamp(micro_val):
    """Decode Debezium MicroTimestamp → epoch seconds (float).
    Debezium kirim timestamp sebagai microseconds since epoch.
    """
    if micro_val is None:
        return None
    try:
        return float(micro_val) / 1_000_000.0
    except Exception:
        return None


# Register UDFs buat Spark
decode_decimal_udf = udf(_decode_debezium_decimal, DoubleType())
decode_microtimestamp_udf = udf(_decode_debezium_microtimestamp, DoubleType())


# Kolom mana yang perlu di-decode, dan tipe Debezium-nya
DEBEZIUM_CONVERSIONS = {
    "users": {
        "registered_at": "microtimestamp",
    },
    "products": {
        "price": "decimal",
    },
    "orders": {
        "order_date": "microtimestamp",
        "total_amount": "decimal",
    },
    "order_items": {
        "price": "decimal",
    },
    "payments": {
        "amount": "decimal",
        "paid_at": "microtimestamp",
    },
}


def apply_debezium_conversions(df, table_name):
    """Decode kolom Debezium ke tipe yang bener."""
    conversions = DEBEZIUM_CONVERSIONS.get(table_name, {})
    for col_name, conv_type in conversions.items():
        if conv_type == "decimal":
            df = df.withColumn(col_name, decode_decimal_udf(col(col_name)).cast(DecimalType(12, 2)))
        elif conv_type == "microtimestamp":
            # microseconds → epoch seconds → timestamp
            df = df.withColumn(col_name, decode_microtimestamp_udf(col(col_name)).cast(TimestampType()))
    return df


def get_max_offset(spark, silver_path):
    """
    Ambil offset terakhir yang udah diproses di Silver.
    Return -1 kalo Silver belum ada (first run).
    """
    try:
        silver_df = spark.read.format("delta").load(silver_path)
        result = silver_df.select(spark_max("_bronze_offset")).collect()[0][0]
        return result if result is not None else -1
    except Exception:
        return -1


def transform_clickstream(spark):
    """
    Clickstream: incremental append.
    Cek offset terakhir di Silver → cuma proses data baru dari Bronze → append.
    """
    print("Transforming: clickstream-events")

    silver_path = "s3a://silver/clickstream"

    # Cek offset terakhir yang udah diproses
    last_offset = get_max_offset(spark, silver_path)
    print(f"  Last processed offset: {last_offset}")

    # Baca dari Bronze
    df = spark.read.format("delta").load("s3a://bronze/clickstream-events")

    # Filter: cuma ambil data baru (offset > last_offset)
    new_data = df.filter(col("offset") > last_offset)

    row_count = new_data.count()
    if row_count == 0:
        print("  → No new data to process")
        return

    # Parse JSON → kolom terpisah
    parsed = (
        new_data.select(
            from_json(col("value"), CLICKSTREAM_SCHEMA).alias("data"),
            col("offset").alias("_bronze_offset"),
        )
        .select("data.*", "_bronze_offset")
        .withColumn("timestamp", col("timestamp").cast(TimestampType()))
    )

    # Deduplicate berdasarkan event_id
    deduped = parsed.dropDuplicates(["event_id"])

    # Append ke Silver (data lama tetep ada, cuma nambah yang baru)
    deduped.write.format("delta").mode("append").save(silver_path)
    print(f"  → {deduped.count()} new rows appended to silver/clickstream")


def transform_cdc_table(spark, topic, schema, table_name, id_col="id"):
    """
    CDC tables: merge/upsert.
    - INSERT/UPDATE → merge ke Silver (update kalo ada, insert kalo baru)
    - DELETE → hapus dari Silver
    """
    print(f"Transforming: {topic}")

    silver_path = f"s3a://silver/{table_name}"

    # Baca dari Bronze
    df = spark.read.format("delta").load(f"s3a://bronze/{topic}")

    # Ambil field "after" dan "op" dari Debezium JSON
    # Debezium format: {"schema":{...}, "payload":{"before":{...}, "after":{...}, "op":"u"}}
    # Data ada di dalam "payload", makanya path-nya $.payload.after
    parsed = df.select(
        get_json_object(col("value"), "$.payload.after").alias("after_json"),
        get_json_object(col("value"), "$.payload.op").alias("op"),
        col("offset"),
    )

    # Ambil id dari after_json (buat grouping)
    parsed_with_id = parsed.withColumn(
        "row_id",
        get_json_object(col("after_json"), f"$.{id_col}").cast(IntegerType())
    )

    # Deduplicate: per id, ambil state terakhir (offset terbesar)
    window = Window.partitionBy("row_id").orderBy(col("offset").desc())
    latest = (
        parsed_with_id
        .filter(col("row_id").isNotNull())
        .withColumn("rn", row_number().over(window))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    # Pisah DELETE dan INSERT/UPDATE
    deletes = latest.filter(col("op") == "d")
    upserts = (
        latest.filter(col("after_json").isNotNull())
        .select(from_json(col("after_json"), schema).alias("data"))
        .select("data.*")
    )

    # Decode Debezium format (Base64 decimal, microsecond timestamps)
    upserts = apply_debezium_conversions(upserts, table_name)

    upsert_count = upserts.count()
    delete_count = deletes.count()

    if upsert_count == 0 and delete_count == 0:
        print("  → No new data to process")
        return

    # Cek apakah Silver table udah ada
    try:
        DeltaTable.forPath(spark, silver_path)
        silver_exists = True
    except Exception:
        silver_exists = False

    if not silver_exists:
        # First run: langsung tulis semua
        upserts.write.format("delta").mode("overwrite").save(silver_path)
        print(f"  → {upsert_count} rows written to silver/{table_name} (initial load)")
    else:
        # MERGE: update kalo id match, insert kalo nggak ada
        if upsert_count > 0:
            silver_table = DeltaTable.forPath(spark, silver_path)
            silver_table.alias("target").merge(
                upserts.alias("source"),
                f"target.{id_col} = source.{id_col}"
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()
            print(f"  → {upsert_count} rows merged to silver/{table_name}")

        # DELETE: hapus row yang di-delete di source
        if delete_count > 0:
            delete_ids = [row.row_id for row in deletes.collect()]
            silver_table = DeltaTable.forPath(spark, silver_path)
            silver_table.delete(col(id_col).isin(delete_ids))
            print(f"  → {delete_count} rows deleted from silver/{table_name}")


def main():
    spark = create_spark_session()

    transform_clickstream(spark)

    transform_cdc_table(spark, "ecommerce.public.users", USERS_SCHEMA, "users")
    transform_cdc_table(spark, "ecommerce.public.products", PRODUCTS_SCHEMA, "products")
    transform_cdc_table(spark, "ecommerce.public.orders", ORDERS_SCHEMA, "orders")
    transform_cdc_table(spark, "ecommerce.public.order_items", ORDER_ITEMS_SCHEMA, "order_items")
    transform_cdc_table(spark, "ecommerce.public.payments", PAYMENTS_SCHEMA, "payments")

    print("\nSilver transform complete!")
    spark.stop()


if __name__ == "__main__":
    main()
