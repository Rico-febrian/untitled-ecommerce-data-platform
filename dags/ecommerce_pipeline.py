"""
DAG: ecommerce_pipeline
Orchestrate the full data pipeline:
  bronze_ingestion (Spark) → silver_transform (Spark) → dbt_run → dbt_test
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Spark submit command template — jalanin via docker exec ke Spark master container
SPARK_SUBMIT = (
    "docker exec ecommerce-spark-master "
    "spark-submit "
    "--master spark://spark-master:7077 "
    "--deploy-mode client "
    "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 "
    "--conf spark.driver.extraJavaOptions=-Duser.home=/home/spark "
    "--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 "
    "--conf spark.hadoop.fs.s3a.access.key=minioadmin "
    "--conf spark.hadoop.fs.s3a.secret.key=minioadmin123 "
    "--conf spark.hadoop.fs.s3a.path.style.access=true "
    "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
    "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
    "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
)

with DAG(
    dag_id="ecommerce_pipeline",
    default_args=default_args,
    description="End-to-end e-commerce data pipeline: Bronze → Silver → Gold",
    # Jalanin daily jam 2 pagi UTC
    schedule_interval="0 2 * * *",
    start_date=datetime(2026, 3, 1),
    catchup=False,
    tags=["ecommerce", "pipeline"],
) as dag:

    # Task 1: Bronze ingestion — Spark streaming dari Redpanda ke MinIO (Delta Lake)
    # Pake timeout karena streaming job, kita jalanin batch mode
    bronze_ingestion = BashOperator(
        task_id="bronze_ingestion",
        bash_command=f"{SPARK_SUBMIT} /opt/spark-jobs/ingestion/bronze_ingestion.py",
        execution_timeout=timedelta(minutes=10),
    )

    # Task 2: Silver transform — Bronze ke Silver (Debezium decoding, Delta MERGE)
    silver_transform = BashOperator(
        task_id="silver_transform",
        bash_command=f"{SPARK_SUBMIT} /opt/spark-jobs/processing/silver_transform.py",
        execution_timeout=timedelta(minutes=10),
    )

    # Task 3: dbt run — Silver ke Gold (DuckDB)
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/gold_layer && dbt run --profiles-dir /opt/airflow/gold_layer",
        execution_timeout=timedelta(minutes=5),
    )

    # Task 4: dbt test — data quality checks
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/gold_layer && dbt test --profiles-dir /opt/airflow/gold_layer",
        execution_timeout=timedelta(minutes=5),
    )

    # Pipeline dependency: sequential
    bronze_ingestion >> silver_transform >> dbt_run >> dbt_test
