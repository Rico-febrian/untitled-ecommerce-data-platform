# Base image
FROM bitnami/spark:3.5.5

# Switch ke root buat setup
USER root

# Register user "spark" di /etc/passwd
# Bitnami jalan sebagai uid 1001 tapi nggak register di /etc/passwd
# Hadoop butuh username yang resolvable → kita register manual
RUN echo "spark:x:1001:0:Spark User:/home/spark:/bin/bash" >> /etc/passwd && \
    mkdir -p /home/spark && \
    chown 1001:0 /home/spark

# Pre-download JAR dependencies biar nggak download tiap kali spark-submit
# - spark-sql-kafka: buat baca dari Kafka/Redpanda
# - delta-spark: buat nulis format Delta Lake
# - hadoop-aws: buat nulis ke MinIO (S3-compatible)
RUN /opt/bitnami/spark/bin/spark-submit \
    --conf spark.driver.extraJavaOptions=-Duser.home=/home/spark \
    --ivy /home/spark/.ivy2 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 \
    --class org.apache.spark.deploy.SparkSubmit \
    /dev/null 2>/dev/null || true

# Set ownership ke user spark
RUN chown -R 1001:0 /home/spark

# Switch balik ke non-root (security best practice)
USER 1001

# Environment variables
ENV HOME=/home/spark
ENV HADOOP_USER_NAME=spark
