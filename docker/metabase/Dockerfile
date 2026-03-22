FROM eclipse-temurin:21-jre

ENV MB_PLUGINS_DIR=/plugins
RUN mkdir -p ${MB_PLUGINS_DIR} /app

# Download Metabase JAR
ADD https://downloads.metabase.com/latest/metabase.jar /app/metabase.jar

# Copy DuckDB driver
COPY plugins/duckdb.metabase-driver.jar ${MB_PLUGINS_DIR}/

EXPOSE 3000
CMD ["java", "-jar", "/app/metabase.jar"]
