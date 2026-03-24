# E-Commerce Real-Time Data Platform

End-to-end data pipeline that captures changes from a transactional database using CDC, streams them through a Bronze, Silver, and Gold lakehouse architecture, and serves analytics on a Metabase dashboard. Fully orchestrated with Airflow and running entirely in Docker.

&nbsp;

## Architecture

![E-commerce Data Platform Architecture](images/E-commerce%20Data%20Platform%20Architecture.png)

<!-- TODO: Add 1-2 dashboard screenshots here -->

&nbsp;

## How It Works

This project uses simulated data. There is no real e-commerce business behind it. Two Python scripts generate everything.

### Data Sources (both simulated)

**`seed.py`** generates a one-time batch of e-commerce data into PostgreSQL: 200 users, 50 products, and 500 orders with items and payments. Debezium then watches the database's internal change log (WAL) and streams every row as a CDC event to Redpanda. Any future INSERT, UPDATE, or DELETE in this database is also captured automatically.

**`producer.py`** continuously generates clickstream events (page views, add to cart, checkout) and sends them directly to Redpanda. This powers the conversion funnel analysis on the dashboard. If you don't run this, the pipeline still works but clickstream charts will be empty.

### Processing Layers

**Bronze (raw)** — Spark reads all events from Redpanda and stores them as-is in MinIO as Delta Lake tables. No transformation, just a faithful archive. If anything breaks downstream, you can always reprocess from here.

**Silver (cleaned)** — Spark reads from Bronze, decodes the Debezium format (which encodes timestamps and decimals in non-obvious ways), and applies upserts so the Silver tables mirror the current state of the source database.

**Gold (analytics-ready)** — dbt reads from Silver and builds a star schema in DuckDB with dimension tables (users, products, dates) and fact tables (orders, clickstream). This is what the dashboard queries.

Airflow orchestrates the full pipeline in sequence: Bronze, Silver, Gold, then data quality tests at the end.

&nbsp;

## Tech Stack

| Layer | Tool |
|-------|------|
| Source Database | PostgreSQL 16 |
| Change Data Capture | Debezium 2.6 |
| Message Broker | Redpanda v24.1.1 |
| Object Storage | MinIO |
| Processing | Apache Spark 3.5.5 |
| Table Format | Delta Lake 3.2.0 |
| Transformation | dbt-DuckDB |
| Analytical DB | DuckDB |
| Dashboard | Metabase |
| Orchestration | Apache Airflow 2.10.4 |
| Containerization | Docker Compose |

&nbsp;

## Project Structure

```
ecommerce-data-platform/
├── .env                          # Credentials (not committed)
├── docker-compose.yml            # All services
├── docker/
│   ├── spark/Dockerfile
│   ├── metabase/Dockerfile
│   └── airflow/Dockerfile
├── src/
│   ├── generators/
│   │   ├── schema.sql            # Source table DDL
│   │   ├── seed.py               # Fake data generator
│   │   └── producer.py           # Clickstream event producer
│   ├── ingestion/
│   │   └── bronze_ingestion.py   # Redpanda → MinIO Bronze
│   └── processing/
│       └── silver_transform.py   # Bronze → Silver (Debezium decoding, Delta MERGE)
├── gold_layer/                   # dbt project
│   ├── models/
│   │   ├── staging/              # 6 staging views
│   │   └── datamart/             # dim_users, dim_products, dim_date, fact_orders, fact_clickstream
│   └── profiles.yml
├── dags/
│   └── ecommerce_pipeline.py    # Airflow DAG
└── dev.duckdb
```

&nbsp;

## Getting Started

### Prerequisites

- Docker and Docker Compose
- Python 3.11+ with [uv](https://github.com/astral-sh/uv)
- Around 8GB RAM for Docker
- These ports need to be free: 3000, 4040, 5433, 7077, 8080, 8081, 8082, 8083, 9000, 9001, 19092

&nbsp;

### 1. Clone and configure

```bash
git clone https://github.com/<your-username>/ecommerce-data-platform.git
cd ecommerce-data-platform
```

Create a `.env` file in the project root. All services read credentials from this file so nothing is hardcoded in the code:

```env
POSTGRES_USER=ecommerce
POSTGRES_PASSWORD=ecommerce123
POSTGRES_DB=ecommerce
POSTGRES_PORT=5433

REDPANDA_BROKER_EXTERNAL_PORT=19092
REDPANDA_PROXY_EXTERNAL_PORT=18082
REDPANDA_CONSOLE_PORT=8080

MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin123
MINIO_API_PORT=9000
MINIO_CONSOLE_PORT=9001

AIRFLOW_POSTGRES_USER=airflow
AIRFLOW_POSTGRES_PASSWORD=airflow123
AIRFLOW_POSTGRES_DB=airflow
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=admin123
```

&nbsp;

### 2. Start all services

This spins up 14 Docker containers including the source database, message broker, object storage, Spark cluster, Metabase, Airflow, and supporting services.

```bash
docker compose up -d
```

The first run takes 5 to 10 minutes because Docker needs to download images and build custom Dockerfiles. After that, starting up takes about 30 seconds.

Check that everything is healthy:

```bash
docker compose ps
```

&nbsp;

### 3. Create storage buckets

The lakehouse needs two buckets in MinIO. One called `bronze` for raw data and one called `silver` for cleaned data.

Open [MinIO Console](http://localhost:9001) (login with `minioadmin` / `minioadmin123`) and create both buckets.

&nbsp;

### 4. Seed the source database

This populates PostgreSQL with fake e-commerce data: 200 users, 50 products, and 500 orders with items and payments. This is the transactional data that CDC will capture and stream through the pipeline.

```bash
uv run python src/generators/seed.py
```

&nbsp;

### 5. Register the CDC connector

This tells Debezium to start watching the PostgreSQL tables. Once registered, Debezium takes a snapshot of all existing rows and then continuously captures any future changes.

```bash
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d '{
  "name": "ecommerce-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "ecommerce",
    "database.password": "ecommerce123",
    "database.dbname": "ecommerce",
    "database.server.name": "ecommerce",
    "topic.prefix": "ecommerce",
    "table.include.list": "public.users,public.products,public.orders,public.order_items,public.payments",
    "plugin.name": "pgoutput",
    "slot.name": "ecommerce_slot"
  }
}'
```

To verify, open [Redpanda Console](http://localhost:8080). You should see topics like `ecommerce.public.orders` with messages in them.

&nbsp;

### 6. Start the clickstream producer

This generates the second data source, which is simulated user browsing events. Without running this, the CDC pipeline still works fine but clickstream-related dashboard charts like the conversion funnel will be empty.

Run it in a separate terminal:

```bash
uv run python src/generators/producer.py
```

&nbsp;

### 7. Run the pipeline

Open [Airflow](http://localhost:8082) (login with `admin` / `admin123`). Find the `ecommerce_pipeline` DAG, toggle it on, and trigger a run.

It executes four tasks in sequence:

1. **bronze_ingestion** — reads events from Redpanda and writes raw data to MinIO
2. **silver_transform** — decodes the Debezium format and upserts into Silver tables
3. **dbt_run** — transforms Silver into the Gold star schema in DuckDB
4. **dbt_test** — validates data quality (unique keys, not null, referential integrity)

&nbsp;

### 8. View the dashboard

Open [Metabase](http://localhost:3000). On first launch it will ask you to create an admin account.

After setup, go to **Settings > Admin > Databases > Add Database**:

- Database type: **DuckDB**
- Database file: `/data/dev.duckdb` (this is the path inside the container, not on your machine)
- Toggle **read-only mode** on so it doesn't conflict with dbt writes

Once connected, you can query tables like `fact_orders`, `dim_products`, and `dim_users` directly, or build your own dashboard.

&nbsp;

## Services

| Service | URL |
|---------|-----|
| PostgreSQL | `localhost:5433` |
| Redpanda Console | [localhost:8080](http://localhost:8080) |
| MinIO Console | [localhost:9001](http://localhost:9001) |
| Spark Master UI | [localhost:8081](http://localhost:8081) |
| Kafka Connect | [localhost:8083](http://localhost:8083) |
| Airflow | [localhost:8082](http://localhost:8082) |
| Metabase | [localhost:3000](http://localhost:3000) |

&nbsp;

## Acknowledgements

Thank you for checking out this project. This was built as a learning project to explore how modern data engineering tools work together. If you have any questions, feedback, or suggestions, feel free to reach out.

You can connect with me on:

- [LinkedIn](https://linkedin.com/in/<ricofebrian>)
- [Medium](https://medium.com/@ricofebrian731)
