# E-Commerce Real-Time Data Platform

## 1. Project Overview

Platform data end-to-end untuk e-commerce yang mencakup batch processing, real-time streaming, dan change data capture (CDC). Project ini dibangun sebagai portfolio piece sekaligus sarana belajar konsep-konsep data engineering modern.

### Goals
- Menjawab 7 business questions seputar sales, customer behavior, operations, dan payment
- Hands-on dengan modern data stack (Kafka, CDC, Delta Lake/Iceberg, Snowflake/Databricks)
- Menerapkan software engineering best practices (clean code, testing, CI/CD)
- Semua berjalan di local environment, **zero cost**

---

## 2. Business Questions

### Sales & Revenue
1. Berapa total revenue per hari/minggu/bulan, breakdown per product category?
2. Apa top 10 produk paling laris dan trend-nya over time?

### Customer Behavior (Streaming)
3. Berapa conversion rate: page_view → add_to_cart → checkout → payment?
4. Berapa rata-rata waktu user dari first visit sampai checkout (time to convert)?

### Operations (CDC)
5. Berapa rata-rata waktu dari order dibuat sampai delivered, per kota?
6. Berapa cancellation rate dan di stage mana paling sering cancel?

### Payment
7. Payment method mana yang paling banyak dipake dan mana yang paling sering gagal?

---

## 3. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                │
│                                                                     │
│   ┌──────────────┐              ┌──────────────────┐                │
│   │  PostgreSQL   │              │  Python Event     │               │
│   │  (OLTP DB)   │              │  Generator         │              │
│   │              │              │  (Clickstream)     │               │
│   └──────┬───────┘              └────────┬───────────┘              │
│          │                               │                          │
└──────────┼───────────────────────────────┼──────────────────────────┘
           │                               │
           ▼                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        INGESTION LAYER                              │
│                                                                     │
│   ┌──────────────┐              ┌──────────────────┐                │
│   │  Debezium     │              │  Kafka            │               │
│   │  (CDC)       │──────────────▶│  (Message Broker) │              │
│   └──────────────┘              └────────┬───────────┘              │
│                                          │                          │
└──────────────────────────────────────────┼──────────────────────────┘
                                           │
                                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        STORAGE LAYER                                │
│                                                                     │
│   ┌──────────────────────────────────────────┐                      │
│   │  MinIO (S3-compatible Object Storage)    │                      │
│   │                                          │                      │
│   │  ┌────────┐  ┌──────────┐  ┌─────────┐  │                      │
│   │  │ Bronze │  │  Silver  │  │  Gold   │  │                      │
│   │  │ (Raw)  │  │ (Clean)  │  │ (Agg)   │  │                      │
│   │  └────────┘  └──────────┘  └─────────┘  │                      │
│   │                                          │                      │
│   │  Format: Delta Lake / Apache Iceberg     │                      │
│   └──────────────────────────────────────────┘                      │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
                                           │
                                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     TRANSFORMATION LAYER                            │
│                                                                     │
│   ┌──────────────┐              ┌──────────────────┐                │
│   │  Spark        │              │  dbt              │               │
│   │  (Processing) │              │  (Modeling &      │               │
│   │              │              │   Testing)        │               │
│   └──────────────┘              └──────────────────┘                │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
                                           │
                                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       SERVING LAYER                                 │
│                                                                     │
│   ┌──────────────────────────────────────────┐                      │
│   │  DuckDB / Snowflake (Free Trial)         │                      │
│   │  → Optimized for analytical queries      │                      │
│   └──────────────────────────────────────────┘                      │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
                                           │
                                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     VISUALIZATION LAYER                             │
│                                                                     │
│   ┌──────────────┐              ┌──────────────────┐                │
│   │  Metabase     │              │  Grafana          │               │
│   │  (Dashboard)  │              │  (Pipeline        │               │
│   │              │              │   Monitoring)     │               │
│   └──────────────┘              └──────────────────┘                │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘

                    ┌──────────────────────┐
                    │  ORCHESTRATION       │
                    │  Apache Airflow      │
                    │  (Schedules &        │
                    │   monitors all jobs) │
                    └──────────────────────┘

                    ┌──────────────────────┐
                    │  DATA QUALITY        │
                    │  Great Expectations  │
                    │  + dbt tests         │
                    └──────────────────────┘

                    ┌──────────────────────┐
                    │  CI/CD               │
                    │  GitHub Actions      │
                    └──────────────────────┘
```

---

## 4. Data Sources

### 4.1 Transactional Data (PostgreSQL)

Semua data di-generate pake Python (Faker library) dan di-seed ke PostgreSQL.

**Tables:**

| Table | Columns | Description |
|-------|---------|-------------|
| `users` | id, name, email, city, registered_at | Data customer |
| `products` | id, name, category, price, stock | Katalog produk |
| `orders` | id, user_id, order_date, status, total_amount | Transaksi order |
| `order_items` | id, order_id, product_id, quantity, price | Detail item per order |
| `payments` | id, order_id, payment_method, amount, status, paid_at | Data pembayaran |

**Order status flow:** `pending → paid → shipped → delivered` atau `pending → cancelled`

Perubahan status ini yang akan di-capture oleh CDC (Debezium).

### 4.2 Clickstream Events (Kafka)

Di-generate oleh Python producer script yang continuously produce events.

```json
{
  "event_id": "uuid",
  "user_id": 123,
  "event_type": "page_view | add_to_cart | checkout | search",
  "product_id": 456,
  "timestamp": "2026-03-17T22:50:00Z",
  "device": "mobile | desktop",
  "session_id": "uuid"
}
```

---

## 5. Medallion Architecture (Bronze → Silver → Gold)

### Bronze (Raw)
- Data mentah dari Kafka dan CDC, tanpa transformasi
- Format: Delta Lake / Iceberg
- Retention: semua data disimpan as-is

### Silver (Cleaned)
- Deduplicated, validated, typed correctly
- Data quality checks applied
- Relasi antar tabel sudah bisa di-join

### Gold (Aggregated)
- Dim & fact tables (star schema)
- Pre-aggregated metrics siap di-query dashboard
- Optimized untuk analytical queries

---

## 6. Tech Stack Summary

| Layer | Tool | Kenapa |
|-------|------|--------|
| Source DB | PostgreSQL | Standard OLTP, Debezium support |
| CDC | Debezium | Industry standard, open source |
| Message Broker | Kafka (Redpanda) | Streaming backbone, Redpanda lebih ringan |
| Processing | Apache Spark | Batch + streaming capable |
| Table Format | Delta Lake / Iceberg | Lakehouse pattern, ACID transactions |
| Object Storage | MinIO | S3-compatible, local, free |
| Transformation | dbt-core | Modeling, testing, documentation |
| Data Quality | Great Expectations | Automated data validation |
| Orchestration | Apache Airflow | Scheduling & monitoring |
| Serving | DuckDB / Snowflake | Analytical queries |
| Dashboard | Metabase | Open source BI |
| Monitoring | Prometheus + Grafana | Pipeline health |
| CI/CD | GitHub Actions | Automated testing & deployment |
| Containerization | Docker + Docker Compose | Semua service jalan di container |

---

## 7. Implementation Phases

### Phase 1: Foundation & Ingestion
- [ ] Setup Docker Compose (PostgreSQL, Kafka/Redpanda, MinIO)
- [ ] Bikin Python data generator (seed PostgreSQL + Kafka producer)
- [ ] Setup Debezium CDC dari PostgreSQL ke Kafka
- [ ] Landing data dari Kafka ke Bronze layer (MinIO + Delta Lake/Iceberg)

### Phase 2: Transformation & Modeling
- [ ] Setup Spark untuk batch processing Bronze → Silver
- [ ] Setup dbt untuk Silver → Gold transformation
- [ ] Design & implement dim/fact tables (star schema)
- [ ] Implement dbt tests

### Phase 3: Data Quality & Serving
- [ ] Setup Great Expectations untuk data validation di Bronze & Silver
- [ ] Setup DuckDB / Snowflake sebagai serving layer
- [ ] Connect Metabase ke serving layer
- [ ] Build dashboards untuk 7 business questions

### Phase 4: Orchestration & Monitoring
- [ ] Setup Airflow DAGs untuk orchestrate semua pipeline
- [ ] Setup Prometheus + Grafana untuk pipeline monitoring
- [ ] Alerting untuk pipeline failures & data quality issues

### Phase 5: CI/CD & Polish
- [ ] GitHub Actions: lint, test, deploy
- [ ] Automated dbt test + Great Expectations di CI
- [ ] Project documentation (README, architecture diagram, data dictionary)

---

## 8. Project Structure (Planned)

```
ecommerce-data-platform/
├── docker/                  # Docker Compose files per layer
├── src/
│   ├── generators/          # Python data generators (Faker + Kafka producer)
│   ├── ingestion/           # CDC config, Kafka consumers, landing scripts
│   ├── processing/          # Spark jobs (Bronze → Silver)
│   └── quality/             # Great Expectations suites
├── dbt/                     # dbt project (Silver → Gold)
├── airflow/                 # DAGs
├── dashboards/              # Metabase export / config
├── monitoring/              # Prometheus + Grafana config
├── .github/workflows/       # CI/CD
├── docs/                    # Documentation (this file)
└── README.md
```

---

## 9. Learning Objectives

Setiap phase punya learning objective yang jelas:

| Phase | Yang dipelajari |
|-------|----------------|
| Phase 1 | Kafka, CDC (Debezium), Delta Lake/Iceberg, event-driven architecture |
| Phase 2 | Medallion architecture, star schema modeling, dbt best practices |
| Phase 3 | Data quality engineering, serving layer patterns |
| Phase 4 | Pipeline orchestration patterns, observability |
| Phase 5 | CI/CD for data pipelines, software engineering practices |
