# Real-time Cdc Etl Pipeline With Spark, Kafka & Debezium

## Overview

This project implements **a real-time Change Data Capture (CDC) ETL pipeline** that streams changes from a PostgreSQL source database to a PostgreSQL sink database using Debezium, Kafka, and Spark Structured **Streaming**.

The pipeline supports **full CRUD semantics, data validation, deduplication,** and **graceful handling of malformed or invalid records**, making it suitable for production-like scenarios.

High-Level Architecture
┌────────────┐
│ PostgreSQL │ (Source DB)
└─────┬──────┘
│ WAL (CDC)
▼
┌────────────┐
│ Debezium │
└─────┬──────┘
│ CDC Events
▼
┌────────────┐
│ Kafka │      (cdc.public.Employee)
└─────┬──────┘
│ Streaming
▼
┌────────────┐
│ Spark │      (Structured Streaming)
│ - Parsing │
│ - Validate│
│ - Dedup │
│ - CRUD │
└─────┬──────┘
│
├── Valid records ──▶ PostgreSQL (Sink DB)
│
└── Invalid / malformed records ──▶ Parquet (Dead Letter Queue)


# Setup Instructions (Docker Compose)

## Prerequisites

- Docker & Docker Compose

- Python 3.10+

- Java 11+


1. Start Infrastructure
`docker compose up -d`

This starts:

- PostgreSQL (source)

- Kafka

- Zookeeper

- Debezium Kafka Connect


2. # Configure Debezium Connector

Register the PostgreSQL connector:

`curl -X POST http://localhost:8083/connectors \`
`-H "Content-Type: application/json" \`
`-d @debezium-postgres-connector.json`


This streams table changes to:
`cdc.public.Employee`

3. # Configure Environment Variables
Create a `.env` file:
`DATABASE_URL=postgresql://user:password@host:5432/dbname?sslmode=require`

4. # Run Spark Streaming Job
`python spark_cdc_employee.py`



# Design Choices & Rationale

## PostgreSQL (Source & Sink)

- Familiar relational model

- WAL-based CDC support

- Acts as both OLTP source and lightweight analytical sink

- Trade-off: Not optimized for high-volume analytics compared to data warehouses.



# Debezium

- Log-based CDC (low latency)

- Preserves ordering and operation type (`c, u, d, r`)

- **Trade-off**: Adds operational complexity compared to polling.



# Kafka

- Durable message queue

- Enables replayability and decoupling

- Natural fit for CDC pipelines

- Trade-off: Requires partitioning strategy for scale.



# Spark Structured Streaming

Used for:

- JSON parsing with schema enforcement

- Stateful deduplication

- Data validation

- Business logic

- Fault tolerance via checkpoints


Key features used:

- `foreachBatch` for fine-grained DB control

- Watermarking + `dropDuplicates`

- Dead Letter Queue handling

**Trade-off**: More complex than microservices-based consumers, but far more expressive.


# Deduplication Strategy

`dropDuplicates(["id", "updated_ts"])`

Rationale:

- Kafka + Spark provide at-least-once delivery

- Debezium may emit duplicate events

- Deduplication ensures idempotent writes


# UPSERT & DELETE Handling

- **Insert / Update / Snapshot** → PostgreSQL `UPSERT` (`ON CONFLICT`)

- **Delete** → handled explicitly using `before.id`

Why?

- Spark JDBC does not support DELETE operations

- psycopg2 provides precise transactional control


# Data Validation & Dead Letter Queue

Two layers of validation:

1) **JSON-level** (schema / malformed records)

   - Captured via `_corrupt_record`

2) **Business-level** (domain rules)

   - Invalid role

   - Missing email

   - Invalid timestamps

Invalid records are written to:

`/tmp/badRecords (Parquet)`

This ensures:

- Stream never crashes

- Bad data is observable and replayable


# Edge Cases Handled

- Duplicate CDC events

- Out-of-order updates

- Malformed Kafka messages

- Partial updates

- Snapshot (op = r) handling

- Deletes without after payload


# Limitations & Future Improvements

**Current Limitations**

- Sink database may become a bottleneck under high throughput

- No schema registry (JSON only)

- Exactly-once semantics not guaranteed end-to-end


**Possible Improvements**

- Avro + Schema Registry

- Batch writes or COPY-based ingestion

- Move sink to analytical warehouse (BigQuery / Snowflake)

- Metrics & monitoring (Prometheus)


# Summary

This project demonstrates a **production-grade CDC ETL pipeline** with:

- Real-time streaming

- Full CRUD support

- Robust error handling

- Idempotent data delivery

It is designed to be **extensible**, **fault-tolerant**, **and easy to reason about**, while clearly documenting trade-offs and limitations.