Idempotent API Ingestion Pipeline

A production-style data ingestion pipeline that incrementally collects public GitHub events, stores immutable raw data, and produces a clean partitioned dataset with guaranteed no duplication.

The project demonstrates real Data Engineering patterns: incremental ingestion, checkpointing (watermark), bronze/silver layers, idempotency, and data quality validation.

Architecture
                GitHub Public API
                       │
                       ▼
               Incremental Fetch
                       │
                       ▼
            Bronze Layer (Raw JSONL)
     Immutable append-only source of truth
                       │
                       ▼
        Cleaning + Validation + Deduplication
                       │
                       ▼
         Silver Layer (Partitioned Parquet)
         Analytics-ready reliable dataset
                       │
                       ▼
               Checkpoint Update
        (Only after successful processing)

        What This Pipeline Solves

Real pipelines must be safe to rerun.

If a job crashes, runs twice, or re-downloads overlapping data:

no duplicates must appear

no historical data must be lost

processing must resume from the last success point

This project implements those guarantees.

Key Features
Incremental ingestion

Only new events since the last successful run are processed using a checkpoint watermark.

Idempotency

Running the pipeline multiple times produces the same dataset.

Achieved by:

checkpoint filtering

deduplication by event id

Bronze / Silver data layers

Bronze: raw immutable API responses (audit & recovery layer)
Silver: clean, structured, analytics-ready dataset

Data quality checks

The pipeline fails safely if:

event id is null

timestamp cannot be parsed

duplicate events appear

Checkpoint updates only after success.

Partitioned storage

Silver dataset is partitioned by event date:

data/silver/events/date=YYYY-MM-DD/part-*.parquet

Optimized for analytics engines.

Production logging

All steps are logged with timestamps and counts.

Project Structure
api-ingestion-idempotent-pipeline/
│
├── data/
│   ├── bronze/        # Raw immutable events
│   └── silver/        # Clean partitioned dataset
│
├── state/
│   └── checkpoints.json   # Watermark storage
│
├── src/
│   ├── config.py
│   ├── logger.py
│   ├── api_client.py
│   ├── bronze_writer.py
│   ├── silver_transform.py
│   └── runner.py
│
├── tests/
│   └── test_dedup.py
│
├── requirements.txt
└── README.md

Checkpoint Mechanism

Stored in:

state/checkpoints.json

Example:

{
  "github_events": {
    "last_created_at": "2026-02-20T00:00:00Z"
  }
}

Behavior:

Load last processed timestamp

Fetch recent events

Keep only newer events

Process data

Update checkpoint only if successful

If the job fails → checkpoint does NOT move → safe rerun.

How to Run
1) Create virtual environment
python -m venv .venv
.venv\Scripts\activate
2) Install dependencies
pip install -r requirements.txt
3) Run the pipeline
python -m src.runner
Expected Output
Bronze (raw immutable)
data/bronze/events/ingest_date=YYYY-MM-DD/events_*.jsonl
Silver (analytics dataset)
data/silver/events/date=YYYY-MM-DD/part-*.parquet
Checkpoint updated
state/checkpoints.json
Why This Matters (Data Engineering Perspective)

This project simulates a real ingestion job found in production systems:

external API ingestion

late arriving / overlapping data

safe retries

historical reliability

layered storage architecture

Conceptually similar to ingestion pipelines used with Airflow, Kafka consumers, or scheduled batch jobs.

Skills Demonstrated

Incremental ingestion patterns

Idempotent pipeline design

Checkpoint / watermark processing

Data quality validation

Partitioned Parquet datasets

Raw vs curated storage layers

Production logging & safe failure handling

Future Improvements

Add orchestration (Airflow / Prefect)

Add warehouse load (BigQuery / Snowflake)

Containerize with Docker

Add CI tests

Stream ingestion (Kafka simulation)