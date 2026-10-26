# Real-Time Insurance Data Processing

A simple base project that simulates a live stream of insurance events
(policies, claims, payments) and processes them in real time.

## Pipeline
Producer -> Queue (stream) -> Processor -> SQLite + Live dashboard

- Validation: rejects malformed events (missing fields, bad amounts, unknown policy)
- Enrichment: monthly premium and risk level per policy
- Fraud rules: large claims, claims right after policy start, repeated claims
- Storage: SQLite (`insurance.db`) with tables policies, claims, payments, rejected
- Live console dashboard every few seconds

## Requirements
Python 3.8+ (standard library only, no installs needed)

## Run
    python insurance_stream.py --duration 30 --rate 10

Options:
    --duration      seconds to run (default 30)
    --rate          events per second (default 10)
    --report-every  dashboard interval in seconds (default 5)

## Query results
    sqlite3 insurance.db "SELECT status, COUNT(*) FROM claims GROUP BY status;"
