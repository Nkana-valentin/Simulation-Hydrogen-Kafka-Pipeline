"""
Prometheus metric definitions shared across all processes.
Each process gets its own registry — no multiprocess dir needed.
"""
from prometheus_client import Counter, Gauge

# ---------------------------------------------------------------------------
# Ingestion (consumer process)
# ---------------------------------------------------------------------------

INGEST_MESSAGES = Counter(
    "h2_ingestion_messages_total",
    "Total Kafka messages handled, by outcome",
    ["result"],  # received | auth_failed | valid | invalid
)

INGEST_QUALITY_YIELD = Gauge(
    "h2_ingestion_quality_yield_ratio",
    "Fraction of processed records that passed validation (0–1)",
)

# ---------------------------------------------------------------------------
# Sync worker (FastAPI process)
# ---------------------------------------------------------------------------

SYNC_BATCHES = Counter(
    "h2_sync_batches_total",
    "Auto-sync batch attempts, by status",
    ["status"],  # success | waiting | no_new_data | error
)

SYNC_RECORDS = Counter(
    "h2_sync_records_synced_total",
    "Cumulative records written to sync output",
)

SYNC_LAG = Gauge(
    "h2_sync_lag_seconds",
    "Seconds elapsed since the last successful sync batch",
)
