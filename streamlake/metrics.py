from prometheus_client import Counter, Histogram, Gauge, start_http_server

from .config import settings

RECORDS_PROCESSED = Counter(
    "streamlake_records_processed_total",
    "Total records written to Iceberg",
    ["table"],
)

COMMIT_LATENCY = Histogram(
    "streamlake_commit_latency_seconds",
    "Seconds to flush a batch to Iceberg and commit Kafka offsets",
    ["table"],
    buckets=[0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
)

ACTIVE_WORKERS = Gauge(
    "streamlake_active_workers",
    "Number of live table workers",
)


def start_metrics_server():
    start_http_server(settings.metrics_port)
    print(f"Metrics server listening on :{settings.metrics_port}/metrics")
