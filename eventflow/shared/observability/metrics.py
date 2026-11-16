from __future__ import annotations

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest


HTTP_REQUESTS = Counter(
    "eventflow_http_requests_total",
    "Total HTTP requests handled by EventFlow services",
    ["service", "route", "method", "status"],
)
EVENTS_PROCESSED = Counter(
    "eventflow_events_processed_total",
    "Total events successfully processed",
    ["service", "topic"],
)
EVENT_RETRIES = Counter(
    "eventflow_event_retries_total",
    "Total retry attempts for event processing",
    ["service", "topic"],
)
EVENT_DLQ = Counter(
    "eventflow_event_dlq_total",
    "Total events routed to the DLQ",
    ["service", "topic"],
)
PROCESSING_LATENCY = Histogram(
    "eventflow_event_processing_seconds",
    "Event processing duration in seconds",
    ["service", "topic"],
)
SERVICE_HEALTH = Gauge(
    "eventflow_service_health",
    "Health status for the current service (1 healthy, 0 unhealthy)",
    ["service"],
)
OUTBOX_PUBLISHED = Counter(
    "eventflow_outbox_published_total",
    "Outbox events published to Kafka",
    ["service", "topic"],
)
OUTBOX_PUBLISH_FAILURES = Counter(
    "eventflow_outbox_publish_failures_total",
    "Failed attempts to publish an outbox event",
    ["service", "topic"],
)


def metrics_payload() -> tuple[bytes, str]:
    return generate_latest(), CONTENT_TYPE_LATEST
