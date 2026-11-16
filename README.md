# StreamForge

**An event-driven e-commerce backend built on FastAPI, Apache Kafka and PostgreSQL.**

StreamForge contains **EventFlow**, a set of four independent Python microservices that talk to each other only through Kafka topics. It is built to show the patterns that make event-driven systems reliable in practice: consumer groups, manual offset commits, idempotent consumers, retries with exponential backoff, a dead-letter queue, and observability through structured logs and Prometheus metrics.

![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-0.115-009688?logo=fastapi&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.7-231F20?logo=apachekafka&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-7-DC382D?logo=redis&logoColor=white)
![Docker](https://img.shields.io/badge/Docker%20Compose-ready-2496ED?logo=docker&logoColor=white)
![Kubernetes](https://img.shields.io/badge/Kubernetes-manifests-326CE5?logo=kubernetes&logoColor=white)

---

## Contents

- [How it works](#how-it-works)
- [Services](#services)
- [Reliability patterns](#reliability-patterns)
- [Tech stack](#tech-stack)
- [Getting started](#getting-started)
- [API](#api)
- [Event contract](#event-contract)
- [Observability](#observability)
- [Testing](#testing)
- [Deploying to Kubernetes](#deploying-to-kubernetes)
- [Project structure](#project-structure)
- [Known limitations and roadmap](#known-limitations-and-roadmap)

---

## How it works

A customer places an order over REST. From there, every step is triggered by an event rather than a direct service call, so each service can be deployed, scaled and replayed on its own.

```mermaid
flowchart LR
    C((Customer)) -->|POST /orders| O[Order Service]
    O -->|write| DB[(PostgreSQL)]
    O --> T1[[order.created]]
    T1 --> P[Payment Service]
    T1 --> A[Analytics Service]
    P --> T2[[payment.success]]
    P --> T3[[payment.failed]]
    T2 --> N[Notification Service]
    N --> T4[[notification.sent]]
    T2 --> A
    T3 --> A
    T4 --> A
    P -. after 3 failed attempts .-> DLQ[[dead.letter.queue]]
    N -.-> DLQ
    A -.-> DLQ
    DLQ --> D[DLQ debug consumer]
```

```mermaid
sequenceDiagram
    participant Customer
    participant Order as Order Service
    participant Kafka
    participant Payment as Payment Service
    participant Notify as Notification Service
    participant Analytics as Analytics Service

    Customer->>Order: POST /orders
    Order->>Order: persist order
    Order->>Kafka: order.created
    Kafka->>Payment: order.created
    Kafka->>Analytics: order.created
    Payment->>Kafka: payment.success / payment.failed
    Kafka->>Notify: payment.success
    Notify->>Kafka: notification.sent
    Kafka->>Analytics: payment.* and notification.sent
```

## Services

| Service | Port | Consumes | Produces | Owns (PostgreSQL) |
|---|---|---|---|---|
| **Order** | 8000 | – (REST API) | `order.created` | `orders` |
| **Payment** | 8001 | `order.created` | `payment.success`, `payment.failed` | `payments` |
| **Notification** | 8002 | `payment.success` | `notification.sent` | `notifications` |
| **Analytics** | 8003 | all four business topics | – | `analytics_snapshots` |

- **Order Service** validates the request, persists the order, and publishes `order.created`.
- **Payment Service** simulates a payment provider. It records a payment, generates a provider reference, and publishes `payment.success`, or `payment.failed` when the amount is not positive.
- **Notification Service** records a notification log for each successful payment (simulated e-mail) and publishes `notification.sent`.
- **Analytics Service** maintains a single read-model row with total orders, total order value, payment success/failure counts, payment success rate and notifications sent.

Each consumer runs in its own consumer group (`payment-service-group`, `notification-service-group`, `analytics-service-group`), so every service receives every message on its topics, while replicas of the same service split partitions between them.

## Reliability patterns

| Pattern | Where | What it guarantees |
|---|---|---|
| **Manual offset commits** | `shared/kafka/worker.py` | Auto-commit is off. An offset is committed only after the message was handled or safely parked in the DLQ, so a crash mid-processing means redelivery, not loss. |
| **Idempotent consumers** | `shared/kafka/idempotency.py` | Each handler first inserts `(service, event_id)` into `processed_events`. A unique-constraint violation means "already seen", and the duplicate is skipped. The claim and the business write share one transaction. |
| **Retry with exponential backoff** | `shared/kafka/retry.py` | Up to 3 attempts per message, waiting 0.5 s then 1 s, each in a fresh database session. |
| **Dead-letter queue** | `shared/kafka/dlq.py` | After the final failed attempt, the original event, the error, the topic and the attempt count go to `dead.letter.queue`, and the consumer moves on instead of blocking the partition. |
| **Idempotent producer** | `shared/kafka/client.py` | `enable_idempotence=True` and `acks="all"` avoid duplicate writes on producer retries. |
| **Correlation IDs** | `shared/logging/middleware.py` | An `X-Correlation-ID` from the HTTP request travels inside every downstream event, so one order can be followed across all services in the logs. |
| **Rate limiting** | `shared/rate_limiting.py` | Fixed-window limit per client, per path, per service, backed by Redis (120 requests/minute by default). It fails open if Redis is unreachable. |

## Tech stack

- **API:** FastAPI, Uvicorn, Pydantic v2
- **Messaging:** Apache Kafka 3.7 (via `aiokafka`), ZooKeeper
- **Storage:** PostgreSQL 16 with async SQLAlchemy 2.0 and `asyncpg`
- **Cache / rate limiting:** Redis 7
- **Observability:** `prometheus-client`, Prometheus, JSON logging (`python-json-logger`)
- **Packaging:** Docker, Docker Compose, Kubernetes manifests
- **Testing:** pytest, pytest-asyncio, aiosqlite

## Getting started

### Prerequisites

- Docker and Docker Compose

### Run the whole stack

```bash
git clone https://github.com/jindalyash07/streamforge.git
cd streamforge/eventflow
cp .env.example .env          # optional; the defaults work with Compose
docker compose up --build
```

This starts ZooKeeper, Kafka, PostgreSQL, Redis, Prometheus and the four services. A one-off `kafka-init` container creates the five topics with 3 partitions each (topic auto-creation is disabled on the broker).

| Endpoint | URL |
|---|---|
| Order API | http://localhost:8000 |
| Interactive API docs | http://localhost:8000/docs |
| Payment / Notification / Analytics | http://localhost:8001, :8002, :8003 |
| Prometheus | http://localhost:9090 |

### Place an order

```bash
curl -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -H "X-Correlation-ID: demo-123" \
  -d '{"customer_name": "Yash", "product_name": "Laptop", "amount": 50000}'
```

```json
{ "order_id": "…", "status": "created" }
```

Then follow the order through the logs with `docker compose logs -f payment_service notification_service analytics_service`.

### Inspect the dead-letter queue

```bash
docker compose exec payment_service python -m shared.kafka.dlq_consumer
```

### Configuration

All services read the same settings from environment variables or `.env` (see `shared/config.py`).

| Variable | Default | Purpose |
|---|---|---|
| `SERVICE_NAME` | `eventflow` | Name used in logs, metrics and the idempotency table |
| `ENVIRONMENT` | `development` | Deployment environment label |
| `LOG_LEVEL` | `INFO` | Log verbosity |
| `DATABASE_URL` | `postgresql+asyncpg://postgres:postgres@postgres:5432/eventflow` | Async SQLAlchemy URL |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka brokers |
| `REDIS_URL` | `redis://redis:6379/0` | Rate-limiter backend |
| `RATE_LIMIT_PER_MINUTE` | `120` | Requests allowed per window |
| `RATE_LIMIT_WINDOW_SECONDS` | `60` | Window length |

### Troubleshooting

If `docker compose up --build` fails while pulling images with errors like `failed to fetch oauth token` or `http: server gave HTTP response to HTTPS client`, the cause is usually Docker Desktop's proxy settings rather than this project. Check them with:

```powershell
docker info --format "HTTPProxy={{.HTTPProxy}} HTTPSProxy={{.HTTPSProxy}} NoProxy={{.NoProxy}}"
```

If Docker is routed through a proxy such as `http.docker.internal:3128`, disable it in Docker Desktop or set the correct proxy for your network, then retry.

## API

Every service exposes `GET /health` (checks the database connection) and `GET /metrics` (Prometheus format). The Order Service also exposes:

| Method | Path | Description |
|---|---|---|
| `POST` | `/orders` | Create an order and emit `order.created`. Body: `customer_name`, `product_name` (1–255 chars) and `amount` (> 0). Returns `order_id` and `status`. |
| `GET` | `/orders/{order_id}` | Fetch an order, or `404` if it does not exist. |

Optional headers `X-Request-ID` and `X-Correlation-ID` are picked up by the logging middleware and generated when missing.

## Event contract

Every message on every topic uses the same envelope (`shared/schemas/events.py`):

```json
{
  "event_id": "uuid",
  "event_type": "order.created",
  "source": "order-service",
  "timestamp": "2026-11-17T10:00:00Z",
  "correlation_id": "demo-123",
  "trace_id": null,
  "attempt": 0,
  "payload": { "order_id": "…", "customer_name": "Yash", "product_name": "Laptop", "amount": "50000" }
}
```

| Topic | Payload fields |
|---|---|
| `order.created` | `order_id`, `customer_name`, `product_name`, `amount` |
| `payment.success` | `order_id`, `payment_id`, `amount`, `provider_reference` |
| `payment.failed` | `order_id`, `reason` |
| `notification.sent` | `order_id`, `notification_id`, `channel` (`email`) |
| `dead.letter.queue` | `service_name`, `original_topic`, `event`, `error`, `attempts` |

## Observability

- **Structured JSON logs** with timestamp, level, request ID and correlation ID, plus `service`, `topic`, `event_id` and `order_id` when present.
- **Prometheus metrics** on every service, scraped every 15 s through `monitoring/prometheus.yml`:

| Metric | Type | Labels |
|---|---|---|
| `eventflow_http_requests_total` | Counter | service, route, method, status |
| `eventflow_events_processed_total` | Counter | service, topic |
| `eventflow_event_retries_total` | Counter | service, topic |
| `eventflow_event_dlq_total` | Counter | service, topic |
| `eventflow_event_processing_seconds` | Histogram | service, topic |
| `eventflow_service_health` | Gauge | service (1 = healthy) |

## Testing

Tests run against SQLite (`aiosqlite`) with a fake Kafka producer, so no broker or database is needed.

```bash
cd eventflow
pip install -r requirements.txt
pytest                       # or: pytest --cov
```

- `tests/unit/test_retry_and_dlq.py`: retry recovers from transient failures, an event is claimed only once, DLQ payloads are recorded, and a failing worker routes the message to the DLQ.
- `tests/integration/test_eventflow_flow.py`: order creation publishes `order.created`, and the payment → notification → analytics chain updates every table.

> **Note:** the integration tests currently fail at import time. `order_service/app.py` builds the app (and its `AIOKafkaProducer`) at module level, which `aiokafka` 0.10 rejects outside a running event loop. The unit tests are unaffected. See the roadmap below.

## Deploying to Kubernetes

`kubernetes/` contains a namespace plus a Deployment and Service for each microservice (two replicas each), reading `DATABASE_URL` from an `eventflow-secrets` secret.

```bash
kubectl apply -f kubernetes/namespace.yaml
kubectl -n eventflow create secret generic eventflow-secrets \
  --from-literal=database-url='postgresql+asyncpg://user:pass@host:5432/eventflow'
kubectl apply -f kubernetes/
```

The manifests expect Kafka reachable at `kafka:9092` and images tagged `eventflow/<service>:latest`. Kafka, PostgreSQL and Redis themselves are not included.

## Project structure

```
eventflow/
├── order_service/          # REST API: models, repository, service, FastAPI app
├── payment_service/        # order.created  -> payment.success | payment.failed
├── notification_service/   # payment.success -> notification.sent
├── analytics_service/      # all events -> analytics_snapshots read model
├── shared/
│   ├── kafka/              # producer client, worker loop, retry, idempotency, DLQ, topics
│   ├── database/           # SQLAlchemy base, async session, models
│   ├── schemas/            # event envelope and payload models
│   ├── logging/            # JSON logging and request-context middleware
│   ├── observability/      # Prometheus metrics
│   ├── rate_limiting.py    # Redis-backed rate-limit middleware
│   └── config.py           # pydantic-settings configuration
├── tests/                  # unit and integration tests
├── kubernetes/             # namespace, deployments and services
├── monitoring/             # Prometheus scrape config
├── architecture/           # architecture notes
├── Dockerfile.*            # one image per service (python:3.12-slim)
└── docker-compose.yml      # full local stack
```

## Known limitations and roadmap

**Current limitations**

- Order Service commits to PostgreSQL and publishes to Kafka as two separate steps, so a crash between them can leave an order without its event.
- Payments are simulated. The API already rejects non-positive amounts, so orders placed through it always end in `payment.success`.
- Analytics "total revenue" sums order amounts at `order.created`, before payment succeeds.
- Integration tests fail to import (see [Testing](#testing)).

**Planned**

- Transactional outbox for atomic write-and-publish in Order Service
- Lazy producer creation so the app can be imported in tests
- Grafana dashboards and Prometheus/Grafana Kubernetes manifests
- Kubernetes HPA and Ingress
- Distributed tracing with OpenTelemetry (the envelope already carries `trace_id`)
- Schema registry and event versioning
- An analytics read endpoint
