# EventFlow

The application code for [StreamForge](../README.md): four FastAPI microservices (Order, Payment, Notification, Analytics) that communicate through Apache Kafka, with PostgreSQL for state and Redis for rate limiting.

**Full documentation (architecture, reliability patterns, API, event contract, configuration and deployment) is in the [root README](../README.md).**

## Quick start

```bash
cp .env.example .env
docker compose up --build
```

```bash
curl -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{"customer_name": "Yash", "product_name": "Laptop", "amount": 50000}'
```

| Service | Port |
|---|---|
| Order (REST API, docs at `/docs`) | 8000 |
| Payment | 8001 |
| Notification | 8002 |
| Analytics | 8003 |
| Prometheus | 9090 |

## Common commands

```bash
pytest                                   # run tests (SQLite + fake producer, no Kafka needed)
pytest --cov                             # with coverage
python -m shared.kafka.dlq_consumer      # print messages from dead.letter.queue
docker compose logs -f payment_service   # follow one service
```

See also [`architecture/overview.md`](architecture/overview.md).
