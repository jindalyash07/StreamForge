# EventFlow Architecture

```mermaid
flowchart LR
    Customer((Customer)) --> OrderAPI[Order Service REST API]
    OrderAPI -->|write order| Postgres[(PostgreSQL)]
    OrderAPI -->|publish order.created| Kafka[(Apache Kafka)]
    Kafka --> Payment[Payment Service]
    Kafka --> Analytics[Analytics Service]
    Payment -->|publish payment.success| Kafka
    Payment -->|publish payment.failed| Kafka
    Kafka --> Notification[Notification Service]
    Notification -->|publish notification.sent| Kafka
    Kafka --> DLQ[dead.letter.queue]
    DLQ --> DLQConsumer[DLQ Debug Consumer]
```

The system uses Kafka as the event backbone and PostgreSQL as the source of truth for service-local state and analytics projections. Each consumer group is isolated, which allows independent scaling and replay without coupling services together.
