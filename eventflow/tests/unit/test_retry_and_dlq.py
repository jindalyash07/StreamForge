from __future__ import annotations

from datetime import datetime, timezone

import pytest

from shared.kafka.client import KafkaProducerClient
from shared.kafka.dlq import publish_to_dlq
from shared.kafka.idempotency import claim_event, has_event_been_processed
from shared.kafka.retry import run_with_retry
from shared.kafka.topics import DEAD_LETTER_QUEUE_TOPIC
from shared.kafka.worker import KafkaWorker
from shared.schemas.events import EventEnvelope


@pytest.mark.asyncio
async def test_retry_succeeds_after_transient_failures() -> None:
    attempts = {"count": 0}

    async def operation(attempt: int) -> str:
        attempts["count"] += 1
        if attempt < 3:
            raise RuntimeError("transient")
        return "ok"

    result = await run_with_retry(operation, attempts=3, initial_delay=0)

    assert result == "ok"
    assert attempts["count"] == 3


@pytest.mark.asyncio
async def test_idempotency_claims_event_once(session_factory) -> None:
    async with session_factory() as session:
        claimed = await claim_event(session, "payment-service", "event-1", "order.created")
        await session.commit()

    async with session_factory() as session:
        duplicate = await claim_event(session, "payment-service", "event-1", "order.created")
        already_processed = await has_event_been_processed(session, "payment-service", "event-1")

    assert claimed is True
    assert duplicate is False
    assert already_processed is True


@pytest.mark.asyncio
async def test_publish_to_dlq_records_payload(fake_producer) -> None:
    event = {"event_id": "evt-1", "event_type": "order.created", "source": "order-service", "payload": {}}

    await publish_to_dlq(
        fake_producer,
        service_name="payment-service",
        original_topic="order.created",
        event=event,
        error="boom",
        attempts=3,
    )

    topic, payload = fake_producer.sent[0]
    assert topic == DEAD_LETTER_QUEUE_TOPIC
    assert payload["error"] == "boom"
    assert payload["original_topic"] == "order.created"
    assert payload["event"] == event
    assert payload["attempts"] == 3


@pytest.mark.asyncio
async def test_worker_routes_failed_message_to_dlq(session_factory, fake_producer) -> None:
    async def failing_handler(event: EventEnvelope, session, producer) -> None:
        raise RuntimeError("broken")

    worker = KafkaWorker(
        service_name="payment-service",
        bootstrap_servers="unused:9092",
        topics=["order.created"],
        group_id="payment-service-group",
        handler=failing_handler,
        session_factory=session_factory,
        producer=fake_producer,
    )

    handled = await worker.process_message(
        "order.created",
        {
            "event_id": "evt-2",
            "event_type": "order.created",
            "source": "order-service",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "payload": {"order_id": "o-1", "customer_name": "Yash", "product_name": "Laptop", "amount": "1.00"},
        },
    )

    assert handled is False
    assert fake_producer.sent[0][0] == DEAD_LETTER_QUEUE_TOPIC


@pytest.mark.asyncio
async def test_worker_sends_malformed_message_to_dlq_without_retry(session_factory, fake_producer) -> None:
    calls = {"count": 0}

    async def handler(event: EventEnvelope, session, producer) -> None:
        calls["count"] += 1

    worker = KafkaWorker(
        service_name="payment-service",
        bootstrap_servers="unused:9092",
        topics=["order.created"],
        group_id="payment-service-group",
        handler=handler,
        session_factory=session_factory,
        producer=fake_producer,
    )

    handled = await worker.process_message("order.created", {"not": "an envelope"})

    assert handled is False
    assert calls["count"] == 0
    topic, payload = fake_producer.sent[0]
    assert topic == DEAD_LETTER_QUEUE_TOPIC
    assert payload["attempts"] == 1
    assert payload["error"].startswith("invalid event envelope")


@pytest.mark.asyncio
async def test_worker_hands_handlers_the_producer_client_and_signals_commit(session_factory, fake_producer) -> None:
    seen = {}
    commits = []

    async def handler(event: EventEnvelope, session, producer) -> None:
        seen["producer"] = producer

    worker = KafkaWorker(
        service_name="payment-service",
        bootstrap_servers="unused:9092",
        topics=["order.created"],
        group_id="payment-service-group",
        handler=handler,
        session_factory=session_factory,
        producer=fake_producer,
        after_commit=lambda: commits.append(True),
    )

    handled = await worker.process_message(
        "order.created",
        {"event_id": "evt-3", "event_type": "order.created", "source": "order-service", "payload": {}},
    )

    assert handled is True
    assert seen["producer"] is fake_producer
    assert commits == [True]


def test_producer_client_can_be_built_outside_an_event_loop() -> None:
    client = KafkaProducerClient("unused:9092", client_id="test")

    with pytest.raises(RuntimeError, match="start"):
        _ = client.producer


def test_service_apps_import_without_a_running_loop() -> None:
    import analytics_service.app
    import notification_service.app
    import order_service.app
    import payment_service.app

    for module in (order_service.app, payment_service.app, notification_service.app, analytics_service.app):
        assert module.app is not None
