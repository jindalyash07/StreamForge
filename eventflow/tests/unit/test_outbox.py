from __future__ import annotations

from decimal import Decimal

import pytest
from sqlalchemy import select

from order_service.repository import OrderRepository
from order_service.schemas import CreateOrderRequest
from order_service.service import OrderService
from shared.database.models import Order, OutboxEvent
from shared.kafka.topics import ORDER_CREATED_TOPIC
from shared.outbox import OutboxRelay, enqueue
from shared.schemas.events import EventEnvelope


class FlakyProducer:
    """Fails the first ``failures`` sends, then succeeds."""

    def __init__(self, failures: int) -> None:
        self.failures = failures
        self.sent: list[tuple[str, dict]] = []

    async def send_event(self, topic: str, event) -> None:
        if self.failures > 0:
            self.failures -= 1
            raise ConnectionError("broker unavailable")
        self.sent.append((topic, event))


def _event(source: str, n: int) -> EventEnvelope:
    return EventEnvelope(event_type="order.created", source=source, payload={"n": n})


async def _create_order(session_factory) -> None:
    service = OrderService(OrderRepository(), "order-service")
    async with session_factory() as session:
        await service.create_order(
            session, CreateOrderRequest(customer_name="Yash", product_name="Laptop", amount=Decimal("10"))
        )
        await session.commit()


@pytest.mark.asyncio
async def test_order_and_event_commit_together(session_factory) -> None:
    await _create_order(session_factory)

    async with session_factory() as session:
        orders = (await session.execute(select(Order))).scalars().all()
        outbox = (await session.execute(select(OutboxEvent))).scalars().all()

    assert len(orders) == 1
    assert len(outbox) == 1
    assert outbox[0].topic == ORDER_CREATED_TOPIC
    assert outbox[0].payload["payload"]["order_id"] == orders[0].id
    assert outbox[0].published_at is None


@pytest.mark.asyncio
async def test_rolled_back_order_leaves_no_event(session_factory) -> None:
    service = OrderService(OrderRepository(), "order-service")
    async with session_factory() as session:
        await service.create_order(
            session, CreateOrderRequest(customer_name="Yash", product_name="Laptop", amount=Decimal("10"))
        )
        await session.rollback()

    async with session_factory() as session:
        assert (await session.execute(select(Order))).first() is None
        assert (await session.execute(select(OutboxEvent))).first() is None


@pytest.mark.asyncio
async def test_relay_publishes_and_marks_events(session_factory, fake_producer) -> None:
    await _create_order(session_factory)
    relay = OutboxRelay(service_name="order-service", session_factory=session_factory, producer=fake_producer)

    assert await relay.publish_pending() == 1
    assert await relay.publish_pending() == 0  # nothing is published twice

    topic, payload = fake_producer.sent[0]
    assert topic == ORDER_CREATED_TOPIC
    assert payload["event_type"] == ORDER_CREATED_TOPIC
    async with session_factory() as session:
        row = (await session.execute(select(OutboxEvent))).scalar_one()
    assert row.published_at is not None


@pytest.mark.asyncio
async def test_relay_keeps_order_and_retries_after_broker_failure(session_factory) -> None:
    async with session_factory() as session:
        for n in range(3):
            enqueue(session, "order.created", _event("order-service", n))
        await session.commit()

    producer = FlakyProducer(failures=1)
    relay = OutboxRelay(service_name="order-service", session_factory=session_factory, producer=producer)

    assert await relay.publish_pending() == 0  # first send fails; later events wait behind it
    async with session_factory() as session:
        first = (await session.execute(select(OutboxEvent).order_by(OutboxEvent.id))).scalars().first()
    assert first.publish_attempts == 1
    assert "broker unavailable" in first.last_error

    await relay.drain()
    assert [payload["payload"]["n"] for _, payload in producer.sent] == [0, 1, 2]


@pytest.mark.asyncio
async def test_relay_only_publishes_its_own_service_events(session_factory, fake_producer) -> None:
    async with session_factory() as session:
        enqueue(session, "order.created", _event("order-service", 1))
        enqueue(session, "payment.success", _event("payment-service", 2))
        await session.commit()

    relay = OutboxRelay(service_name="payment-service", session_factory=session_factory, producer=fake_producer)
    await relay.drain()

    assert [topic for topic, _ in fake_producer.sent] == ["payment.success"]
