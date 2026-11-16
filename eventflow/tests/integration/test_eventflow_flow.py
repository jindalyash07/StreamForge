from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select

from analytics_service.app import create_app as create_analytics_app
from analytics_service.service import AnalyticsService
from notification_service.service import NotificationService
from order_service.app import create_app as create_order_app
from payment_service.service import PaymentService
from shared.database.models import AnalyticsSnapshot, Order, OutboxEvent, Payment
from shared.kafka.topics import NOTIFICATION_SENT_TOPIC, ORDER_CREATED_TOPIC, PAYMENT_FAILED_TOPIC, PAYMENT_SUCCESS_TOPIC
from shared.outbox import OutboxRelay
from shared.schemas.events import EventEnvelope, OrderCreatedPayload


def _order_event(order_id: str, amount: str) -> EventEnvelope:
    return EventEnvelope(
        event_type=ORDER_CREATED_TOPIC,
        source="order-service",
        payload=OrderCreatedPayload(
            order_id=order_id,
            customer_name="Yash",
            product_name="Laptop",
            amount=Decimal(amount),
        ).model_dump(mode="json"),
    )


async def _relay(session_factory, producer, service_name: str) -> EventEnvelope:
    """Publish the service's pending outbox events and return the last one as an envelope."""
    await OutboxRelay(service_name=service_name, session_factory=session_factory, producer=producer).drain()
    return EventEnvelope.model_validate(producer.sent[-1][1])


def test_order_creation_publishes_event(session_factory, fake_producer, test_settings) -> None:
    app = create_order_app(test_settings, session_factory=session_factory, producer=fake_producer)

    with TestClient(app) as client:
        response = client.post(
            "/orders",
            json={"customer_name": "Yash", "product_name": "Laptop", "amount": 50000},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "created"
        assert client.get(f"/orders/{body['order_id']}").json()["customer_name"] == "Yash"

    # The relay publishes after commit (and drains on shutdown).
    topic, payload = fake_producer.sent[0]
    assert topic == ORDER_CREATED_TOPIC
    assert payload["payload"]["order_id"] == body["order_id"]

    async def verify_rows() -> None:
        async with session_factory() as session:
            order = await session.get(Order, body["order_id"])
            assert order is not None
            outbox = (await session.execute(select(OutboxEvent))).scalar_one()
            assert outbox.published_at is not None

    asyncio.run(verify_rows())


@pytest.mark.asyncio
async def test_payment_notification_and_analytics_chain(session_factory, fake_producer) -> None:
    payment_service = PaymentService()
    notification_service = NotificationService()
    analytics_service = AnalyticsService()
    order_event = _order_event("00000000-0000-0000-0000-000000000001", "50000.00")

    async with session_factory() as session:
        await analytics_service.handle_event(order_event, session, fake_producer)
        await payment_service.handle_order_created(order_event, session, fake_producer)
        await session.commit()

    payment_event = await _relay(session_factory, fake_producer, "payment-service")
    assert payment_event.event_type == PAYMENT_SUCCESS_TOPIC

    async with session_factory() as session:
        await analytics_service.handle_event(payment_event, session, fake_producer)
        await notification_service.handle_payment_success(payment_event, session, fake_producer)
        await session.commit()

    notification_event = await _relay(session_factory, fake_producer, "notification-service")
    assert notification_event.event_type == NOTIFICATION_SENT_TOPIC

    async with session_factory() as session:
        await analytics_service.handle_event(notification_event, session, fake_producer)
        await session.commit()

        snapshot = await session.get(AnalyticsSnapshot, 1)
        assert snapshot.total_orders == 1
        assert snapshot.total_revenue == Decimal("50000.00")
        assert snapshot.payment_success_count == 1
        assert snapshot.notification_sent_count == 1
        assert snapshot.payment_success_rate == Decimal("100.00")


@pytest.mark.asyncio
async def test_declined_payment_is_published_and_adds_no_revenue(session_factory, fake_producer) -> None:
    payment_service = PaymentService(max_amount=Decimal("100000"))
    analytics_service = AnalyticsService()
    order_event = _order_event("00000000-0000-0000-0000-000000000002", "250000.00")

    async with session_factory() as session:
        await analytics_service.handle_event(order_event, session, fake_producer)
        await payment_service.handle_order_created(order_event, session, fake_producer)
        await session.commit()

    failed_event = await _relay(session_factory, fake_producer, "payment-service")
    assert failed_event.event_type == PAYMENT_FAILED_TOPIC
    assert "limit" in failed_event.payload["reason"]

    async with session_factory() as session:
        await analytics_service.handle_event(failed_event, session, fake_producer)
        await session.commit()

        payment = (await session.execute(select(Payment))).scalar_one()
        assert payment.status == "failed"
        snapshot = await session.get(AnalyticsSnapshot, 1)
        assert snapshot.total_orders == 1
        assert snapshot.total_revenue == Decimal("0.00")
        assert snapshot.payment_failed_count == 1
        assert snapshot.payment_success_rate == Decimal("0.00")


@pytest.mark.parametrize(
    ("amount", "declined"),
    [("0", True), ("-5", True), ("100000", False), ("100000.01", True), ("1", False)],
)
def test_payment_decline_rule(amount: str, declined: bool) -> None:
    service = PaymentService(max_amount=Decimal("100000"))
    assert (service.decline_reason(Decimal(amount)) is not None) is declined


@pytest.mark.asyncio
async def test_duplicate_order_event_is_ignored(session_factory, fake_producer) -> None:
    payment_service = PaymentService()
    order_event = _order_event("00000000-0000-0000-0000-000000000003", "10.00")

    for _ in range(2):
        async with session_factory() as session:
            await payment_service.handle_order_created(order_event, session, fake_producer)
            await session.commit()

    async with session_factory() as session:
        assert len((await session.execute(select(Payment))).scalars().all()) == 1
        assert len((await session.execute(select(OutboxEvent))).scalars().all()) == 1


def test_analytics_summary_endpoint(session_factory, fake_producer, test_settings) -> None:
    app = create_analytics_app(test_settings, session_factory=session_factory, producer=fake_producer)
    app.router.lifespan_context = _no_kafka_lifespan(session_factory)

    with TestClient(app) as client:
        empty = client.get("/analytics/summary").json()
        assert empty["total_orders"] == 0
        assert empty["total_revenue"] == "0.00"

        async def seed() -> None:
            async with session_factory() as session:
                await AnalyticsService().handle_event(_order_event("00000000-0000-0000-0000-000000000004", "5"), session, fake_producer)
                await session.commit()

        asyncio.run(seed())
        assert client.get("/analytics/summary").json()["total_orders"] == 1


def _no_kafka_lifespan(session_factory):
    """Lifespan that sets up the database but skips starting the Kafka consumer."""
    from contextlib import asynccontextmanager

    from shared.database.session import init_database

    @asynccontextmanager
    async def lifespan(app):
        await init_database(session_factory)
        yield

    return lifespan
