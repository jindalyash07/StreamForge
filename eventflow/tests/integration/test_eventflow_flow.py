from __future__ import annotations

from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

from analytics_service.service import AnalyticsService
from notification_service.service import NotificationService
from order_service.app import create_app as create_order_app
from payment_service.service import PaymentService
from shared.database.models import AnalyticsSnapshot, Order
from shared.kafka.topics import NOTIFICATION_SENT_TOPIC, ORDER_CREATED_TOPIC, PAYMENT_SUCCESS_TOPIC
from shared.schemas.events import EventEnvelope, OrderCreatedPayload


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
    assert fake_producer.sent[0][0] == ORDER_CREATED_TOPIC

    async def verify_order_row() -> None:
        async with session_factory() as session:
            order = await session.get(Order, body["order_id"])
            assert order is not None
            assert order.customer_name == "Yash"

    import asyncio

    asyncio.run(verify_order_row())


@pytest.mark.asyncio
async def test_payment_notification_and_analytics_chain(session_factory, fake_producer) -> None:
    payment_service = PaymentService()
    notification_service = NotificationService()
    analytics_service = AnalyticsService()

    order_event = EventEnvelope(
        event_type=ORDER_CREATED_TOPIC,
        source="order-service",
        payload=OrderCreatedPayload(
            order_id="order-1",
            customer_name="Yash",
            product_name="Laptop",
            amount=Decimal("50000.00"),
        ).model_dump(mode="json"),
    )

    async with session_factory() as session:
        await payment_service.handle_order_created(order_event, session, fake_producer)
        await analytics_service.handle_event(order_event, session, fake_producer)
        await session.commit()

    payment_event = fake_producer.sent[-1][1]
    async with session_factory() as session:
        await analytics_service.handle_event(payment_event, session, fake_producer)
        await notification_service.handle_payment_success(payment_event, session, fake_producer)
        await session.commit()

    notification_event = fake_producer.sent[-1][1]
    async with session_factory() as session:
        await analytics_service.handle_event(notification_event, session, fake_producer)
        await session.commit()

        snapshot = await session.get(AnalyticsSnapshot, 1)
        assert snapshot is not None
        assert snapshot.total_orders == 1
        assert snapshot.total_revenue == Decimal("50000.00")
        assert snapshot.payment_success_count == 1
        assert snapshot.notification_sent_count == 1
        assert snapshot.payment_success_rate == Decimal("100.00")
