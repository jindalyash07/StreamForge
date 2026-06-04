from __future__ import annotations

from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from shared.database.models import AnalyticsSnapshot
from shared.kafka.client import KafkaProducerClient
from shared.kafka.idempotency import claim_event
from shared.kafka.topics import NOTIFICATION_SENT_TOPIC, ORDER_CREATED_TOPIC, PAYMENT_FAILED_TOPIC, PAYMENT_SUCCESS_TOPIC
from shared.observability.metrics import EVENTS_PROCESSED, PROCESSING_LATENCY
from shared.schemas.events import EventEnvelope, NotificationSentPayload, OrderCreatedPayload, PaymentFailedPayload, PaymentSuccessPayload


class AnalyticsService:
    def __init__(self, service_name: str = "analytics-service") -> None:
        self.service_name = service_name

    async def handle_event(self, event: EventEnvelope, session: AsyncSession, producer: KafkaProducerClient) -> None:
        with PROCESSING_LATENCY.labels(self.service_name, event.event_type).time():
            if not await claim_event(session, self.service_name, event.event_id, event.event_type):
                return

            snapshot = await self._get_snapshot(session)
            if event.event_type == ORDER_CREATED_TOPIC:
                payload = OrderCreatedPayload.model_validate(event.payload)
                snapshot.total_orders += 1
                snapshot.total_revenue = Decimal(snapshot.total_revenue) + Decimal(payload.amount)
            elif event.event_type == PAYMENT_SUCCESS_TOPIC:
                PaymentSuccessPayload.model_validate(event.payload)
                snapshot.payment_success_count += 1
            elif event.event_type == PAYMENT_FAILED_TOPIC:
                PaymentFailedPayload.model_validate(event.payload)
                snapshot.payment_failed_count += 1
            elif event.event_type == NOTIFICATION_SENT_TOPIC:
                NotificationSentPayload.model_validate(event.payload)
                snapshot.notification_sent_count += 1

            total_payments = snapshot.payment_success_count + snapshot.payment_failed_count
            snapshot.payment_success_rate = Decimal("0.00")
            if total_payments:
                snapshot.payment_success_rate = (Decimal(snapshot.payment_success_count) / Decimal(total_payments)) * Decimal("100")
            session.add(snapshot)
            await session.flush()
            EVENTS_PROCESSED.labels(self.service_name, event.event_type).inc()

    async def _get_snapshot(self, session: AsyncSession) -> AnalyticsSnapshot:
        result = await session.execute(select(AnalyticsSnapshot).where(AnalyticsSnapshot.id == 1))
        snapshot = result.scalar_one_or_none()
        if snapshot is None:
            snapshot = AnalyticsSnapshot(id=1)
            session.add(snapshot)
            await session.flush()
        return snapshot
