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
                OrderCreatedPayload.model_validate(event.payload)
                snapshot.total_orders += 1
            elif event.event_type == PAYMENT_SUCCESS_TOPIC:
                # Revenue is recognised only once money is actually captured, so declined
                # orders never inflate it.
                payload = PaymentSuccessPayload.model_validate(event.payload)
                snapshot.payment_success_count += 1
                snapshot.total_revenue = Decimal(snapshot.total_revenue) + Decimal(payload.amount)
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

    async def get_summary(self, session: AsyncSession) -> dict:
        result = await session.execute(select(AnalyticsSnapshot).where(AnalyticsSnapshot.id == 1))
        snapshot = result.scalar_one_or_none()
        if snapshot is None:
            snapshot = AnalyticsSnapshot(
                total_orders=0,
                total_revenue=Decimal("0.00"),
                payment_success_count=0,
                payment_failed_count=0,
                notification_sent_count=0,
                payment_success_rate=Decimal("0.00"),
            )
        return {
            "total_orders": snapshot.total_orders,
            "total_revenue": str(Decimal(snapshot.total_revenue).quantize(Decimal("0.01"))),
            "payment_success_count": snapshot.payment_success_count,
            "payment_failed_count": snapshot.payment_failed_count,
            "payment_success_rate": str(Decimal(snapshot.payment_success_rate).quantize(Decimal("0.01"))),
            "notification_sent_count": snapshot.notification_sent_count,
            "updated_at": snapshot.updated_at.isoformat() if snapshot.updated_at else None,
        }

    async def _get_snapshot(self, session: AsyncSession) -> AnalyticsSnapshot:
        result = await session.execute(select(AnalyticsSnapshot).where(AnalyticsSnapshot.id == 1))
        snapshot = result.scalar_one_or_none()
        if snapshot is None:
            snapshot = AnalyticsSnapshot(id=1)
            session.add(snapshot)
            await session.flush()
        return snapshot
