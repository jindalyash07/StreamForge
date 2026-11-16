from __future__ import annotations

from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from shared.database.models import Notification
from shared.kafka.client import KafkaProducerClient
from shared.kafka.idempotency import claim_event
from shared.kafka.topics import NOTIFICATION_SENT_TOPIC, PAYMENT_SUCCESS_TOPIC
from shared.logging.middleware import correlation_id_var
from shared.outbox import enqueue
from shared.observability.metrics import EVENTS_PROCESSED, PROCESSING_LATENCY
from shared.schemas.events import EventEnvelope, NotificationSentPayload, PaymentSuccessPayload


class NotificationService:
    def __init__(self, service_name: str = "notification-service") -> None:
        self.service_name = service_name

    async def handle_payment_success(self, event: EventEnvelope, session: AsyncSession, producer: KafkaProducerClient) -> None:
        with PROCESSING_LATENCY.labels(self.service_name, PAYMENT_SUCCESS_TOPIC).time():
            if not await claim_event(session, self.service_name, event.event_id, event.event_type):
                return

            payload = PaymentSuccessPayload.model_validate(event.payload)
            notification = Notification(
                order_id=payload.order_id,
                status="sent",
                channel="email",
                message=f"Payment received for order {payload.order_id}",
            )
            session.add(notification)
            await session.flush()

            notification_event = EventEnvelope(
                event_type=NOTIFICATION_SENT_TOPIC,
                source=self.service_name,
                correlation_id=event.correlation_id or correlation_id_var.get(),
                payload=NotificationSentPayload(
                    order_id=payload.order_id,
                    notification_id=notification.id,
                    channel="email",
                ).model_dump(mode="json"),
            )
            enqueue(session, NOTIFICATION_SENT_TOPIC, notification_event)
            EVENTS_PROCESSED.labels(self.service_name, PAYMENT_SUCCESS_TOPIC).inc()
