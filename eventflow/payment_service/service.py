from __future__ import annotations

from decimal import Decimal
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from shared.database.models import Payment
from shared.kafka.client import KafkaProducerClient
from shared.kafka.idempotency import claim_event
from shared.kafka.topics import ORDER_CREATED_TOPIC, PAYMENT_FAILED_TOPIC, PAYMENT_SUCCESS_TOPIC
from shared.logging.middleware import correlation_id_var
from shared.observability.metrics import EVENTS_PROCESSED, PROCESSING_LATENCY
from shared.outbox import enqueue
from shared.schemas.events import EventEnvelope, OrderCreatedPayload, PaymentFailedPayload, PaymentSuccessPayload


DEFAULT_MAX_AMOUNT = Decimal("100000")


class PaymentService:
    """Simulated payment provider.

    A payment is declined when the amount is not positive or exceeds ``max_amount``
    (a per-transaction limit, configurable with ``PAYMENT_MAX_AMOUNT``); otherwise it succeeds.
    """

    def __init__(self, service_name: str = "payment-service", max_amount: Decimal = DEFAULT_MAX_AMOUNT) -> None:
        self.service_name = service_name
        self.max_amount = Decimal(max_amount)

    def decline_reason(self, amount: Decimal) -> str | None:
        if amount <= 0:
            return "amount must be positive"
        if amount > self.max_amount:
            return f"amount exceeds per-transaction limit of {self.max_amount}"
        return None

    async def handle_order_created(self, event: EventEnvelope, session: AsyncSession, producer: KafkaProducerClient) -> None:
        with PROCESSING_LATENCY.labels(self.service_name, ORDER_CREATED_TOPIC).time():
            if not await claim_event(session, self.service_name, event.event_id, event.event_type):
                return

            payload = OrderCreatedPayload.model_validate(event.payload)
            payment = Payment(order_id=payload.order_id, status="pending")
            session.add(payment)
            await session.flush()
            correlation_id = event.correlation_id or correlation_id_var.get()

            reason = self.decline_reason(Decimal(payload.amount))
            if reason is not None:
                payment.status = "failed"
                failure_event = EventEnvelope(
                    event_type=PAYMENT_FAILED_TOPIC,
                    source=self.service_name,
                    correlation_id=correlation_id,
                    payload=PaymentFailedPayload(order_id=payload.order_id, reason=reason).model_dump(mode="json"),
                )
                enqueue(session, PAYMENT_FAILED_TOPIC, failure_event)
                EVENTS_PROCESSED.labels(self.service_name, ORDER_CREATED_TOPIC).inc()
                return

            payment.status = "success"
            payment.provider_reference = f"pay_{uuid4().hex[:16]}"
            success_event = EventEnvelope(
                event_type=PAYMENT_SUCCESS_TOPIC,
                source=self.service_name,
                correlation_id=correlation_id,
                payload=PaymentSuccessPayload(
                    order_id=payload.order_id,
                    payment_id=payment.id,
                    amount=Decimal(payload.amount),
                    provider_reference=payment.provider_reference,
                ).model_dump(mode="json"),
            )
            enqueue(session, PAYMENT_SUCCESS_TOPIC, success_event)
            EVENTS_PROCESSED.labels(self.service_name, ORDER_CREATED_TOPIC).inc()
