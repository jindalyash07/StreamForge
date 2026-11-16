from __future__ import annotations

from decimal import Decimal

from shared.kafka.topics import ORDER_CREATED_TOPIC
from shared.logging.middleware import correlation_id_var
from shared.outbox import enqueue
from shared.schemas.events import EventEnvelope, OrderCreatedPayload

from order_service.repository import OrderRepository
from order_service.schemas import CreateOrderRequest, CreateOrderResponse, OrderReadResponse


class OrderService:
    def __init__(self, repository: OrderRepository, service_name: str = "order-service") -> None:
        self.repository = repository
        self.service_name = service_name

    async def create_order(self, session, request: CreateOrderRequest) -> CreateOrderResponse:
        """Persist the order and stage ``order.created`` in the outbox.

        Both rows are written with the caller's session, so they commit atomically; the
        caller commits, and the outbox relay publishes the event afterwards.
        """
        order = await self.repository.create(session, request)
        event = EventEnvelope(
            event_type=ORDER_CREATED_TOPIC,
            source=self.service_name,
            correlation_id=correlation_id_var.get(),
            payload=OrderCreatedPayload(
                order_id=order.id,
                customer_name=order.customer_name,
                product_name=order.product_name,
                amount=Decimal(order.amount),
            ).model_dump(mode="json"),
        )
        enqueue(session, ORDER_CREATED_TOPIC, event)
        return CreateOrderResponse(order_id=order.id, status=order.status)

    async def get_order(self, session, order_id: str) -> OrderReadResponse | None:
        order = await self.repository.get(session, order_id)
        if not order:
            return None
        return OrderReadResponse(
            order_id=order.id,
            customer_name=order.customer_name,
            product_name=order.product_name,
            amount=order.amount,
            status=order.status,
        )
