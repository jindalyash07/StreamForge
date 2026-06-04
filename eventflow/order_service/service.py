from __future__ import annotations

from decimal import Decimal

from shared.kafka.client import KafkaProducerClient
from shared.kafka.topics import ORDER_CREATED_TOPIC
from shared.logging.middleware import correlation_id_var
from shared.schemas.events import EventEnvelope, OrderCreatedPayload

from order_service.repository import OrderRepository
from order_service.schemas import CreateOrderRequest, CreateOrderResponse, OrderReadResponse


class OrderService:
    def __init__(self, repository: OrderRepository, producer: KafkaProducerClient) -> None:
        self.repository = repository
        self.producer = producer

    async def create_order(self, session, request: CreateOrderRequest) -> CreateOrderResponse:
        order = await self.repository.create(session, request)
        event = EventEnvelope(
            event_type=ORDER_CREATED_TOPIC,
            source="order-service",
            correlation_id=correlation_id_var.get(),
            payload=OrderCreatedPayload(
                order_id=order.id,
                customer_name=order.customer_name,
                product_name=order.product_name,
                amount=Decimal(order.amount),
            ).model_dump(mode="json"),
        )
        await self.producer.send_event(ORDER_CREATED_TOPIC, event)
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
