from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from shared.database.models import Order
from order_service.schemas import CreateOrderRequest


class OrderRepository:
    async def create(self, session: AsyncSession, request: CreateOrderRequest) -> Order:
        order = Order(
            customer_name=request.customer_name,
            product_name=request.product_name,
            amount=request.amount,
            status="created",
        )
        session.add(order)
        await session.flush()
        return order

    async def get(self, session: AsyncSession, order_id: str) -> Order | None:
        return await session.get(Order, order_id)
