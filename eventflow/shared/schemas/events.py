from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, Field


class EventEnvelope(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    event_type: str
    source: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    correlation_id: str | None = None
    trace_id: str | None = None
    attempt: int = 0
    payload: dict[str, Any]


class OrderCreatedPayload(BaseModel):
    order_id: str
    customer_name: str
    product_name: str
    amount: Decimal


class PaymentSuccessPayload(BaseModel):
    order_id: str
    payment_id: str
    amount: Decimal
    provider_reference: str


class PaymentFailedPayload(BaseModel):
    order_id: str
    reason: str


class NotificationSentPayload(BaseModel):
    order_id: str
    notification_id: str
    channel: Literal["email"] = "email"


class DLQPayload(BaseModel):
    service_name: str
    original_topic: str
    event: dict[str, Any]
    error: str
    attempts: int
