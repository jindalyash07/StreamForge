from decimal import Decimal

from pydantic import BaseModel, Field


class CreateOrderRequest(BaseModel):
    customer_name: str = Field(min_length=1, max_length=255)
    product_name: str = Field(min_length=1, max_length=255)
    amount: Decimal = Field(gt=0)


class CreateOrderResponse(BaseModel):
    order_id: str
    status: str


class OrderReadResponse(BaseModel):
    order_id: str
    customer_name: str
    product_name: str
    amount: Decimal
    status: str
