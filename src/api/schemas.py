import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, HttpUrl

from src.models.enums import Currency, PaymentStatus


class PaymentCreateRequest(BaseModel):
    """Request body for `POST /api/v1/payments`."""

    # PostgreSQL NUMERIC(12, 2) holds up to 10 integer digits and 2 fractional digits
    # (strictly less than 10^10). `lt=Decimal("10000000000")` rejects 11-12 digit integers
    # at the API boundary with 422 instead of hitting a Postgres numeric overflow.
    amount: Decimal = Field(
        ...,
        gt=Decimal("0"),
        lt=Decimal("10000000000"),
        max_digits=12,
        decimal_places=2,
        description="Positive payment amount (< 10^10) with up to 2 decimal places.",
    )
    currency: Currency
    description: str = Field(default="", max_length=500)
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: HttpUrl = Field(..., max_length=2048)


class PaymentCreateResponse(BaseModel):
    """202 Accepted response body returned when a payment is queued."""

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    # Accepts either `payment.id` (from the SQLAlchemy Payment instance) or `payment_id`.
    payment_id: uuid.UUID = Field(validation_alias=AliasChoices("payment_id", "id"))
    status: PaymentStatus
    created_at: datetime


class PaymentResponse(BaseModel):
    """Full payment representation returned by `GET /api/v1/payments/{payment_id}`."""

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    id: uuid.UUID
    amount: Decimal
    currency: Currency
    description: str
    # Reads `payment.payment_metadata` from the ORM model and serializes it as `"metadata"`.
    metadata: dict[str, Any] = Field(
        validation_alias=AliasChoices("payment_metadata", "metadata"),
    )
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None = None
