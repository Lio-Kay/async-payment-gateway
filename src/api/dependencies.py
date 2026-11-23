from typing import Annotated

from fastapi import Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.database import get_db_session
from src.core.security import verify_api_key
from src.services.payment import PaymentService

IDEMPOTENCY_KEY_HEADER_NAME = "Idempotency-Key"


async def require_idempotency_key(
    idempotency_key: Annotated[
        str,
        Header(
            alias=IDEMPOTENCY_KEY_HEADER_NAME,
            min_length=1,
            max_length=255,
            description="Client-generated key used to deduplicate payment creation requests.",
        ),
    ],
) -> str:
    """Validate and normalize the `Idempotency-Key` header.

    FastAPI's `min_length=1` catches empty strings (`""`), but a header consisting
    only of spaces (`"   "`) would pass length validation. We strip surrounding
    whitespace and reject blank keys with 422.
    """
    cleaned = idempotency_key.strip()
    if not cleaned:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="Idempotency-Key header must not be blank.",
        )
    return cleaned


async def get_payment_service(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> PaymentService:
    return PaymentService(session)


__all__ = [
    "IDEMPOTENCY_KEY_HEADER_NAME",
    "get_payment_service",
    "require_idempotency_key",
    "verify_api_key",
]
