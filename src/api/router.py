import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status

from src.api.dependencies import (
    get_payment_service,
    require_idempotency_key,
    verify_api_key,
)
from src.api.schemas import (
    PaymentCreateRequest,
    PaymentCreateResponse,
    PaymentResponse,
)
from src.services.payment import (
    IdempotencyConflictError,
    PaymentNotFoundError,
    PaymentService,
)

# Attach X-API-Key verification at the router level so every /api/v1/payments*
# endpoint requires authentication automatically.
router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payments"],
    dependencies=[Depends(verify_api_key)],
)


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentCreateResponse,
)
async def create_payment(
    payload: PaymentCreateRequest,
    idempotency_key: Annotated[str, Depends(require_idempotency_key)],
    service: Annotated[PaymentService, Depends(get_payment_service)],
) -> PaymentCreateResponse:
    """Queue a new payment for asynchronous processing (`202 Accepted`).

    Returns the existing payment if `Idempotency-Key` was already used with the
     exact same body, or `409 Conflict` if the body differs from the original request.
    """
    try:
        payment = await service.create_payment(
            payload=payload,
            idempotency_key=idempotency_key,
        )
    except IdempotencyConflictError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(exc),
        ) from exc

    return PaymentCreateResponse.model_validate(payment)


@router.get(
    "/{payment_id}",
    status_code=status.HTTP_200_OK,
    response_model=PaymentResponse,
)
async def get_payment(
    payment_id: uuid.UUID,
    service: Annotated[PaymentService, Depends(get_payment_service)],
) -> PaymentResponse:
    """Fetch full payment details by ID, or return `404 Not Found`."""
    try:
        payment = await service.get_payment(payment_id)
    except PaymentNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc

    return PaymentResponse.model_validate(payment)
