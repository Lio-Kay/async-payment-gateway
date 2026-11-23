import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.schemas import PaymentCreateRequest
from src.models.payment import Payment
from src.repositories.outbox import OutboxRepository
from src.repositories.payment import PaymentRepository

PAYMENT_CREATED_EVENT_TYPE = "payment.created"


class PaymentNotFoundError(Exception):
    """Raised when no payment row exists for the requested UUID."""

    def __init__(self, payment_id: uuid.UUID) -> None:
        self.payment_id = payment_id
        super().__init__(f"Payment {payment_id} not found.")


class IdempotencyConflictError(Exception):
    """Raised when an existing Idempotency-Key is reused with different parameters."""

    def __init__(self, idempotency_key: str) -> None:
        self.idempotency_key = idempotency_key
        super().__init__(
            f"Idempotency-Key '{idempotency_key}' was already used with a different payload."
        )


class PaymentService:
    """Handles payment creation (with transactional outbox) and lookups."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._payment_repo = PaymentRepository(session)
        self._outbox_repo = OutboxRepository(session)

    async def create_payment(
        self,
        payload: PaymentCreateRequest,
        idempotency_key: str,
    ) -> Payment:
        """Persist a new `Payment` and its `OutboxEvent` in one DB transaction.

        Idempotency is enforced in two layers:
        1. Before inserting, we check if `idempotency_key` already exists. If so,
           we compare all request fields and either return the stored payment (202)
           or raise `IdempotencyConflictError` (409).
        2. If two concurrent requests with the same key both pass step 1 before
           either commits, PostgreSQL's unique index `ix_payments_idempotency_key`
           rejects the second insert with `IntegrityError`. The `session.begin()`
           block rolls back automatically, and the `except IntegrityError` handler
           loads the winning row and runs the same payload check.
        """
        try:
            async with self._session.begin():
                existing = await self._payment_repo.get_by_idempotency_key(idempotency_key)
                if existing is not None:
                    self._ensure_payload_matches(existing, payload, idempotency_key)
                    return existing

                payment_id = uuid.uuid4()
                created_at = datetime.now(UTC)
                webhook_url_str = str(payload.webhook_url)
                # Normalize scale to 2 decimal places so in-memory Decimal values and
                # JSON outbox payloads match PostgreSQL's NUMERIC(12, 2) representation.
                normalized_amount = payload.amount.quantize(Decimal("0.01"))

                payment = await self._payment_repo.create(
                    payment_id=payment_id,
                    amount=normalized_amount,
                    currency=payload.currency,
                    description=payload.description,
                    payment_metadata=payload.metadata,
                    idempotency_key=idempotency_key,
                    webhook_url=webhook_url_str,
                )

                outbox_payload: dict[str, Any] = {
                    "payment_id": str(payment.id),
                    "amount": str(payment.amount),
                    "currency": payment.currency.value,
                    "description": payment.description,
                    "metadata": payment.payment_metadata,
                    "webhook_url": payment.webhook_url,
                    "idempotency_key": payment.idempotency_key,
                    "created_at": (payment.created_at or created_at).isoformat(),
                }
                await self._outbox_repo.add_event(
                    aggregate_id=payment.id,
                    event_type=PAYMENT_CREATED_EVENT_TYPE,
                    payload=outbox_payload,
                )
                return payment
        except IntegrityError:
            # The first transaction rolled back on exit from `session.begin()`.
            # Open a fresh transaction to inspect the row committed by the concurrent winner.
            async with self._session.begin():
                existing = await self._payment_repo.get_by_idempotency_key(idempotency_key)
                if existing is None:
                    raise
                self._ensure_payload_matches(existing, payload, idempotency_key)
                return existing

    async def get_payment(self, payment_id: uuid.UUID) -> Payment:
        payment = await self._payment_repo.get_by_id(payment_id)
        if payment is None:
            raise PaymentNotFoundError(payment_id)
        return payment

    @staticmethod
    def _ensure_payload_matches(
        existing: Payment,
        payload: PaymentCreateRequest,
        idempotency_key: str,
    ) -> None:
        """Verify that a retried request carries the exact same parameters as the original."""
        existing_amount = Decimal(str(existing.amount)).quantize(Decimal("0.01"))
        requested_amount = payload.amount.quantize(Decimal("0.01"))

        matches = (
            existing_amount == requested_amount
            and existing.currency == payload.currency
            and existing.description == payload.description
            and existing.payment_metadata == payload.metadata
            and existing.webhook_url == str(payload.webhook_url)
        )
        if not matches:
            raise IdempotencyConflictError(idempotency_key)
