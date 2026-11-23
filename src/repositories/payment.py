import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import select, update

from src.models.enums import Currency, PaymentStatus
from src.models.payment import Payment
from src.repositories.base import BaseRepository


class PaymentRepository(BaseRepository[Payment]):
    """Queries and state transitions for `Payment` rows (`payments` table)."""

    model = Payment

    async def create(
        self,
        *,
        amount: Decimal,
        currency: Currency,
        description: str,
        payment_metadata: dict[str, Any],
        idempotency_key: str,
        webhook_url: str,
        payment_id: uuid.UUID | None = None,
    ) -> Payment:
        """Stage and flush a new pending payment inside the caller's transaction.

        We set `id` and `created_at` in Python (rather than waiting for SQLAlchemy's
        INSERT-time column defaults) and call `session.flush()` immediately so:
        1. unique-key violations on `idempotency_key` surface inside `session.begin()`;
        2. the parent `payments` row exists before `OutboxRepository.add_event`
           flushes the child `outbox` row referencing `payments.id`.
        """
        payment = Payment(
            id=payment_id or uuid.uuid4(),
            amount=amount,
            currency=currency,
            description=description,
            payment_metadata=payment_metadata,
            status=PaymentStatus.PENDING,
            idempotency_key=idempotency_key,
            webhook_url=webhook_url,
            created_at=datetime.now(UTC),
        )
        self._session.add(payment)
        await self._session.flush()
        return payment

    async def get_by_id(self, payment_id: uuid.UUID) -> Payment | None:
        stmt = select(Payment).where(Payment.id == payment_id)
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_by_idempotency_key(self, idempotency_key: str) -> Payment | None:
        stmt = select(Payment).where(Payment.idempotency_key == idempotency_key)
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def update_status(
        self,
        payment_id: uuid.UUID,
        status: PaymentStatus,
        processed_at: datetime | None = None,
        *,
        only_if_pending: bool = True,
    ) -> Payment | None:
        """Atomically move a payment to a final status (`succeeded` or `failed`).

        With `only_if_pending=True` (the default), the SQL statement is:
            `UPDATE payments SET status = ..., processed_at = ...`
            `WHERE id = ... AND status = 'pending' RETURNING *`

        Because the outbox relay guarantees at-least-once delivery, two consumers
        could theoretically receive the same payment. The `AND status = 'pending'`
        guard ensures only the first UPDATE matches a row; the second matches 0
        rows and receives `None`, preventing double finalization.
        """
        if status == PaymentStatus.PENDING:
            raise ValueError("Cannot transition a payment back to 'pending' status.")

        stmt = update(Payment).where(Payment.id == payment_id)
        if only_if_pending:
            stmt = stmt.where(Payment.status == PaymentStatus.PENDING)
        # `populate_existing=True` refreshes the instance in SQLAlchemy's identity
        # map if the same session loaded the Payment earlier.
        stmt = (
            stmt.values(status=status, processed_at=processed_at or datetime.now(UTC))
            .returning(Payment)
            .execution_options(populate_existing=True)
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()
