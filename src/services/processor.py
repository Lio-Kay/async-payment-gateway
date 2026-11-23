import asyncio
import logging
import random
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime

from sqlalchemy.ext.asyncio import AsyncSession

from src.core.config import Settings, get_settings
from src.models.enums import PaymentStatus
from src.models.payment import Payment
from src.repositories.payment import PaymentRepository
from src.services.payment import PaymentNotFoundError
from src.services.webhook import WebhookService

logger = logging.getLogger(__name__)

SleepFunc = Callable[[float], Awaitable[None]]
UniformFunc = Callable[[float, float], float]
RandomFunc = Callable[[], float]


class PaymentProcessorService:
    """Runs the simulated gateway for one payment, saves the result, and sends the webhook.

    The relay delivers messages at least once, so this service must cope with
    seeing the same payment more than once:

    * Payment already final when loaded (a redelivery, e.g. the consumer crashed
      after committing but before the webhook went out): skip the gateway and
      resend the webhook.
    * Two consumers process the same payment at once: the status UPDATE only
      matches while the row is still `pending`, so exactly one consumer wins.
      The loser sends no webhook, because the winner sends it.

    No DB transaction is held during the 2-5s gateway sleep or the webhook call.
    """

    def __init__(
        self,
        session: AsyncSession,
        webhook_service: WebhookService | None = None,
        settings: Settings | None = None,
        *,
        sleep_func: SleepFunc = asyncio.sleep,
        uniform_func: UniformFunc = random.uniform,
        random_func: RandomFunc = random.random,
    ) -> None:
        self._session = session
        self._payment_repo = PaymentRepository(session)
        self._settings = settings or get_settings()
        self._webhook_service = webhook_service or WebhookService(settings=self._settings)
        self._sleep_func = sleep_func
        self._uniform_func = uniform_func
        self._random_func = random_func

    def _choose_terminal_status(self) -> PaymentStatus:
        """Return `succeeded` with probability `payment_success_rate` (0.9), else `failed`."""
        if self._random_func() < self._settings.payment_success_rate:
            return PaymentStatus.SUCCEEDED
        return PaymentStatus.FAILED

    async def _load_payment(self, payment_id: uuid.UUID) -> Payment:
        async with self._session.begin():
            payment = await self._payment_repo.get_by_id(payment_id)
        if payment is None:
            raise PaymentNotFoundError(payment_id)
        return payment

    async def process_payment(self, payment_id: uuid.UUID) -> Payment:
        payment = await self._load_payment(payment_id)
        log_ctx = {"payment_id": str(payment_id)}

        if payment.status != PaymentStatus.PENDING:
            logger.info(
                "Payment already finalized; resending webhook only",
                extra={**log_ctx, "status": payment.status.value},
            )
            await self._webhook_service.send_payment_webhook(payment)
            return payment

        # Simulated external gateway call: asyncio.sleep(random.uniform(2, 5)).
        await self._sleep_func(
            self._uniform_func(
                self._settings.payment_processing_min_seconds,
                self._settings.payment_processing_max_seconds,
            )
        )
        terminal_status = self._choose_terminal_status()

        async with self._session.begin():
            updated = await self._payment_repo.update_status(
                payment_id=payment_id,
                status=terminal_status,
                processed_at=datetime.now(UTC),
            )

        if updated is None:
            # Another consumer finalized this payment while we were sleeping; that
            # consumer sends the webhook. Return the stored result, not our roll.
            current = await self._load_payment(payment_id)
            logger.info(
                "Payment finalized concurrently by another consumer; skipping webhook",
                extra={**log_ctx, "status": current.status.value},
            )
            return current

        logger.info(
            "Payment processed",
            extra={
                **log_ctx,
                "status": updated.status.value,
                "processed_at": updated.processed_at.isoformat() if updated.processed_at else None,
            },
        )
        await self._webhook_service.send_payment_webhook(updated)
        return updated
