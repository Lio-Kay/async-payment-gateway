import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

import httpx

from src.core.config import Settings, get_settings
from src.models.payment import Payment

logger = logging.getLogger(__name__)

SleepFunc = Callable[[float], Awaitable[None]]

# 4xx responses that are worth retrying. Any other 4xx means the receiver rejected
# the request itself, so sending the same body again will not help.
RETRYABLE_CLIENT_STATUS_CODES = frozenset({408, 425, 429})


class WebhookDeliveryError(Exception):
    """Raised when an outbound webhook fails all configured retry attempts."""


def is_retryable_webhook_error(exc: Exception) -> bool:
    """Retry on network errors, 5xx, and 408/425/429; give up on other 4xx and bad URLs."""
    if isinstance(exc, httpx.InvalidURL):
        return False
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        return code >= 500 or code in RETRYABLE_CLIENT_STATUS_CODES
    return isinstance(exc, httpx.HTTPError)


class WebhookService:
    """Sends payment result webhooks (HTTP POST), retrying with exponential backoff.

    `webhook_max_retries` is the total number of attempts (3 by default). The
    service sleeps only *between* attempts (1s, then 2s), never after the last one.

    Delivery is at least once: the consumer may resend the webhook for the same
    payment after a redelivery, so receivers should deduplicate on `payment_id`.
    """

    def __init__(
        self,
        settings: Settings | None = None,
        http_client: httpx.AsyncClient | None = None,
        sleep_func: SleepFunc = asyncio.sleep,
        *,
        raise_on_failure: bool = True,
    ) -> None:
        self._settings = settings or get_settings()
        self._http_client = http_client
        self._sleep_func = sleep_func
        self._raise_on_failure = raise_on_failure

    @staticmethod
    def build_webhook_payload(payment: Payment) -> dict[str, Any]:
        """Serialize a processed `Payment` into the outbound webhook JSON body."""
        return {
            "payment_id": str(payment.id),
            "status": payment.status.value,
            "amount": str(payment.amount),
            "currency": payment.currency.value,
            "description": payment.description,
            "metadata": payment.payment_metadata,
            "idempotency_key": payment.idempotency_key,
            "created_at": payment.created_at.isoformat() if payment.created_at else None,
            "processed_at": payment.processed_at.isoformat() if payment.processed_at else None,
        }

    def compute_backoff_seconds(self, attempt: int) -> float:
        """Delay after failed attempt `attempt`: `base * 2^(attempt - 1)`."""
        return self._settings.webhook_retry_base_delay_seconds * (2 ** (attempt - 1))

    async def send_payment_webhook(self, payment: Payment) -> bool:
        """POST the payment result to `payment.webhook_url`.

        Returns `True` on a 2xx response. If every attempt fails, or the error
        is not retryable, returns `False` (or raises `WebhookDeliveryError`
        when `raise_on_failure=True`).
        """
        payload = self.build_webhook_payload(payment)

        if self._http_client is not None:
            return await self._deliver_with_retries(self._http_client, payment, payload)

        async with httpx.AsyncClient(timeout=self._settings.webhook_timeout_seconds) as client:
            return await self._deliver_with_retries(client, payment, payload)

    async def _deliver_with_retries(
        self,
        client: httpx.AsyncClient,
        payment: Payment,
        payload: dict[str, Any],
    ) -> bool:
        max_attempts = self._settings.webhook_max_retries
        log_ctx = {"payment_id": str(payment.id), "webhook_url": payment.webhook_url}
        last_error: Exception | None = None

        for attempt in range(1, max_attempts + 1):
            try:
                response = await client.post(payment.webhook_url, json=payload)
                response.raise_for_status()
            except (httpx.HTTPError, httpx.InvalidURL) as exc:
                last_error = exc
                retryable = is_retryable_webhook_error(exc)
                if not retryable or attempt == max_attempts:
                    logger.warning(
                        "Webhook delivery attempt failed; giving up",
                        extra={
                            **log_ctx,
                            "attempt": attempt,
                            "retryable": retryable,
                            "error": str(exc),
                        },
                    )
                    break

                backoff = self.compute_backoff_seconds(attempt)
                logger.warning(
                    "Webhook delivery attempt failed; retrying",
                    extra={
                        **log_ctx,
                        "attempt": attempt,
                        "max_attempts": max_attempts,
                        "backoff_seconds": backoff,
                        "error": str(exc),
                    },
                )
                await self._sleep_func(backoff)
            else:
                logger.info(
                    "Webhook delivered",
                    extra={**log_ctx, "status_code": response.status_code, "attempt": attempt},
                )
                return True

        logger.error(
            "Webhook delivery failed",
            extra={**log_ctx, "max_attempts": max_attempts, "error": str(last_error)},
        )
        if self._raise_on_failure:
            raise WebhookDeliveryError(
                f"Failed to deliver webhook for payment {payment.id}."
            ) from last_error
        return False
