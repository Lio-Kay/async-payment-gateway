"""FastStream RabbitMQ consumer and its topology.

Run with: `faststream run src.workers.consumer:app`

Topology:

    publisher (OutboxRelayWorker)
        -> exchange `payments.new` (direct) -> queue `payments.new`
                                                   | reject(requeue=False)
                                                   v
           exchange `payments.dlx` (direct) -> queue `payments.dlq`

Dead-lettering is configured on the `payments.new` queue itself
(`x-dead-letter-exchange` / `x-dead-letter-routing-key`). When the consumer
rejects a message without requeue, RabbitMQ moves it to `payments.dlq`.
"""

import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

from faststream import AckPolicy, FastStream
from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitMessage, RabbitQueue
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.core.config import Settings, get_settings
from src.core.database import async_session_factory, dispose_engine
from src.core.logging import setup_logging
from src.services.payment import PaymentNotFoundError
from src.services.processor import PaymentProcessorService
from src.workers.outbox_relay import OutboxRelayWorker

logger = logging.getLogger(__name__)

SleepFunc = Callable[[float], Awaitable[None]]
ProcessorFactory = Callable[[AsyncSession], PaymentProcessorService]

_settings = get_settings()

payments_exchange = RabbitExchange(
    _settings.rabbitmq_exchange,
    type=ExchangeType.DIRECT,
    durable=True,
)
dlx_exchange = RabbitExchange(
    _settings.rabbitmq_dlx,
    type=ExchangeType.DIRECT,
    durable=True,
)
dlq_queue = RabbitQueue(
    _settings.rabbitmq_dlq,
    durable=True,
    routing_key=_settings.rabbitmq_dlq,
)
payments_queue = RabbitQueue(
    _settings.rabbitmq_queue,
    durable=True,
    routing_key=_settings.rabbitmq_queue,
    arguments={
        "x-dead-letter-exchange": _settings.rabbitmq_dlx,
        "x-dead-letter-routing-key": _settings.rabbitmq_dlq,
    },
)

broker = RabbitBroker(_settings.rabbitmq_url)


class NonRetryableMessageError(Exception):
    """Message can never succeed (bad payload, unknown payment); send it straight to the DLQ."""


def compute_consumer_backoff(attempt: int, base_delay: float | None = None) -> float:
    """Delay before retry number `attempt`: 2s, 4s, 8s for retries 1-3 when base=2.0."""
    base = (
        base_delay if base_delay is not None else get_settings().consumer_retry_base_delay_seconds
    )
    return base * (2 ** (attempt - 1))


def parse_payment_id(payload: Any) -> uuid.UUID:
    if not isinstance(payload, dict):
        raise NonRetryableMessageError(f"Expected JSON object payload, got: {payload!r}")
    try:
        return uuid.UUID(str(payload["payment_id"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise NonRetryableMessageError(f"Invalid message payload: {payload!r}") from exc


async def declare_dlq_topology(target_broker: RabbitBroker) -> None:
    """Declare `payments.dlx` and `payments.dlq`, and bind the queue to the exchange.

    The broker must already be connected, so call this from `after_startup`.
    RabbitMQ silently drops dead-lettered messages if the DLX doesn't exist,
    so this has to run before the consumer can reject anything.
    """
    declared_dlx = await target_broker.declare_exchange(dlx_exchange)
    declared_dlq = await target_broker.declare_queue(dlq_queue)
    await declared_dlq.bind(declared_dlx, routing_key=dlq_queue.routing_key)


async def process_message_with_retry(
    payload: dict[str, Any],
    msg: RabbitMessage,
    *,
    session_factory: async_sessionmaker[AsyncSession] = async_session_factory,
    processor_factory: ProcessorFactory | None = None,
    settings: Settings | None = None,
    sleep_func: SleepFunc = asyncio.sleep,
) -> None:
    """Process one `payments.new` message across up to 3 attempts with exponential backoff.

    Failed attempts wait 2s, 4s, and 8s (`base * 2^(attempt - 1)`). On success
    the message is acknowledged (`msg.ack()`). After 3 failed attempts, or on a
    non-retryable error (malformed payload, unknown payment), the message is
    rejected with `requeue=False` and routed by RabbitMQ through `payments.dlx`
    into `payments.dlq`.
    """
    cfg = settings or get_settings()
    build_processor = processor_factory or (
        lambda session: PaymentProcessorService(session=session, settings=cfg)
    )
    max_attempts = cfg.consumer_max_retries
    log_ctx: dict[str, Any] = {
        "payment_id": payload.get("payment_id") if isinstance(payload, dict) else None
    }

    try:
        payment_id = parse_payment_id(payload)
    except NonRetryableMessageError as exc:
        logger.error("Rejecting malformed message to DLQ", extra={**log_ctx, "error": str(exc)})
        await msg.reject(requeue=False)
        return

    for attempt in range(1, max_attempts + 1):
        try:
            async with session_factory() as session:
                await build_processor(session).process_payment(payment_id)
        except PaymentNotFoundError as exc:
            logger.error(
                "Payment not found; rejecting to DLQ without retries",
                extra={**log_ctx, "error": str(exc)},
            )
            break
        except Exception as exc:
            backoff = compute_consumer_backoff(attempt, cfg.consumer_retry_base_delay_seconds)
            logger.warning(
                "Payment processing attempt failed",
                extra={
                    **log_ctx,
                    "attempt": attempt,
                    "max_attempts": max_attempts,
                    "backoff_seconds": backoff,
                    "error": str(exc),
                },
            )
            await sleep_func(backoff)
        else:
            # Ack outside the try so a failed ack can't trigger a second processing run.
            await msg.ack()
            return

    logger.error(
        "Routing message to dead-letter queue",
        extra={**log_ctx, "dlx": cfg.rabbitmq_dlx, "dlq": cfg.rabbitmq_dlq},
    )
    await msg.reject(requeue=False)


@broker.subscriber(
    queue=payments_queue,
    exchange=payments_exchange,
    ack_policy=AckPolicy.MANUAL,
)
async def consume_payment_created(payload: dict[str, Any], msg: RabbitMessage) -> None:
    """The single consumer for `payments.new`."""
    await process_message_with_retry(payload, msg)


_relay_worker: OutboxRelayWorker | None = None
app = FastStream(broker)


@app.on_startup
async def on_startup() -> None:
    # Runs before the broker connects, so only set up things that don't need RabbitMQ.
    setup_logging()


@app.after_startup
async def after_startup() -> None:
    """Declare the DLQ topology, then start the outbox relay (the broker is connected by now)."""
    global _relay_worker
    await declare_dlq_topology(broker)
    _relay_worker = OutboxRelayWorker(publisher=broker)
    _relay_worker.start()


@app.on_shutdown
async def on_shutdown() -> None:
    global _relay_worker
    if _relay_worker is not None:
        await _relay_worker.stop()
        _relay_worker = None
    await dispose_engine()


if __name__ == "__main__":
    asyncio.run(app.run())
