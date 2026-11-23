import asyncio
import contextlib
import logging
from typing import Protocol

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.core.config import Settings, get_settings
from src.core.database import async_session_factory
from src.repositories.outbox import OutboxRepository

logger = logging.getLogger(__name__)


class MessagePublisher(Protocol):
    """Subset of FastStream's `RabbitBroker` interface used by the outbox relay."""

    async def publish(
        self,
        message: object,
        queue: str = "",
        exchange: str | None = None,
        *,
        routing_key: str = "",
        persist: bool = False,
        message_id: str | None = None,
    ) -> object: ...


class OutboxRelayWorker:
    """Background worker that forwards pending `outbox` rows to RabbitMQ (`payments.new`).

    On each tick:
    1. Opens a transaction and claims up to `outbox_batch_size` pending rows with
       `SELECT ... FOR UPDATE SKIP LOCKED`.
    2. Publishes each event payload to the `payments.new` exchange with
       `persist=True` (AMQP delivery_mode=2) and `message_id=str(event.id)`.
    3. Marks published rows as `published`. If a publish call fails, increments
       `retry_count` on that row (moving it to `failed` after `outbox_max_retries`)
       without rolling back the rest of the batch.
    """

    def __init__(
        self,
        publisher: MessagePublisher,
        session_factory: async_sessionmaker[AsyncSession] = async_session_factory,
        settings: Settings | None = None,
    ) -> None:
        self._publisher = publisher
        self._session_factory = session_factory
        self._settings = settings or get_settings()
        self._stop_event = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    async def process_batch(self) -> int:
        """Lock and publish one batch of pending outbox events; return count published."""
        published_count = 0

        async with self._session_factory() as session, session.begin():
            outbox_repo = OutboxRepository(session)
            events = await outbox_repo.fetch_unprocessed_with_lock(
                batch_size=self._settings.outbox_batch_size,
            )

            for event in events:
                try:
                    await self._publisher.publish(
                        event.payload,
                        queue=self._settings.rabbitmq_queue,
                        exchange=self._settings.rabbitmq_exchange,
                        routing_key=self._settings.rabbitmq_queue,
                        persist=True,
                        message_id=str(event.id),
                    )
                    await outbox_repo.mark_processed(event.id)
                    published_count += 1
                    logger.info(
                        "Outbox event published to RabbitMQ",
                        extra={
                            "event_id": str(event.id),
                            "aggregate_id": str(event.aggregate_id),
                            "event_type": event.event_type,
                            "queue": self._settings.rabbitmq_queue,
                        },
                    )
                except Exception as exc:
                    # Isolate per-event publish errors so already-published events
                    # in the current batch still commit their `published` status.
                    await outbox_repo.increment_retry(
                        event.id,
                        max_retries=self._settings.outbox_max_retries,
                    )
                    logger.error(
                        "Failed to publish outbox event",
                        extra={
                            "event_id": str(event.id),
                            "aggregate_id": str(event.aggregate_id),
                            "retry_count": event.retry_count,
                            "error": str(exc),
                        },
                    )

        return published_count

    async def run_forever(self) -> None:
        """Poll `outbox` at `outbox_poll_interval_seconds` until `stop()` sets the stop event."""
        logger.info(
            "OutboxRelayWorker started",
            extra={
                "poll_interval_seconds": self._settings.outbox_poll_interval_seconds,
                "batch_size": self._settings.outbox_batch_size,
            },
        )
        while not self._stop_event.is_set():
            try:
                await self.process_batch()
            except Exception as exc:
                logger.exception(
                    "OutboxRelayWorker polling cycle failed",
                    extra={"error": str(exc)},
                )

            # Waiting on `_stop_event` with a timeout (instead of `asyncio.sleep`)
            # lets `stop()` wake the loop immediately during container shutdown.
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=self._settings.outbox_poll_interval_seconds,
                )

        logger.info("OutboxRelayWorker stopped")

    def start(self) -> asyncio.Task[None]:
        if self._task is None or self._task.done():
            self._stop_event.clear()
            self._task = asyncio.create_task(self.run_forever(), name="outbox-relay-worker")
        return self._task

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task is not None:
            await self._task
            self._task = None
