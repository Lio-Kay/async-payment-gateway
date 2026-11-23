import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import Select, select

from src.models.enums import OutboxStatus
from src.models.outbox import OutboxEvent
from src.repositories.base import BaseRepository


class OutboxRepository(BaseRepository[OutboxEvent]):
    """Queries and status transitions for transactional outbox rows (`outbox` table)."""

    model = OutboxEvent

    async def add_event(
        self,
        *,
        aggregate_id: uuid.UUID,
        event_type: str,
        payload: dict[str, Any],
        event_id: uuid.UUID | None = None,
    ) -> OutboxEvent:
        """Stage and flush a pending outbox event in the caller's DB transaction."""
        event = OutboxEvent(
            id=event_id or uuid.uuid4(),
            aggregate_id=aggregate_id,
            event_type=event_type,
            payload=payload,
            status=OutboxStatus.PENDING,
            retry_count=0,
            created_at=datetime.now(UTC),
        )
        self._session.add(event)
        await self._session.flush()
        return event

    @staticmethod
    def build_fetch_unprocessed_stmt(batch_size: int = 50) -> Select[tuple[OutboxEvent]]:
        """Build the `SELECT ... FOR UPDATE SKIP LOCKED` query for pending events.

        - `FOR UPDATE` locks the selected rows until the relay's transaction ends.
        - `SKIP LOCKED` tells Postgres to skip rows currently locked by another
          relay worker instead of blocking, so multiple relay workers can poll in
          parallel without publishing duplicate messages.
        - Backed by the composite index `ix_outbox_status_created_at`.
        """
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1.")

        return (
            select(OutboxEvent)
            .where(OutboxEvent.status == OutboxStatus.PENDING)
            .order_by(OutboxEvent.created_at.asc())
            .limit(batch_size)
            .with_for_update(skip_locked=True)
        )

    async def fetch_unprocessed_with_lock(self, batch_size: int = 50) -> list[OutboxEvent]:
        """Claim up to `batch_size` pending outbox events with row-level locks."""
        stmt = self.build_fetch_unprocessed_stmt(batch_size=batch_size)
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def mark_processed(
        self,
        event_id: uuid.UUID,
        processed_at: datetime | None = None,
    ) -> OutboxEvent | None:
        """Mark an outbox event as `published` once RabbitMQ accepts the message."""
        stmt = select(OutboxEvent).where(OutboxEvent.id == event_id)
        result = await self._session.execute(stmt)
        event = result.scalar_one_or_none()
        if event is None:
            return None

        event.status = OutboxStatus.PUBLISHED
        event.processed_at = processed_at or datetime.now(UTC)
        await self._session.flush()
        return event

    async def increment_retry(
        self,
        event_id: uuid.UUID,
        max_retries: int = 5,
    ) -> OutboxEvent | None:
        """Increment `retry_count` after a failed broker publish.

        When `retry_count` reaches `max_retries`, the row is moved to `failed`
        so a poison event cannot stall the outbox queue forever.
        """
        if max_retries < 1:
            raise ValueError("max_retries must be >= 1.")

        stmt = select(OutboxEvent).where(OutboxEvent.id == event_id)
        result = await self._session.execute(stmt)
        event = result.scalar_one_or_none()
        if event is None:
            return None

        event.retry_count += 1
        if event.retry_count >= max_retries:
            event.status = OutboxStatus.FAILED
            event.processed_at = datetime.now(UTC)

        await self._session.flush()
        return event
