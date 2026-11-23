import uuid
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.enums import Currency, OutboxStatus, PaymentStatus
from src.models.outbox import OutboxEvent
from src.models.payment import Payment
from src.repositories import BaseRepository, OutboxRepository, PaymentRepository


def _make_mock_session() -> AsyncMock:
    session = AsyncMock(spec=AsyncSession)
    session.add = MagicMock()
    return session


async def test_payment_repository_create_and_queries() -> None:
    session = _make_mock_session()
    repo = PaymentRepository(session)
    assert isinstance(repo, BaseRepository)
    assert repo.session is session

    payment = await repo.create(
        amount=Decimal("250.00"),
        currency=Currency.EUR,
        description="Subscription renewal",
        payment_metadata={"plan": "pro"},
        idempotency_key="idem-eur-1",
        webhook_url="https://merchant.example/webhook",
    )

    session.add.assert_called_once_with(payment)
    session.flush.assert_awaited_once()
    assert payment.status == PaymentStatus.PENDING
    assert payment.amount == Decimal("250.00")
    assert payment.currency == Currency.EUR
    assert payment.payment_metadata == {"plan": "pro"}
    assert payment.created_at is not None
    assert payment.processed_at is None

    # Mock get_by_id returning the created payment
    scalar_result = MagicMock()
    scalar_result.scalar_one_or_none.return_value = payment
    session.execute.return_value = scalar_result

    found_by_id = await repo.get_by_id(payment.id)
    assert found_by_id is payment

    found_by_key = await repo.get_by_idempotency_key("idem-eur-1")
    assert found_by_key is payment


async def test_payment_repository_update_status_is_conditional_on_pending() -> None:
    session = _make_mock_session()
    repo = PaymentRepository(session)
    payment_id = uuid.uuid4()
    finalized = Payment(id=payment_id, status=PaymentStatus.SUCCEEDED)

    exec_result = MagicMock()
    exec_result.scalar_one_or_none.return_value = finalized
    session.execute.return_value = exec_result

    fixed_time = datetime(2026, 11, 22, 22, 0, tzinfo=UTC)
    updated = await repo.update_status(payment_id, PaymentStatus.SUCCEEDED, processed_at=fixed_time)
    assert updated is finalized

    # Single UPDATE ... WHERE id = ? AND status = 'pending' RETURNING (no read-modify-write).
    stmt = session.execute.await_args.args[0]
    sql = str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
    assert sql.startswith("UPDATE payments SET")
    assert f"payments.id = '{payment_id}'" in sql
    assert "payments.status = 'pending'" in sql
    assert "RETURNING" in sql

    # Zero matched rows (missing or already finalized) -> None
    exec_result.scalar_one_or_none.return_value = None
    assert await repo.update_status(uuid.uuid4(), PaymentStatus.FAILED) is None

    # only_if_pending=False drops the status guard
    await repo.update_status(payment_id, PaymentStatus.FAILED, only_if_pending=False)
    stmt = session.execute.await_args.args[0]
    sql = str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
    assert "payments.status = 'pending'" not in sql

    # Transitioning back to PENDING is rejected
    with pytest.raises(ValueError, match="pending"):
        await repo.update_status(payment_id, PaymentStatus.PENDING)


def test_outbox_repository_for_update_skip_locked_sql() -> None:
    stmt = OutboxRepository.build_fetch_unprocessed_stmt(batch_size=25)
    compiled_sql = str(
        stmt.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )

    assert "WHERE outbox.status = 'pending'" in compiled_sql
    assert "ORDER BY outbox.created_at ASC" in compiled_sql
    assert "LIMIT 25" in compiled_sql
    assert "FOR UPDATE SKIP LOCKED" in compiled_sql

    with pytest.raises(ValueError, match="batch_size"):
        OutboxRepository.build_fetch_unprocessed_stmt(batch_size=0)


async def test_outbox_repository_add_fetch_and_mark_processed() -> None:
    session = _make_mock_session()
    repo = OutboxRepository(session)

    payment_id = uuid.uuid4()
    event = await repo.add_event(
        aggregate_id=payment_id,
        event_type="payment.created",
        payload={"payment_id": str(payment_id)},
    )
    session.add.assert_called_once_with(event)
    session.flush.assert_awaited_once()
    assert event.status == OutboxStatus.PENDING
    assert event.retry_count == 0

    # Mock fetch_unprocessed_with_lock
    scalars_mock = MagicMock()
    scalars_mock.all.return_value = [event]
    exec_result = MagicMock()
    exec_result.scalars.return_value = scalars_mock
    session.execute.return_value = exec_result

    batch = await repo.fetch_unprocessed_with_lock(batch_size=10)
    assert batch == [event]

    # Mock mark_processed
    exec_result.scalar_one_or_none.return_value = event
    processed_event = await repo.mark_processed(event.id)
    assert processed_event is event
    assert event.status == OutboxStatus.PUBLISHED
    assert event.processed_at is not None

    # Missing event returns None
    exec_result.scalar_one_or_none.return_value = None
    assert await repo.mark_processed(uuid.uuid4()) is None


async def test_outbox_repository_increment_retry_and_exhaustion() -> None:
    session = _make_mock_session()
    repo = OutboxRepository(session)

    event = OutboxEvent(
        id=uuid.uuid4(),
        aggregate_id=uuid.uuid4(),
        event_type="payment.created",
        payload={},
        status=OutboxStatus.PENDING,
        retry_count=0,
    )
    exec_result = MagicMock()
    exec_result.scalar_one_or_none.return_value = event
    session.execute.return_value = exec_result

    # Retry 1 of 2: stays PENDING
    await repo.increment_retry(event.id, max_retries=2)
    assert event.retry_count == 1
    assert event.status == OutboxStatus.PENDING
    assert event.processed_at is None

    # Retry 2 of 2: transitions to FAILED and records processed_at
    await repo.increment_retry(event.id, max_retries=2)
    assert event.retry_count == 2
    assert event.status == OutboxStatus.FAILED
    assert event.processed_at is not None

    # Missing event returns None
    exec_result.scalar_one_or_none.return_value = None
    assert await repo.increment_retry(uuid.uuid4(), max_retries=2) is None

    with pytest.raises(ValueError, match="max_retries"):
        await repo.increment_retry(uuid.uuid4(), max_retries=0)
