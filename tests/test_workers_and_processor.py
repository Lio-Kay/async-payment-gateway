import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from faststream.rabbit import RabbitMessage, TestRabbitBroker
from sqlalchemy.ext.asyncio import AsyncSession

import src.workers.consumer as consumer_module
from src.core.config import Settings
from src.models.enums import Currency, OutboxStatus, PaymentStatus
from src.models.outbox import OutboxEvent
from src.models.payment import Payment
from src.services.payment import PaymentNotFoundError
from src.services.processor import PaymentProcessorService
from src.services.webhook import WebhookDeliveryError, WebhookService
from src.workers.consumer import (
    broker,
    compute_consumer_backoff,
    declare_dlq_topology,
    dlq_queue,
    dlx_exchange,
    payments_exchange,
    payments_queue,
    process_message_with_retry,
)
from src.workers.outbox_relay import OutboxRelayWorker


def _settings(**overrides: Any) -> Settings:
    return Settings(_env_file=None, **overrides)  # type: ignore[call-arg]


def _sample_payment(status: PaymentStatus = PaymentStatus.PENDING) -> Payment:
    return Payment(
        id=uuid.uuid4(),
        amount=Decimal("99.50"),
        currency=Currency.USD,
        description="Test payment",
        payment_metadata={"order_id": "ord-9"},
        status=status,
        idempotency_key="idem-9",
        webhook_url="https://merchant.example/webhook",
        created_at=datetime(2026, 11, 22, 12, 0, tzinfo=UTC),
        processed_at=None
        if status == PaymentStatus.PENDING
        else datetime(2026, 11, 22, 12, 0, 3, tzinfo=UTC),
    )


class _SleepRecorder:
    def __init__(self) -> None:
        self.delays: list[float] = []

    async def __call__(self, delay: float) -> None:
        self.delays.append(delay)


def _session_with_begin() -> AsyncMock:
    session = AsyncMock(spec=AsyncSession)

    @asynccontextmanager
    async def fake_begin() -> Any:
        yield

    session.begin = MagicMock(side_effect=fake_begin)
    return session


@asynccontextmanager
async def _fake_session_factory() -> Any:
    yield AsyncMock(spec=AsyncSession)


# --- WebhookService ---------------------------------------------------------


async def _send_with(
    handler: Any, *, raise_on_failure: bool = False
) -> tuple[bool, int, list[float]]:
    calls = 0

    def counting_handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return handler(calls)

    sleep = _SleepRecorder()
    async with httpx.AsyncClient(transport=httpx.MockTransport(counting_handler)) as client:
        service = WebhookService(
            settings=_settings(webhook_max_retries=3, webhook_retry_base_delay_seconds=1.0),
            http_client=client,
            sleep_func=sleep,
            raise_on_failure=raise_on_failure,
        )
        delivered = await service.send_payment_webhook(_sample_payment(PaymentStatus.SUCCEEDED))
    return delivered, calls, sleep.delays


async def test_webhook_succeeds_on_third_attempt_with_backoff_between_attempts() -> None:
    delivered, calls, delays = await _send_with(
        lambda n: httpx.Response(503) if n < 3 else httpx.Response(200)
    )
    assert delivered is True
    assert calls == 3
    assert delays == [1.0, 2.0]


async def test_webhook_gives_up_after_three_attempts_without_trailing_sleep() -> None:
    delivered, calls, delays = await _send_with(lambda _n: httpx.Response(500))
    assert delivered is False
    assert calls == 3
    # No sleep after the final attempt.
    assert delays == [1.0, 2.0]

    with pytest.raises(WebhookDeliveryError):
        await _send_with(lambda _n: httpx.Response(500), raise_on_failure=True)


async def test_webhook_does_not_retry_permanent_client_errors() -> None:
    delivered, calls, delays = await _send_with(lambda _n: httpx.Response(400))
    assert delivered is False
    assert calls == 1
    assert delays == []


async def test_webhook_retries_rate_limited_responses() -> None:
    delivered, calls, _ = await _send_with(
        lambda n: httpx.Response(429) if n == 1 else httpx.Response(204)
    )
    assert delivered is True
    assert calls == 2


async def test_webhook_retries_network_errors() -> None:
    def handler(n: int) -> httpx.Response:
        if n == 1:
            raise httpx.ConnectError("connection refused")
        return httpx.Response(200)

    delivered, calls, delays = await _send_with(handler)
    assert delivered is True
    assert calls == 2
    assert delays == [1.0]


# --- PaymentProcessorService -----------------------------------------------


def _processor(
    payment: Payment | None,
    *,
    roll: float,
    sleep: _SleepRecorder | None = None,
) -> tuple[PaymentProcessorService, AsyncMock]:
    webhook = AsyncMock(spec=WebhookService)
    processor = PaymentProcessorService(
        session=_session_with_begin(),
        webhook_service=webhook,
        settings=_settings(),
        sleep_func=sleep or _SleepRecorder(),
        uniform_func=lambda _a, _b: 3.25,
        random_func=lambda: roll,
    )
    processor._payment_repo.get_by_id = AsyncMock(return_value=payment)  # type: ignore[method-assign]

    async def fake_update_status(
        payment_id: uuid.UUID, status: PaymentStatus, processed_at: datetime | None = None
    ) -> Payment | None:
        assert payment is not None
        payment.status = status
        payment.processed_at = processed_at
        return payment

    processor._payment_repo.update_status = AsyncMock(side_effect=fake_update_status)  # type: ignore[method-assign]
    return processor, webhook


async def test_processor_succeeds_updates_status_and_sends_webhook() -> None:
    payment = _sample_payment()
    sleep = _SleepRecorder()
    processor, webhook = _processor(payment, roll=0.45, sleep=sleep)

    result = await processor.process_payment(payment.id)

    assert sleep.delays == [3.25]  # asyncio.sleep(random.uniform(2, 5))
    assert result.status == PaymentStatus.SUCCEEDED
    assert result.processed_at is not None
    webhook.send_payment_webhook.assert_awaited_once_with(payment)


async def test_processor_failure_roll_marks_payment_failed() -> None:
    payment = _sample_payment()
    processor, webhook = _processor(payment, roll=0.95)

    result = await processor.process_payment(payment.id)

    assert result.status == PaymentStatus.FAILED
    webhook.send_payment_webhook.assert_awaited_once_with(payment)


async def test_processor_success_rate_boundary_is_ninety_percent() -> None:
    processor, _ = _processor(_sample_payment(), roll=0.0)
    processor._random_func = lambda: 0.8999
    assert processor._choose_terminal_status() == PaymentStatus.SUCCEEDED
    processor._random_func = lambda: 0.9
    assert processor._choose_terminal_status() == PaymentStatus.FAILED


async def test_processor_redelivery_of_finalized_payment_skips_gateway_but_resends_webhook() -> (
    None
):
    payment = _sample_payment(status=PaymentStatus.SUCCEEDED)
    sleep = _SleepRecorder()
    processor, webhook = _processor(payment, roll=0.95, sleep=sleep)

    result = await processor.process_payment(payment.id)

    assert result.status == PaymentStatus.SUCCEEDED  # not overwritten by the 0.95 "failed" roll
    assert sleep.delays == []
    processor._payment_repo.update_status.assert_not_awaited()
    webhook.send_payment_webhook.assert_awaited_once_with(payment)


async def test_processor_losing_concurrent_race_does_not_overwrite_or_send_webhook() -> None:
    pending = _sample_payment()
    winner_view = _sample_payment(status=PaymentStatus.SUCCEEDED)
    winner_view.id = pending.id
    processor, webhook = _processor(pending, roll=0.95)

    # First load: pending. Conditional UPDATE matches 0 rows. Reload: winner's result.
    processor._payment_repo.get_by_id = AsyncMock(side_effect=[pending, winner_view])  # type: ignore[method-assign]
    processor._payment_repo.update_status = AsyncMock(return_value=None)  # type: ignore[method-assign]

    result = await processor.process_payment(pending.id)

    assert result.status == PaymentStatus.SUCCEEDED
    webhook.send_payment_webhook.assert_not_awaited()


async def test_processor_missing_payment_raises() -> None:
    processor, _ = _processor(None, roll=0.1)
    with pytest.raises(PaymentNotFoundError):
        await processor.process_payment(uuid.uuid4())


# --- OutboxRelayWorker -----------------------------------------------------


async def test_outbox_relay_publishes_marks_processed_and_retries_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def make_event() -> OutboxEvent:
        return OutboxEvent(
            id=uuid.uuid4(),
            aggregate_id=uuid.uuid4(),
            event_type="payment.created",
            payload={"payment_id": str(uuid.uuid4())},
            status=OutboxStatus.PENDING,
            retry_count=0,
        )

    event_ok, event_fail = make_event(), make_event()
    marked: list[uuid.UUID] = []
    retried: list[tuple[uuid.UUID, int]] = []
    fetch_batch_sizes: list[int] = []

    class FakeOutboxRepo:
        def __init__(self, _session: Any) -> None:
            pass

        async def fetch_unprocessed_with_lock(self, batch_size: int = 50) -> list[OutboxEvent]:
            fetch_batch_sizes.append(batch_size)
            return [event_ok, event_fail]

        async def mark_processed(self, event_id: uuid.UUID) -> None:
            marked.append(event_id)

        async def increment_retry(self, event_id: uuid.UUID, max_retries: int = 5) -> None:
            retried.append((event_id, max_retries))

    monkeypatch.setattr("src.workers.outbox_relay.OutboxRepository", FakeOutboxRepo)

    @asynccontextmanager
    async def fake_session_factory() -> Any:
        yield _session_with_begin()

    publisher = AsyncMock()
    publisher.publish.side_effect = [None, RuntimeError("broker disconnected")]
    settings = _settings(
        outbox_poll_interval_seconds=0.01, outbox_batch_size=7, outbox_max_retries=4
    )

    relay = OutboxRelayWorker(
        publisher=publisher,
        session_factory=fake_session_factory,  # type: ignore[arg-type]
        settings=settings,
    )
    assert await relay.process_batch() == 1
    assert fetch_batch_sizes == [7]
    assert marked == [event_ok.id]
    assert retried == [(event_fail.id, 4)]

    first_call = publisher.publish.await_args_list[0]
    assert first_call.args[0] == event_ok.payload
    assert first_call.kwargs["exchange"] == "payments.new"
    assert first_call.kwargs["routing_key"] == "payments.new"
    assert first_call.kwargs["persist"] is True
    assert first_call.kwargs["message_id"] == str(event_ok.id)

    publisher.publish.side_effect = None
    task = relay.start()
    assert not task.done()
    await relay.stop()
    assert task.done()


async def test_outbox_relay_run_forever_recovers_from_polling_error() -> None:
    relay = OutboxRelayWorker(
        publisher=AsyncMock(),
        settings=_settings(outbox_poll_interval_seconds=0.01),
    )
    calls = 0

    async def flaky_batch() -> int:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("transient db disconnect")
        await relay.stop()
        return 0

    relay.process_batch = flaky_batch  # type: ignore[method-assign]
    await relay.run_forever()
    assert calls == 2


# --- RabbitMQ topology & consumer -------------------------------------------


def test_topology_names_and_dead_letter_arguments() -> None:
    assert payments_exchange.name == "payments.new"
    assert payments_queue.name == "payments.new"
    assert dlx_exchange.name == "payments.dlx"
    assert dlq_queue.name == "payments.dlq"
    assert payments_queue.arguments is not None
    assert payments_queue.arguments["x-dead-letter-exchange"] == "payments.dlx"
    assert payments_queue.arguments["x-dead-letter-routing-key"] == "payments.dlq"
    assert [compute_consumer_backoff(i, base_delay=2.0) for i in (1, 2, 3)] == [2.0, 4.0, 8.0]


async def test_declare_dlq_topology_declares_and_binds() -> None:
    mock_broker, mock_dlx, mock_dlq = AsyncMock(), AsyncMock(), AsyncMock()
    mock_broker.declare_exchange.return_value = mock_dlx
    mock_broker.declare_queue.return_value = mock_dlq

    await declare_dlq_topology(mock_broker)

    mock_broker.declare_exchange.assert_awaited_once_with(dlx_exchange)
    mock_broker.declare_queue.assert_awaited_once_with(dlq_queue)
    mock_dlq.bind.assert_awaited_once_with(mock_dlx, routing_key="payments.dlq")


async def test_dlq_topology_is_declared_after_broker_connects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # on_startup runs before the broker connects; declaring there raises IncorrectState.
    declare = AsyncMock()
    relay_instance = MagicMock()
    monkeypatch.setattr(consumer_module, "declare_dlq_topology", declare)
    monkeypatch.setattr(
        consumer_module, "OutboxRelayWorker", MagicMock(return_value=relay_instance)
    )
    monkeypatch.setattr(consumer_module, "setup_logging", MagicMock())

    await consumer_module.on_startup()
    declare.assert_not_awaited()

    await consumer_module.after_startup()
    declare.assert_awaited_once_with(consumer_module.broker)
    relay_instance.start.assert_called_once()

    dispose = AsyncMock()
    relay_instance.stop = AsyncMock()
    monkeypatch.setattr(consumer_module, "dispose_engine", dispose)
    await consumer_module.on_shutdown()
    relay_instance.stop.assert_awaited_once()
    dispose.assert_awaited_once()
    assert consumer_module._relay_worker is None


async def _run_consumer(
    processor: AsyncMock, payload: dict[str, Any]
) -> tuple[AsyncMock, list[float]]:
    sleep = _SleepRecorder()
    msg = AsyncMock(spec=RabbitMessage)
    await process_message_with_retry(
        payload,
        msg,
        session_factory=_fake_session_factory,  # type: ignore[arg-type]
        processor_factory=lambda _s: processor,
        settings=_settings(consumer_max_retries=3, consumer_retry_base_delay_seconds=2.0),
        sleep_func=sleep,
    )
    return msg, sleep.delays


async def test_consumer_acks_after_transient_failure() -> None:
    processor = AsyncMock(spec=PaymentProcessorService)
    processor.process_payment.side_effect = [RuntimeError("db blip"), _sample_payment()]

    msg, delays = await _run_consumer(processor, {"payment_id": str(uuid.uuid4())})

    assert delays == [2.0]
    msg.ack.assert_awaited_once()
    msg.reject.assert_not_awaited()


async def test_consumer_retries_at_2_4_8_seconds_then_dead_letters() -> None:
    processor = AsyncMock(spec=PaymentProcessorService)
    processor.process_payment.side_effect = RuntimeError("persistent failure")

    msg, delays = await _run_consumer(processor, {"payment_id": str(uuid.uuid4())})

    # 3 attempts with exponential backoff (2s, 4s, 8s), then rejected to payments.dlq.
    assert processor.process_payment.await_count == 3
    assert delays == [2.0, 4.0, 8.0]
    msg.ack.assert_not_awaited()
    msg.reject.assert_awaited_once_with(requeue=False)


@pytest.mark.parametrize(
    "payload",
    [{}, {"payment_id": "not-a-uuid"}, {"payment_id": None}],
)
async def test_consumer_sends_malformed_messages_straight_to_dlq(payload: dict[str, Any]) -> None:
    processor = AsyncMock(spec=PaymentProcessorService)

    msg, delays = await _run_consumer(processor, payload)

    processor.process_payment.assert_not_awaited()
    assert delays == []
    msg.reject.assert_awaited_once_with(requeue=False)


async def test_consumer_does_not_retry_unknown_payment() -> None:
    processor = AsyncMock(spec=PaymentProcessorService)
    processor.process_payment.side_effect = PaymentNotFoundError(uuid.uuid4())

    msg, delays = await _run_consumer(processor, {"payment_id": str(uuid.uuid4())})

    assert processor.process_payment.await_count == 1
    assert delays == []
    msg.reject.assert_awaited_once_with(requeue=False)


async def test_consumer_ack_failure_does_not_reprocess_payment() -> None:
    processor = AsyncMock(spec=PaymentProcessorService)
    msg = AsyncMock(spec=RabbitMessage)
    msg.ack.side_effect = RuntimeError("channel closed")

    with pytest.raises(RuntimeError, match="channel closed"):
        await process_message_with_retry(
            {"payment_id": str(uuid.uuid4())},
            msg,
            session_factory=_fake_session_factory,  # type: ignore[arg-type]
            processor_factory=lambda _s: processor,
            settings=_settings(),
            sleep_func=_SleepRecorder(),
        )
    assert processor.process_payment.await_count == 1


async def test_faststream_subscriber_receives_from_payments_new(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    received: list[uuid.UUID] = []

    async def fake_process(payload: dict[str, Any], msg: RabbitMessage) -> None:
        received.append(uuid.UUID(payload["payment_id"]))
        await msg.ack()

    monkeypatch.setattr(consumer_module, "process_message_with_retry", fake_process)

    target_id = uuid.uuid4()
    async with TestRabbitBroker(broker) as test_broker:
        await test_broker.publish(
            {"payment_id": str(target_id)},
            queue=payments_queue,
            exchange=payments_exchange,
        )

    assert received == [target_id]
