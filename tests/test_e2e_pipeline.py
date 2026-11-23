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

from src.api.dependencies import get_payment_service
from src.core.config import Settings, get_settings
from src.main import create_app
from src.models.enums import Currency, OutboxStatus, PaymentStatus
from src.models.outbox import OutboxEvent
from src.models.payment import Payment
from src.services.payment import PaymentService
from src.services.processor import PaymentProcessorService
from src.services.webhook import WebhookService
from src.workers.consumer import (
    broker,
    payments_exchange,
    payments_queue,
    process_message_with_retry,
)
from src.workers.outbox_relay import OutboxRelayWorker


class InMemoryGatewayStore:
    """Shared in-memory state connecting API, Outbox Relay, Consumer, and Webhook Receiver."""

    def __init__(self) -> None:
        self.payments_by_id: dict[uuid.UUID, Payment] = {}
        self.payments_by_key: dict[str, Payment] = {}
        self.outbox_events: dict[uuid.UUID, OutboxEvent] = {}
        self.received_webhooks: list[dict[str, Any]] = []

    def make_payment_service(self) -> PaymentService:
        session = AsyncMock(spec=AsyncSession)

        @asynccontextmanager
        async def fake_begin() -> Any:
            yield

        session.begin = MagicMock(side_effect=fake_begin)
        service = PaymentService(session)

        async def create_payment(
            *,
            amount: Decimal,
            currency: Currency,
            description: str,
            payment_metadata: dict[str, Any],
            idempotency_key: str,
            webhook_url: str,
            payment_id: uuid.UUID | None = None,
        ) -> Payment:
            pid = payment_id or uuid.uuid4()
            payment = Payment(
                id=pid,
                amount=amount,
                currency=currency,
                description=description,
                payment_metadata=payment_metadata,
                status=PaymentStatus.PENDING,
                idempotency_key=idempotency_key,
                webhook_url=webhook_url,
                created_at=datetime.now(UTC),
                processed_at=None,
            )
            self.payments_by_id[pid] = payment
            self.payments_by_key[idempotency_key] = payment
            return payment

        async def get_by_id(payment_id: uuid.UUID) -> Payment | None:
            return self.payments_by_id.get(payment_id)

        async def get_by_key(idempotency_key: str) -> Payment | None:
            return self.payments_by_key.get(idempotency_key)

        async def add_event(
            *,
            aggregate_id: uuid.UUID,
            event_type: str,
            payload: dict[str, Any],
            event_id: uuid.UUID | None = None,
        ) -> OutboxEvent:
            eid = event_id or uuid.uuid4()
            event = OutboxEvent(
                id=eid,
                aggregate_id=aggregate_id,
                event_type=event_type,
                payload=payload,
                status=OutboxStatus.PENDING,
                retry_count=0,
                created_at=datetime.now(UTC),
            )
            self.outbox_events[eid] = event
            return event

        service._payment_repo.create = AsyncMock(side_effect=create_payment)  # type: ignore[method-assign]
        service._payment_repo.get_by_id = AsyncMock(side_effect=get_by_id)  # type: ignore[method-assign]
        service._payment_repo.get_by_idempotency_key = AsyncMock(side_effect=get_by_key)  # type: ignore[method-assign]
        service._outbox_repo.add_event = AsyncMock(side_effect=add_event)  # type: ignore[method-assign]
        return service

    def make_outbox_repo_cls(self) -> type[Any]:
        store = self

        class BoundOutboxRepo:
            def __init__(self, _session: Any) -> None:
                pass

            async def fetch_unprocessed_with_lock(self, batch_size: int = 50) -> list[OutboxEvent]:
                pending = [
                    e for e in store.outbox_events.values() if e.status == OutboxStatus.PENDING
                ]
                return pending[:batch_size]

            async def mark_processed(self, event_id: uuid.UUID) -> OutboxEvent | None:
                event = store.outbox_events.get(event_id)
                if event is not None:
                    event.status = OutboxStatus.PUBLISHED
                    event.processed_at = datetime.now(UTC)
                return event

            async def increment_retry(
                self, event_id: uuid.UUID, max_retries: int = 5
            ) -> OutboxEvent | None:
                event = store.outbox_events.get(event_id)
                if event is not None:
                    event.retry_count += 1
                    if event.retry_count >= max_retries:
                        event.status = OutboxStatus.FAILED
                        event.processed_at = datetime.now(UTC)
                return event

        return BoundOutboxRepo

    def make_processor(
        self,
        *,
        webhook_client: httpx.AsyncClient,
        roll: float = 0.2,
        raise_on_webhook_failure: bool = True,
    ) -> PaymentProcessorService:
        session = AsyncMock(spec=AsyncSession)

        @asynccontextmanager
        async def fake_begin() -> Any:
            yield

        session.begin = MagicMock(side_effect=fake_begin)
        settings = Settings(_env_file=None)  # type: ignore[call-arg]
        webhook_service = WebhookService(
            settings=settings,
            http_client=webhook_client,
            sleep_func=AsyncMock(),
            raise_on_failure=raise_on_webhook_failure,
        )
        processor = PaymentProcessorService(
            session=session,
            webhook_service=webhook_service,
            settings=settings,
            sleep_func=AsyncMock(),
            uniform_func=lambda _a, _b: 2.5,
            random_func=lambda: roll,
        )

        async def get_by_id(payment_id: uuid.UUID) -> Payment | None:
            return self.payments_by_id.get(payment_id)

        async def update_status(
            payment_id: uuid.UUID,
            status: PaymentStatus,
            processed_at: datetime | None = None,
            *,
            only_if_pending: bool = True,
        ) -> Payment | None:
            payment = self.payments_by_id.get(payment_id)
            if payment is None:
                return None
            if only_if_pending and payment.status != PaymentStatus.PENDING:
                return None
            payment.status = status
            payment.processed_at = processed_at or datetime.now(UTC)
            return payment

        processor._payment_repo.get_by_id = AsyncMock(side_effect=get_by_id)  # type: ignore[method-assign]
        processor._payment_repo.update_status = AsyncMock(side_effect=update_status)  # type: ignore[method-assign]
        return processor


async def test_full_e2e_payment_flow_from_post_to_outbox_rabbit_and_webhook(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify full lifecycle: POST -> Outbox -> RabbitMQ Consumer -> Processor -> Webhook -> GET."""
    store = InMemoryGatewayStore()
    app = create_app()
    test_settings = Settings(_env_file=None, api_key="e2e-secret-key")  # type: ignore[call-arg]
    payment_service = store.make_payment_service()

    app.dependency_overrides[get_settings] = lambda: test_settings
    app.dependency_overrides[get_payment_service] = lambda: payment_service
    monkeypatch.setattr("src.workers.outbox_relay.OutboxRepository", store.make_outbox_repo_cls())

    @asynccontextmanager
    async def fake_session_factory() -> Any:
        session = AsyncMock(spec=AsyncSession)

        @asynccontextmanager
        async def fake_begin() -> Any:
            yield

        session.begin = MagicMock(side_effect=fake_begin)
        yield session

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        # Wire consumer to use our in-memory processor & ASGI client for webhook delivery
        async def e2e_consume(payload: dict[str, Any], msg: RabbitMessage) -> None:
            await process_message_with_retry(
                payload,
                msg,
                session_factory=fake_session_factory,  # type: ignore[arg-type]
                processor_factory=lambda _s: store.make_processor(
                    webhook_client=client,
                    roll=0.15,  # < 0.9 -> SUCCEEDED
                ),
                settings=test_settings,
                sleep_func=AsyncMock(),
            )

        monkeypatch.setattr("src.workers.consumer.process_message_with_retry", e2e_consume)

        # 1. Create payment via HTTP POST /api/v1/payments
        post_resp = await client.post(
            "/api/v1/payments",
            headers={
                "X-API-Key": "e2e-secret-key",
                "Idempotency-Key": "e2e-key-1",
            },
            json={
                "amount": "450.75",
                "currency": "EUR",
                "description": "E2E order #1",
                "metadata": {"user_id": 99},
                "webhook_url": "http://test/webhooks/mock",
            },
        )
        assert post_resp.status_code == 202
        created = post_resp.json()
        payment_id = created["payment_id"]
        assert created["status"] == "pending"

        # Confirm outbox event is pending in the store before the relay runs
        assert len(store.outbox_events) == 1
        outbox_event = next(iter(store.outbox_events.values()))
        assert outbox_event.status == OutboxStatus.PENDING

        # 2. Run OutboxRelayWorker batch against FastStream TestRabbitBroker
        async with TestRabbitBroker(broker) as test_broker:
            relay = OutboxRelayWorker(
                publisher=test_broker,
                session_factory=fake_session_factory,  # type: ignore[arg-type]
                settings=test_settings,
            )
            published_count = await relay.process_batch()
            assert published_count == 1

        # Outbox event is now marked PUBLISHED
        assert outbox_event.status == OutboxStatus.PUBLISHED
        assert outbox_event.processed_at is not None

        # 3. Query GET /api/v1/payments/{payment_id} -> status is now SUCCEEDED
        get_resp = await client.get(
            f"/api/v1/payments/{payment_id}",
            headers={"X-API-Key": "e2e-secret-key"},
        )
        assert get_resp.status_code == 200
        final_payment = get_resp.json()
        assert final_payment["id"] == payment_id
        assert final_payment["status"] == "succeeded"
        assert final_payment["amount"] == "450.75"
        assert final_payment["currency"] == "EUR"
        assert final_payment["metadata"] == {"user_id": 99}
        assert final_payment["processed_at"] is not None


async def test_e2e_webhook_failure_triggers_consumer_retries_and_routes_to_dlq() -> None:
    """When webhook delivery fails all attempts, consumer retries at 2s/4s/8s and routes to DLQ."""
    store = InMemoryGatewayStore()
    payment_service = store.make_payment_service()

    from src.api.schemas import PaymentCreateRequest

    payment = await payment_service.create_payment(
        PaymentCreateRequest(
            amount=Decimal("80.00"),
            currency=Currency.USD,
            description="DLQ test",
            metadata={"dlq": True},
            webhook_url="https://dead-merchant.example/webhook",  # type: ignore[arg-type]
        ),
        idempotency_key="dlq-idem-1",
    )

    @asynccontextmanager
    async def fake_session_factory() -> Any:
        yield AsyncMock(spec=AsyncSession)

    failing_transport = httpx.MockTransport(lambda _req: httpx.Response(502, text="Bad Gateway"))
    consumer_sleeps: list[float] = []

    async def record_consumer_sleep(delay: float) -> None:
        consumer_sleeps.append(delay)

    msg = AsyncMock(spec=RabbitMessage)

    async with httpx.AsyncClient(transport=failing_transport) as failing_client:
        await process_message_with_retry(
            {"payment_id": str(payment.id)},
            msg,
            session_factory=fake_session_factory,  # type: ignore[arg-type]
            processor_factory=lambda _s: store.make_processor(
                webhook_client=failing_client,
                roll=0.1,
                raise_on_webhook_failure=True,
            ),
            settings=Settings(
                _env_file=None,  # type: ignore[call-arg]
                consumer_max_retries=3,
                consumer_retry_base_delay_seconds=2.0,
            ),
            sleep_func=record_consumer_sleep,
        )

    # Payment status in DB was updated once to SUCCEEDED before webhook failure
    assert payment.status == PaymentStatus.SUCCEEDED
    assert payment.processed_at is not None

    # Consumer retried 3 times with 2s, 4s, 8s exponential backoff, then rejected to DLQ
    assert consumer_sleeps == [2.0, 4.0, 8.0]
    msg.ack.assert_not_awaited()
    msg.reject.assert_awaited_once_with(requeue=False)
    assert payments_exchange.name == "payments.new"
    assert payments_queue.arguments is not None
    assert payments_queue.arguments["x-dead-letter-exchange"] == "payments.dlx"
    assert payments_queue.arguments["x-dead-letter-routing-key"] == "payments.dlq"
