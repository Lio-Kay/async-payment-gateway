import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.dependencies import get_payment_service
from src.api.schemas import PaymentCreateRequest
from src.core.config import Settings, get_settings
from src.main import create_app
from src.models.enums import Currency, OutboxStatus, PaymentStatus
from src.models.outbox import OutboxEvent
from src.models.payment import Payment
from src.services.payment import (
    PAYMENT_CREATED_EVENT_TYPE,
    IdempotencyConflictError,
    PaymentNotFoundError,
    PaymentService,
)


class _InMemoryBeginContext:
    """Async context manager simulating `async with session.begin():`."""

    def __init__(self, store: "InMemoryDbSession") -> None:
        self._store = store

    async def __aenter__(self) -> "_InMemoryBeginContext":
        self._store.begin_calls += 1
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        if exc_type is None:
            self._store.commit_calls += 1
        else:
            self._store.rollback_calls += 1
        return False


class InMemoryDbSession:
    """Lightweight fake session backing `PaymentRepository` and `OutboxRepository`."""

    def __init__(self) -> None:
        self.payments_by_id: dict[uuid.UUID, Payment] = {}
        self.payments_by_key: dict[str, Payment] = {}
        self.outbox_events: list[OutboxEvent] = []
        self.begin_calls = 0
        self.commit_calls = 0
        self.rollback_calls = 0

    def begin(self) -> _InMemoryBeginContext:
        return _InMemoryBeginContext(self)


def _wire_service_with_memory_store() -> tuple[PaymentService, InMemoryDbSession]:
    store = InMemoryDbSession()
    mock_session = AsyncMock(spec=AsyncSession)
    mock_session.begin = MagicMock(side_effect=store.begin)
    service = PaymentService(mock_session)

    async def _create_payment(
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
        store.payments_by_id[pid] = payment
        store.payments_by_key[idempotency_key] = payment
        return payment

    async def _get_by_id(payment_id: uuid.UUID) -> Payment | None:
        return store.payments_by_id.get(payment_id)

    async def _get_by_key(idempotency_key: str) -> Payment | None:
        return store.payments_by_key.get(idempotency_key)

    async def _add_event(
        *,
        aggregate_id: uuid.UUID,
        event_type: str,
        payload: dict[str, Any],
        event_id: uuid.UUID | None = None,
    ) -> OutboxEvent:
        event = OutboxEvent(
            id=event_id or uuid.uuid4(),
            aggregate_id=aggregate_id,
            event_type=event_type,
            payload=payload,
            status=OutboxStatus.PENDING,
            retry_count=0,
            created_at=datetime.now(UTC),
        )
        store.outbox_events.append(event)
        return event

    service._payment_repo.create = AsyncMock(side_effect=_create_payment)  # type: ignore[method-assign]
    service._payment_repo.get_by_id = AsyncMock(side_effect=_get_by_id)  # type: ignore[method-assign]
    service._payment_repo.get_by_idempotency_key = AsyncMock(side_effect=_get_by_key)  # type: ignore[method-assign]
    service._outbox_repo.add_event = AsyncMock(side_effect=_add_event)  # type: ignore[method-assign]
    return service, store


async def test_payment_service_atomic_outbox_and_idempotency() -> None:
    service, store = _wire_service_with_memory_store()
    req = PaymentCreateRequest(
        amount=Decimal("199.95"),
        currency=Currency.USD,
        description="Invoice #42",
        metadata={"customer_id": "cust-7"},
        webhook_url="https://example.com/webhooks/payments",  # type: ignore[arg-type]
    )

    created = await service.create_payment(req, idempotency_key="idem-42")
    assert store.commit_calls == 1
    assert len(store.outbox_events) == 1
    outbox = store.outbox_events[0]
    assert outbox.aggregate_id == created.id
    assert outbox.event_type == PAYMENT_CREATED_EVENT_TYPE
    assert outbox.payload["payment_id"] == str(created.id)
    assert outbox.payload["metadata"] == {"customer_id": "cust-7"}

    # Replaying the exact same Idempotency-Key + payload returns the existing payment
    # without inserting another Payment or OutboxEvent (even if amount is "199.95" vs 199.95).
    replayed = await service.create_payment(req, idempotency_key="idem-42")
    assert replayed.id == created.id
    assert len(store.outbox_events) == 1

    # Reusing the same Idempotency-Key with any modified field raises IdempotencyConflictError
    mismatched_variants = [
        req.model_copy(update={"amount": Decimal("200.00")}),
        req.model_copy(update={"currency": Currency.EUR}),
        req.model_copy(update={"description": "Different description"}),
        req.model_copy(update={"metadata": {"customer_id": "other"}}),
        PaymentCreateRequest(
            amount=Decimal("199.95"),
            currency=Currency.USD,
            description="Invoice #42",
            metadata={"customer_id": "cust-7"},
            webhook_url="https://example.com/webhooks/different",  # type: ignore[arg-type]
        ),
    ]
    for variant in mismatched_variants:
        try:
            await service.create_payment(variant, idempotency_key="idem-42")
            raise AssertionError(f"Expected IdempotencyConflictError for {variant}")
        except IdempotencyConflictError:
            pass


async def test_payment_service_handles_concurrent_integrity_error() -> None:
    service, store = _wire_service_with_memory_store()
    req = PaymentCreateRequest(
        amount=Decimal("50.00"),
        currency=Currency.RUB,
        description="Concurrent order",
        metadata={"attempt": 1},
        webhook_url="https://example.com/hook",  # type: ignore[arg-type]
    )

    winner = Payment(
        id=uuid.uuid4(),
        amount=Decimal("50.00"),
        currency=Currency.RUB,
        description="Concurrent order",
        payment_metadata={"attempt": 1},
        status=PaymentStatus.PENDING,
        idempotency_key="race-key",
        webhook_url="https://example.com/hook",
        created_at=datetime.now(UTC),
    )

    # First lookup sees None; create raises IntegrityError; fallback lookup sees winner.
    service._payment_repo.get_by_idempotency_key = AsyncMock(  # type: ignore[method-assign]
        side_effect=[None, winner]
    )
    service._payment_repo.create = AsyncMock(  # type: ignore[method-assign]
        side_effect=IntegrityError("INSERT", {}, Exception("duplicate key"))
    )

    resolved = await service.create_payment(req, idempotency_key="race-key")
    assert resolved.id == winner.id
    assert store.rollback_calls == 1
    assert store.commit_calls == 1

    # If the concurrent winner had a different payload, raise IdempotencyConflictError
    mismatched_winner = Payment(
        id=uuid.uuid4(),
        amount=Decimal("999.00"),
        currency=Currency.RUB,
        description="Concurrent order",
        payment_metadata={"attempt": 1},
        status=PaymentStatus.PENDING,
        idempotency_key="race-key-conflict",
        webhook_url="https://example.com/hook",
        created_at=datetime.now(UTC),
    )
    service._payment_repo.get_by_idempotency_key = AsyncMock(  # type: ignore[method-assign]
        side_effect=[None, mismatched_winner]
    )
    try:
        await service.create_payment(req, idempotency_key="race-key-conflict")
        raise AssertionError("Expected IdempotencyConflictError on concurrent mismatch")
    except IdempotencyConflictError:
        pass

    # If IntegrityError was unrelated and the key still does not exist, re-raise IntegrityError
    service._payment_repo.get_by_idempotency_key = AsyncMock(  # type: ignore[method-assign]
        side_effect=[None, None]
    )
    try:
        await service.create_payment(req, idempotency_key="race-key-unknown")
        raise AssertionError("Expected IntegrityError to be re-raised")
    except IntegrityError:
        pass


async def test_api_endpoints_create_idempotency_get_and_auth() -> None:
    app = create_app()
    service, _store = _wire_service_with_memory_store()

    test_settings = Settings(_env_file=None, api_key="test-api-key")  # type: ignore[call-arg]
    app.dependency_overrides[get_settings] = lambda: test_settings
    app.dependency_overrides[get_payment_service] = lambda: service

    headers = {
        "X-API-Key": "test-api-key",
        "Idempotency-Key": "key-100",
    }
    body = {
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Order #100",
        "metadata": {"cart_id": "c-100"},
        "webhook_url": "https://merchant.example/callback",
    }

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        # Healthcheck works without API key
        health = await client.get("/health")
        assert health.status_code == 200

        # 401 when X-API-Key is missing or invalid
        unauth = await client.post("/api/v1/payments", json=body)
        assert unauth.status_code == 401

        # 422 when Idempotency-Key is missing or whitespace-only
        no_idem = await client.post(
            "/api/v1/payments",
            json=body,
            headers={"X-API-Key": "test-api-key"},
        )
        assert no_idem.status_code == 422

        blank_idem = await client.post(
            "/api/v1/payments",
            json=body,
            headers={"X-API-Key": "test-api-key", "Idempotency-Key": "   "},
        )
        assert blank_idem.status_code == 422

        # 202 Accepted on valid payment creation
        create_resp = await client.post("/api/v1/payments", json=body, headers=headers)
        assert create_resp.status_code == 202
        created_data = create_resp.json()
        assert created_data["status"] == "pending"
        assert "payment_id" in created_data
        assert "created_at" in created_data
        payment_id = created_data["payment_id"]

        # Duplicate Idempotency-Key with identical payload -> 202 Accepted with same payment_id
        dup_resp = await client.post("/api/v1/payments", json=body, headers=headers)
        assert dup_resp.status_code == 202
        assert dup_resp.json()["payment_id"] == payment_id

        # Duplicate Idempotency-Key with modified payload -> 409 Conflict
        conflict_body = {**body, "amount": "9999.00"}
        conflict_resp = await client.post(
            "/api/v1/payments",
            json=conflict_body,
            headers=headers,
        )
        assert conflict_resp.status_code == 409

        # GET /api/v1/payments/{payment_id} -> 200 OK with full details including "metadata"
        get_resp = await client.get(
            f"/api/v1/payments/{payment_id}",
            headers={"X-API-Key": "test-api-key"},
        )
        assert get_resp.status_code == 200
        detail = get_resp.json()
        assert detail["id"] == payment_id
        assert detail["amount"] == "1500.00"
        assert detail["currency"] == "RUB"
        assert detail["description"] == "Order #100"
        assert detail["metadata"] == {"cart_id": "c-100"}
        assert detail["status"] == "pending"
        assert detail["idempotency_key"] == "key-100"
        assert detail["webhook_url"] == "https://merchant.example/callback"
        assert detail["processed_at"] is None

        # GET unknown UUID -> 404 Not Found
        unknown_resp = await client.get(
            f"/api/v1/payments/{uuid.uuid4()}",
            headers={"X-API-Key": "test-api-key"},
        )
        assert unknown_resp.status_code == 404


async def test_get_payment_not_found_raises() -> None:
    service, _store = _wire_service_with_memory_store()
    try:
        await service.get_payment(uuid.uuid4())
        raise AssertionError("Expected PaymentNotFoundError")
    except PaymentNotFoundError:
        pass


async def test_api_validation_edge_cases_and_get_auth() -> None:
    app = create_app()
    service, _store = _wire_service_with_memory_store()

    test_settings = Settings(_env_file=None, api_key="test-api-key")  # type: ignore[call-arg]
    app.dependency_overrides[get_settings] = lambda: test_settings
    app.dependency_overrides[get_payment_service] = lambda: service

    headers = {
        "X-API-Key": "test-api-key",
        "Idempotency-Key": "val-key",
    }
    valid_body = {
        "amount": "100.00",
        "currency": "USD",
        "description": "Valid",
        "metadata": {},
        "webhook_url": "https://merchant.example/callback",
    }

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        # GET /api/v1/payments/{id} without or with wrong X-API-Key -> 401
        get_no_key = await client.get(f"/api/v1/payments/{uuid.uuid4()}")
        assert get_no_key.status_code == 401

        get_bad_key = await client.get(
            f"/api/v1/payments/{uuid.uuid4()}",
            headers={"X-API-Key": "wrong-key"},
        )
        assert get_bad_key.status_code == 401

        # GET with malformed UUID path parameter -> 422
        get_bad_uuid = await client.get(
            "/api/v1/payments/not-a-uuid",
            headers={"X-API-Key": "test-api-key"},
        )
        assert get_bad_uuid.status_code == 422

        # Invalid payloads that must be rejected with 422
        invalid_payloads = [
            {**valid_body, "amount": "0"},  # must be > 0
            {**valid_body, "amount": "-10.00"},  # negative
            {**valid_body, "amount": "10.999"},  # > 2 decimal places
            {**valid_body, "amount": "10000000000"},  # >= 10^10 overflows NUMERIC(12, 2)
            {**valid_body, "currency": "GBP"},  # unsupported currency
            {**valid_body, "webhook_url": "not-a-url"},  # invalid URL
            {**valid_body, "webhook_url": "ftp://example.com/hook"},  # non-http scheme
        ]
        for idx, bad_body in enumerate(invalid_payloads):
            resp = await client.post(
                "/api/v1/payments",
                json=bad_body,
                headers={**headers, "Idempotency-Key": f"bad-{idx}"},
            )
            assert resp.status_code == 422, f"Expected 422 for payload {bad_body}"
