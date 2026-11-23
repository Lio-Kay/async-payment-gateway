import json
import logging
import sys

import httpx
import pytest
from fastapi import Depends, FastAPI
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.config import Settings, get_settings
from src.core.database import (
    create_db_engine,
    create_session_factory,
    dispose_engine,
    get_db_session,
)
from src.core.logging import JsonFormatter, setup_logging
from src.core.security import is_valid_api_key, verify_api_key


def test_settings_defaults_and_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_KEY", "super-secret-key")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("OUTBOX_BATCH_SIZE", "25")

    settings = Settings(_env_file=None)  # type: ignore[call-arg]
    assert settings.api_key == "super-secret-key"
    assert settings.log_level == "DEBUG"
    assert settings.outbox_batch_size == 25
    assert settings.rabbitmq_exchange == "payments.new"
    assert settings.rabbitmq_queue == "payments.new"
    assert settings.rabbitmq_dlx == "payments.dlx"
    assert settings.rabbitmq_dlq == "payments.dlq"


def test_settings_rejects_inverted_processing_delay_bounds() -> None:
    with pytest.raises(ValidationError, match="payment_processing_min_seconds"):
        Settings(
            _env_file=None,  # type: ignore[call-arg]
            payment_processing_min_seconds=10.0,
            payment_processing_max_seconds=5.0,
        )


def test_env_example_loads_into_settings() -> None:
    settings = Settings(_env_file=".env.example")  # type: ignore[call-arg]
    assert settings.api_key == "dev-secret-api-key"
    assert settings.database_url == "postgresql+asyncpg://postgres:postgres@postgres:5432/payments"
    assert settings.rabbitmq_url == "amqp://guest:guest@rabbitmq:5672/"
    assert settings.rabbitmq_exchange == "payments.new"
    assert settings.rabbitmq_queue == "payments.new"
    assert settings.rabbitmq_dlx == "payments.dlx"
    assert settings.rabbitmq_dlq == "payments.dlq"


def test_json_formatter_includes_extra_fields_and_protects_core_keys() -> None:
    formatter = JsonFormatter()

    try:
        raise ValueError("boom")
    except ValueError:
        record = logging.LogRecord(
            name="test.logger",
            level=logging.ERROR,
            pathname=__file__,
            lineno=42,
            msg="Failed payment %s",
            args=("pay-123",),
            exc_info=sys.exc_info(),
        )
        record.payment_id = "pay-123"
        # Attempt to overwrite a core field via extra; formatter must ignore it.
        record.level = "SPOOFED"
        record._private = "hidden"

    payload = json.loads(formatter.format(record))
    assert payload["level"] == "ERROR"
    assert payload["logger"] == "test.logger"
    assert payload["message"] == "Failed payment pay-123"
    assert payload["payment_id"] == "pay-123"
    assert "_private" not in payload
    assert "timestamp" in payload
    assert "ValueError: boom" in payload["exception"]


def test_setup_logging_configures_json_handler() -> None:
    setup_logging("WARNING")
    root_logger = logging.getLogger()
    assert root_logger.level == logging.WARNING
    assert len(root_logger.handlers) == 1
    assert isinstance(root_logger.handlers[0].formatter, JsonFormatter)


async def test_database_factory_and_session_dependency() -> None:
    test_settings = Settings(
        _env_file=None,  # type: ignore[call-arg]
        database_url="postgresql+asyncpg://user:pass@localhost:5432/testdb",
    )
    test_engine = create_db_engine(test_settings)
    factory = create_session_factory(test_engine)

    async with factory() as session:
        assert isinstance(session, AsyncSession)
        assert factory.kw["expire_on_commit"] is False

    await test_engine.dispose()

    gen = get_db_session()
    session = await anext(gen)
    assert isinstance(session, AsyncSession)
    await gen.aclose()
    await dispose_engine()


def test_is_valid_api_key() -> None:
    assert is_valid_api_key("secret", "secret") is True
    assert is_valid_api_key("wrong", "secret") is False
    assert is_valid_api_key("ключ-не-ascii", "secret") is False
    assert is_valid_api_key(None, "secret") is False
    assert is_valid_api_key("", "secret") is False
    assert is_valid_api_key("secret", "") is False


async def test_verify_api_key_dependency() -> None:
    app = FastAPI()
    test_settings = Settings(_env_file=None, api_key="valid-key")  # type: ignore[call-arg]
    app.dependency_overrides[get_settings] = lambda: test_settings

    @app.get("/api/v1/protected", dependencies=[Depends(verify_api_key)])
    async def protected_endpoint() -> dict[str, str]:
        return {"status": "ok"}

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        missing = await client.get("/api/v1/protected")
        assert missing.status_code == 401

        invalid = await client.get("/api/v1/protected", headers={"X-API-Key": "bad-key"})
        assert invalid.status_code == 401

        valid = await client.get("/api/v1/protected", headers={"X-API-Key": "valid-key"})
        assert valid.status_code == 200
        assert valid.json() == {"status": "ok"}
