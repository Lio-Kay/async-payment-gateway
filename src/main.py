import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI

from src.api.router import router as payments_router
from src.core.config import get_settings
from src.core.database import dispose_engine
from src.core.logging import setup_logging

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    # Configure JSON stdout logging before serving requests, and close the
    # asyncpg connection pool cleanly when Uvicorn shuts down.
    setup_logging()
    yield
    await dispose_engine()


def create_app() -> FastAPI:
    settings = get_settings()
    application = FastAPI(
        title=settings.app_name,
        version="0.1.0",
        lifespan=lifespan,
    )
    application.include_router(payments_router)

    @application.get("/health", tags=["health"])
    async def healthcheck() -> dict[str, str]:
        # Used by Docker Compose to verify the API container is ready after migrations.
        return {"status": "ok"}

    @application.post("/webhooks/mock", tags=["webhooks"])
    async def mock_webhook_receiver(payload: dict[str, Any]) -> dict[str, str]:
        """Built-in webhook sink for local testing inside the Docker network.

        Pointing `webhook_url` to `http://api:8000/webhooks/mock` lets manual
        `curl` tests verify end-to-end webhook delivery without an external server.
        """
        logger.info(
            "Mock webhook receiver got payment notification",
            extra={
                "payment_id": payload.get("payment_id"),
                "status": payload.get("status"),
            },
        )
        return {"status": "received"}

    return application


app = create_app()
