import secrets
from typing import Annotated

from fastapi import Depends, HTTPException, Security, status
from fastapi.security import APIKeyHeader

from src.core.config import Settings, get_settings

API_KEY_HEADER_NAME = "X-API-Key"

# `auto_error=False` lets us return 401 Unauthorized when the header is missing
# (FastAPI's default `auto_error=True` on APIKeyHeader returns 403 Forbidden).
api_key_header = APIKeyHeader(
    name=API_KEY_HEADER_NAME,
    auto_error=False,
    description="Static API key required for all /api/v1/* endpoints.",
)


def is_valid_api_key(provided_key: str | None, expected_key: str) -> bool:
    """Compare API keys in constant time using `secrets.compare_digest`.

    Strings are encoded to UTF-8 bytes first because `secrets.compare_digest(str, str)`
    raises `TypeError` when either string contains non-ASCII characters.
    """
    if not provided_key or not expected_key:
        return False
    return secrets.compare_digest(
        provided_key.encode("utf-8"),
        expected_key.encode("utf-8"),
    )


async def verify_api_key(
    api_key: Annotated[str | None, Security(api_key_header)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> str:
    """FastAPI dependency enforcing static X-API-Key authentication."""
    if not is_valid_api_key(api_key, settings.api_key):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key.",
        )
    assert api_key is not None
    return api_key
