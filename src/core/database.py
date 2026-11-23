from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from src.core.config import Settings, get_settings


def create_db_engine(settings: Settings | None = None) -> AsyncEngine:
    """Build an async SQLAlchemy engine.

    `pool_pre_ping=True` validates pooled connections before use so dropped
    Postgres connections after container restarts are recycled automatically.
    """
    cfg = settings or get_settings()
    return create_async_engine(
        cfg.database_url,
        echo=cfg.db_echo,
        pool_size=cfg.db_pool_size,
        max_overflow=cfg.db_max_overflow,
        pool_pre_ping=True,
    )


def create_session_factory(db_engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Build an async session factory.

    `expire_on_commit=False` is required in async SQLAlchemy so model attributes
    can still be read after `async with session.begin():` commits without
    triggering implicit synchronous lazy-load queries.
    """
    return async_sessionmaker(
        bind=db_engine,
        class_=AsyncSession,
        autoflush=False,
        expire_on_commit=False,
    )


# Note: create_async_engine is lazy and does not open a socket at import time.
engine: AsyncEngine = create_db_engine()
async_session_factory: async_sessionmaker[AsyncSession] = create_session_factory(engine)


async def get_db_session() -> AsyncIterator[AsyncSession]:
    """Yield a scoped AsyncSession per HTTP request.

    Transaction boundaries (`async with session.begin():`) are managed explicitly
    in the service layer rather than auto-committing in this dependency.
    """
    async with async_session_factory() as session:
        yield session


async def dispose_engine() -> None:
    await engine.dispose()
