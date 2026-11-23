from sqlalchemy.ext.asyncio import AsyncSession

from src.models.base import Base


class BaseRepository[ModelT: Base]:
    """Base data access object bound to an active SQLAlchemy AsyncSession.

    Repositories never call `session.commit()` themselves so that service methods
    can compose multiple repository calls (such as `PaymentRepository.create` and
    `OutboxRepository.add_event`) inside a single atomic transaction block
    (`async with session.begin():`).
    """

    model: type[ModelT]

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    @property
    def session(self) -> AsyncSession:
        return self._session
