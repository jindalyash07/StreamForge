from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from shared.config import Settings, get_settings
from shared.database.base import Base


def create_engine_from_settings(settings: Settings | None = None):
    resolved = settings or get_settings()
    return create_async_engine(resolved.database_url, future=True, pool_pre_ping=True)


def create_session_factory(settings: Settings | None = None) -> async_sessionmaker[AsyncSession]:
    engine = create_engine_from_settings(settings)
    return async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)


async def get_session(session_factory: async_sessionmaker[AsyncSession]) -> AsyncGenerator[AsyncSession, None]:
    async with session_factory() as session:
        yield session


async def init_database(session_factory: async_sessionmaker[AsyncSession]) -> None:
    engine = session_factory.bind
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
