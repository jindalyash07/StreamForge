from __future__ import annotations

from collections.abc import AsyncIterator
from decimal import Decimal
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from shared.database.base import Base
from shared.config import Settings


class FakeProducer:
    is_test_double = True

    def __init__(self) -> None:
        self.sent: list[tuple[str, object]] = []

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def send_event(self, topic: str, event) -> None:
        self.sent.append((topic, event))

    async def send_and_wait(self, topic: str, payload) -> None:
        self.sent.append((topic, payload))


@pytest.fixture
def fake_producer() -> FakeProducer:
    return FakeProducer()


@pytest_asyncio.fixture
async def session_factory(tmp_path: Path) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    database_path = tmp_path / "eventflow.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{database_path}", future=True)
    factory = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    try:
        yield factory
    finally:
        await engine.dispose()


@pytest.fixture
def test_settings(tmp_path: Path) -> Settings:
    return Settings(
        service_name="test-service",
        environment="test",
        log_level="INFO",
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'app.db'}",
        kafka_bootstrap_servers="unused:9092",
    )
