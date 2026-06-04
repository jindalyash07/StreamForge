from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from shared.database.models import ProcessedEvent


async def claim_event(session: AsyncSession, service_name: str, event_id: str, event_type: str) -> bool:
    processed = ProcessedEvent(service_name=service_name, event_id=event_id, event_type=event_type)
    session.add(processed)
    try:
        await session.flush()
        return True
    except IntegrityError:
        await session.rollback()
        return False


async def has_event_been_processed(session: AsyncSession, service_name: str, event_id: str) -> bool:
    query = select(ProcessedEvent.id).where(
        ProcessedEvent.service_name == service_name,
        ProcessedEvent.event_id == event_id,
    )
    result = await session.execute(query)
    return result.scalar_one_or_none() is not None
