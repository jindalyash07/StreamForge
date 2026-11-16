"""Transactional outbox.

Services never publish to Kafka inside a request or event handler. Instead they call
``enqueue`` with the same session that writes their business rows, so the event and the
state change commit (or roll back) together. ``OutboxRelay`` then publishes pending rows
in order and marks them as published.

Delivery is at-least-once: if the relay publishes a row but crashes before marking it,
the row is published again with the same ``event_id``, and downstream consumers drop the
duplicate through the ``processed_events`` idempotency table.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from shared.database.models import OutboxEvent
from shared.kafka.client import KafkaProducerClient
from shared.observability.metrics import OUTBOX_PUBLISHED, OUTBOX_PUBLISH_FAILURES
from shared.schemas.events import EventEnvelope


logger = logging.getLogger(__name__)


def enqueue(session: AsyncSession, topic: str, event: EventEnvelope) -> OutboxEvent:
    """Stage ``event`` for publishing on ``topic`` as part of the session's transaction."""
    row = OutboxEvent(
        event_id=event.event_id,
        source=event.source,
        topic=topic,
        payload=event.model_dump(mode="json"),
    )
    session.add(row)
    return row


class OutboxRelay:
    def __init__(
        self,
        *,
        service_name: str,
        session_factory: async_sessionmaker[AsyncSession],
        producer: KafkaProducerClient,
        batch_size: int = 100,
        poll_interval_seconds: float = 1.0,
    ) -> None:
        self.service_name = service_name
        self.session_factory = session_factory
        self.producer = producer
        self.batch_size = batch_size
        self.poll_interval_seconds = poll_interval_seconds
        self._wakeup = asyncio.Event()

    def notify(self) -> None:
        """Ask the relay loop to publish now instead of waiting for the next poll."""
        self._wakeup.set()

    async def publish_pending(self) -> int:
        """Publish one batch of pending events in creation order. Returns how many were published."""
        published = 0
        async with self.session_factory() as session:
            query = (
                select(OutboxEvent)
                .where(OutboxEvent.source == self.service_name, OutboxEvent.published_at.is_(None))
                .order_by(OutboxEvent.id)
                .limit(self.batch_size)
                # Lets several replicas relay concurrently without picking the same rows (PostgreSQL).
                .with_for_update(skip_locked=True)
            )
            rows = (await session.execute(query)).scalars().all()
            for row in rows:
                try:
                    await self.producer.send_event(row.topic, row.payload)
                except Exception as exc:  # noqa: BLE001
                    row.publish_attempts += 1
                    row.last_error = str(exc)[:2000]
                    OUTBOX_PUBLISH_FAILURES.labels(self.service_name, row.topic).inc()
                    logger.warning(
                        "Outbox publish failed; will retry",
                        extra={"service": self.service_name, "topic": row.topic, "event_id": row.event_id},
                    )
                    # Stop here so later events are not published ahead of this one.
                    break
                row.published_at = datetime.now(timezone.utc)
                row.publish_attempts += 1
                row.last_error = None
                published += 1
                OUTBOX_PUBLISHED.labels(self.service_name, row.topic).inc()
            await session.commit()
        return published

    async def drain(self) -> None:
        """Publish until nothing is pending or a publish fails."""
        while True:
            batch = await self.publish_pending()
            if batch < self.batch_size:
                return

    async def run(self) -> None:
        while True:
            try:
                await self.drain()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                logger.exception("Outbox relay iteration failed", extra={"service": self.service_name})
            try:
                await asyncio.wait_for(self._wakeup.wait(), timeout=self.poll_interval_seconds)
            except asyncio.TimeoutError:
                pass
            self._wakeup.clear()
