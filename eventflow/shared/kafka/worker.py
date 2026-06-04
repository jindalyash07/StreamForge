from __future__ import annotations

import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from shared.kafka.dlq import publish_to_dlq
from shared.kafka.retry import run_with_retry
from shared.observability.metrics import EVENT_DLQ, EVENT_RETRIES
from shared.schemas.events import EventEnvelope


logger = logging.getLogger(__name__)

MessageHandler = Callable[[EventEnvelope, AsyncSession, AIOKafkaProducer], Awaitable[None]]


class KafkaWorker:
    def __init__(
        self,
        *,
        service_name: str,
        bootstrap_servers: str,
        topics: list[str],
        group_id: str,
        handler: MessageHandler,
        session_factory: async_sessionmaker[AsyncSession],
        producer: AIOKafkaProducer,
        attempts: int = 3,
    ) -> None:
        self.service_name = service_name
        self.bootstrap_servers = bootstrap_servers
        self.topics = topics
        self.group_id = group_id
        self.handler = handler
        self.session_factory = session_factory
        self.producer = producer
        self.attempts = attempts
        self._consumer = AIOKafkaConsumer(
            *topics,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        )

    async def start(self) -> None:
        await self._consumer.start()
        logger.info("Kafka worker started", extra={"service": self.service_name, "topics": self.topics})
        try:
            async for message in self._consumer:
                await self.process_message(message.topic, message.value)
                await self._consumer.commit()
        finally:
            await self._consumer.stop()

    async def process_message(self, topic: str, raw_event: dict[str, Any]) -> bool:
        event = EventEnvelope.model_validate(raw_event)
        try:
            await self._process_event(event)
            return True
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "Kafka worker failed after retries",
                extra={"service": self.service_name, "event_id": event.event_id},
            )
            await publish_to_dlq(
                self.producer,
                service_name=self.service_name,
                original_topic=topic,
                event=raw_event,
                error=str(exc),
                attempts=self.attempts,
            )
            EVENT_DLQ.labels(self.service_name, topic).inc()
            return False

    async def _process_event(self, event: EventEnvelope) -> None:
        async def _operation(attempt: int) -> None:
            async with self.session_factory() as session:
                event.attempt = attempt
                try:
                    await self.handler(event, session, self.producer)
                    await session.commit()
                except Exception:  # noqa: BLE001
                    if attempt < self.attempts:
                        EVENT_RETRIES.labels(self.service_name, event.event_type).inc()
                    raise

        await run_with_retry(_operation, attempts=self.attempts)

    async def stop(self) -> None:
        await self._consumer.stop()
