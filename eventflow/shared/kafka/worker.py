from __future__ import annotations

import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from aiokafka import AIOKafkaConsumer
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from shared.kafka.client import KafkaProducerClient
from shared.kafka.dlq import publish_to_dlq
from shared.kafka.retry import run_with_retry
from shared.observability.metrics import EVENT_DLQ, EVENT_RETRIES
from shared.schemas.events import EventEnvelope


logger = logging.getLogger(__name__)

MessageHandler = Callable[[EventEnvelope, AsyncSession, KafkaProducerClient], Awaitable[None]]


def _deserialize(value: bytes) -> Any:
    """Decode a Kafka message value; undecodable bytes become a marker dict instead of crashing the consumer."""
    try:
        return json.loads(value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return {"__undecodable__": value.decode("utf-8", errors="replace")}


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
        producer: KafkaProducerClient,
        attempts: int = 3,
        after_commit: Callable[[], None] | None = None,
    ) -> None:
        self.service_name = service_name
        self.bootstrap_servers = bootstrap_servers
        self.topics = topics
        self.group_id = group_id
        self.handler = handler
        self.session_factory = session_factory
        self.producer = producer
        self.attempts = attempts
        self.after_commit = after_commit
        # Created in start(): aiokafka needs a running event loop.
        self._consumer: AIOKafkaConsumer | None = None

    async def start(self) -> None:
        self._consumer = AIOKafkaConsumer(
            *self.topics,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            value_deserializer=_deserialize,
        )
        await self._consumer.start()
        logger.info("Kafka worker started", extra={"service": self.service_name, "topic": ",".join(self.topics)})
        try:
            async for message in self._consumer:
                await self.process_message(message.topic, message.value)
                # Commit only after the message was handled or parked in the DLQ.
                await self._consumer.commit()
        finally:
            await self._consumer.stop()

    async def process_message(self, topic: str, raw_event: Any) -> bool:
        try:
            event = EventEnvelope.model_validate(raw_event)
        except ValidationError as exc:
            # A malformed message will never succeed; retrying it would only block the partition.
            logger.error("Malformed event sent to DLQ without retry", extra={"service": self.service_name, "topic": topic})
            await self._dead_letter(topic, raw_event, f"invalid event envelope: {exc}", attempts=1)
            return False

        try:
            await self._process_event(event)
            return True
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "Kafka worker failed after retries",
                extra={"service": self.service_name, "event_id": event.event_id},
            )
            await self._dead_letter(topic, raw_event, str(exc), attempts=self.attempts)
            return False

    async def _dead_letter(self, topic: str, raw_event: Any, error: str, attempts: int) -> None:
        event = raw_event if isinstance(raw_event, dict) else {"raw": raw_event}
        await publish_to_dlq(
            self.producer,
            service_name=self.service_name,
            original_topic=topic,
            event=event,
            error=error,
            attempts=attempts,
        )
        EVENT_DLQ.labels(self.service_name, topic).inc()

    async def _process_event(self, event: EventEnvelope) -> None:
        async def _operation(attempt: int) -> None:
            async with self.session_factory() as session:
                event.attempt = attempt
                try:
                    await self.handler(event, session, self.producer)
                    await session.commit()
                    if self.after_commit is not None:
                        self.after_commit()
                except Exception:  # noqa: BLE001
                    if attempt < self.attempts:
                        EVENT_RETRIES.labels(self.service_name, event.event_type).inc()
                    raise

        await run_with_retry(_operation, attempts=self.attempts)

    async def stop(self) -> None:
        if self._consumer is not None:
            await self._consumer.stop()
