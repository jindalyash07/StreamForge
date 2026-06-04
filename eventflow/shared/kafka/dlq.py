from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime, timezone
import json
import logging

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from shared.kafka.topics import DEAD_LETTER_QUEUE_TOPIC


logger = logging.getLogger(__name__)


async def publish_to_dlq(
    producer: AIOKafkaProducer,
    *,
    service_name: str,
    original_topic: str,
    event: dict,
    error: str,
    attempts: int,
) -> None:
    payload = {
        "service_name": service_name,
        "original_topic": original_topic,
        "event": event,
        "error": error,
        "attempts": attempts,
        "failed_at": datetime.now(timezone.utc).isoformat(),
    }
    await producer.send_and_wait(DEAD_LETTER_QUEUE_TOPIC, json.dumps(payload).encode("utf-8"))


async def consume_dlq(bootstrap_servers: str, group_id: str = "dlq-debugger") -> AsyncIterator[dict]:
    consumer = AIOKafkaConsumer(
        DEAD_LETTER_QUEUE_TOPIC,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        enable_auto_commit=True,
        auto_offset_reset="earliest",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )
    await consumer.start()
    try:
        async for message in consumer:
            logger.warning("DLQ message observed", extra={"payload": message.value})
            yield message.value
    finally:
        await consumer.stop()
