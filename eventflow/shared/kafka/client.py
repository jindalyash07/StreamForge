from __future__ import annotations

import json
from typing import Any

from aiokafka import AIOKafkaProducer

from shared.schemas.events import EventEnvelope


class KafkaProducerClient:
    def __init__(self, bootstrap_servers: str, client_id: str):
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        self._producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            client_id=client_id,
            value_serializer=lambda payload: json.dumps(payload).encode("utf-8"),
            acks="all",
            enable_idempotence=True,
        )

    async def start(self) -> None:
        await self._producer.start()

    async def stop(self) -> None:
        await self._producer.stop()

    async def send_event(self, topic: str, event: EventEnvelope | dict[str, Any]) -> None:
        payload = event.model_dump(mode="json") if isinstance(event, EventEnvelope) else event
        await self._producer.send_and_wait(topic, payload)

    @property
    def producer(self) -> AIOKafkaProducer:
        return self._producer
