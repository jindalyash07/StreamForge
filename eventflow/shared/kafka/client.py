from __future__ import annotations

import json
from typing import Any

from aiokafka import AIOKafkaProducer

from shared.schemas.events import EventEnvelope


class KafkaProducerClient:
    def __init__(self, bootstrap_servers: str, client_id: str):
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        # Created in start(): aiokafka requires a running event loop, so building it
        # here would break importing the app module (e.g. under pytest or uvicorn --reload).
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        if self._producer is not None:
            return
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            client_id=self.client_id,
            value_serializer=lambda payload: json.dumps(payload, default=str).encode("utf-8"),
            acks="all",
            enable_idempotence=True,
        )
        await self._producer.start()

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None

    async def send_event(self, topic: str, event: EventEnvelope | dict[str, Any]) -> None:
        payload = event.model_dump(mode="json") if isinstance(event, EventEnvelope) else event
        await self.producer.send_and_wait(topic, payload)

    @property
    def producer(self) -> AIOKafkaProducer:
        if self._producer is None:
            raise RuntimeError("KafkaProducerClient.start() must be awaited before sending")
        return self._producer
