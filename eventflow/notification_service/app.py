from __future__ import annotations

from contextlib import asynccontextmanager, suppress
import asyncio

from fastapi import Depends, FastAPI, Response
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from shared.config import Settings, get_settings
from shared.database.session import create_session_factory, init_database
from shared.kafka.client import KafkaProducerClient
from shared.kafka.topics import PAYMENT_SUCCESS_TOPIC
from shared.kafka.worker import KafkaWorker
from shared.logging.config import configure_logging
from shared.logging.middleware import RequestContextMiddleware
from shared.observability.metrics import HTTP_REQUESTS, SERVICE_HEALTH, metrics_payload
from shared.outbox import OutboxRelay
from shared.rate_limiting import RateLimitMiddleware

from notification_service.service import NotificationService


def create_app(
    settings: Settings | None = None,
    session_factory: async_sessionmaker[AsyncSession] | None = None,
    producer: KafkaProducerClient | None = None,
) -> FastAPI:
    resolved = settings or get_settings()
    configure_logging(resolved.log_level)
    session_factory = session_factory or create_session_factory(resolved)
    owns_producer = producer is None
    producer = producer or KafkaProducerClient(resolved.kafka_bootstrap_servers, client_id="notification-service")
    handler = NotificationService(resolved.service_name)
    relay = OutboxRelay(
        service_name=resolved.service_name,
        session_factory=session_factory,
        producer=producer,
        poll_interval_seconds=resolved.outbox_poll_interval_seconds,
    )
    worker = KafkaWorker(
        service_name=resolved.service_name,
        bootstrap_servers=resolved.kafka_bootstrap_servers,
        topics=[PAYMENT_SUCCESS_TOPIC],
        group_id="notification-service-group",
        handler=handler.handle_payment_success,
        session_factory=session_factory,
        producer=producer,
        after_commit=relay.notify,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await init_database(session_factory)
        if owns_producer:
            await producer.start()
        consumer_task = asyncio.create_task(worker.start())
        relay_task = asyncio.create_task(relay.run())
        try:
            yield
        finally:
            consumer_task.cancel()
            relay_task.cancel()
            for task in (consumer_task, relay_task):
                with suppress(asyncio.CancelledError):
                    await task
            with suppress(Exception):
                await relay.drain()
            if owns_producer:
                await producer.stop()

    app = FastAPI(title="EventFlow Notification Service", version="1.0.0", lifespan=lifespan)
    app.add_middleware(
        RequestContextMiddleware,
        request_id_header=resolved.request_id_header,
        correlation_id_header=resolved.correlation_id_header,
    )
    app.add_middleware(
        RateLimitMiddleware,
        redis_url=resolved.redis_url,
        service_name=resolved.service_name,
        limit=resolved.rate_limit_per_minute,
        window_seconds=resolved.rate_limit_window_seconds,
    )

    async def get_session() -> AsyncSession:
        async with session_factory() as session:
            yield session

    @app.middleware("http")
    async def metrics_middleware(request, call_next):
        response = await call_next(request)
        HTTP_REQUESTS.labels(resolved.service_name, request.url.path, request.method, str(response.status_code)).inc()
        return response

    @app.get("/health")
    async def health(session: AsyncSession = Depends(get_session)):
        await session.execute(text("SELECT 1"))
        SERVICE_HEALTH.labels(resolved.service_name).set(1)
        return {"status": "ok", "service": resolved.service_name}

    @app.get("/metrics")
    async def metrics() -> Response:
        body, content_type = metrics_payload()
        return Response(content=body, media_type=content_type)

    return app


app = create_app()
