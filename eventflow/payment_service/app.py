from __future__ import annotations

from contextlib import asynccontextmanager, suppress
import asyncio

from fastapi import Depends, FastAPI, Response
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from shared.config import Settings, get_settings
from shared.database.session import create_session_factory, init_database
from shared.kafka.client import KafkaProducerClient
from shared.kafka.topics import ORDER_CREATED_TOPIC
from shared.kafka.worker import KafkaWorker
from shared.logging.config import configure_logging
from shared.logging.middleware import RequestContextMiddleware
from shared.observability.metrics import HTTP_REQUESTS, SERVICE_HEALTH, metrics_payload
from shared.rate_limiting import RateLimitMiddleware

from payment_service.service import PaymentService


def create_app(
    settings: Settings | None = None,
    session_factory: async_sessionmaker[AsyncSession] | None = None,
    producer: KafkaProducerClient | None = None,
) -> FastAPI:
    resolved = settings or get_settings()
    configure_logging(resolved.log_level)
    session_factory = session_factory or create_session_factory(resolved)
    owns_producer = producer is None
    producer = producer or KafkaProducerClient(resolved.kafka_bootstrap_servers, client_id="payment-service")
    handler = PaymentService()
    worker = KafkaWorker(
        service_name=resolved.service_name,
        bootstrap_servers=resolved.kafka_bootstrap_servers,
        topics=[ORDER_CREATED_TOPIC],
        group_id="payment-service-group",
        handler=handler.handle_order_created,
        session_factory=session_factory,
        producer=producer.producer,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await init_database(session_factory)
        if owns_producer:
            await producer.start()
        consumer_task = asyncio.create_task(worker.start())
        try:
            yield
        finally:
            consumer_task.cancel()
            with suppress(asyncio.CancelledError):
                await consumer_task
            if owns_producer:
                await producer.stop()

    app = FastAPI(title="EventFlow Payment Service", version="1.0.0", lifespan=lifespan)
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
