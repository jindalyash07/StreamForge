from __future__ import annotations

import logging
import time

from redis.asyncio import Redis
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response


logger = logging.getLogger(__name__)


class RateLimitMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, redis_url: str, service_name: str, limit: int, window_seconds: int) -> None:
        super().__init__(app)
        self.service_name = service_name
        self.limit = limit
        self.window_seconds = window_seconds
        self.redis = Redis.from_url(redis_url, encoding="utf-8", decode_responses=True)

    async def dispatch(self, request: Request, call_next) -> Response:
        client_host = request.client.host if request.client else "unknown"
        window_key = int(time.time() // self.window_seconds)
        key = f"ratelimit:{self.service_name}:{client_host}:{request.url.path}:{window_key}"
        try:
            count = await self.redis.incr(key)
            if count == 1:
                await self.redis.expire(key, self.window_seconds)
            if count > self.limit:
                return JSONResponse(status_code=429, content={"detail": "Rate limit exceeded"})
        except Exception:  # noqa: BLE001
            logger.warning("Rate limiting backend unavailable; allowing request", exc_info=True)
        return await call_next(request)
