from __future__ import annotations

from contextvars import ContextVar
from uuid import uuid4

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

request_id_var: ContextVar[str] = ContextVar("request_id", default="-")
correlation_id_var: ContextVar[str] = ContextVar("correlation_id", default="-")


class RequestContextMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, request_id_header: str = "X-Request-ID", correlation_id_header: str = "X-Correlation-ID"):
        super().__init__(app)
        self.request_id_header = request_id_header
        self.correlation_id_header = correlation_id_header

    async def dispatch(self, request: Request, call_next) -> Response:
        request_id = request.headers.get(self.request_id_header, str(uuid4()))
        correlation_id = request.headers.get(self.correlation_id_header, request_id)
        token_request = request_id_var.set(request_id)
        token_correlation = correlation_id_var.set(correlation_id)
        try:
            response = await call_next(request)
        finally:
            request_id_var.reset(token_request)
            correlation_id_var.reset(token_correlation)
        response.headers[self.request_id_header] = request_id
        response.headers[self.correlation_id_header] = correlation_id
        return response
