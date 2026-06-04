from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable


async def run_with_retry(
    operation: Callable[[int], Awaitable[object]],
    attempts: int = 3,
    initial_delay: float = 0.5,
    backoff: float = 2.0,
) -> object:
    last_error: Exception | None = None
    delay = initial_delay
    for attempt in range(1, attempts + 1):
        try:
            return await operation(attempt)
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt == attempts:
                break
            await asyncio.sleep(delay)
            delay *= backoff
    assert last_error is not None
    raise last_error
