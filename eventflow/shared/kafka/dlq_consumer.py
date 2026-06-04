from __future__ import annotations

import asyncio
import json
import logging

from shared.config import get_settings
from shared.kafka.dlq import consume_dlq


async def run_dlq_consumer() -> None:
    settings = get_settings()
    logging.basicConfig(level=settings.log_level)
    async for message in consume_dlq(settings.kafka_bootstrap_servers):
        logging.getLogger(__name__).warning("DLQ payload: %s", json.dumps(message, default=str))


def main() -> None:
    asyncio.run(run_dlq_consumer())


if __name__ == "__main__":
    main()
