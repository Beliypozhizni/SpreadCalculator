from __future__ import annotations

import asyncio

from src.config import load_config
from src.services.spread_service import SpreadService
from src.storage.quote_storage import QuoteStorage
from src.storage.spread_storage import SpreadStorage
from src.utils.logger import logger


async def async_main() -> None:
    config = load_config()
    logger.info("Spread calculator started for exchanges: %s", ",".join(config.exchanges))

    quote_storage = QuoteStorage(
        redis_config=config.input_redis,
        key_prefix=config.quote_key_prefix,
        events_key=config.quote_events_key,
    )
    spread_storage = SpreadStorage(
        redis_config=config.output_redis,
        key_prefix=config.spread_key_prefix,
        events_key=config.spread_events_key,
        stream_max_len=config.stream_max_len,
    )

    service = SpreadService(
        exchanges=config.exchanges,
        quote_updated_type=config.quote_updated_type,
        spread_match_mode=config.spread_match_mode,
        quote_storage=quote_storage,
        spread_storage=spread_storage,
    )

    try:
        await service.recalculate_all()
        await service.run_forever(
            block_ms=config.stream_block_ms,
            batch_size=config.stream_batch_size,
        )
    except Exception:
        logger.exception("Spread calculator terminated with error")
        raise
    finally:
        await quote_storage.close()
        await spread_storage.close()
        logger.info("Spread calculator stopped")


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
