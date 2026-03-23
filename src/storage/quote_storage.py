from __future__ import annotations

import redis.asyncio as redis
from quotes import Quote, quote_from_redis

from src.config import RedisConfig
from src.utils.logger import logger


class QuoteStorage:
    def __init__(
        self,
        redis_config: RedisConfig,
        key_prefix: str = "quotes",
        events_key: str = "quotes:events",
    ) -> None:
        self._redis = redis.Redis(
            host=redis_config.host,
            port=redis_config.port,
            db=redis_config.db,
            username=redis_config.username,
            password=redis_config.password,
            decode_responses=redis_config.decode_responses,
        )
        self._key_prefix = key_prefix
        self._events_key = events_key

    async def close(self) -> None:
        await self._redis.aclose()

    def quotes_key(self, exchange: str) -> str:
        return f"{self._key_prefix}:{exchange.lower()}"

    @property
    def events_key(self) -> str:
        return self._events_key

    async def get_quotes(self, exchange: str) -> dict[str, Quote]:
        raw_quotes = await self._redis.hgetall(self.quotes_key(exchange))
        quotes: dict[str, Quote] = {}
        for asset_id, payload in raw_quotes.items():
            try:
                quotes[asset_id] = quote_from_redis(asset_id=asset_id, payload=payload)
            except Exception as exc:
                logger.warning(
                    "Failed to parse quote from Redis: exchange=%s asset_id=%s error=%s",
                    exchange,
                    asset_id,
                    exc,
                )
        return quotes

    async def read_events(self, last_id: str, block_ms: int, count: int) -> list[tuple[str, dict[str, str]]]:
        events = await self._redis.xread(
            streams={self._events_key: last_id},
            count=count,
            block=block_ms,
        )
        if not events:
            return []

        _, entries = events[0]
        return [(event_id, payload) for event_id, payload in entries]

    async def list_exchanges(self) -> list[str]:
        exchanges: list[str] = []
        async for key in self._redis.scan_iter(match=f"{self._key_prefix}:*"):
            if key == self._events_key:
                continue
            exchanges.append(key.removeprefix(f"{self._key_prefix}:"))
        return exchanges
