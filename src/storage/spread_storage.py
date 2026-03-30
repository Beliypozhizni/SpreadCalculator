from __future__ import annotations

from collections.abc import Iterable, Mapping

import redis.asyncio as redis
from spreads import Spread

from src.config import RedisConfig


class SpreadStorage:
    def __init__(
        self,
        redis_config: RedisConfig,
        key_prefix: str = "spreads",
        events_key: str = "spreads:events",
        stream_max_len: int = 10_000,
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
        self._stream_max_len = stream_max_len

    async def close(self) -> None:
        await self._redis.aclose()

    def spreads_key(self, exchange_buy: str, exchange_sell: str) -> str:
        return f"{self._key_prefix}:{exchange_buy.lower()}:{exchange_sell.lower()}"

    async def list_spread_keys(self) -> list[str]:
        keys: list[str] = []
        async for key in self._redis.scan_iter(match=f"{self._key_prefix}:*:*"):
            keys.append(key)
        return keys

    async def get_ts_found_by_pair(
        self,
        exchange_pairs: Iterable[tuple[str, str]] | None = None,
    ) -> dict[tuple[str, str], dict[str, int]]:
        if exchange_pairs is None:
            keys_to_load = await self.list_spread_keys()
        else:
            keys_to_load = [
                self.spreads_key(exchange_buy, exchange_sell)
                for exchange_buy, exchange_sell in exchange_pairs
            ]

        if not keys_to_load:
            return {}

        async with self._redis.pipeline(transaction=False) as pipeline:
            for key in keys_to_load:
                pipeline.hgetall(key)
            loaded = await pipeline.execute()

        ts_found_by_pair: dict[tuple[str, str], dict[str, int]] = {}
        for redis_key, spread_mapping in zip(keys_to_load, loaded, strict=True):
            if not spread_mapping:
                continue

            exchange_buy, exchange_sell = redis_key.split(":")[-2:]
            pair_ts_found: dict[str, int] = {}
            for asset_id, payload in spread_mapping.items():
                try:
                    spread = Spread.model_validate_json(payload)
                except Exception:
                    continue
                pair_ts_found[asset_id] = spread.ts_found

            if pair_ts_found:
                ts_found_by_pair[(exchange_buy, exchange_sell)] = pair_ts_found

        return ts_found_by_pair

    async def sync(self, spreads_by_pair: Mapping[tuple[str, str], Mapping[str, Spread]]) -> None:
        desired_keys = {
            self.spreads_key(exchange_buy, exchange_sell)
            for exchange_buy, exchange_sell in spreads_by_pair
        }
        existing_keys = set(await self.list_spread_keys())
        keys_to_load = list(existing_keys | desired_keys)

        current_state: dict[str, dict[str, str]] = {}
        if keys_to_load:
            async with self._redis.pipeline(transaction=False) as pipeline:
                for key in keys_to_load:
                    pipeline.hgetall(key)
                loaded = await pipeline.execute()
            current_state = {
                key: value
                for key, value in zip(keys_to_load, loaded, strict=True)
            }

        async with self._redis.pipeline(transaction=True) as pipeline:
            for exchange_pair, spreads in spreads_by_pair.items():
                exchange_buy, exchange_sell = exchange_pair
                redis_key = self.spreads_key(exchange_buy, exchange_sell)
                existing_mapping = current_state.get(redis_key, {})

                desired_mapping = {
                    asset_id: spread.to_json()
                    for asset_id, spread in spreads.items()
                }

                changed_mapping = {
                    asset_id: payload
                    for asset_id, payload in desired_mapping.items()
                    if existing_mapping.get(asset_id) != payload
                }
                stale_fields = set(existing_mapping) - set(desired_mapping)

                if changed_mapping:
                    pipeline.hset(redis_key, mapping=changed_mapping)
                    for asset_id in changed_mapping:
                        pipeline.xadd(
                            self._events_key,
                            fields=spreads[asset_id].to_event(action="upsert"),
                            maxlen=self._stream_max_len,
                            approximate=True,
                        )

                if stale_fields:
                    pipeline.hdel(redis_key, *stale_fields)
                    for asset_id in stale_fields:
                        pipeline.xadd(
                            self._events_key,
                            fields={
                                "action": "delete",
                                "asset_id": asset_id,
                                "exchange_buy": exchange_buy,
                                "exchange_sell": exchange_sell,
                            },
                            maxlen=self._stream_max_len,
                            approximate=True,
                        )

                if not desired_mapping and existing_mapping:
                    pipeline.delete(redis_key)

            obsolete_keys = existing_keys - desired_keys
            for redis_key in obsolete_keys:
                exchange_buy, exchange_sell = redis_key.split(":")[-2:]
                for asset_id in current_state.get(redis_key, {}):
                    pipeline.xadd(
                        self._events_key,
                        fields={
                            "action": "delete",
                            "asset_id": asset_id,
                            "exchange_buy": exchange_buy,
                            "exchange_sell": exchange_sell,
                        },
                        maxlen=self._stream_max_len,
                        approximate=True,
                    )
                pipeline.delete(redis_key)

            await pipeline.execute()
