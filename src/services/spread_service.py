from __future__ import annotations

import asyncio
from collections.abc import Mapping

from quotes import Quote
from spreads import Spread, create_spread
from src.storage.quote_storage import QuoteStorage
from src.storage.spread_storage import SpreadStorage
from src.utils.logger import logger


class SpreadService:
    def __init__(
        self,
        exchanges: tuple[str, ...],
        quote_storage: QuoteStorage,
        spread_storage: SpreadStorage,
    ) -> None:
        self._exchanges = exchanges
        self._quote_storage = quote_storage
        self._spread_storage = spread_storage

    async def recalculate_all(self) -> int:
        quotes_by_exchange = await self._load_quotes()
        spreads_by_pair = self._calculate_spreads(quotes_by_exchange)
        await self._spread_storage.sync(spreads_by_pair)

        spread_count = sum(len(spreads) for spreads in spreads_by_pair.values())
        logger.info("Calculated %s spreads across %s exchange pairs", spread_count, len(spreads_by_pair))
        return spread_count

    async def run_forever(self, block_ms: int, batch_size: int) -> None:
        last_id = "$"
        while True:
            events = await self._quote_storage.read_events(
                last_id=last_id,
                block_ms=block_ms,
                count=batch_size,
            )
            if not events:
                continue

            last_id = events[-1][0]
            logger.debug("Received %s quote events, recalculating spreads", len(events))
            try:
                await self.recalculate_all()
            except Exception:
                logger.exception("Spread recalculation failed after quote event batch")

    async def _load_quotes(self) -> dict[str, dict[str, Quote]]:
        loaded_quotes = await asyncio.gather(
            *(self._quote_storage.get_quotes(exchange) for exchange in self._exchanges)
        )
        return {
            exchange: quotes
            for exchange, quotes in zip(self._exchanges, loaded_quotes, strict=True)
        }

    def _calculate_spreads(
        self,
        quotes_by_exchange: Mapping[str, Mapping[str, Quote]],
    ) -> dict[tuple[str, str], dict[str, Spread]]:
        spreads_by_pair: dict[tuple[str, str], dict[str, Spread]] = {}
        for exchange_buy in self._exchanges:
            for exchange_sell in self._exchanges:
                if exchange_buy == exchange_sell:
                    continue

                pair_key = (exchange_buy, exchange_sell)
                pair_spreads: dict[str, Spread] = {}
                buy_quotes = quotes_by_exchange.get(exchange_buy, {})
                sell_quotes = quotes_by_exchange.get(exchange_sell, {})

                common_asset_ids = set(buy_quotes) & set(sell_quotes)
                for asset_id in common_asset_ids:
                    quote_buy = buy_quotes[asset_id]
                    quote_sell = sell_quotes[asset_id]
                    if quote_sell.bid <= quote_buy.ask:
                        continue

                    try:
                        pair_spreads[asset_id] = create_spread(
                            quote_buy=quote_buy,
                            exchange_buy=exchange_buy,
                            quote_sell=quote_sell,
                            exchange_sell=exchange_sell,
                        )
                    except Exception as exc:
                        logger.warning(
                            "Failed to create spread: pair=%s->%s asset_id=%s error=%s",
                            exchange_buy,
                            exchange_sell,
                            asset_id,
                            exc,
                        )

                spreads_by_pair[pair_key] = pair_spreads

        return spreads_by_pair
