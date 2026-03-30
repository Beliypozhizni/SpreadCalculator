from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Literal

from quotes import Quote
from spreads import Spread, create_spread
from src.storage.quote_storage import QuoteStorage
from src.storage.spread_storage import SpreadStorage
from src.utils.logger import logger


class SpreadService:
    def __init__(
        self,
        exchanges: tuple[str, ...],
        quote_updated_type: str,
        spread_match_mode: Literal["address_network", "address"],
        quote_storage: QuoteStorage,
        spread_storage: SpreadStorage,
    ) -> None:
        self._exchanges = exchanges
        self._quote_updated_type = quote_updated_type.strip().lower()
        normalized_match_mode = spread_match_mode.strip().lower()
        if normalized_match_mode not in {"address_network", "address"}:
            raise ValueError("spread_match_mode must be one of: address_network, address")
        self._spread_match_mode: Literal["address_network", "address"] = normalized_match_mode
        self._quote_storage = quote_storage
        self._spread_storage = spread_storage

    async def recalculate_all(self) -> int:
        quotes_by_exchange = await self._load_quotes()
        exchange_pairs = {
            (exchange_buy, exchange_sell)
            for exchange_buy in self._exchanges
            for exchange_sell in self._exchanges
            if exchange_buy != exchange_sell
        }
        previous_ts_found_by_pair = await self._spread_storage.get_ts_found_by_pair(exchange_pairs)
        spreads_by_pair = self._calculate_spreads(
            quotes_by_exchange,
            previous_ts_found_by_pair=previous_ts_found_by_pair,
        )
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
            trigger_count = sum(1 for _, payload in events if self._is_batch_update_event(payload))
            if trigger_count == 0:
                logger.debug(
                    "Received %s quote events, no batch event with type=%s",
                    len(events),
                    self._quote_updated_type,
                )
                continue

            logger.debug(
                "Received %s quote events (%s matched type=%s), recalculating spreads",
                len(events),
                trigger_count,
                self._quote_updated_type,
            )
            try:
                await self.recalculate_all()
            except Exception:
                logger.exception("Spread recalculation failed after quote event batch")

    def _is_batch_update_event(self, payload: Mapping[str, str]) -> bool:
        event_type = payload.get("type")
        if not isinstance(event_type, str):
            return False
        return event_type.strip().lower() == self._quote_updated_type

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
        previous_ts_found_by_pair: Mapping[tuple[str, str], Mapping[str, int]] | None = None,
    ) -> dict[tuple[str, str], dict[str, Spread]]:
        spreads_by_pair: dict[tuple[str, str], dict[str, Spread]] = {}
        for exchange_buy in self._exchanges:
            for exchange_sell in self._exchanges:
                if exchange_buy == exchange_sell:
                    continue

                pair_key = (exchange_buy, exchange_sell)
                pair_spreads: dict[str, Spread] = {}
                previous_ts_found = (
                    previous_ts_found_by_pair.get(pair_key, {})
                    if previous_ts_found_by_pair is not None
                    else {}
                )
                buy_quotes = quotes_by_exchange.get(exchange_buy, {})
                sell_quotes = quotes_by_exchange.get(exchange_sell, {})
                if self._spread_match_mode == "address_network":
                    common_asset_ids = set(buy_quotes) & set(sell_quotes)
                    for asset_id in common_asset_ids:
                        quote_buy = buy_quotes[asset_id]
                        quote_sell = sell_quotes[asset_id]
                        self._add_spread(
                            pair_spreads=pair_spreads,
                            previous_ts_found=previous_ts_found,
                            quote_buy=quote_buy,
                            exchange_buy=exchange_buy,
                            quote_sell=quote_sell,
                            exchange_sell=exchange_sell,
                        )
                else:
                    buy_quotes_by_address = self._index_quotes_by_address_for_buy(buy_quotes)
                    sell_quotes_by_address = self._index_quotes_by_address_for_sell(sell_quotes)
                    common_addresses = set(buy_quotes_by_address) & set(sell_quotes_by_address)
                    for address in common_addresses:
                        quote_buy = buy_quotes_by_address[address]
                        quote_sell = sell_quotes_by_address[address]
                        self._add_spread(
                            pair_spreads=pair_spreads,
                            previous_ts_found=previous_ts_found,
                            quote_buy=quote_buy,
                            exchange_buy=exchange_buy,
                            quote_sell=quote_sell,
                            exchange_sell=exchange_sell,
                        )

                spreads_by_pair[pair_key] = pair_spreads

        return spreads_by_pair

    @staticmethod
    def _index_quotes_by_address_for_buy(quotes: Mapping[str, Quote]) -> dict[str, Quote]:
        indexed_quotes: dict[str, Quote] = {}
        for quote in quotes.values():
            address_key = quote.address.strip().lower()
            existing = indexed_quotes.get(address_key)
            if existing is None or quote.ask < existing.ask:
                indexed_quotes[address_key] = quote
        return indexed_quotes

    @staticmethod
    def _index_quotes_by_address_for_sell(quotes: Mapping[str, Quote]) -> dict[str, Quote]:
        indexed_quotes: dict[str, Quote] = {}
        for quote in quotes.values():
            address_key = quote.address.strip().lower()
            existing = indexed_quotes.get(address_key)
            if existing is None or quote.bid > existing.bid:
                indexed_quotes[address_key] = quote
        return indexed_quotes

    @staticmethod
    def _add_spread(
        pair_spreads: dict[str, Spread],
        previous_ts_found: Mapping[str, int],
        quote_buy: Quote,
        exchange_buy: str,
        quote_sell: Quote,
        exchange_sell: str,
    ) -> None:
        if quote_sell.bid <= quote_buy.ask:
            return

        asset_id = quote_buy.asset_id
        try:
            pair_spreads[asset_id] = create_spread(
                quote_buy=quote_buy,
                exchange_buy=exchange_buy,
                quote_sell=quote_sell,
                exchange_sell=exchange_sell,
                ts_found=previous_ts_found.get(asset_id),
            )
        except Exception as exc:
            logger.warning(
                "Failed to create spread: pair=%s->%s asset_id=%s error=%s",
                exchange_buy,
                exchange_sell,
                asset_id,
                exc,
            )
