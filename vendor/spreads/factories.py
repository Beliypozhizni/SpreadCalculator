from quotes.schema import Quote
from quotes.utils import now_ms

from .schema import Spread
from .utils import calculate_spread, ensure_quotes_compatible, normalize_exchange


def create_spread(
    *,
    quote_buy: Quote,
    quote_sell: Quote,
    exchange_buy: str,
    exchange_sell: str,
    ts_found: int | None = None,
    ts_calculated: int | None = None,
) -> Spread:
    ensure_quotes_compatible(quote_buy, quote_sell)

    normalized_exchange_buy = normalize_exchange(exchange_buy, "exchange_buy")
    normalized_exchange_sell = normalize_exchange(exchange_sell, "exchange_sell")
    spread_absolute, spread_percent = calculate_spread(quote_buy.ask, quote_sell.bid)
    resolved_ts_calculated = ts_calculated if ts_calculated is not None else now_ms()
    resolved_ts_found = ts_found if ts_found is not None else resolved_ts_calculated

    return Spread(
        asset_id=quote_buy.asset_id,
        address=quote_buy.address,
        network=quote_buy.network,
        exchange_buy=normalized_exchange_buy,
        exchange_sell=normalized_exchange_sell,
        name_buy=quote_buy.name,
        name_sell=quote_sell.name,
        price_buy=quote_buy.ask,
        price_sell=quote_sell.bid,
        spread_absolute=spread_absolute,
        spread_percent=spread_percent,
        ts_found=resolved_ts_found,
        ts_calculated=resolved_ts_calculated,
    )
