from quotes.schema import Quote


def normalize_exchange(exchange: str, field_name: str) -> str:
    value = exchange.strip().lower()
    if not value:
        raise ValueError(f"{field_name} must be a non-empty string")
    return value


def ensure_quotes_compatible(quote_buy: Quote, quote_sell: Quote) -> None:
    if quote_buy.asset_id != quote_sell.asset_id:
        raise ValueError("quote_buy.asset_id must match quote_sell.asset_id")
    if quote_buy.address != quote_sell.address:
        raise ValueError("quote_buy.address must match quote_sell.address")
    if quote_buy.network != quote_sell.network:
        raise ValueError("quote_buy.network must match quote_sell.network")


def calculate_spread(price_buy: float, price_sell: float) -> tuple[float, float]:
    if price_buy <= 0:
        raise ValueError("price_buy must be greater than zero")

    spread_absolute = price_sell - price_buy
    spread_percent = (spread_absolute / price_buy) * 100
    return spread_absolute, spread_percent
