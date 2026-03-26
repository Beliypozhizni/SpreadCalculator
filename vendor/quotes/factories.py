import json
from typing import Any, Mapping

from .schema import Quote
from .utils import as_bool, as_float, as_int, now_ms, require_non_empty


def create_quote(
    *,
    asset_id: str,
    name: str,
    address: str,
    network: str,
    bid: float,
    ask: float,
    last: float,
    withdraw_status: bool,
    deposit_status: bool,
    ts_exchange: int,
    ts_written: int | None = None,
) -> Quote:
    return Quote(
        asset_id=require_non_empty(asset_id, "asset_id"),
        name=require_non_empty(name, "name"),
        address=require_non_empty(address, "address"),
        network=require_non_empty(network, "network"),
        bid=float(bid),
        ask=float(ask),
        last=float(last),
        withdraw_status=as_bool(withdraw_status, "withdraw_status"),
        deposit_status=as_bool(deposit_status, "deposit_status"),
        ts_exchange=int(ts_exchange),
        ts_written=ts_written if ts_written is not None else now_ms(),
    )


def quote_from_payload(payload: str | Mapping[str, Any], *, default_asset_id: str | None = None) -> Quote:
    data = _parse_payload(payload)
    resolved_asset_id = data.get("asset_id", default_asset_id)
    if resolved_asset_id is None:
        raise ValueError("asset_id is required in payload or as default_asset_id")

    return create_quote(
        asset_id=str(resolved_asset_id),
        name=str(data["name"]),
        address=str(data["address"]),
        network=str(data["network"]),
        bid=as_float(data.get("bid"), "bid"),
        ask=as_float(data.get("ask"), "ask"),
        last=as_float(data.get("last"), "last"),
        withdraw_status=as_bool(data.get("withdraw_status"), "withdraw_status"),
        deposit_status=as_bool(data.get("deposit_status"), "deposit_status"),
        ts_exchange=as_int(data.get("ts_exchange"), "ts_exchange"),
        ts_written=as_int(data.get("ts_written"), "ts_written")
        if data.get("ts_written") is not None
        else None,
    )


def quote_from_redis(asset_id: str, payload: str | Mapping[str, Any]) -> Quote:
    return quote_from_payload(payload, default_asset_id=asset_id)


def _parse_payload(payload: str | Mapping[str, Any]) -> Mapping[str, Any]:
    if isinstance(payload, str):
        parsed = json.loads(payload)
    else:
        parsed = dict(payload)

    if not isinstance(parsed, dict):
        raise ValueError("payload must represent a JSON object")
    return parsed
