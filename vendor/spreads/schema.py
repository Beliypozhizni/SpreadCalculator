import json

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class Spread(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        str_strip_whitespace=True,
        allow_inf_nan=False,
    )

    asset_id: str
    address: str
    network: str
    exchange_buy: str
    exchange_sell: str
    name_buy: str
    name_sell: str
    price_buy: float = Field(gt=0)
    price_sell: float = Field(ge=0)
    spread_absolute: float
    spread_percent: float
    ts_found: int = Field(ge=0)
    ts_calculated: int = Field(ge=0)

    @field_validator(
        "asset_id",
        "address",
        "network",
        "exchange_buy",
        "exchange_sell",
        "name_buy",
        "name_sell",
    )
    @classmethod
    def _validate_non_empty_string(cls, value: str) -> str:
        if not value:
            raise ValueError("string field must be non-empty")
        return value

    @field_validator("exchange_buy", "exchange_sell")
    @classmethod
    def _normalize_exchange(cls, value: str) -> str:
        return value.lower()

    @model_validator(mode="after")
    def _validate_timestamps(self) -> "Spread":
        if self.ts_calculated < self.ts_found:
            raise ValueError("ts_calculated must be greater than or equal to ts_found")
        return self

    def to_dict(self) -> dict[str, str | float | int]:
        return self.model_dump(mode="python")

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"))

    def to_event(self, action: str = "upsert") -> dict[str, str | int | float]:
        return {
            "action": action,
            "asset_id": self.asset_id,
            "address": self.address,
            "network": self.network,
            "exchange_buy": self.exchange_buy,
            "exchange_sell": self.exchange_sell,
            "name_buy": self.name_buy,
            "name_sell": self.name_sell,
            "spread_absolute": self.spread_absolute,
            "spread_percent": self.spread_percent,
            "ts_found": self.ts_found,
            "ts_calculated": self.ts_calculated,
        }
