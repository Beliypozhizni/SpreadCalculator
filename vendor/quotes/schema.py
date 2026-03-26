from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from .utils import compact_json


class Quote(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        str_strip_whitespace=True,
        allow_inf_nan=False,
    )

    asset_id: str
    name: str
    address: str
    network: str
    bid: float = Field(ge=0)
    ask: float = Field(gt=0)
    last: float
    withdraw_status: bool
    deposit_status: bool
    ts_exchange: int = Field(ge=0)
    ts_written: int = Field(ge=0)

    @field_validator("asset_id", "name", "address", "network")
    @classmethod
    def _validate_non_empty_string(cls, value: str) -> str:
        if not value:
            raise ValueError("string field must be non-empty")
        return value

    def to_dict(self) -> dict[str, str | float | int | bool]:
        return self.model_dump(mode="python")

    def to_json(self) -> str:
        return compact_json(self.to_dict())

    def to_event(self, exchange: str) -> dict[str, str | int]:
        normalized_exchange = exchange.strip().lower()
        if not normalized_exchange:
            raise ValueError("exchange must be non-empty")

        return {
            "type": "quote_upserted",
            "exchange": normalized_exchange,
            "asset_id": self.asset_id,
            "quote": self.to_json(),
            "ts_exchange": self.ts_exchange,
            "ts_written": self.ts_written,
        }

    @property
    def bid_decimal(self) -> Decimal:
        return Decimal(str(self.bid))

    @property
    def ask_decimal(self) -> Decimal:
        return Decimal(str(self.ask))
