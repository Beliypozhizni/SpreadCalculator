from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


def _get_optional(name: str) -> str | None:
    value = os.getenv(name)
    return value if value else None


def _get_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return int(value) if value else default


def _get_exchanges(name: str) -> tuple[str, ...]:
    raw_value = os.getenv(name, "")
    exchanges = tuple(item.strip().lower() for item in raw_value.split(",") if item.strip())
    if not exchanges:
        raise ValueError(f"Environment variable {name} must contain at least one exchange")
    return exchanges


@dataclass(frozen=True, slots=True)
class RedisConfig:
    host: str
    port: int
    db: int
    username: str | None
    password: str | None
    decode_responses: bool = True


@dataclass(frozen=True, slots=True)
class AppConfig:
    exchanges: tuple[str, ...]
    quote_key_prefix: str
    quote_events_key: str
    quote_updated_type: str
    spread_key_prefix: str
    spread_events_key: str
    stream_block_ms: int
    stream_batch_size: int
    stream_max_len: int
    input_redis: RedisConfig
    output_redis: RedisConfig


def load_config(env_file: str = ".env") -> AppConfig:
    env_path = Path(env_file)
    if env_path.exists():
        load_dotenv(env_path)

    return AppConfig(
        exchanges=_get_exchanges("SPREAD_EXCHANGES"),
        quote_key_prefix=os.getenv("QUOTE_KEY_PREFIX", "quotes"),
        quote_events_key=os.getenv("QUOTE_EVENTS_KEY", "quotes:events"),
        quote_updated_type=os.getenv("QUOTE_UPDATED_TYPE", "quotes_updated"),
        spread_key_prefix=os.getenv("SPREAD_KEY_PREFIX", "spreads"),
        spread_events_key=os.getenv("SPREAD_EVENTS_KEY", "spreads:events"),
        stream_block_ms=_get_int("STREAM_BLOCK_MS", 5000),
        stream_batch_size=_get_int("STREAM_BATCH_SIZE", 100),
        stream_max_len=_get_int("STREAM_MAX_LEN", 10_000),
        input_redis=RedisConfig(
            host=os.getenv("INPUT_REDIS_HOST", "localhost"),
            port=_get_int("INPUT_REDIS_PORT", 6379),
            db=_get_int("INPUT_REDIS_DB", 0),
            username=_get_optional("INPUT_REDIS_USERNAME"),
            password=_get_optional("INPUT_REDIS_PASSWORD"),
        ),
        output_redis=RedisConfig(
            host=os.getenv("OUTPUT_REDIS_HOST", "localhost"),
            port=_get_int("OUTPUT_REDIS_PORT", 6379),
            db=_get_int("OUTPUT_REDIS_DB", 0),
            username=_get_optional("OUTPUT_REDIS_USERNAME"),
            password=_get_optional("OUTPUT_REDIS_PASSWORD"),
        ),
    )
