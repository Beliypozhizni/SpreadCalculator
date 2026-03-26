import json
import time
from typing import Any


def now_ms() -> int:
    return int(time.time() * 1000)


def require_non_empty(value: str, field_name: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must be a non-empty string")
    return normalized


def as_float(value: Any, field_name: str) -> float:
    if value is None:
        raise ValueError(f"{field_name} must be numeric, got None")
    return float(value)


def as_int(value: Any, field_name: str) -> int:
    if value is None:
        raise ValueError(f"{field_name} must be integer-like, got None")
    return int(value)


def as_bool(value: Any, field_name: str) -> bool:
    if isinstance(value, bool):
        return value

    if isinstance(value, (int, float)) and value in (0, 1):
        return bool(value)

    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "y"}:
            return True
        if normalized in {"false", "0", "no", "n"}:
            return False

    raise ValueError(f"{field_name} must be boolean-like")


def compact_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
