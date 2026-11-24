from __future__ import annotations

import math
import time
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from enum import Enum
from typing import Any


SCHEMA_VERSION = 1
GATEWAY_NAME = "LME"
DEFAULT_PUB_PORT = 5625
DEFAULT_REP_PORT = 5626


class Action(str, Enum):
    PING = "ping"
    STATUS = "status"
    SUBSCRIBE_MARKET_DATA = "subscribe_market_data"
    UNSUBSCRIBE_MARKET_DATA = "unsubscribe_market_data"
    GET_ACCOUNT = "get_account"
    GET_POSITIONS = "get_positions"
    GET_ORDERS = "get_orders"
    GET_TRADES = "get_trades"
    PLACE_ORDER = "place_order"
    CANCEL_ORDER = "cancel_order"


TRADING_ACTIONS = frozenset(
    {
        Action.GET_ACCOUNT,
        Action.GET_POSITIONS,
        Action.GET_ORDERS,
        Action.GET_TRADES,
        Action.PLACE_ORDER,
        Action.CANCEL_ORDER,
    }
)


class Event(str, Enum):
    MARKET_DATA = "marketdata"
    STATUS = "status"
    ERROR = "error"


class ErrorCode(str, Enum):
    INVALID_REQUEST = "invalid_request"
    UNSUPPORTED_ACTION = "unsupported_action"
    NOT_IMPLEMENTED = "not_implemented"
    NOT_READY = "not_ready"
    TIMEOUT = "timeout"
    INTERNAL_ERROR = "internal_error"


class ProtocolError(ValueError):
    def __init__(self, message: str, code: ErrorCode = ErrorCode.INVALID_REQUEST):
        super().__init__(message)
        self.code = code


def _looks_like_instrument_id(value: str) -> bool:
    return value.isdigit() and int(value) >= 0


def canonical_symbol(value: Any) -> str:
    """Normalize a subscribe/publish symbol.

    Accepted forms:
    - TAP-style ``LME:F:NI:3M`` (four non-empty segments; trimmed and uppercased)
    - Raw TradableInstrumentID, e.g. ``42`` (no 115 definition feed in this proxy)

    Unmapped instrument IDs are published as the decimal ID string.
    """
    if not isinstance(value, str):
        raise ValueError("symbol must be a string")
    text = value.strip()
    if not text:
        raise ValueError("symbol must not be empty")
    if _looks_like_instrument_id(text):
        return str(int(text))
    parts = [part.strip().upper() for part in text.split(":")]
    if len(parts) != 4 or any(not part for part in parts):
        raise ValueError(
            "symbol must be ExchangeNo:CommodityType:CommodityNo:ContractNo "
            "(for example LME:F:NI:3M) or a numeric TradableInstrumentID"
        )
    return ":".join(parts)


def normalize_symbols(value: Any) -> list[str]:
    if isinstance(value, str):
        values = [item.strip() for item in value.split(",") if item.strip()]
    elif isinstance(value, (list, tuple)):
        values = list(value)
    else:
        raise ProtocolError("symbols must be a string or an array")
    normalized: list[str] = []
    seen: set[str] = set()
    try:
        for item in values:
            symbol = canonical_symbol(item)
            if symbol not in seen:
                seen.add(symbol)
                normalized.append(symbol)
    except ValueError as exc:
        raise ProtocolError(str(exc)) from exc
    return normalized


def market_data_topic(symbol: str) -> str:
    text = str(symbol).strip()
    if not text:
        raise ValueError("symbol must not be empty")
    return f"marketdata.{GATEWAY_NAME}.{text}"


def status_topic() -> str:
    return f"status.{GATEWAY_NAME}"


def errors_topic() -> str:
    return f"errors.{GATEWAY_NAME}"


def parse_action(request: Any) -> Action:
    if not isinstance(request, dict):
        raise ProtocolError("request must be a JSON object")
    value = str(request.get("action", "")).strip().lower()
    if not value:
        raise ProtocolError("Missing required field: action")
    try:
        return Action(value)
    except ValueError as exc:
        raise ProtocolError(
            f"Unsupported action: {value}",
            ErrorCode.UNSUPPORTED_ACTION,
        ) from exc


def require_fields(request: dict[str, Any], *fields: str) -> None:
    missing = [name for name in fields if request.get(name) in (None, "")]
    if missing:
        raise ProtocolError(f"Missing required fields: {', '.join(missing)}")


def normalize_identity(request: dict[str, Any], *fields: str) -> None:
    require_fields(request, *fields)
    for field in fields:
        value = request[field]
        if not isinstance(value, str) or not value.strip():
            raise ProtocolError(f"{field} must be a non-empty string")
        request[field] = value.strip()


def validate_request(request: Any) -> tuple[Action, dict[str, Any]]:
    action = parse_action(request)
    assert isinstance(request, dict)
    normalized = dict(request)
    normalized["action"] = action.value
    if action in {Action.SUBSCRIBE_MARKET_DATA, Action.UNSUBSCRIBE_MARKET_DATA}:
        normalize_identity(normalized, "client_id", "strategy_id")
        require_fields(normalized, "symbols")
        normalized["symbols"] = normalize_symbols(normalized["symbols"])
        if not normalized["symbols"]:
            raise ProtocolError("symbols must contain at least one symbol")
    return action, normalized


def json_value(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return json_value(asdict(value))
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_value(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def response_ok(data: Any = None, request_id: str | None = None) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "ok",
        "request_id": request_id,
        "data": json_value(data),
        "error": None,
    }


def response_error(
    message: str,
    request_id: str | None = None,
    *,
    code: ErrorCode = ErrorCode.INVALID_REQUEST,
    retryable: bool = False,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "error",
        "request_id": request_id,
        "data": None,
        "error": {
            "code": code.value,
            "message": message,
            "retryable": retryable,
        },
    }


def event_payload(event: str | Event, data: Any) -> dict[str, Any]:
    event_value = event.value if isinstance(event, Event) else str(event).strip()
    if not event_value:
        raise ValueError("event must not be empty")
    return {
        "schema_version": SCHEMA_VERSION,
        "event": event_value,
        "published_at": int(time.time() * 1000),
        "data": json_value(data),
    }
