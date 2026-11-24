from __future__ import annotations

import pytest

from protocol import (
    Action,
    ErrorCode,
    Event,
    GATEWAY_NAME,
    SCHEMA_VERSION,
    ProtocolError,
    canonical_symbol,
    errors_topic,
    event_payload,
    market_data_topic,
    normalize_symbols,
    response_error,
    response_ok,
    status_topic,
    validate_request,
)


def test_canonical_tap_and_instrument_id():
    assert canonical_symbol("lme:f:ni:3m") == "LME:F:NI:3M"
    assert canonical_symbol(" 42 ") == "42"
    with pytest.raises(ValueError):
        canonical_symbol("NI3M")
    with pytest.raises(ValueError):
        canonical_symbol("LME:F:NI")


def test_normalize_symbols_dedup():
    assert normalize_symbols(["LME:F:NI:3M", "lme:f:ni:3m", "42"]) == [
        "LME:F:NI:3M",
        "42",
    ]


def test_topics():
    assert market_data_topic("LME:F:NI:3M") == "marketdata.LME.LME:F:NI:3M"
    assert status_topic() == "status.LME"
    assert errors_topic() == "errors.LME"
    assert GATEWAY_NAME == "LME"


def test_validate_subscribe_and_ping():
    action, req = validate_request(
        {
            "action": "subscribe_market_data",
            "request_id": "s1",
            "client_id": "engine-01",
            "strategy_id": "ni",
            "symbols": ["LME:F:NI:3M"],
        }
    )
    assert action is Action.SUBSCRIBE_MARKET_DATA
    assert req["symbols"] == ["LME:F:NI:3M"]
    action, req = validate_request({"action": "ping", "request_id": "p1"})
    assert action is Action.PING


def test_validate_errors():
    with pytest.raises(ProtocolError) as exc:
        validate_request({})
    assert exc.value.code is ErrorCode.INVALID_REQUEST
    with pytest.raises(ProtocolError) as exc:
        validate_request({"action": "explode"})
    assert exc.value.code is ErrorCode.UNSUPPORTED_ACTION
    with pytest.raises(ProtocolError):
        validate_request(
            {
                "action": "subscribe_market_data",
                "client_id": "c",
                "strategy_id": "s",
                "symbols": [],
            }
        )


def test_envelopes():
    ok = response_ok({"ready": True}, "r1")
    assert ok["schema_version"] == SCHEMA_VERSION == 1
    assert ok["status"] == "ok"
    assert ok["error"] is None
    err = response_error("nope", "r2", code=ErrorCode.NOT_IMPLEMENTED)
    assert err["status"] == "error"
    assert err["data"] is None
    assert err["error"]["code"] == "not_implemented"
    payload = event_payload(Event.MARKET_DATA, {"symbol": "42"})
    assert payload["schema_version"] == 1
    assert payload["event"] == "marketdata"
    assert isinstance(payload["published_at"], int)
    assert payload["published_at"] > 0
