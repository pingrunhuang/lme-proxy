from __future__ import annotations

import json
import time

import zmq

from decoder import FLAG_REPLAYED, ROLE_SNAPSHOT, Datagram
from fixtures import control, lme_book, lme_packet, lme_trade
from helpers import core_feeds, feed_file_for, make_settings
from protocol import ErrorCode
from proxy import LmeProxy


def _sync_and_trade(proxy: LmeProxy, instrument_id: int = 42) -> None:
    proxy.engine.feed(
        Datagram(
            feed_id=112,
            role=0,
            data=lme_packet(100, [lme_book(instrument_id, "B", 1, 1, 100_000_000, 30, 7)]),
            receive_time_ns=1_000_000,
            flags=FLAG_REPLAYED,
        )
    )
    proxy.engine.feed(
        Datagram(
            feed_id=112,
            role=ROLE_SNAPSHOT,
            data=lme_packet(1, [control(203, 98)]),
            receive_time_ns=1_000_000,
            flags=FLAG_REPLAYED,
        )
    )
    proxy.engine.feed(
        Datagram(
            feed_id=112,
            role=ROLE_SNAPSHOT,
            data=lme_packet(
                2,
                [lme_book(instrument_id, "B", 1, 0, 100_000_000, 20, 5), control(203, 99)],
            ),
            receive_time_ns=1_000_000,
            flags=FLAG_REPLAYED,
        )
    )
    proxy.engine.feed(
        Datagram(
            feed_id=112,
            role=0,
            data=lme_packet(101, [lme_trade(instrument_id, 101_000_000, 4, 999)]),
            receive_time_ns=1_000_000,
            flags=FLAG_REPLAYED,
        )
    )


def test_ping_status_and_trading_not_implemented():
    file = feed_file_for(core_feeds())
    file.symbol_map = {42: "LME:F:NI:3M"}
    proxy = LmeProxy(make_settings(), file)
    ping = proxy.handle_command({"action": "ping", "request_id": "p1"})
    assert ping["status"] == "ok"
    assert ping["data"]["service"] == "lme-proxy"
    assert ping["data"]["gateway"] == "LME"
    assert ping["data"]["ready"] is False
    assert ping["data"]["trading_enabled"] is False
    place = proxy.handle_command(
        {
            "action": "place_order",
            "request_id": "o1",
            "client_id": "c",
            "strategy_id": "s",
        }
    )
    assert place["status"] == "error"
    assert place["error"]["code"] == ErrorCode.NOT_IMPLEMENTED.value
    for action in ("cancel_order", "get_account", "get_positions", "get_orders", "get_trades"):
        resp = proxy.handle_command({"action": action, "request_id": action})
        assert resp["error"]["code"] == ErrorCode.NOT_IMPLEMENTED.value


def test_subscribe_refcount():
    file = feed_file_for(core_feeds())
    file.symbol_map = {42: "LME:F:NI:3M"}
    proxy = LmeProxy(make_settings(), file)
    first = proxy.handle_command(
        {
            "action": "subscribe_market_data",
            "request_id": "s1",
            "client_id": "engine-01",
            "strategy_id": "a",
            "symbols": ["LME:F:NI:3M"],
        }
    )
    assert first["data"]["newly_active"] == ["LME:F:NI:3M"]
    second = proxy.handle_command(
        {
            "action": "subscribe_market_data",
            "request_id": "s2",
            "client_id": "engine-01",
            "strategy_id": "b",
            "symbols": ["LME:F:NI:3M"],
        }
    )
    assert second["data"]["newly_active"] == []
    unsub_a = proxy.handle_command(
        {
            "action": "unsubscribe_market_data",
            "request_id": "u1",
            "client_id": "engine-01",
            "strategy_id": "a",
            "symbols": ["LME:F:NI:3M"],
        }
    )
    assert unsub_a["data"]["newly_inactive"] == []
    assert "LME:F:NI:3M" in unsub_a["data"]["active_symbols"]
    unsub_b = proxy.handle_command(
        {
            "action": "unsubscribe_market_data",
            "request_id": "u2",
            "client_id": "engine-01",
            "strategy_id": "b",
            "symbols": ["LME:F:NI:3M"],
        }
    )
    assert unsub_b["data"]["newly_inactive"] == ["LME:F:NI:3M"]
    assert unsub_b["data"]["active_symbols"] == []


def test_zmq_book_and_trade_envelope():
    file = feed_file_for(core_feeds())
    file.symbol_map = {42: "LME:F:NI:3M"}
    proxy = LmeProxy(make_settings(), file)
    proxy.start()
    try:
        proxy.engine.transport_started = True
        proxy.handle_command(
            {
                "action": "subscribe_market_data",
                "request_id": "s1",
                "client_id": "engine-01",
                "strategy_id": "ni",
                "symbols": ["LME:F:NI:3M"],
            }
        )
        context = zmq.Context()
        sub = context.socket(zmq.SUB)
        sub.setsockopt(zmq.LINGER, 0)
        sub.setsockopt(zmq.RCVTIMEO, 2000)
        sub.setsockopt_string(zmq.SUBSCRIBE, "marketdata.LME.")
        sub.setsockopt_string(zmq.SUBSCRIBE, "status.LME")
        sub.connect(f"tcp://127.0.0.1:{proxy.bound_pub_port}")
        time.sleep(0.15)
        _sync_and_trade(proxy)
        topics = []
        bodies = []
        deadline = time.monotonic() + 2
        kinds: dict = {}
        while time.monotonic() < deadline and not {"book", "trade"} <= set(kinds):
            try:
                frames = sub.recv_multipart()
            except zmq.Again:
                continue
            assert len(frames) == 2
            topics.append(frames[0].decode())
            body = json.loads(frames[1])
            assert body["schema_version"] == 1
            assert isinstance(body["published_at"], int)
            bodies.append(body)
            if body.get("event") == "marketdata":
                kinds[body["data"].get("kind")] = body
        assert "book" in kinds
        book = kinds["book"]
        assert book["data"]["symbol"] == "LME:F:NI:3M"
        assert book["data"]["bid_price_1"] == 100
        assert book["data"]["bid_volume_1"] == 30
        assert book["data"]["raw"]["tradable_instrument_id"] == 42
        assert "marketdata.LME.LME:F:NI:3M" in topics
        assert "trade" in kinds
        trade = kinds["trade"]
        assert trade["data"]["last_price"] == 101
        assert trade["data"]["volume"] == 4
        ping = proxy.handle_command({"action": "ping", "request_id": "p2"})
        assert ping["data"]["ready"] is True
        sub.close(0)
        context.term()
    finally:
        proxy.stop()
