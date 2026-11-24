from __future__ import annotations

from decoder import (
    EVENT_BOOK,
    EVENT_TRADE,
    FLAG_BOOK_VALID,
    FLAG_EVENT_COMPLETE,
    DecimalValue,
    Event,
    Level,
    TradeData,
)
from mapper import SymbolResolver, map_market_data, scale_price


def test_scale_price_null_and_exponent():
    assert scale_price(DecimalValue(0, -6, False)) is None
    assert scale_price(DecimalValue(100_000_000, -6, True)) == 100
    assert scale_price(DecimalValue(100_500_000, -6, True)) == 100.5


def test_symbol_resolver_mapping():
    resolver = SymbolResolver({42: "LME:F:NI:3M"})
    assert resolver.resolve(42) == "LME:F:NI:3M"
    assert resolver.resolve(99) == "99"
    assert resolver.matches("LME:F:NI:3M", ["42"])
    assert resolver.matches("42", ["LME:F:NI:3M"])


def test_book_envelope_and_null_prices():
    invalid = Level(price=DecimalValue(0, -6, False), quantity=0, quantity_valid=False)
    bid = Level(price=DecimalValue(101_000_000, -6, True), quantity=10, quantity_valid=True)
    event = Event(
        type=EVENT_BOOK,
        feed_id=112,
        channel_id=112,
        flags=FLAG_BOOK_VALID | FLAG_EVENT_COMPLETE,
        message_type=322,
        instrument_id=42,
        sequence=99,
        event_time_ns=123000,
        book_depth=15,
        book_sides=[[bid], [], [], []],
    )
    resolver = SymbolResolver({42: "LME:F:NI:3M"})
    topic, data = map_market_data(event, resolver)
    assert topic == "marketdata.LME.LME:F:NI:3M"
    assert data["symbol"] == "LME:F:NI:3M"
    assert data["exchange"] == "LME"
    assert data["kind"] == "book"
    assert data["bid_price_1"] == 101
    assert data["bid_volume_1"] == 10
    assert data["ask_price_1"] is None
    assert data["last_price"] is None
    assert data["raw"]["tradable_instrument_id"] == 42
    assert data["raw"]["seq"] == 99
    assert "BOOK_VALID" in data["raw"]["flags"]
    # An invalid book is not published as a fresh tick.
    event.flags = FLAG_EVENT_COMPLETE
    assert map_market_data(event, resolver) is None
    # Explicit null price must not become 0.
    assert scale_price(invalid.price) is None


def test_trade_envelope():
    event = Event(
        type=EVENT_TRADE,
        feed_id=112,
        channel_id=112,
        flags=FLAG_EVENT_COMPLETE,
        message_type=341,
        instrument_id=42,
        sequence=103,
        event_time_ns=123000,
        trade=TradeData(
            price=DecimalValue(101_000_000, -6, True),
            quantity=4,
            trade_id=999,
            quantity_valid=True,
            trade_id_valid=True,
            action=0,
        ),
    )
    topic, data = map_market_data(event, SymbolResolver())
    assert topic == "marketdata.LME.42"
    assert data["kind"] == "trade"
    assert data["last_price"] == 101
    assert data["volume"] == 4
    assert data["raw"]["trade_id"] == 999
    assert data["raw"]["trade_cancel"] is False
