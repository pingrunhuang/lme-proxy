"""Map LME decoder events to the frozen TAP/FIU market-data dict."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Iterable

from decoder import (
    EVENT_BOOK,
    EVENT_GAP,
    EVENT_INDICATIVE,
    EVENT_QUOTE_REQUEST,
    EVENT_REFERENCE_PRICE,
    EVENT_STATUS,
    EVENT_TRADE,
    EVENT_TRADING_STATUS,
    FLAG_BOOK_VALID,
    FLAG_RECOVERED,
    FLAG_REPLAYED,
    FLAG_STREAM_CONTIGUOUS,
    SIDE_ASK,
    SIDE_BID,
    SIDE_IMPLIED_ASK,
    SIDE_IMPLIED_BID,
    DecimalValue,
    Event,
    Level,
    flag_names,
)
from protocol import market_data_topic

# Unix nanoseconds that can be formatted as a wall clock (year 2001+).
_PLAUSIBLE_NS = 1_000_000_000_000_000


class SymbolResolver:
    """TradableInstrumentID <-> TAP-style symbol, plus reference-price keys."""

    def __init__(self, mapping: dict[int, str] | None = None) -> None:
        self.by_id: dict[int, str] = dict(mapping or {})
        self.by_symbol: dict[str, int] = {symbol: i for i, symbol in self.by_id.items()}

    def resolve(self, instrument_id: int) -> str:
        if instrument_id in self.by_id:
            return self.by_id[instrument_id]
        return str(instrument_id)

    def aliases(self, symbol: str) -> set[str]:
        names = {symbol}
        if symbol in self.by_symbol:
            names.add(str(self.by_symbol[symbol]))
            names.add(symbol)
        if symbol.isdigit():
            names.add(self.resolve(int(symbol)))
            names.add(str(int(symbol)))
        return names

    def matches(self, published: str, subscribed: Iterable[str]) -> bool:
        published_aliases = self.aliases(published)
        for item in subscribed:
            if published_aliases & self.aliases(item):
                return True
        return False


def scale_price(value: DecimalValue | None) -> int | float | None:
    if value is None or not value.valid:
        return None
    number = Decimal(value.mantissa) * (Decimal(10) ** value.exponent)
    if number == number.to_integral_value():
        return int(number)
    return float(number)


def ns_to_ms(ns: int | None) -> int | None:
    if not ns:
        return 0 if ns == 0 else None
    return int(ns) // 1_000_000


def format_exchange_time(ns: int | None) -> str | None:
    if not ns or ns < _PLAUSIBLE_NS:
        return None
    moment = datetime.fromtimestamp(ns / 1_000_000_000, tz=timezone.utc)
    return moment.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def _level_price(level: Level | None) -> int | float | None:
    return scale_price(level.price) if level is not None else None


def _level_qty(level: Level | None) -> int | None:
    if level is None or not level.quantity_valid:
        return None
    return int(level.quantity)


def _side_fields(prefix: str, levels: list[Level], depth: int) -> dict[str, Any]:
    fields: dict[str, Any] = {}
    count = min(len(levels), depth if depth else len(levels))
    for index in range(count):
        rank = index + 1
        fields[f"{prefix}_price_{rank}"] = _level_price(levels[index])
        fields[f"{prefix}_volume_{rank}"] = _level_qty(levels[index])
        if levels[index].order_count_valid:
            fields[f"{prefix}_orders_{rank}"] = levels[index].order_count
    return fields


def _book_arrays(sides: list[list[Level]]) -> dict[str, Any]:
    def rows(levels: list[Level]) -> list[dict[str, Any]]:
        out = []
        for level in levels:
            out.append(
                {
                    "price": _level_price(level),
                    "volume": _level_qty(level),
                    "orders": level.order_count if level.order_count_valid else None,
                }
            )
        return out

    return {
        "bids": rows(sides[SIDE_BID] if len(sides) > SIDE_BID else []),
        "asks": rows(sides[SIDE_ASK] if len(sides) > SIDE_ASK else []),
        "implied_bids": rows(sides[SIDE_IMPLIED_BID] if len(sides) > SIDE_IMPLIED_BID else []),
        "implied_asks": rows(sides[SIDE_IMPLIED_ASK] if len(sides) > SIDE_IMPLIED_ASK else []),
    }


def reference_symbol(event: Event) -> str:
    ref = event.reference_price
    if ref is None:
        return "LME:R:UNKNOWN"
    return f"LME:R:{ref.commodity}:{ref.session}"


def event_symbol(event: Event, resolver: SymbolResolver) -> str:
    if event.type == EVENT_REFERENCE_PRICE:
        return reference_symbol(event)
    return resolver.resolve(event.instrument_id)


def _common(
    event: Event,
    resolver: SymbolResolver,
    *,
    local_time_ms: int | None = None,
) -> dict[str, Any]:
    symbol = event_symbol(event, resolver)
    event_ms = ns_to_ms(event.event_time_ns)
    receive_ms = ns_to_ms(event.receive_time_ns)
    return {
        "symbol": symbol,
        "exchange": "LME",
        "last_price": None,
        "volume": None,
        "open_interest": None,
        "upper_limit": None,
        "lower_limit": None,
        "bid_price_1": None,
        "bid_volume_1": None,
        "ask_price_1": None,
        "ask_volume_1": None,
        "exchange_time": format_exchange_time(event.event_time_ns),
        "exchange_timestamp": event_ms,
        "local_time": local_time_ms if local_time_ms is not None else receive_ms,
        "raw": {
            "tradable_instrument_id": event.instrument_id,
            "channel_id": event.channel_id,
            "feed_id": event.feed_id,
            "seq": event.sequence,
            "msg_type": event.message_type,
            "message_index": event.message_index,
            "epoch": event.epoch,
            "flags": flag_names(event.flags),
            "event_time_ns": event.event_time_ns,
            "send_time_ns": event.send_time_ns,
            "receive_time_ns": event.receive_time_ns,
            "book_valid": bool(event.flags & FLAG_BOOK_VALID),
            "recovered": bool(event.flags & FLAG_RECOVERED),
            "replayed": bool(event.flags & FLAG_REPLAYED),
            "contiguous": bool(event.flags & FLAG_STREAM_CONTIGUOUS),
        },
    }


def map_market_data(
    event: Event,
    resolver: SymbolResolver,
    *,
    local_time_ms: int | None = None,
) -> tuple[str, dict[str, Any]] | None:
    """Return (topic, data) for a market-data event, or None if it should not be published."""
    if event.type == EVENT_BOOK:
        if not (event.flags & FLAG_BOOK_VALID) and event.book_sides is not None:
            # Invalid / unsynchronized books are not published as fresh ticks.
            return None
        data = _common(event, resolver, local_time_ms=local_time_ms)
        data["kind"] = "book"
        sides = event.book_sides or [[], [], [], []]
        data.update(_side_fields("bid", sides[SIDE_BID] if len(sides) > SIDE_BID else [], event.book_depth))
        data.update(_side_fields("ask", sides[SIDE_ASK] if len(sides) > SIDE_ASK else [], event.book_depth))
        data.update(
            _side_fields(
                "implied_bid",
                sides[SIDE_IMPLIED_BID] if len(sides) > SIDE_IMPLIED_BID else [],
                event.book_depth,
            )
        )
        data.update(
            _side_fields(
                "implied_ask",
                sides[SIDE_IMPLIED_ASK] if len(sides) > SIDE_IMPLIED_ASK else [],
                event.book_depth,
            )
        )
        data.update(_book_arrays(sides))
        data["book_valid"] = bool(event.flags & FLAG_BOOK_VALID)
        return market_data_topic(data["symbol"]), data

    if event.type == EVENT_TRADE and event.trade is not None:
        data = _common(event, resolver, local_time_ms=local_time_ms)
        data["kind"] = "trade"
        data["last_price"] = scale_price(event.trade.price)
        data["volume"] = event.trade.quantity if event.trade.quantity_valid else None
        data["raw"]["trade_id"] = event.trade.trade_id if event.trade.trade_id_valid else None
        data["raw"]["trade_cancel"] = bool(event.trade.action)
        return market_data_topic(data["symbol"]), data

    if event.type == EVENT_INDICATIVE and event.indicative is not None:
        data = _common(event, resolver, local_time_ms=local_time_ms)
        data["kind"] = "indicative"
        data["last_price"] = scale_price(event.indicative.price)
        data["volume"] = event.indicative.volume
        data["raw"]["mid_price"] = scale_price(event.indicative.mid_price)
        return market_data_topic(data["symbol"]), data

    if event.type == EVENT_QUOTE_REQUEST and event.quote_request is not None:
        data = _common(event, resolver, local_time_ms=local_time_ms)
        data["kind"] = "quote_request"
        data["volume"] = event.quote_request.volume
        data["raw"]["request_type"] = event.quote_request.request_type
        side = event.quote_request.side
        data["raw"]["side"] = chr(side) if 32 <= side < 127 else side
        return market_data_topic(data["symbol"]), data

    if event.type == EVENT_TRADING_STATUS and event.status is not None:
        data = _common(event, resolver, local_time_ms=local_time_ms)
        data["kind"] = "trading_status"
        data["raw"]["trading_state"] = event.status.code
        data["raw"]["condition"] = event.status.text
        return market_data_topic(data["symbol"]), data

    if event.type == EVENT_REFERENCE_PRICE and event.reference_price is not None:
        ref = event.reference_price
        data = _common(event, resolver, local_time_ms=local_time_ms)
        data["kind"] = "reference_price"
        data["last_price"] = scale_price(ref.price)
        data["bid_volume_1"] = ref.bid_volume
        data["ask_volume_1"] = ref.ask_volume
        data["raw"].update(
            {
                "business_date": ref.business_date,
                "commodity": ref.commodity,
                "session": ref.session,
                "currency": ref.currency,
                "price_type": ref.price_type,
                "bid_volume": ref.bid_volume,
                "ask_volume": ref.ask_volume,
            }
        )
        # Disambiguate currencies/price types that share LME:R:PT:AM.
        data["symbol"] = (
            f"LME:R:{ref.commodity}:{ref.session}:{ref.currency}:{ref.price_type}"
        )
        return market_data_topic(data["symbol"]), data

    return None


def map_status_or_error(event: Event) -> tuple[str, str, dict[str, Any]] | None:
    """Return (topic, event_name, data) for status/gap events."""
    if event.type not in {EVENT_STATUS, EVENT_GAP} or event.status is None:
        return None
    from protocol import errors_topic, status_topic

    status = event.status
    occurred = ns_to_ms(event.receive_time_ns) or ns_to_ms(event.send_time_ns)
    ready = status.code == 0 and "synchronized" in (status.text or "")
    if event.type == EVENT_GAP or status.code != 0:
        data = {
            "code": "lme_gap" if event.type == EVENT_GAP else "lme_error",
            "message": status.text,
            "retryable": True,
            "occurred_at": occurred,
            "status": "FEED_GAP" if event.type == EVENT_GAP else "BOOK_INVALID",
            "expected": status.expected,
            "received": status.received,
            "feed_id": event.feed_id,
            "channel_id": event.channel_id,
            "seq": event.sequence,
        }
        return errors_topic(), "error", data
    data = {
        "ready": ready,
        "md_ready": ready,
        "td_ready": False,
        "status": "FEED_SYNCHRONIZED" if ready else None,
        "reason": status.text,
        "occurred_at": occurred,
        "feed_id": event.feed_id,
        "channel_id": event.channel_id,
        "seq": event.sequence,
    }
    if data["status"] is None:
        data.pop("status")
    return status_topic(), "status", data
