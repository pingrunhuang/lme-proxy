from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Callable, Iterator

from config import FeedConfig
from loguru import logger

NULL_I64 = -(2**63)
NULL_U32 = 0xFFFFFFFF
NULL_U64 = 0xFFFFFFFFFFFFFFFF
UINT32_MAX = 0xFFFFFFFF
MAX_DEPTH = 64
HISTORY_SLOT = 16384
GAP_TIMEOUT_NS = 2_000_000

ROLE_INCREMENTAL = 0
ROLE_SNAPSHOT = 1
ROLE_DEFINITION = 2
ROLE_DR = 3

SIDE_BID = 0
SIDE_ASK = 1
SIDE_IMPLIED_BID = 2
SIDE_IMPLIED_ASK = 3

FLAG_BOOK_VALID = 1
FLAG_METADATA_READY = 2
FLAG_STREAM_CONTIGUOUS = 4
FLAG_RECOVERED = 8
FLAG_REPLAYED = 16
FLAG_EVENT_COMPLETE = 32

EVENT_STATUS = "status"
EVENT_GAP = "gap"
EVENT_BOOK = "book"
EVENT_BOOK_DELTA = "book_delta"
EVENT_TRADE = "trade"
EVENT_REFERENCE_PRICE = "reference_price"
EVENT_RAW = "raw"
EVENT_TRADING_STATUS = "trading_status"
EVENT_INDICATIVE = "indicative"
EVENT_QUOTE_REQUEST = "quote_request"

CODE_OK = 0
CODE_EINVAL = -1
CODE_EPARSE = -4
CODE_ECAPACITY = -6

PROFILE_ORDER_BOOK = "order_book"
PROFILE_TRADE_REPLAY = "trade_replay"


class ParseError(ValueError):
    pass


def flag_names(flags: int) -> list[str]:
    names = []
    for bit, name in (
        (FLAG_BOOK_VALID, "BOOK_VALID"),
        (FLAG_METADATA_READY, "METADATA_READY"),
        (FLAG_STREAM_CONTIGUOUS, "STREAM_CONTIGUOUS"),
        (FLAG_RECOVERED, "RECOVERED"),
        (FLAG_REPLAYED, "REPLAYED"),
        (FLAG_EVENT_COMPLETE, "EVENT_COMPLETE"),
    ):
        if flags & bit:
            names.append(name)
    return names


class Bytes:
    __slots__ = ("data",)

    def __init__(self, data: bytes | memoryview, start: int = 0, size: int | None = None) -> None:
        view = memoryview(data)
        if size is None:
            self.data = view[start:]
        else:
            self.data = view[start : start + size]

    def __len__(self) -> int:
        return len(self.data)

    @property
    def n(self) -> int:
        return len(self.data)

    def require(self, offset: int, count: int) -> None:
        if offset < 0 or count < 0 or offset > self.n or count > self.n - offset:
            raise ParseError("truncated message")

    def u8(self, offset: int) -> int:
        self.require(offset, 1)
        return self.data[offset]

    def u16(self, offset: int) -> int:
        self.require(offset, 2)
        return self.data[offset] | (self.data[offset + 1] << 8)

    def u32(self, offset: int) -> int:
        return self.u16(offset) | (self.u16(offset + 2) << 16)

    def u64(self, offset: int) -> int:
        return self.u32(offset) | (self.u32(offset + 4) << 32)

    def i64(self, offset: int) -> int:
        u = self.u64(offset)
        if u >= 2**63:
            return u - 2**64
        return u

    def sub(self, offset: int, count: int) -> "Bytes":
        self.require(offset, count)
        return Bytes(self.data, offset, count)

    def raw(self) -> bytes:
        return self.data.tobytes()

    def text(self, offset: int, count: int) -> str:
        self.require(offset, count)
        data = bytes(self.data[offset : offset + count])
        return data.decode("ascii", "replace").rstrip(" \x00")


class LmePacketView:
    """16-byte little-endian LMEsource header plus per-message MsgSize/MsgType."""

    def __init__(self, data: bytes) -> None:
        self.bytes = Bytes(data)
        self.bytes.require(0, 16)
        if self.bytes.u16(0) != self.bytes.n:
            raise ParseError("LME PktSize mismatch")
        self.count = self.bytes.u8(2)
        self.sequence = self.bytes.u32(4)
        self.send_time = self.bytes.u64(8)
        if self.sequence + self.count > UINT32_MAX + 1:
            raise ParseError("LME sequence overflow without reset")
        offset = 16
        for _ in range(self.count):
            size = self.bytes.u16(offset)
            if size < 4:
                raise ParseError("invalid LME MsgSize")
            self.bytes.sub(offset, size)
            offset += size
        if offset != self.bytes.n:
            raise ParseError("LME message count does not match packet length")

    def messages(self) -> Iterator[tuple[int, int, Bytes, int]]:
        offset = 16
        for index in range(self.count):
            size = self.bytes.u16(offset)
            message = self.bytes.sub(offset, size)
            yield self.sequence + index, index, message, self.send_time
            offset += size


@dataclass
class Datagram:
    feed_id: int
    role: int
    data: bytes
    receive_time_ns: int = 0
    flags: int = 0
    line: int = 0


@dataclass
class DecimalValue:
    mantissa: int = 0
    exponent: int = 0
    valid: bool = False


def decimal(mantissa: int, exponent: int, valid: bool = True) -> DecimalValue:
    return DecimalValue(mantissa=mantissa, exponent=exponent, valid=bool(valid))


@dataclass
class Level:
    price: DecimalValue = field(default_factory=DecimalValue)
    quantity: int = 0
    order_count: int = 0
    quantity_valid: bool = False
    order_count_valid: bool = False


@dataclass
class TradeData:
    price: DecimalValue = field(default_factory=DecimalValue)
    quantity: int = 0
    trade_id: int = 0
    quantity_valid: bool = False
    trade_id_valid: bool = False
    action: int = 0


@dataclass
class IndicativeData:
    price: DecimalValue = field(default_factory=DecimalValue)
    mid_price: DecimalValue = field(default_factory=DecimalValue)
    volume: int = 0


@dataclass
class QuoteRequestData:
    volume: int = 0
    request_type: int = 0
    side: int = 0


@dataclass
class ReferencePriceData:
    price: DecimalValue = field(default_factory=DecimalValue)
    business_date: int = 0
    commodity: str = ""
    session: str = ""
    currency: str = ""
    price_type: str = ""
    bid_volume: int = 0
    ask_volume: int = 0


@dataclass
class StatusData:
    code: int = 0
    text: str = ""
    expected: int = 0
    received: int = 0


@dataclass
class DeltaData:
    level: Level = field(default_factory=Level)
    position: int = 0
    side: int = 0
    action: int = 0


@dataclass
class Event:
    type: str
    feed_id: int = 0
    channel_id: int = 0
    flags: int = 0
    message_type: int = 0
    epoch: int = 1
    instrument_id: int = 0
    sequence: int = 0
    event_time_ns: int = 0
    send_time_ns: int = 0
    receive_time_ns: int = 0
    report_sequence: int = 0
    message_index: int = 0
    book_sides: list[list[Level]] | None = None
    book_depth: int = 0
    delta: DeltaData | None = None
    trade: TradeData | None = None
    indicative: IndicativeData | None = None
    quote_request: QuoteRequestData | None = None
    reference_price: ReferencePriceData | None = None
    status: StatusData | None = None
    raw: bytes | None = None
    raw_schema_version: int = 0


@dataclass
class QueuedMessage:
    data: bytes
    send: int = 0
    receive: int = 0
    flags: int = 0
    index: int = 0
    seq: int = 0


class Book:
    def __init__(self) -> None:
        self.levels: list[list[Level]] = [[] for _ in range(4)]
        self.time: int = 0
        self.valid: bool = False
        self.metadata: bool = False
        self.dirty: bool = False
        self.rpt: int = 0

    def counts(self, side: int) -> int:
        return len(self.levels[side])

    def clear(self) -> None:
        self.levels = [[] for _ in range(4)]
        self.valid = False
        self.dirty = False
        self.rpt = 0

    def update(self, side: int, position: int, action: int, value: Level, depth: int) -> None:
        if side >= 4 or position == 0 or position > depth or depth > MAX_DEPTH or action > 5:
            raise ParseError("invalid book side/level/action")
        index = position - 1
        levels = self.levels[side]
        count = len(levels)
        if action == 0:
            if index > count:
                raise ParseError("book insert leaves a level gap")
            levels.insert(index, value)
            if len(levels) > depth:
                del levels[depth:]
        elif action == 1:
            if index >= count:
                raise ParseError("book change missing level")
            levels[index] = value
        elif action == 2:
            if index >= count:
                raise ParseError("book delete missing level")
            del levels[index]
        elif action == 3:
            if index >= count:
                raise ParseError("book delete-through missing level")
            self.levels[side] = levels[position:]
        elif action == 4:
            if index >= count:
                raise ParseError("book delete-from missing level")
            del levels[index:]
        else:
            if index > count:
                raise ParseError("book overlay leaves gap")
            if index == count:
                levels.append(value)
            else:
                levels[index] = value
        self.dirty = True

    def snapshot_level(self, side: int, position: int, value: Level, depth: int) -> None:
        if side >= 4 or not position or position > depth:
            raise ParseError("invalid snapshot level")
        levels = self.levels[side]
        while len(levels) < position:
            levels.append(Level())
        if levels[position - 1].price.valid:
            raise ParseError("duplicate snapshot level")
        levels[position - 1] = value

    def validate_snapshot(self) -> None:
        for side in range(4):
            for level in self.levels[side]:
                if not level.price.valid:
                    raise ParseError("snapshot contains level gap")


@dataclass
class HistoryRecord:
    seq: int
    data: bytes
    received: int = 0
    sent: int = 0
    flags: int = 0
    index: int = 0


class History:
    def __init__(self, budget: int) -> None:
        self._slots = max(1, budget // HISTORY_SLOT)
        if budget > 0 and budget < HISTORY_SLOT:
            raise RuntimeError("recovery buffer too small")
        if budget <= 0:
            self._slots = 64
        self._items: list[HistoryRecord] = []

    def clear(self) -> None:
        self._items.clear()

    def empty(self) -> bool:
        return not self._items

    def first(self) -> int:
        return self._items[0].seq if self._items else 0

    def last(self) -> int:
        return self._items[-1].seq if self._items else 0

    def push(
        self,
        seq: int,
        data: Bytes,
        received: int,
        sent: int = 0,
        flags: int = 0,
        message_index: int = 0,
    ) -> None:
        raw = data.raw()
        if len(raw) > HISTORY_SLOT:
            raise ParseError("datagram exceeds recovery slot limit (16384)")
        if len(self._items) == self._slots:
            self._items.pop(0)
        self._items.append(
            HistoryRecord(
                seq=seq,
                data=raw,
                received=received,
                sent=sent,
                flags=flags,
                index=message_index,
            )
        )

    def __iter__(self) -> Iterator[tuple[HistoryRecord, Bytes]]:
        for record in self._items:
            yield record, Bytes(record.data)


Emit = Callable[[Event], None]


def quantity(value: int) -> int:
    if value > 2**63 - 1:
        raise ParseError("unsigned quantity exceeds int64 range")
    return value


def level_from_message(message: Bytes, pos: int, implied: bool) -> Level:
    out = Level()
    price = message.i64(pos + 8)
    out.price = decimal(price, -6, price != NULL_I64)
    volume = message.u64(pos + (32 if implied else 20))
    orders = message.u32(pos + (28 if implied else 16))
    if volume != NULL_U64:
        out.quantity = quantity(volume)
        out.quantity_valid = True
    if orders != NULL_U32:
        if orders > 2**31 - 1:
            raise ParseError("LME order count exceeds int32 range")
        out.order_count = orders
        out.order_count_valid = True
    return out


def apply_book(message: Bytes, book: Book, depth: int, snapshot: bool) -> None:
    for i in range(message.u8(22)):
        pos = 23 + i * 43
        side = SIDE_BID if message.u8(pos + 40) == ord("B") else SIDE_ASK
        position = message.u8(pos + 41)
        action = message.u8(pos + 42)
        if position > depth:
            raise ParseError("LME price level exceeds configured depth")
        explicit = level_from_message(message, pos, False)
        implied = level_from_message(message, pos, True)
        if snapshot and action == 0:
            book.snapshot_level(side, position, explicit, depth)
            book.snapshot_level(side + 2, position, implied, depth)
        else:
            book.update(side, position, action, explicit, depth)
            book.update(side + 2, position, action, implied, depth)
    book.time = message.u64(6)


def validate_core_message(message: Bytes, profile: str) -> None:
    typ = message.u16(2)
    expected = 0
    if typ in (100, 203):
        expected = 8
    elif typ == 341:
        message.require(0, 63)
        expected = 63 + message.u16(57) * 21
    elif typ == 311:
        expected = 46
    elif typ in (312, 320):
        expected = 44
    elif typ == 322:
        message.require(0, 23)
        expected = 23 + message.u8(22) * 43
    elif typ == 327:
        expected = 22
    elif typ == 329:
        expected = 30
    else:
        raise ParseError("unsupported LME Core L2 message type")
    if message.n != expected:
        raise ParseError("invalid LME Core L2 message length")
    if profile == PROFILE_TRADE_REPLAY and typ not in (100, 203, 341):
        raise ParseError("unexpected message on LME trade replay channel")
    if typ == 322:
        for i in range(message.u8(22)):
            pos = 23 + i * 43
            side = message.u8(pos + 40)
            if side not in (ord("B"), ord("S")) or message.u8(pos + 41) < 1 or message.u8(pos + 41) > 15 or message.u8(pos + 42) > 2:
                raise ParseError("invalid LME aggregate book side/level/action")
            if message.u8(pos + 42) != 2 and message.i64(pos + 8) == NULL_I64:
                raise ParseError("null LME book price on new/change")
    if typ == 341 and message.u8(32) > 1:
        raise ParseError("invalid LME trade cancellation flag")
    if typ == 341 and (message.u16(55) or message.u16(57) or message.u32(59)):
        raise ParseError("unexpected LME electronic trade leg records")
    if typ in (341, 311, 312, 320, 322, 327, 329):
        venue = 20 if typ == 341 else 4
        if message.u8(venue) != ord("E") or message.u8(venue + 1) != ord("L"):
            raise ParseError("LME Core channel requires Electronic venue")


def decode_electronic_trade(base: Event, message: Bytes) -> Event:
    event = _copy_event(base)
    event.type = EVENT_TRADE
    event.message_type = 341
    event.instrument_id = message.u64(12)
    event.event_time_ns = message.u64(4)
    event.flags |= FLAG_EVENT_COMPLETE
    price = message.i64(33)
    trade = TradeData()
    trade.price = decimal(price, -6, price != NULL_I64)
    volume = message.u32(41)
    if volume != NULL_U32:
        trade.quantity = volume
        trade.quantity_valid = True
    trade.trade_id = message.u64(45)
    trade.trade_id_valid = trade.trade_id != NULL_U64
    trade.action = message.u8(32)
    event.trade = trade
    return event


def _copy_event(base: Event) -> Event:
    return Event(
        type=base.type,
        feed_id=base.feed_id,
        channel_id=base.channel_id,
        flags=base.flags,
        message_type=base.message_type,
        epoch=base.epoch,
        instrument_id=base.instrument_id,
        sequence=base.sequence,
        event_time_ns=base.event_time_ns,
        send_time_ns=base.send_time_ns,
        receive_time_ns=base.receive_time_ns,
        report_sequence=base.report_sequence,
        message_index=base.message_index,
    )


class Adapter:
    def __init__(self, config: FeedConfig, emit: Emit) -> None:
        self.config = config
        self.emit_fn = emit
        self.epoch = 1
        self.contiguous = False
        self.base = Event(type="")
        self.last_error = ""

    def prepare(self, datagram: Datagram) -> None:
        self.base = Event(
            type="",
            feed_id=self.config.id,
            channel_id=self.config.channel_id,
            epoch=self.epoch,
            receive_time_ns=datagram.receive_time_ns,
            flags=datagram.flags & FLAG_REPLAYED,
        )

    def status(self, code: int, message: str, expected: int = 0, received: int = 0) -> None:
        event = _copy_event(self.base)
        event.type = EVENT_GAP if expected or received else EVENT_STATUS
        event.status = StatusData(code=code, text=message, expected=expected, received=received)
        self.emit_fn(event)

    def raw(self, message: Bytes, typ: int, revision: int) -> None:
        if not self.config.raw_messages:
            return
        event = _copy_event(self.base)
        event.type = EVENT_RAW
        event.message_type = typ
        event.raw = message.raw()
        event.raw_schema_version = revision
        self.emit_fn(event)

    def selected(self, instrument_id: int) -> bool:
        if self.config.instrument_ids and instrument_id not in self.config.instrument_ids:
            return False
        return True

    def publish_book(self, instrument_id: int, book: Book, recovered: bool = False) -> None:
        if not self.selected(instrument_id):
            return
        if not book.valid and not self.config.publish_invalid_books:
            return
        event = _copy_event(self.base)
        event.type = EVENT_BOOK
        event.instrument_id = instrument_id
        event.flags = (
            self.base.flags
            | (FLAG_BOOK_VALID if book.valid else 0)
            | (FLAG_METADATA_READY if book.metadata else 0)
            | (FLAG_STREAM_CONTIGUOUS if self.contiguous else 0)
            | FLAG_EVENT_COMPLETE
            | (FLAG_RECOVERED if recovered else 0)
        )
        event.event_time_ns = book.time
        event.report_sequence = book.rpt
        event.book_depth = self.config.depth
        event.book_sides = [list(side) for side in book.levels]
        self.emit_fn(event)
        book.dirty = False

    def feed(self, datagram: Datagram) -> None:
        raise NotImplementedError

    def tick(self, time_ns: int) -> None:
        return

    def invalidate(self, reason: str) -> None:
        raise NotImplementedError

    def capabilities(self) -> set[str]:
        return set()

    def readiness(self) -> dict:
        return {"ready": False}


class CoreL2OrderBook(Adapter):
    def __init__(self, config: FeedConfig, emit: Emit) -> None:
        super().__init__(config, emit)
        self.history = History(config.recovery_buffer_bytes)
        self.books: dict[int, Book] = {}
        self.staged: dict[int, Book] = {}
        self.next = 0
        self.refresh_next = 0
        self.gap_started = 0
        self.last_reset_time = 0
        self.refresh_reset_time = 0
        self.watermark = 0
        self.heartbeat_seq = 0
        self.synced = False
        self.collecting = False
        self.complete = False
        self.saw_heartbeat = False
        self.pending: OrderedDict[int, QueuedMessage] = OrderedDict()
        self.snapshot_events: list[QueuedMessage] = []

    def capabilities(self) -> set[str]:
        return {"raw", "mbp", "trades", "snapshot", "arbitration"}

    def readiness(self) -> dict:
        return {
            "ready": bool(self.synced and not self.pending),
            "synced": self.synced,
            "gap_pending": len(self.pending),
            "next_seq": self.next,
            "watermark": self.watermark,
            "instruments": len(self.books),
        }

    def _book(self, store: dict[int, Book], instrument_id: int) -> Book:
        book = store.get(instrument_id)
        if book is not None:
            return book
        if len(store) >= self.config.max_instruments:
            raise ParseError("LME Core L2 instrument capacity exceeded")
        book = Book()
        store[instrument_id] = book
        return book

    def invalidate(self, reason: str) -> None:
        self.synced = False
        self.contiguous = False
        self.collecting = False
        self.complete = False
        self.staged.clear()
        self.snapshot_events.clear()
        self.history.clear()
        for book in self.books.values():
            book.valid = False
        self.last_error = reason
        self.status(CODE_EPARSE, reason)

    def delta(self, message: Bytes) -> None:
        instrument_id = message.u64(14)
        if not self.selected(instrument_id):
            return
        for i in range(message.u8(22)):
            pos = 23 + i * 43
            side = SIDE_BID if message.u8(pos + 40) == ord("B") else SIDE_ASK
            for implied in range(2):
                event = _copy_event(self.base)
                event.type = EVENT_BOOK_DELTA
                event.message_type = 322
                event.instrument_id = instrument_id
                event.event_time_ns = message.u64(6)
                event.flags |= FLAG_EVENT_COMPLETE
                if self.synced:
                    event.flags |= FLAG_BOOK_VALID
                if self.contiguous:
                    event.flags |= FLAG_STREAM_CONTIGUOUS
                event.delta = DeltaData(
                    level=level_from_message(message, pos, implied != 0),
                    position=message.u8(pos + 41),
                    side=side + implied * 2,
                    action=message.u8(pos + 42),
                )
                self.emit_fn(event)

    def trade(self, message: Bytes) -> None:
        if not self.selected(message.u64(12)):
            return
        event = decode_electronic_trade(self.base, message)
        if self.contiguous:
            event.flags |= FLAG_STREAM_CONTIGUOUS
        self.emit_fn(event)

    def trading_state(self, message: Bytes) -> None:
        typ = message.u16(2)
        if typ == 312 and not self.selected(message.u64(14)):
            return
        event = _copy_event(self.base)
        event.type = EVENT_TRADING_STATUS
        event.message_type = typ
        event.event_time_ns = message.u64(6)
        event.instrument_id = message.u64(14) if typ == 312 else 0
        event.flags |= FLAG_EVENT_COMPLETE
        if typ == 311:
            text = message.text(14, 12)
        else:
            text = f"LME instrument state condition={chr(message.u8(40))}"
        event.status = StatusData(
            code=message.u8(23) if typ == 312 else message.u8(26),
            text=text,
        )
        self.emit_fn(event)

    def indicative(self, message: Bytes) -> None:
        instrument_id = message.u64(14)
        if not self.selected(instrument_id):
            return
        event = _copy_event(self.base)
        event.type = EVENT_INDICATIVE
        event.message_type = 320
        event.instrument_id = instrument_id
        event.event_time_ns = message.u64(6)
        event.flags |= FLAG_EVENT_COMPLETE
        price = message.i64(22)
        mid = message.i64(34)
        event.indicative = IndicativeData(
            price=decimal(price, -6, price != NULL_I64),
            mid_price=decimal(mid, -6, mid != NULL_I64),
            volume=message.u32(30),
        )
        self.emit_fn(event)

    def quote_request(self, message: Bytes) -> None:
        instrument_id = message.u64(14)
        if not self.selected(instrument_id):
            return
        event = _copy_event(self.base)
        event.type = EVENT_QUOTE_REQUEST
        event.message_type = 329
        event.instrument_id = instrument_id
        event.event_time_ns = message.u64(6)
        event.flags |= FLAG_EVENT_COMPLETE
        event.quote_request = QuoteRequestData(
            request_type=message.u8(22),
            side=message.u8(23),
            volume=message.u32(24),
        )
        self.emit_fn(event)

    def process(self, message: Bytes, publish: bool) -> None:
        typ = message.u16(2)
        if typ == 322:
            if self.synced:
                instrument_id = message.u64(14)
                book = self._book(self.books, instrument_id)
                apply_book(message, book, self.config.depth, False)
                book.valid = True
                if publish:
                    self.publish_book(instrument_id, book)
            if publish:
                self.delta(message)
        elif typ == 327:
            if self.synced:
                instrument_id = message.u64(14)
                book = self._book(self.books, instrument_id)
                book.clear()
                book.valid = True
                book.time = message.u64(6)
                if publish:
                    self.publish_book(instrument_id, book)
        elif publish and typ == 341:
            self.trade(message)
        elif publish and typ in (311, 312):
            self.trading_state(message)
        elif publish and typ == 320:
            self.indicative(message)
        elif publish and typ == 329:
            self.quote_request(message)

    def accept(
        self,
        seq: int,
        message: Bytes,
        send: int,
        receive: int,
        flags: int,
        index: int,
    ) -> None:
        self.base.sequence = seq
        self.base.message_index = index
        self.base.message_type = message.u16(2)
        self.base.send_time_ns = send
        self.base.receive_time_ns = receive
        self.base.flags = flags & FLAG_REPLAYED
        self.history.push(seq, message, receive, send, flags, index)
        self.next = seq + 1
        self.contiguous = self.synced
        self.raw(message, message.u16(2), 413)
        self.process(message, True)
        self.try_complete()

    def drain(self) -> None:
        while self.next <= UINT32_MAX:
            queued = self.pending.get(self.next)
            if queued is None:
                break
            seq = self.next
            del self.pending[seq]
            self.accept(seq, Bytes(queued.data), queued.send, queued.receive, queued.flags, queued.index)
        if not self.pending:
            self.gap_started = 0

    def realtime(self, seq: int, message: Bytes, datagram: Datagram, send: int, index: int) -> None:
        if not self.next:
            self.next = seq
        if seq < self.next:
            return
        if seq > self.next:
            if seq in self.pending:
                return
            if len(self.pending) >= 128 or (seq - self.next) > 128:
                self.invalidate("LME Core L2 reorder window exceeded; waiting for refresh")
                self.pending.clear()
                self.next = seq
            else:
                if not self.pending:
                    self.status(CODE_OK, "LME Core L2 gap: awaiting redundant line", self.next, seq)
                    self.gap_started = datagram.receive_time_ns
                self.pending[seq] = QueuedMessage(
                    data=message.raw(),
                    send=send,
                    receive=datagram.receive_time_ns,
                    flags=datagram.flags,
                    index=index,
                    seq=seq,
                )
                return
        self.accept(seq, message, send, datagram.receive_time_ns, datagram.flags, index)
        self.drain()

    def try_complete(self) -> None:
        if not self.complete:
            return
        if (not self.next or (self.next == 1 and self.history.empty())) and self.saw_heartbeat and self.heartbeat_seq == self.watermark:
            self.next = self.watermark + 1
        if not self.next:
            return
        if self.watermark + 1 > self.next:
            self.history.clear()
            self.pending = OrderedDict(
                (seq, item) for seq, item in self.pending.items() if seq > self.watermark
            )
            self.next = self.watermark + 1
            if self.pending:
                self.gap_started = self.base.receive_time_ns
            self.drain()
            if not self.complete:
                return
        if self.pending:
            return
        expected = self.watermark + 1
        contiguous = True
        for record, _ in self.history:
            if record.seq > self.watermark:
                if record.seq != expected:
                    contiguous = False
                expected = record.seq + 1
        if not contiguous or expected != self.next:
            self.invalidate("LME Core L2 refresh cannot bridge realtime history")
            return
        for book in self.staged.values():
            book.validate_snapshot()
        self.books = self.staged
        self.staged = {}
        self.synced = True
        self.contiguous = True
        self.collecting = False
        self.complete = False
        saved = _copy_event(self.base)
        for record, message in self.history:
            if record.seq > self.watermark:
                self.base.sequence = record.seq
                self.base.send_time_ns = record.sent
                self.base.receive_time_ns = record.received
                self.base.message_index = record.index
                self.base.flags = (record.flags & FLAG_REPLAYED) | FLAG_RECOVERED
                self.process(message, False)
        for entry in self.snapshot_events:
            message = Bytes(entry.data)
            self.base.sequence = entry.seq
            self.base.message_type = message.u16(2)
            self.base.send_time_ns = entry.send
            self.base.receive_time_ns = entry.receive
            self.base.message_index = entry.index
            self.base.flags = (entry.flags & FLAG_REPLAYED) | FLAG_RECOVERED
            self.process(message, True)
        self.snapshot_events.clear()
        self.base = saved
        for instrument_id, book in self.books.items():
            book.valid = True
            self.base.sequence = self.watermark
            self.base.message_type = 322
            self.publish_book(instrument_id, book, True)
        self.base = saved
        self.status(CODE_OK, "LME Core L2 refresh synchronized")

    def tick(self, time_ns: int) -> None:
        if self.pending and time_ns >= self.gap_started and time_ns - self.gap_started >= GAP_TIMEOUT_NS:
            restart = next(iter(self.pending))
            self.invalidate("LME Core L2 gap timed out; waiting for refresh")
            self.next = restart
            self.drain()
            if self.pending:
                self.gap_started = time_ns

    def feed(self, datagram: Datagram) -> None:
        self.prepare(datagram)
        packet = LmePacketView(datagram.data)
        self.tick(datagram.receive_time_ns)
        for _, _, message, _ in packet.messages():
            validate_core_message(message, PROFILE_ORDER_BOOK)
        if packet.count == 0:
            logger.debug(
                "112 heartbeat role={} line={} seq={} synced={} complete={} watermark={} next={}",
                "incremental" if datagram.role == ROLE_INCREMENTAL else "snapshot",
                datagram.line,
                packet.sequence,
                self.synced,
                self.complete,
                self.watermark,
                self.next,
            )
            if datagram.role == ROLE_INCREMENTAL:
                self.saw_heartbeat = True
                self.heartbeat_seq = packet.sequence
                self.try_complete()
            return
        for seq, index, message, send in packet.messages():
            typ = message.u16(2)
            self.base.sequence = seq
            self.base.message_index = index
            self.base.send_time_ns = send
            self.base.message_type = typ
            if typ == 100:
                if datagram.role == ROLE_INCREMENTAL:
                    if send <= self.last_reset_time:
                        continue
                    self.last_reset_time = send
                    self.epoch += 1
                    self.base.epoch = self.epoch
                    self.invalidate("LME Core L2 realtime sequence reset")
                    self.pending.clear()
                    self.next = message.u32(4)
                    logger.debug(
                        "112 realtime sequence reset line={} next={}",
                        datagram.line,
                        self.next,
                    )
                elif datagram.role == ROLE_SNAPSHOT:
                    if send <= self.refresh_reset_time:
                        continue
                    self.refresh_reset_time = send
                    self.refresh_next = message.u32(4)
                    self.collecting = False
                    self.complete = False
                    self.staged.clear()
                    self.snapshot_events.clear()
                    logger.debug(
                        "112 snapshot sequence reset line={} refresh_next={}",
                        datagram.line,
                        self.refresh_next,
                    )
                else:
                    raise ParseError("invalid LME Core L2 endpoint role")
                continue
            if datagram.role == ROLE_INCREMENTAL:
                if self.last_reset_time and send < self.last_reset_time:
                    continue
                if typ == 203:
                    raise ParseError("refresh marker on LME realtime channel")
                self.realtime(seq, message, datagram, send, index)
            elif datagram.role == ROLE_SNAPSHOT:
                if self.refresh_reset_time and send < self.refresh_reset_time:
                    continue
                if self.refresh_next and seq < self.refresh_next:
                    continue
                if self.refresh_next and seq > self.refresh_next:
                    self.collecting = False
                    self.complete = False
                    self.staged.clear()
                    self.snapshot_events.clear()
                self.refresh_next = seq + 1
                if typ == 203:
                    if self.collecting:
                        self.watermark = message.u32(4)
                        self.complete = True
                        self.collecting = False
                        logger.debug(
                            "112 refresh cycle complete line={} watermark={} next={} pending={} saw_heartbeat={} heartbeat_seq={} synced={}",
                            datagram.line,
                            self.watermark,
                            self.next,
                            len(self.pending),
                            self.saw_heartbeat,
                            self.heartbeat_seq,
                            self.synced,
                        )
                        self.try_complete()
                    elif not self.complete:
                        self.collecting = True
                        self.staged.clear()
                        self.snapshot_events.clear()
                        logger.debug(
                            "112 refresh cycle started line={} seq={}",
                            datagram.line,
                            seq,
                        )
                elif self.collecting and typ == 322:
                    apply_book(message, self._book(self.staged, message.u64(14)), self.config.depth, True)
                elif self.collecting and typ == 327:
                    self._book(self.staged, message.u64(14)).clear()
                elif self.collecting and typ in (311, 312, 320, 329):
                    if len(self.snapshot_events) >= self.config.max_instruments * 4:
                        raise ParseError("LME Core L2 snapshot status capacity exceeded")
                    self.snapshot_events.append(
                        QueuedMessage(
                            data=message.raw(),
                            send=send,
                            receive=datagram.receive_time_ns,
                            flags=datagram.flags,
                            index=index,
                            seq=seq,
                        )
                    )
                if self.config.raw_messages:
                    self.raw(message, typ, 413)
            else:
                raise ParseError("invalid LME Core L2 endpoint role")


class CoreTradeReplay(Adapter):
    def __init__(self, config: FeedConfig, emit: Emit) -> None:
        super().__init__(config, emit)
        self.next = 0
        self.last_reset_time = 0
        self.collecting = False
        self.contiguous_cycle = False
        self.cycles_completed = 0

    def capabilities(self) -> set[str]:
        return {"raw", "trades", "snapshot", "arbitration"}

    def readiness(self) -> dict:
        return {
            "ready": self.cycles_completed > 0 or self.collecting,
            "collecting": self.collecting,
            "cycles_completed": self.cycles_completed,
            "next_seq": self.next,
        }

    def trade(self, message: Bytes) -> None:
        if not self.selected(message.u64(12)):
            return
        event = decode_electronic_trade(self.base, message)
        event.flags |= FLAG_RECOVERED
        if self.contiguous_cycle:
            event.flags |= FLAG_STREAM_CONTIGUOUS
        self.emit_fn(event)

    def invalidate(self, reason: str) -> None:
        self.collecting = False
        self.contiguous_cycle = False
        self.next = 0
        self.last_error = reason
        self.status(CODE_EPARSE, reason)

    def feed(self, datagram: Datagram) -> None:
        if datagram.role != ROLE_SNAPSHOT:
            raise ParseError("LME 614 requires snapshot endpoint role")
        self.prepare(datagram)
        packet = LmePacketView(datagram.data)
        for _, _, message, _ in packet.messages():
            validate_core_message(message, PROFILE_TRADE_REPLAY)
        for seq, index, message, send in packet.messages():
            typ = message.u16(2)
            self.base.sequence = seq
            self.base.message_index = index
            self.base.message_type = typ
            self.base.send_time_ns = send
            if typ == 100:
                if send <= self.last_reset_time:
                    continue
                self.last_reset_time = send
                self.epoch += 1
                self.base.epoch = self.epoch
                self.collecting = False
                self.next = message.u32(4)
                self.contiguous_cycle = True
                continue
            if self.next and seq < self.next:
                continue
            if self.last_reset_time and send < self.last_reset_time:
                continue
            if self.next and seq > self.next:
                self.contiguous_cycle = False
                self.status(CODE_EPARSE, "LME 614 replay gap", self.next, seq)
            self.next = seq + 1
            if typ == 203:
                if self.collecting:
                    self.collecting = False
                    self.cycles_completed += 1
                    self.status(CODE_OK, "LME 614 trade replay cycle complete")
                else:
                    self.collecting = True
                    self.contiguous_cycle = True
                continue
            self.raw(message, typ, 413)
            if self.collecting and typ == 341:
                self.trade(message)


class LbmaAuctionPrices(Adapter):
    def __init__(self, config: FeedConfig, emit: Emit) -> None:
        super().__init__(config, emit)
        self.history = History(config.recovery_buffer_bytes)
        self.next = 0
        self.refresh_next = 0
        self.gap_started = 0
        self.last_reset_time = 0
        self.live = False
        self.collecting = False
        self.complete = False
        self.watermark = 0
        self.dr_status = 0
        self.refresh_reset_time = 0
        self.dr_time = 0
        self.saw_heartbeat = False
        self.heartbeat_seq = 0
        self.pending: OrderedDict[int, QueuedMessage] = OrderedDict()
        self.staging: list[Event] = []

    def capabilities(self) -> set[str]:
        return {"raw", "reference", "arbitration", "snapshot"}

    def readiness(self) -> dict:
        return {
            "ready": bool(self.live and not self.pending),
            "live": self.live,
            "gap_pending": len(self.pending),
            "next_seq": self.next,
            "watermark": self.watermark,
        }

    def validate_message(self, message: Bytes) -> None:
        typ = message.u16(2)
        if typ in (100, 105, 203):
            if message.n != 8:
                raise ParseError("invalid LME control length")
            return
        if typ != 405:
            raise ParseError("unsupported message on LME LBMA profile")
        message.require(0, 27)
        pos = 27
        for _ in range(message.u16(25)):
            message.require(pos, 3)
            count = message.u16(pos + 1)
            pos += 3
            if count > (message.n - pos) // 11:
                raise ParseError("LME currency group exceeds message")
            for _j in range(count):
                price = message.u64(pos + 3)
                if price != NULL_U64 and price > 2**63 - 1:
                    raise ParseError("LME reference price exceeds int64 range")
                pos += 11
        if pos != message.n:
            raise ParseError("LME 405 trailing bytes")

    def references(self, message: Bytes, snapshot: bool) -> None:
        pos = 27
        for _ in range(message.u16(25)):
            kind = chr(message.u8(pos))
            count = message.u16(pos + 1)
            pos += 3
            for _j in range(count):
                event = _copy_event(self.base)
                event.type = EVENT_REFERENCE_PRICE
                event.message_type = 405
                event.event_time_ns = message.u64(4)
                price = message.u64(pos + 3)
                ref = ReferencePriceData(
                    business_date=message.u32(13),
                    commodity=message.text(17, 2),
                    session=message.text(19, 2),
                    currency=message.text(pos, 3),
                    price_type=kind,
                    price=decimal(0 if price == NULL_U64 else price, -6, price != NULL_U64),
                    bid_volume=message.u16(21),
                    ask_volume=message.u16(23),
                )
                event.reference_price = ref
                pos += 11
                if self.config.commodities and ref.commodity not in self.config.commodities:
                    continue
                if snapshot:
                    if len(self.staging) >= self.config.max_instruments:
                        raise ParseError("LME refresh capacity exceeded")
                    self.staging.append(event)
                else:
                    event.flags |= FLAG_EVENT_COMPLETE
                    if self.live:
                        event.flags |= FLAG_STREAM_CONTIGUOUS
                    self.emit_fn(event)

    def try_complete(self) -> None:
        if not self.complete:
            return
        if (not self.next) and self.saw_heartbeat and (
            self.heartbeat_seq == self.watermark or (not self.watermark and self.heartbeat_seq == 1)
        ):
            self.next = self.watermark + 1
        if not self.next or self.watermark + 1 > self.next:
            return
        if (not self.history.empty()) and self.watermark + 1 < self.history.first():
            self.invalidate("LME refresh predates retained realtime messages")
            return
        expected = self.watermark + 1
        contiguous = True
        for record, _ in self.history:
            if record.seq > self.watermark:
                if record.seq != expected:
                    contiguous = False
                expected = record.seq + 1
        if not contiguous or expected != self.next:
            self.invalidate("LME refresh cannot bridge realtime gap")
            return
        self.live = True
        self.complete = False
        self.collecting = False
        for event in self.staging:
            event.flags |= FLAG_RECOVERED | FLAG_EVENT_COMPLETE
            self.emit_fn(event)
        self.staging.clear()
        saved = _copy_event(self.base)
        for record, message in self.history:
            if record.seq > self.watermark:
                self.base.sequence = record.seq
                self.base.receive_time_ns = record.received
                self.base.send_time_ns = record.sent
                self.base.message_index = record.index
                self.base.flags = (record.flags & FLAG_REPLAYED) | FLAG_RECOVERED
                if message.u16(2) == 405:
                    self.references(message, False)
        self.base = saved
        self.status(CODE_OK, "LME LBMA refresh synchronized")

    def accept(self, seq: int, message: Bytes, send: int, receive: int, flags: int) -> None:
        self.base.sequence = seq
        self.base.send_time_ns = send
        self.base.receive_time_ns = receive
        self.base.flags = flags & FLAG_REPLAYED
        self.history.push(seq, message, receive, send, flags, self.base.message_index)
        self.next = seq + 1
        self.raw(message, message.u16(2), 413)
        if message.u16(2) == 405 and self.live:
            self.references(message, False)

    def drain(self) -> None:
        while self.next <= UINT32_MAX:
            queued = self.pending.get(self.next)
            if queued is None:
                break
            seq = self.next
            del self.pending[seq]
            self.base.message_index = queued.index
            self.accept(seq, Bytes(queued.data), queued.send, queued.receive, queued.flags)
        if not self.pending:
            self.gap_started = 0

    def realtime(self, seq: int, message: Bytes, datagram: Datagram, send: int) -> None:
        if not self.next:
            self.next = seq
        if seq < self.next:
            return
        if seq > self.next:
            if seq in self.pending:
                return
            if len(self.pending) >= 128 or (seq - self.next) > 128:
                self.status(CODE_ECAPACITY, "LME reorder window exceeded", self.next, seq)
                self.invalidate("LME gap requires refresh")
                self.pending.clear()
                self.next = seq
                self.gap_started = 0
            else:
                if not self.pending:
                    self.status(CODE_OK, "LME message gap: awaiting redundant line", self.next, seq)
                    self.gap_started = datagram.receive_time_ns
                self.pending[seq] = QueuedMessage(
                    data=message.raw(),
                    send=send,
                    receive=datagram.receive_time_ns,
                    flags=datagram.flags,
                    index=self.base.message_index,
                    seq=seq,
                )
                return
        self.accept(seq, message, send, datagram.receive_time_ns, datagram.flags)
        self.drain()

    def invalidate(self, reason: str) -> None:
        self.live = False
        self.collecting = False
        self.complete = False
        self.staging.clear()
        self.history.clear()
        self.saw_heartbeat = False
        self.last_error = reason
        self.status(CODE_EPARSE, reason)

    def tick(self, time_ns: int) -> None:
        if self.pending and time_ns >= self.gap_started and time_ns - self.gap_started >= GAP_TIMEOUT_NS:
            self.invalidate("LME gap timed out; waiting for full refresh cycle")
            self.next = next(iter(self.pending))
            self.drain()
            if self.pending:
                self.gap_started = time_ns

    def feed(self, datagram: Datagram) -> None:
        self.prepare(datagram)
        packet = LmePacketView(datagram.data)
        for _, _, message, _ in packet.messages():
            self.validate_message(message)
        self.base.send_time_ns = packet.send_time
        self.base.sequence = packet.sequence
        self.tick(datagram.receive_time_ns)
        if packet.count == 0:
            if datagram.role == ROLE_INCREMENTAL:
                self.saw_heartbeat = True
                self.heartbeat_seq = packet.sequence
                self.try_complete()
            return
        for seq, index, message, send in packet.messages():
            typ = message.u16(2)
            self.base.message_type = typ
            self.base.message_index = index
            self.base.sequence = seq
            if typ == 105:
                if datagram.role != ROLE_DR:
                    raise ParseError("DR signal must use dr endpoint role")
                state = message.u32(4)
                if state not in (1, 2):
                    raise ParseError("invalid LME DR status")
                if send < self.dr_time:
                    continue
                self.dr_time = send
                if state == self.dr_status:
                    continue
                self.dr_status = state
                self.epoch += 1
                self.base.epoch = self.epoch
                self.invalidate(
                    "LME disaster recovery in progress"
                    if state == 1
                    else "LME disaster recovery completed; awaiting refresh"
                )
                self.pending.clear()
                self.gap_started = 0
                self.refresh_next = 0
                self.next = 0
                continue
            if typ == 100:
                if datagram.role == ROLE_SNAPSHOT:
                    if send <= self.refresh_reset_time:
                        continue
                    self.refresh_reset_time = send
                    self.refresh_next = message.u32(4)
                    self.collecting = False
                    self.complete = False
                    self.staging.clear()
                    continue
                if datagram.role != ROLE_INCREMENTAL:
                    raise ParseError("sequence reset on invalid LME role")
                if send <= self.last_reset_time:
                    continue
                self.last_reset_time = send
                self.epoch += 1
                self.base.epoch = self.epoch
                self.invalidate("LME realtime sequence reset")
                self.pending.clear()
                self.gap_started = 0
                self.refresh_next = 0
                self.dr_status = 0
                self.next = message.u32(4)
                continue
            if self.dr_status == 1:
                continue
            if datagram.role == ROLE_INCREMENTAL and self.last_reset_time and send < self.last_reset_time:
                continue
            if datagram.role == ROLE_SNAPSHOT and self.refresh_reset_time and send < self.refresh_reset_time:
                continue
            if datagram.role == ROLE_INCREMENTAL:
                if typ != 405:
                    raise ParseError("unexpected LME realtime control message")
                self.realtime(seq, message, datagram, send)
            elif datagram.role == ROLE_SNAPSHOT:
                if self.live:
                    continue
                if self.refresh_next and seq < self.refresh_next:
                    continue
                if self.refresh_next and seq > self.refresh_next:
                    self.collecting = False
                    self.complete = False
                    self.staging.clear()
                self.refresh_next = seq + 1
                if typ == 203:
                    if self.collecting:
                        self.watermark = message.u32(4)
                        self.complete = True
                        self.collecting = False
                        self.try_complete()
                    elif not self.complete:
                        self.collecting = True
                        self.staging.clear()
                elif typ == 405 and self.collecting:
                    self.references(message, True)
            else:
                raise ParseError("invalid LME endpoint role")
        self.try_complete()


def make_adapter(config: FeedConfig, emit: Emit) -> Adapter:
    if config.channel_id == 307:
        return LbmaAuctionPrices(config, emit)
    if config.channel_id == 112:
        return CoreL2OrderBook(config, emit)
    if config.channel_id == 614:
        return CoreTradeReplay(config, emit)
    raise RuntimeError(f"unsupported LME logical channel {config.channel_id}")


class LmeEngine:
    """Owns one adapter per configured feed and exposes a synchronous feed() API."""

    def __init__(self, feeds: list[FeedConfig], on_event: Emit) -> None:
        self._on_event = on_event
        self.adapters: dict[int, Adapter] = {}
        self.parse_errors = 0
        self.datagrams = 0
        self.events = 0
        self.last_error = ""
        self.transport_started = False
        for config in feeds:
            self.adapters[config.id] = make_adapter(config, self._emit)

    def _emit(self, event: Event) -> None:
        self.events += 1
        self._on_event(event)

    def feed(self, datagram: Datagram) -> None:
        adapter = self.adapters.get(datagram.feed_id)
        if adapter is None:
            raise ParseError(f"unknown feed id {datagram.feed_id}")
        self.datagrams += 1
        try:
            adapter.feed(datagram)
        except ParseError as exc:
            self.parse_errors += 1
            self.last_error = str(exc)
            raise

    def tick(self, time_ns: int) -> None:
        for adapter in self.adapters.values():
            adapter.tick(time_ns)

    def feed_readiness(self) -> dict:
        return {feed_id: adapter.readiness() for feed_id, adapter in self.adapters.items()}

    def is_feed_ready(self) -> bool:
        if not self.adapters:
            return False
        cores = [
            adapter
            for adapter in self.adapters.values()
            if isinstance(adapter, CoreL2OrderBook)
        ]
        if cores:
            return all(item.readiness()["ready"] for item in cores)
        auctions = [
            adapter
            for adapter in self.adapters.values()
            if isinstance(adapter, LbmaAuctionPrices)
        ]
        if auctions:
            return all(item.readiness()["ready"] for item in auctions)
        replays = [
            adapter
            for adapter in self.adapters.values()
            if isinstance(adapter, CoreTradeReplay)
        ]
        if replays:
            return self.transport_started
        return self.transport_started

    def is_ready(self) -> bool:
        return bool(self.transport_started and self.is_feed_ready())

    def has_gap(self) -> bool:
        for adapter in self.adapters.values():
            pending = adapter.readiness().get("gap_pending", 0)
            if pending:
                return True
        return False
