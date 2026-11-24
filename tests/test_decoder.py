from __future__ import annotations

from decoder import (
    EVENT_BOOK,
    EVENT_INDICATIVE,
    EVENT_QUOTE_REQUEST,
    EVENT_TRADE,
    EVENT_TRADING_STATUS,
    FLAG_BOOK_VALID,
    FLAG_RECOVERED,
    NULL_I64,
    ROLE_SNAPSHOT,
    SIDE_BID,
    SIDE_IMPLIED_BID,
    LmePacketView,
    ParseError,
)
from fixtures import (
    control,
    lme_book,
    lme_clear,
    lme_indicative,
    lme_instrument_state,
    lme_packet,
    lme_quote_request,
    lme_trade,
)
from helpers import Harness


def test_packet_view_rejects_truncated_and_count_mismatch() -> None:
    good = lme_packet(1, [lme_trade(42, 100_000_000, 1, 1)])
    LmePacketView(good)
    for n in range(1, len(good)):
        try:
            LmePacketView(good[:n])
        except ParseError:
            continue
        raise AssertionError(f"expected parse error for length {n}")
    bad = bytearray(good)
    bad[2] = 2
    try:
        LmePacketView(bytes(bad))
        raise AssertionError("expected count mismatch to fail")
    except ParseError:
        pass


def test_core_l2_refresh_and_incremental() -> None:
    h = Harness()
    # Incremental arrives first and is buffered; no book until 612 refresh.
    h.ok(lme_packet(100, [lme_book(42, "B", 1, 1, 100_000_000, 30, 7)]))
    assert h.count(EVENT_BOOK) == 0

    h.ok(lme_packet(1, [control(203, 98)]), 112, ROLE_SNAPSHOT)
    h.ok(
        lme_packet(
            2,
            [
                lme_instrument_state(42, 2, " "),
                lme_book(42, "B", 1, 0, 100_000_000, 20, 5),
                lme_indicative(42, 100_500_000, NULL_I64, 11),
                control(203, 99),
            ],
        ),
        112,
        ROLE_SNAPSHOT,
    )
    assert h.count(EVENT_BOOK) == 1
    book = h.last(EVENT_BOOK)
    assert book.flags & FLAG_BOOK_VALID
    assert book.book_sides[SIDE_BID][0].quantity == 30
    assert book.book_sides[SIDE_IMPLIED_BID][0].quantity == 7
    assert book.book_sides[SIDE_IMPLIED_BID][0].order_count == 3
    assert book.sequence == 99  # 612 message seq is not the 112 watermark
    assert h.count(EVENT_INDICATIVE) == 1
    indicative = h.last(EVENT_INDICATIVE)
    assert indicative.flags & FLAG_RECOVERED
    assert indicative.indicative.price.mantissa == 100_500_000
    assert not indicative.indicative.mid_price.valid
    assert h.last(EVENT_TRADING_STATUS).status.code == 2
    assert h.last(EVENT_TRADING_STATUS).flags & FLAG_RECOVERED

    h.ok(lme_packet(101, [lme_book(42, "B", 1, 0, 101_000_000, 10, 0)]))
    assert len(h.last(EVENT_BOOK).book_sides[SIDE_BID]) == 2
    assert h.last(EVENT_BOOK).book_sides[SIDE_BID][1].quantity == 30

    h.ok(lme_packet(102, [lme_book(42, "B", 2, 2, 100_000_000, 30, 7)]))
    assert len(h.last(EVENT_BOOK).book_sides[SIDE_BID]) == 1

    h.ok(lme_packet(103, [lme_trade(42, 101_000_000, 4, 999)]))
    assert h.last(EVENT_TRADE).trade.trade_id == 999
    assert h.last(EVENT_TRADE).trade.quantity == 4

    h.ok(lme_packet(104, [lme_quote_request(42, 1, "B", 8)]))
    assert h.last(EVENT_QUOTE_REQUEST).quote_request.volume == 8
    assert h.last(EVENT_QUOTE_REQUEST).quote_request.side == ord("B")

    h.ok(lme_packet(105, [lme_clear(42)]))
    assert h.last(EVENT_BOOK).book_sides[SIDE_BID] == []


def test_core_l2_refresh_applies_only_post_watermark_history() -> None:
    h = Harness()
    h.ok(lme_packet(100, [lme_book(42, "B", 1, 0, 100_000_000, 10, 1)]))
    h.ok(lme_packet(102, [lme_book(42, "B", 1, 1, 100_000_000, 20, 2)]))
    h.ok(lme_packet(1, [control(203, 99)]), 112, ROLE_SNAPSHOT)
    h.ok(
        lme_packet(2, [lme_book(42, "B", 1, 0, 100_000_000, 15, 1), control(203, 101)]),
        112,
        ROLE_SNAPSHOT,
    )
    assert h.count(EVENT_BOOK) == 1
    book = h.last(EVENT_BOOK)
    assert book.flags & FLAG_BOOK_VALID
    assert book.book_sides[SIDE_BID][0].quantity == 20


def test_trade_replay_cycle_and_ab_duplicate() -> None:
    h = Harness()
    trades = h.count(EVENT_TRADE)
    h.ok(lme_packet(1, [control(203, 0)]), 614, ROLE_SNAPSHOT)
    h.ok(lme_packet(2, [lme_trade(42, 99_000_000, 2, 900)]), 614, ROLE_SNAPSHOT)
    h.ok(
        lme_packet(2, [lme_trade(42, 99_000_000, 2, 900)]),
        614,
        ROLE_SNAPSHOT,
        time_ns=1_000_000,
        line=1,
    )
    h.ok(lme_packet(3, [control(203, 100)]), 614, ROLE_SNAPSHOT)
    assert h.count(EVENT_TRADE) == trades + 1
    trade = h.last(EVENT_TRADE)
    assert trade.feed_id == 614
    assert trade.flags & FLAG_RECOVERED


def test_invalid_book_action_rejected() -> None:
    h = Harness()
    h.ok(lme_packet(100, [lme_book(42, "B", 1, 1, 100_000_000, 30, 7)]))
    h.ok(lme_packet(1, [control(203, 98)]), 112, ROLE_SNAPSHOT)
    h.ok(
        lme_packet(2, [lme_book(42, "B", 1, 0, 100_000_000, 20, 5), control(203, 99)]),
        112,
        ROLE_SNAPSHOT,
    )
    bad = bytearray(lme_packet(106, [lme_book(42, "B", 1, 0, 100_000_000, 1, 1)]))
    bad[16 + 65] = 9
    assert h.feed_raises(bytes(bad))


def test_truncated_second_message_publishes_nothing() -> None:
    h = Harness()
    bad = bytearray(
        lme_packet(
            10,
            [lme_trade(42, 100_000_000, 1, 1), lme_trade(42, 100_000_000, 1, 2)],
        )
    )
    # Overwrite the second message's MsgSize so it overruns the packet.
    # Offset 16 + 63 is the start of the second message.
    from fixtures import set_u16

    bad = set_u16(bad, 16 + 63, 1000)
    assert h.feed_raises(bytes(bad))
    assert h.count(EVENT_TRADE) == 0
    h.ok(lme_packet(1, [control(203, 0)]), 614, ROLE_SNAPSHOT)
    assert h.feed_raises(bytes(bad), 614, ROLE_SNAPSHOT)
    assert h.count(EVENT_TRADE) == 0
