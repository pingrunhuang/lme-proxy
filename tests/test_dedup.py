from __future__ import annotations

from decoder import (
    EVENT_BOOK,
    EVENT_REFERENCE_PRICE,
    EVENT_TRADE,
    ROLE_INCREMENTAL,
    ROLE_SNAPSHOT,
    LmePacketView,
    ParseError,
)
from fixtures import auction, control, lme_book, lme_packet, lme_trade
from helpers import Harness, lbma_feeds


def _sync_book(h: Harness) -> None:
    h.ok(lme_packet(1, [control(203, 98)]), 112, ROLE_SNAPSHOT)
    h.ok(
        lme_packet(2, [lme_book(42, "B", 1, 0, 100_000_000, 10, 0), control(203, 99)]),
        112,
        ROLE_SNAPSHOT,
    )


def test_heartbeat_does_not_consume_sequence() -> None:
    h = Harness()
    _sync_book(h)
    heartbeat = lme_packet(100, [], send_time=50_000)
    view = LmePacketView(heartbeat)
    assert view.count == 0
    assert view.sequence == 100
    h.ok(heartbeat)
    books = h.count(EVENT_BOOK)
    h.ok(lme_packet(100, [lme_book(42, "B", 1, 1, 101_000_000, 11, 0)]))
    assert h.count(EVENT_BOOK) == books + 1


def test_ab_partial_overlap_is_per_message() -> None:
    h = Harness()
    _sync_book(h)
    books = h.count(EVENT_BOOK)
    # Line A: seq 100,101,102 as three single-message packets would be too coarse.
    # One packet with three messages starting at 100, overlapping a second packet
    # that starts at 101 and adds 103.
    first = lme_packet(
        100,
        [
            lme_book(42, "B", 1, 1, 100_000_000, 11, 0),
            lme_book(42, "B", 1, 1, 100_000_000, 12, 0),
            lme_book(42, "B", 1, 1, 100_000_000, 13, 0),
        ],
    )
    second = lme_packet(
        101,
        [
            lme_book(42, "B", 1, 1, 100_000_000, 12, 0),
            lme_book(42, "B", 1, 1, 100_000_000, 13, 0),
            lme_book(42, "B", 1, 1, 100_000_000, 14, 0),
        ],
    )
    h.ok(first, line=0)
    h.ok(second, time_ns=1_100_000, line=1)
    # 100,101,102 from A plus new 103 from B; 101 and 102 must not be applied twice.
    assert h.count(EVENT_BOOK) == books + 4
    assert h.last(EVENT_BOOK).book_sides[0][0].quantity == 14
    assert h.last(EVENT_BOOK).sequence == 103


def test_whole_packet_drop_is_rejected_when_lines_only_overlap() -> None:
    """A/B overlap must not drop the unique tail of the later packet."""
    h = Harness()
    _sync_book(h)
    h.ok(lme_packet(100, [lme_trade(42, 100_000_000, 1, 1)]))
    trades = h.count(EVENT_TRADE)
    line_b = lme_packet(
        100,
        [lme_trade(42, 100_000_000, 1, 1), lme_trade(42, 100_000_000, 2, 2)],
    )
    h.ok(line_b, time_ns=1_100_000, line=1)
    assert h.count(EVENT_TRADE) == trades + 1
    assert h.last(EVENT_TRADE).trade.trade_id == 2


def test_lbma_arbitration_and_idle_heartbeat() -> None:
    h = Harness(lbma_feeds())
    h.ok(lme_packet(100, [auction(100), auction(200), auction(300)]), 2)
    assert h.count(EVENT_REFERENCE_PRICE) == 0
    h.ok(lme_packet(1, [control(203, 99)]), 2, ROLE_SNAPSHOT)
    h.ok(lme_packet(2, [auction(50), control(203, 99)]), 2, ROLE_SNAPSHOT)
    assert h.count(EVENT_REFERENCE_PRICE) == 12
    assert h.last(EVENT_REFERENCE_PRICE).reference_price.price.mantissa == 302

    h.ok(
        lme_packet(102, [auction(300), auction(400), auction(500)]),
        2,
        ROLE_INCREMENTAL,
        time_ns=1_100_000,
        line=1,
    )
    assert h.count(EVENT_REFERENCE_PRICE) == 18
    assert h.last(EVENT_REFERENCE_PRICE).sequence == 104

    h.ok(lme_packet(104, []), 2)  # heartbeat / duplicate
    assert h.count(EVENT_REFERENCE_PRICE) == 18

    h.ok(lme_packet(106, [auction(700)]), 2, time_ns=1_200_000)
    assert h.count(EVENT_REFERENCE_PRICE) == 18
    h.ok(
        lme_packet(105, [auction(600)]),
        2,
        time_ns=1_300_000,
        line=1,
    )
    assert h.count(EVENT_REFERENCE_PRICE) == 24
    assert h.last(EVENT_REFERENCE_PRICE).reference_price.currency == "GBP"


def test_lbma_truncated_packet_does_not_publish_prefix() -> None:
    h = Harness(lbma_feeds())
    packet = lme_packet(1, [auction()])
    for n in range(1, len(packet)):
        assert h.feed_raises(packet[:n], 2)
    h.ok(lme_packet(1, [control(203, 0)]), 2, ROLE_SNAPSHOT)
    h.ok(lme_packet(2, [auction(), control(203, 0)]), 2, ROLE_SNAPSHOT)
    assert h.count(EVENT_REFERENCE_PRICE) == 0


def test_null_price_on_new_book_level_is_rejected() -> None:
    h = Harness()
    from decoder import NULL_I64

    try:
        h.ok(lme_packet(100, [lme_book(42, "B", 1, 0, NULL_I64, 1, 0)]))
        raise AssertionError("null price should not parse")
    except ParseError:
        pass
