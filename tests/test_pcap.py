from __future__ import annotations

from decoder import EVENT_BOOK, FLAG_BOOK_VALID, FLAG_REPLAYED, LmeEngine
from fixtures import control, lme_book, lme_packet, write_classic_pcap
from helpers import core_feeds
from transport import replay_pcap
from config import Endpoint, FeedConfig, FeedFile


def test_pcap_replay_roundtrip(tmp_path):
    incremental = lme_packet(100, [lme_book(42, "B", 1, 1, 100_000_000, 30, 7)])
    start = lme_packet(1, [control(203, 98)])
    snap = lme_packet(2, [lme_book(42, "B", 1, 0, 100_000_000, 20, 5), control(203, 99)])
    path = tmp_path / "lme.pcap"
    write_classic_pcap(
        path,
        [
            (incremental, "224.0.240.13", 61000, "10.1.2.3", 40000),
            (start, "224.0.240.14", 61000, "10.1.2.3", 40000),
            (snap, "224.0.240.14", 61000, "10.1.2.3", 40000),
        ],
    )
    feeds = [
        FeedConfig(
            id=112,
            protocol="lmesource_v4",
            revision="4.13",
            channel_id=112,
            depth=15,
            recovery_buffer_bytes=1_048_576,
            endpoints=[
                Endpoint(role=0, line=0, group="224.0.240.13", port=61000),
                Endpoint(role=1, line=0, group="224.0.240.14", port=61000),
            ],
        )
    ]
    events = []
    engine = LmeEngine(feeds, events.append)
    engine.transport_started = True
    file = FeedFile(
        config_version=1,
        interface_ip="127.0.0.1",
        receive_buffer_bytes=1024,
        receive_batch_max=16,
        feeds=feeds,
    )
    count = replay_pcap(str(path), file, engine.feed)
    assert count == 3
    books = [e for e in events if e.type == EVENT_BOOK]
    assert len(books) == 1
    assert books[0].flags & FLAG_BOOK_VALID
    assert books[0].flags & FLAG_REPLAYED
    assert books[0].book_sides[0][0].quantity == 30
