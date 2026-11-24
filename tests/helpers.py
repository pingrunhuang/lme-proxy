from __future__ import annotations

from config import FeedConfig, FeedFile, Settings
from decoder import (
    FLAG_REPLAYED,
    ROLE_INCREMENTAL,
    ROLE_SNAPSHOT,
    Datagram,
    Event,
    LmeEngine,
    ParseError,
)


def core_feeds(**kwargs) -> list[FeedConfig]:
    depth = kwargs.get("depth", 15)
    cache = kwargs.get("recovery_buffer_bytes", 1_048_576)
    return [
        FeedConfig(
            id=112,
            protocol="lmesource_v4",
            revision="4.13",
            channel_id=112,
            depth=depth,
            max_instruments=128,
            recovery_buffer_bytes=cache,
        ),
        FeedConfig(
            id=614,
            protocol="lmesource_v4",
            revision="4.13",
            channel_id=614,
            max_instruments=128,
            recovery_buffer_bytes=cache,
        ),
    ]


def lbma_feeds(**kwargs) -> list[FeedConfig]:
    cache = kwargs.get("recovery_buffer_bytes", 1_048_576)
    return [
        FeedConfig(
            id=2,
            protocol="lmesource_v4",
            revision="4.13",
            channel_id=307,
            max_instruments=128,
            recovery_buffer_bytes=cache,
            raw_messages=True,
        )
    ]


class Harness:
    def __init__(self, feeds: list[FeedConfig] | None = None) -> None:
        self.events: list[Event] = []
        self.engine = LmeEngine(feeds or core_feeds(), self.events.append)
        self.engine.transport_started = True

    def feed(
        self,
        payload: bytes,
        feed_id: int = 112,
        role: int = ROLE_INCREMENTAL,
        time_ns: int = 1_000_000,
        line: int = 0,
    ) -> None:
        self.engine.feed(
            Datagram(
                feed_id=feed_id,
                role=role,
                data=payload,
                receive_time_ns=time_ns,
                flags=FLAG_REPLAYED,
                line=line,
            )
        )

    def ok(self, *args, **kwargs) -> None:
        self.feed(*args, **kwargs)

    def feed_raises(self, *args, **kwargs) -> bool:
        try:
            self.feed(*args, **kwargs)
            return False
        except ParseError:
            return True

    def count(self, typ: str) -> int:
        return sum(1 for event in self.events if event.type == typ)

    def last(self, typ: str) -> Event:
        for event in reversed(self.events):
            if event.type == typ:
                return event
        raise AssertionError(f"missing event type {typ}")


def feed_file_for(feeds: list[FeedConfig], interface_ip: str = "127.0.0.1") -> FeedFile:
    return FeedFile(
        config_version=1,
        interface_ip=interface_ip,
        receive_buffer_bytes=1_048_576,
        receive_batch_max=16,
        feeds=feeds,
        symbol_map={},
        path="",
    )


def make_settings(**kwargs) -> Settings:
    settings = Settings(
        zmq_bind_host="127.0.0.1",
        zmq_pub_port=0,
        zmq_rep_port=0,
        publish_queue_size=10_000,
    )
    for key, value in kwargs.items():
        setattr(settings, key, value)
    return settings
