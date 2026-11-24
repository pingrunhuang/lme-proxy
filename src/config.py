from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from protocol import DEFAULT_PUB_PORT, DEFAULT_REP_PORT, canonical_symbol, normalize_symbols


TRUE_VALUES = frozenset({"1", "true", "yes", "on", "y"})

ROLE_NAMES = {
    "incremental": 0,
    "snapshot": 1,
    "definition": 2,
    "dr": 3,
}

LINE_NAMES = {"a": 0, "b": 1, "0": 0, "1": 1}


def _bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    return default if value is None else value.strip().lower() in TRUE_VALUES


def _int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    return default if value is None else int(value)


def _symbols_env(name: str) -> list[str]:
    return [item.strip() for item in os.getenv(name, "").split(",") if item.strip()]


class ConfigError(ValueError):
    pass


@dataclass(slots=True)
class Endpoint:
    role: int
    line: int
    group: str
    port: int
    source_ip: str = ""

    @property
    def role_name(self) -> str:
        return {0: "incremental", 1: "snapshot", 2: "definition", 3: "dr"}.get(
            self.role, str(self.role)
        )


@dataclass(slots=True)
class FeedConfig:
    id: int
    protocol: str
    revision: str
    channel_id: int
    depth: int = 15
    max_instruments: int = 8192
    recovery_buffer_bytes: int = 16 * 1024 * 1024
    raw_messages: bool = False
    publish_invalid_books: bool = False
    endpoints: list[Endpoint] = field(default_factory=list)
    instrument_ids: set[int] = field(default_factory=set)
    commodities: set[str] = field(default_factory=set)
    symbols: set[str] = field(default_factory=set)
    groups: set[str] = field(default_factory=set)


@dataclass(slots=True)
class FeedFile:
    config_version: int
    interface_ip: str
    receive_buffer_bytes: int
    receive_batch_max: int
    feeds: list[FeedConfig]
    symbol_map: dict[int, str] = field(default_factory=dict)
    path: str = ""

    def feed(self, feed_id: int) -> FeedConfig:
        for item in self.feeds:
            if item.id == feed_id:
                return item
        raise ConfigError(f"unknown feed id: {feed_id}")


@dataclass(slots=True)
class Settings:
    config_path: str = "config/lme.yaml"
    symbol_map_path: str = "config/symbols.yaml"
    pcap_path: str = ""
    interface_ip: str = ""
    initial_symbols: list[str] = field(default_factory=list)
    zmq_bind_host: str = "0.0.0.0"
    zmq_pub_port: int = DEFAULT_PUB_PORT
    zmq_rep_port: int = DEFAULT_REP_PORT
    publish_queue_size: int = 100_000
    enable_md: bool = True

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            config_path=os.getenv("LME_CONFIG", "config/lme.yaml"),
            symbol_map_path=os.getenv("LME_SYMBOL_MAP", "config/symbols.yaml"),
            pcap_path=os.getenv("LME_PCAP", ""),
            interface_ip=os.getenv("LME_INTERFACE_IP", ""),
            initial_symbols=_symbols_env("LME_SYMBOLS"),
            zmq_bind_host=os.getenv("ZMQ_BIND_HOST", "0.0.0.0"),
            zmq_pub_port=_int_env("ZMQ_PUB_PORT", DEFAULT_PUB_PORT),
            zmq_rep_port=_int_env("ZMQ_REP_PORT", DEFAULT_REP_PORT),
            publish_queue_size=_int_env("ZMQ_PUBLISH_QUEUE_SIZE", 100_000),
            enable_md=_bool_env("LME_ENABLE_MD", True),
        )

    def validate(self) -> None:
        if not self.zmq_bind_host.strip():
            raise ValueError("ZMQ_BIND_HOST must not be empty")
        for name, value in (
            ("ZMQ_PUB_PORT", self.zmq_pub_port),
            ("ZMQ_REP_PORT", self.zmq_rep_port),
        ):
            if not 0 <= value <= 65535:
                raise ValueError(f"{name} must be between 0 and 65535")
        if self.zmq_pub_port and self.zmq_pub_port == self.zmq_rep_port:
            raise ValueError("ZMQ_PUB_PORT and ZMQ_REP_PORT must be different")
        if self.publish_queue_size < 1:
            raise ValueError("ZMQ_PUBLISH_QUEUE_SIZE must be at least 1")
        if not self.enable_md and self.initial_symbols:
            raise ValueError("LME_SYMBOLS must be empty when LME_ENABLE_MD=false")
        if self.initial_symbols:
            self.initial_symbols = normalize_symbols(self.initial_symbols)


def _parse_role(value: Any) -> int:
    if isinstance(value, int):
        if value not in ROLE_NAMES.values():
            raise ConfigError(f"invalid endpoint role: {value}")
        return value
    name = str(value).strip().lower()
    if name not in ROLE_NAMES:
        raise ConfigError(f"invalid endpoint role: {value}")
    return ROLE_NAMES[name]


def _parse_line(value: Any) -> int:
    if value is None or value == "":
        return 0
    if isinstance(value, int):
        if value not in (0, 1):
            raise ConfigError(f"invalid endpoint line: {value}")
        return value
    name = str(value).strip().lower()
    if name not in LINE_NAMES:
        raise ConfigError(f"invalid endpoint line: {value}")
    return LINE_NAMES[name]


def _parse_endpoint(raw: dict[str, Any]) -> Endpoint:
    group = str(raw.get("group", "")).strip()
    if not group:
        raise ConfigError("endpoint.group is required")
    port = int(raw.get("port", 0))
    if not 1 <= port <= 65535:
        raise ConfigError(f"invalid endpoint port: {raw.get('port')}")
    source = str(raw.get("source_ip") or raw.get("source") or "").strip()
    if source.startswith("REPLACE_WITH_"):
        source = ""
    return Endpoint(
        role=_parse_role(raw.get("role", "incremental")),
        line=_parse_line(raw.get("line", 0)),
        group=group,
        port=port,
        source_ip=source,
    )


def _parse_feed(raw: dict[str, Any]) -> FeedConfig:
    protocol = str(raw.get("protocol", "")).strip()
    revision = str(raw.get("revision", "")).strip()
    if protocol != "lmesource_v4":
        raise ConfigError(f"unsupported protocol: {protocol}")
    if revision != "4.13":
        raise ConfigError(
            f"unsupported LMEsource revision {revision!r}; this proxy implements v4.13 only"
        )
    feed_id = int(raw["id"])
    channel_id = int(raw.get("channel_id", feed_id))
    if channel_id not in {112, 307, 614}:
        raise ConfigError(
            f"unsupported LME channel_id {channel_id}; "
            "built-in decoders exist only for 112 (with 612 refresh), 614, and 307 (with 807 refresh)"
        )
    subs = raw.get("subscriptions") or {}
    instrument_ids = {
        int(item) for item in (subs.get("instrument_ids") or [])
    }
    commodities = {str(item).strip().upper() for item in (subs.get("commodities") or []) if str(item).strip()}
    symbols = {str(item).strip() for item in (subs.get("symbols") or []) if str(item).strip()}
    groups = {
        str(item).strip()
        for item in (subs.get("security_groups") or subs.get("groups") or [])
        if str(item).strip()
    }
    endpoints = [_parse_endpoint(item) for item in (raw.get("endpoints") or [])]
    if channel_id == 614:
        if any(ep.role != 1 for ep in endpoints):
            raise ConfigError("channel 614 endpoints must use role: snapshot")
    return FeedConfig(
        id=feed_id,
        protocol=protocol,
        revision=revision,
        channel_id=channel_id,
        depth=int(raw.get("depth", 15)),
        max_instruments=int(raw.get("max_instruments", 8192)),
        recovery_buffer_bytes=int(raw.get("recovery_buffer_bytes", 16 * 1024 * 1024)),
        raw_messages=bool(raw.get("raw_messages", False)),
        publish_invalid_books=bool(raw.get("publish_invalid_books", False)),
        endpoints=endpoints,
        instrument_ids=instrument_ids,
        commodities=commodities,
        symbols=symbols,
        groups=groups,
    )


def load_symbol_map(path: str | Path | None) -> dict[int, str]:
    if not path:
        return {}
    file = Path(path)
    if not file.is_file():
        return {}
    text = file.read_text(encoding="utf-8")
    if file.suffix.lower() == ".json":
        raw = json.loads(text or "{}")
    else:
        raw = yaml.safe_load(text) or {}
    if isinstance(raw, dict) and "symbols" in raw and isinstance(raw["symbols"], dict):
        raw = raw["symbols"]
    if not isinstance(raw, dict):
        raise ConfigError(f"symbol map in {file} must be a mapping of id -> symbol")
    mapping: dict[int, str] = {}
    for key, value in raw.items():
        if value in (None, ""):
            continue
        try:
            instrument_id = int(key)
        except (TypeError, ValueError) as exc:
            raise ConfigError(f"symbol map key must be a TradableInstrumentID: {key!r}") from exc
        mapping[instrument_id] = canonical_symbol(str(value))
    return mapping


def load_feed_file(path: str | Path, *, symbol_map_path: str | Path | None = None) -> FeedFile:
    file = Path(path)
    if not file.is_file():
        raise ConfigError(f"config file not found: {file}")
    raw = yaml.safe_load(file.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ConfigError("config must be a YAML mapping")
    version = int(raw.get("config_version", 1))
    if version != 1:
        raise ConfigError(f"unsupported config_version: {version}")
    network = raw.get("network") or {}
    interface_ip = str(network.get("interface_ip", "0.0.0.0")).strip() or "0.0.0.0"
    feeds = [_parse_feed(item) for item in (raw.get("feeds") or [])]
    if not feeds:
        raise ConfigError("config must declare at least one feed")
    ids = [item.id for item in feeds]
    if len(ids) != len(set(ids)):
        raise ConfigError("feed ids must be unique")
    map_path = raw.get("symbol_map") or symbol_map_path
    if map_path:
        map_file = Path(str(map_path))
        if not map_file.is_absolute():
            map_file = file.parent / map_file
        symbol_map = load_symbol_map(map_file)
    else:
        symbol_map = {}
    return FeedFile(
        config_version=version,
        interface_ip=interface_ip,
        receive_buffer_bytes=int(network.get("receive_buffer_bytes", 16 * 1024 * 1024)),
        receive_batch_max=int(network.get("receive_batch_max", 16)),
        feeds=feeds,
        symbol_map=symbol_map,
        path=str(file),
    )


def is_placeholder_ip(value: str) -> bool:
    text = (value or "").strip()
    return (not text) or text in {"0.0.0.0", "REPLACE_WITH_LOCAL_IPV4"} or text.startswith("REPLACE_WITH_")


def check_config(path: str | Path, *, symbol_map_path: str | Path | None = None, live: bool = False) -> FeedFile:
    feeds = load_feed_file(path, symbol_map_path=symbol_map_path)
    for feed in feeds.feeds:
        if feed.depth < 1 or feed.depth > 64:
            raise ConfigError(f"feed {feed.id} depth must be between 1 and 64")
        if feed.recovery_buffer_bytes < 16384:
            raise ConfigError(f"feed {feed.id} recovery_buffer_bytes is too small")
        if live and not feed.endpoints:
            raise ConfigError(f"feed {feed.id} has no multicast endpoints")
    if live and is_placeholder_ip(feeds.interface_ip):
        raise ConfigError(
            "live reception requires an explicit local interface_ip "
            "(the IPv4 of the NIC that receives the LME groups)"
        )
    return feeds
