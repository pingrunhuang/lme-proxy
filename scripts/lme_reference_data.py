#!/usr/bin/env python3
"""Read-only LME reference inspector. Explicitly supports official spec 4.18 ONLY.

Prints a point-in-time 115/615 directory, never writes files or changes the proxy.
See docs/reference-data.md for protocol provenance, capture and live prerequisites.
"""
from __future__ import annotations

import argparse
import ipaddress
from pathlib import Path
import struct
import sys
import threading
import time
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from config import Endpoint, FeedConfig, FeedFile  # noqa: E402
from decoder import LmePacketView, ParseError  # noqa: E402
from transport import MulticastReceiver, iter_pcap  # noqa: E402

U64_NULL = 2**64 - 1
# Official LMEsource CIS v4.18 §4.1.2.1, pp34–37. NOT a v4.13 claim.
OUTRIGHT = (
    ("TradingVenue", "2s", 4), ("TradableInstrumentID", "Q", 6),
    ("MergedTradableInstrumentID", "Q", 14), ("LinkedTradableInstrumentID", "Q", 22),
    ("ProductCode", "2s", 30), ("ContractType", "1s", 32),
    ("TradingCurrency", "3s", 33), ("ContractCode", "12s", 36),
    ("ExpiryDate", "I", 48), ("PromptType", "1s", 52),
    ("StrikePrice", "q", 53), ("OptionType", "1s", 61),
    ("ExerciseStyle", "b", 62), ("PromptDateLabel", "6s", 63),
    ("PriceCode", "2s", 69), ("ISIN", "12s", 71), ("CFICode", "6s", 83),
    ("MarketCode", "4s", 89), ("MarketSegmentCode", "12s", 93),
    ("TickSizeID", "I", 105), ("LotSize", "Q", 109),
    ("LotSizeType", "1s", 117), ("LastTradingDate", "I", 118),
    ("SettlementType", "1s", 122), ("SettlementPricingMethod", "1s", 123),
    ("UnderlyingType", "1s", 124),
)
# Official v4.18 §4.1.2.2, pp37–40; the repeated leg is 29 bytes.
STRATEGY = (
    ("TradingVenue", "2s", 4), ("TradableInstrumentID", "Q", 6),
    ("MergedTradableInstrumentID", "Q", 14), ("ProductCode", "2s", 22),
    ("ContractType", "1s", 24), ("TradingCurrency", "3s", 25),
    ("StrategyTypeCode", "I", 28), ("ContractCode", "12s", 32),
    ("ExerciseStyle", "b", 44), ("PriceCode", "2s", 45),
    ("MarketCode", "4s", 47), ("MarketSegmentCode", "12s", 51),
    ("TickSizeID", "I", 63), ("LotSize", "Q", 67),
    ("LotSizeType", "1s", 75), ("LastTradingDate", "I", 76),
    ("SettlementType", "1s", 80), ("SettlementPricingMethod", "1s", 81),
    ("UnderlyingType", "1s", 82), ("StrategyLegCount", "I", 83),
)


def fields(data: bytes, layout: tuple) -> dict[str, Any]:
    result = {}
    for name, fmt, offset in layout:
        value = struct.unpack_from("<" + fmt, data, offset)[0]
        if isinstance(value, bytes):
            # Prevent untrusted wire text from injecting terminal controls.
            if any(c != 0 and not 32 <= c <= 126 for c in value):
                raise ParseError(f"non-printable ASCII in {name}")
            value = value.decode("ascii").rstrip(" \x00")
        result[name] = value
    return result


def decode(data: bytes, version: str) -> tuple[int, dict | None]:
    if version != "4.18":
        raise ParseError("only explicitly selected spec 4.18 is implemented; NOT 4.13")
    if len(data) < 4:
        raise ParseError("truncated reference message")
    size, typ = struct.unpack_from("<HH", data)
    if size != len(data):
        raise ParseError("reference MsgSize mismatch")
    lengths = {100: 8, 203: 8, 300: 138, 301: 125, 305: 36}
    if typ in lengths and size != lengths[typ]:
        raise ParseError(f"type {typ}: expected v4.18 length {lengths[typ]}, got {size}")
    if typ in (100, 203):
        return typ, {"value": struct.unpack_from("<I", data, 4)[0]}
    if typ in (300, 305):
        # Validated/sequence-counted, not needed for the 301/302 identity table.
        return typ, None
    if typ == 301:
        record = fields(data, OUTRIGHT)
    elif typ == 302:
        if size < 87:
            raise ParseError("truncated strategy header")
        count = struct.unpack_from("<I", data, 83)[0]
        if not 2 <= count <= 13 or size != 87 + 29 * count:
            raise ParseError("invalid v4.18 strategy leg count/length")
        record = fields(data, STRATEGY)
        record["Legs"] = [fields(data, (
            ("LegNumber", "I", 87 + 29*i), ("LegBuySell", "1s", 91 + 29*i),
            ("LegRatio", "Q", 92 + 29*i), ("LegDeltaHedgePrice", "q", 100 + 29*i),
            ("LegTradableInstrumentID", "Q", 108 + 29*i),
        )) for i in range(count)]
        if sorted(x["LegNumber"] for x in record["Legs"]) != list(range(1, count + 1)):
            raise ParseError("invalid strategy LegNumber sequence")
    else:
        raise ParseError(f"unexpected type {typ} on 115/615: check channel and actual version")
    if record["TradableInstrumentID"] in (0, U64_NULL):
        raise ParseError("invalid TradableInstrumentID")
    record["MsgType"] = typ
    record["WireHex"] = data.hex()
    return typ, record


class Collector:
    """Owns one bounded point-in-time reference image, independent of 112/614."""
    def __init__(self, version: str, max_messages: int = 100_000,
                 max_bytes: int = 32*1024*1024, max_records: int = 50_000):
        if version != "4.18":
            raise ValueError("only spec 4.18 is supported (not v4.13)")
        self.version = version
        self.max_messages, self.max_bytes, self.max_records = max_messages, max_bytes, max_records
        self.done = threading.Event()
        self.error = ""
        self.resets = {0: -1, 1: -1}
        self.epoch = 0
        self._clear()

    def _clear(self) -> None:
        self.incremental: dict[int, bytes] = {}
        self.refresh: dict[int, bytes] = {}
        self.staged: dict[int, dict] = {}
        self.records: dict[int, dict] = {}
        self.bytes_used = 0
        self.next_refresh: int | None = None
        self.high = -1
        self.collecting = False
        self.watermark: int | None = None
        self.start_watermark: int | None = None
        self.confirmed = False
        self.last_send_ns = 0
        self.reason = "waiting for a complete 615 cycle and 115 watermark coverage"

    def _put(self, table: dict[int, dict], record: dict, duplicate_ok: bool = True) -> None:
        tid = record["TradableInstrumentID"]
        if tid in table and not duplicate_ok and table[tid] != record:
            raise ParseError(f"conflicting definitions in one refresh for {tid}")
        if tid not in table and len(table) >= self.max_records:
            raise ParseError("reference record capacity exceeded")
        table[tid] = record

    def _store(self, store: dict[int, bytes], seq: int, data: bytes) -> bool:
        if seq in store:
            if store[seq] != data:
                raise ParseError(f"conflicting A/B messages at sequence {seq}")
            return False
        if len(self.incremental) + len(self.refresh) >= self.max_messages:
            raise ParseError("reference message buffer limit exceeded")
        if self.bytes_used + len(data) > self.max_bytes:
            raise ParseError("reference byte buffer limit exceeded")
        store[seq] = data
        self.bytes_used += len(data)
        return True

    def feed(self, role: int, payload: bytes) -> None:
        if self.done.is_set():
            return
        try:
            self._feed(role, payload)
        except (ParseError, struct.error, ValueError) as exc:
            self.error = str(exc)
            self.confirmed = False
            self.reason = "invalid/incomplete reference input"
            self.done.set()

    def _feed(self, role: int, payload: bytes) -> None:
        if role not in (0, 1):
            raise ParseError("reference endpoint must be incremental or snapshot")
        packet = LmePacketView(payload)
        # Validate the WHOLE packet before changing any collector state.
        messages = [(seq, msg.raw(), decode(msg.raw(), self.version))
                    for seq, _, msg, _ in packet.messages()]
        if packet.send_time < max(self.resets.values()):
            return
        self.last_send_ns = packet.send_time
        if not messages and role == 0:
            self.high = max(self.high, packet.sequence)
        for seq, data, (typ, item) in messages:
            if typ == 100:
                if packet.send_time <= self.resets[role]:
                    continue
                if item["value"] != 1:
                    raise ParseError("v4.18 Sequence Reset NewSeqNo must be 1")
                self.resets[role] = packet.send_time
                self.epoch += 1
                self._clear()  # Never mix either side of a reset into a confirmed image.
                continue
            if role == 0:
                if typ == 203:
                    raise ParseError("Refresh Complete on realtime endpoint")
                self._store(self.incremental, seq, data)
                self.high = max(self.high, seq)
            else:
                if self.next_refresh is None:
                    self.next_refresh = seq
                if seq < self.next_refresh:
                    # Retained history can detect conflicting A/B duplicates.
                    if seq in self.refresh and self.refresh[seq] != data:
                        raise ParseError("conflicting refresh A/B duplicate")
                    continue
                self._store(self.refresh, seq, data)
                while self.next_refresh in self.refresh and self.watermark is None:
                    current = self.refresh[self.next_refresh]
                    self.next_refresh += 1
                    msg_type, record = decode(current, self.version)
                    if msg_type == 203:
                        if self.collecting:
                            if record["value"] < self.start_watermark:
                                raise ParseError("refresh watermark regressed without a reset")
                            self.watermark = record["value"]
                        else:
                            self.collecting = True
                            self.start_watermark = record["value"]
                    elif self.collecting and record is not None:
                        self._put(self.staged, record, duplicate_ok=False)
        self._complete()

    def _complete(self) -> None:
        w = self.watermark
        if w is None or self.high < w:
            return
        # Count first: never allocate range(w, heartbeat_seq) for malformed input.
        tail = sorted(k for k in self.incremental if w < k <= self.high)
        if len(tail) != self.high - w:
            self.reason = "615 complete, but 115 messages after LastSeqNum are missing"
            return
        image = dict(self.staged)
        for seq in tail:
            _, record = decode(self.incremental[seq], self.version)
            if record is not None:
                self._put(image, record)
        self.records = image
        self.confirmed = True
        self.reason = f"complete 615 image at 115 watermark {w}, caught up through {self.high}"
        self.done.set()

    def finish(self, reason: str) -> None:
        if not self.done.is_set():
            self.reason = reason + "; " + self.reason
            self.done.set()


def problems(record: dict, records: dict[int, dict]) -> list[str]:
    result = []
    for key in ("MergedTradableInstrumentID", "LinkedTradableInstrumentID"):
        target = record.get(key, U64_NULL)
        if target not in (U64_NULL, record["TradableInstrumentID"]):
            if target not in records:
                result.append(f"missing {key}={target}")
    seen = set()
    current = record["TradableInstrumentID"]
    while current in records:
        if current in seen:
            result.append("merged cycle")
            break
        seen.add(current)
        target = records[current].get("MergedTradableInstrumentID", U64_NULL)
        if target in (U64_NULL, current):
            break
        current = target
    for leg in record.get("Legs", []):
        if leg["LegTradableInstrumentID"] not in records:
            result.append(f"missing leg={leg['LegTradableInstrumentID']}")
    return result


def display(collector: Collector, details: bool = False) -> None:
    print("LME reference inspector | EXPLICIT spec 4.18 ONLY; NOT verified for 4.13")
    print("Source version is an operator assertion: multicast payloads do not prove it.")
    print("SYNCED proves sequence coverage only, NOT business-day freshness or exchange authenticity.")
    print("PROMPT: R=rolling, S=single fixed date; dates/labels are official fields, not inferred 3M dates.")
    state = "SYNCED (as-of capture, not proof of live freshness)" if collector.confirmed else "PARTIAL / UNCONFIRMED — do not use as confirmed mappings"
    print(f"Status: {state}\nReason: {collector.error or collector.reason}")
    print(f"epoch={collector.epoch} watermark115={collector.watermark} high115={collector.high} send_ns={collector.last_send_ns}")
    print("No price inference; ProductCode is official code, NOT a guessed product name.")
    print("No TAP symbol is generated. Review contract/venue/currency/PriceCode and prompt before manually editing symbols.yaml.")
    records = collector.records if collector.confirmed else collector.staged
    headers = ["TID", "TYPE", "PRODUCT", "CONTRACT", "VENUE", "CCY", "EXPIRY", "PROMPT", "LABEL", "PRICE", "MERGED", "LINKED", "LEGS", "REVIEW"]
    print("\t".join(headers))
    for tid, record in sorted(records.items()):
        def text(key: str) -> str:
            value = record.get(key)
            return "-" if value is None or value == "" or value == U64_NULL or (key in ("ExpiryDate", "LastTradingDate") and value == 2**32-1) else str(value)
        review = problems(record, records)
        values = [str(tid), "outright" if record["MsgType"] == 301 else "strategy",
                  *(text(k) for k in ("ProductCode", "ContractCode", "TradingVenue", "TradingCurrency", "ExpiryDate", "PromptType", "PromptDateLabel", "PriceCode", "MergedTradableInstrumentID", "LinkedTradableInstrumentID")),
                  ",".join(str(x["LegTradableInstrumentID"]) for x in record.get("Legs", [])) or "-",
                  "; ".join(review) or "manual symbol review"]
        print("\t".join(values))
        if details:
            for key, value in record.items():
                print(f"  {key}={value}")
    print(f"Rows: {len(records)}. Missing/out-of-order relationships remain explicit; no files written.")


def endpoint(value: str) -> tuple[str, int]:
    try:
        group, raw_port = value.rsplit(":", 1)
        ip = ipaddress.IPv4Address(group)
        port = int(raw_port)
        if not ip.is_multicast or not 1 <= port <= 65535:
            raise ValueError
        return str(ip), port
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected verified multicast IPv4:UDP_PORT") from exc


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec-version", required=True, choices=["4.18"], help="must match actual feed; no autodetection, no v4.13 decoder")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--pcap", type=Path, help="classic PCAP containing verified 115 AND 615 groups")
    source.add_argument("--live", action="store_true", help="Linux only; run separately from proxy")
    parser.add_argument("--incremental", type=endpoint, action="append", required=True, metavar="115_GROUP:PORT")
    parser.add_argument("--snapshot", type=endpoint, action="append", required=True, metavar="615_GROUP:PORT")
    parser.add_argument("--interface-ip", help="actual quote NIC IPv4, live only")
    parser.add_argument("--standalone", action="store_true", help="acknowledge live proxy/other listeners are stopped")
    parser.add_argument("--timeout", type=float, default=60)
    parser.add_argument("--max-messages", type=int, default=100_000)
    parser.add_argument("--max-bytes", type=int, default=32*1024*1024)
    parser.add_argument("--max-records", type=int, default=50_000)
    parser.add_argument("--details", action="store_true", help="print every decoded official field and wire hex")
    args = parser.parse_args(argv)
    if not 0 < args.timeout <= 3600 or min(args.max_messages, args.max_bytes, args.max_records) <= 0:
        parser.error("timeout must be >0 and <=3600; capacities must be positive")
    inc, snap = set(args.incremental), set(args.snapshot)
    if inc & snap:
        parser.error("same group/port cannot be both 115 and 615")
    if args.live:
        if not args.standalone or not args.interface_ip:
            parser.error("live requires --standalone and --interface-ip; stop other proxy/listeners first")
        try:
            ip = ipaddress.IPv4Address(args.interface_ip)
            if ip.is_unspecified or ip.is_multicast:
                raise ValueError
        except ValueError:
            parser.error("live requires a valid unicast NIC IPv4")
        if sys.platform != "linux":
            parser.error("live IP_PKTINFO reception is Linux-only; use PCAP elsewhere")
    collector = Collector(args.spec_version, args.max_messages, args.max_bytes, args.max_records)
    deadline = time.monotonic() + args.timeout
    try:
        if args.pcap:
            for payload, dest, port, _, _, _ in iter_pcap(str(args.pcap)):
                if time.monotonic() >= deadline:
                    collector.finish("PCAP processing timed out")
                    break
                route = (dest, port)
                if route in inc or route in snap:
                    collector.feed(0 if route in inc else 1, payload)
                    if collector.done.is_set():
                        break  # Point-in-time inspection, not an ongoing reference service.
            collector.finish("PCAP ended before synchronization")
        else:
            eps = [Endpoint(role, i, group, port) for role, routes in ((0, inc), (1, snap))
                   for i, (group, port) in enumerate(sorted(routes))]
            feed = FeedConfig(115, "lmesource_v4", "4.18", 115, endpoints=eps)
            receiver = MulticastReceiver(FeedFile(1, args.interface_ip, 16*1024*1024, 16, [feed]),
                                         lambda d: collector.feed(d.role, d.data))
            try:
                receiver.start()
                collector.done.wait(max(0, deadline - time.monotonic()))
            finally:
                receiver.stop()
            collector.finish("live collection timed out/stopped")
    except (OSError, ParseError, KeyboardInterrupt) as exc:
        collector.confirmed = False
        collector.finish(f"collection interrupted: {type(exc).__name__}: {exc}")
        collector.error = str(exc) or "interrupted"
    display(collector, args.details)
    return 0 if collector.confirmed else 2


if __name__ == "__main__":
    raise SystemExit(main())
