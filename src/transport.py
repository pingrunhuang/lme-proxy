"""Live UDP multicast (ASM/SSM) and classic PCAP replay for LMEsource payloads."""

from __future__ import annotations

import select
import socket
import struct
import threading
import time
from collections import defaultdict
from collections.abc import Callable, Iterator
from typing import Iterable

# Linux-only; the socket module does not always export this name.
_IP_MULTICAST_ALL = getattr(socket, "IP_MULTICAST_ALL", 49)

from config import Endpoint, FeedFile
from decoder import FLAG_REPLAYED, Datagram, LmePacketView, ParseError
from loguru import logger


OnDatagram = Callable[[Datagram], None]

def dst_ip_from_pktinfo(ancdata: list) -> str | None:
    """Return the IPv4 destination (multicast group) from IP_PKTINFO cmsg data."""
    for level, typ, data in ancdata:
        if level == socket.IPPROTO_IP and typ == socket.IP_PKTINFO and len(data) >= 12:
            _ifindex, _spec_dst, dst = struct.unpack_from("=I4s4s", data)
            return socket.inet_ntoa(dst)
    return None


_ROLE_NAMES = {0: "incremental", 1: "snapshot", 2: "definition", 3: "dr"}


def _role_name(role: int) -> str:
    return _ROLE_NAMES.get(role, str(role))


def payload_header(payload: bytes) -> str:
    """Summarize the 16-byte LMEsource UDP header for logs."""
    if len(payload) < 16:
        return f"truncated={len(payload)}"
    pkt_size, msg_count, seq = struct.unpack_from("<HBxI", payload)
    return f"pkt_size={pkt_size} msg_count={msg_count} seq={seq}"


def _ip_to_int(value: str) -> int:
    return struct.unpack("!I", socket.inet_aton(value))[0]


def _route_key(feed_id: int, endpoint: Endpoint) -> tuple[int, int, int, int, int]:
    return (
        _ip_to_int(endpoint.group),
        endpoint.port,
        _ip_to_int(endpoint.source_ip) if endpoint.source_ip else 0,
        feed_id,
        endpoint.role,
        # line is separate
    )


class MulticastReceiver:
    """Join configured groups on interface_ip and deliver UDP payloads."""

    def __init__(self, feeds: FeedFile, on_datagram: OnDatagram) -> None:
        self._feeds = feeds
        self._on_datagram = on_datagram
        self._sockets: list[socket.socket] = []
        self._routes: list[tuple[int, int, int, int, int, int]] = []
        # dest, port, source_or_0, feed_id, role, line
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        interface = socket.inet_aton(self._feeds.interface_ip)
        by_port: dict[int, list[tuple]] = defaultdict(list)
        for feed in self._feeds.feeds:
            for endpoint in feed.endpoints:
                by_port[endpoint.port].append((feed, endpoint))

        for port, items in by_port.items():
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, self._feeds.receive_buffer_bytes)
            try:
                sock.setsockopt(socket.IPPROTO_IP, _IP_MULTICAST_ALL, 0)
            except OSError:
                pass
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_PKTINFO, 1)
            # One socket per UDP port. Binding each group on 61000 with SO_REUSEPORT
            # lets Linux hash packets across sockets, so a 612 refresh can land on
            # the 112 socket (or vice versa) and never synchronize the book.
            sock.bind(("", port))
            for feed, endpoint in items:
                group = socket.inet_aton(endpoint.group)
                if endpoint.source_ip:
                    mreq = group + interface + socket.inet_aton(endpoint.source_ip)
                    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_SOURCE_MEMBERSHIP, mreq)
                    mode = "SSM"
                else:
                    mreq = group + interface
                    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
                    mode = "ASM"
                self._routes.append(
                    (
                        _ip_to_int(endpoint.group),
                        endpoint.port,
                        _ip_to_int(endpoint.source_ip) if endpoint.source_ip else 0,
                        feed.id,
                        endpoint.role,
                        endpoint.line,
                    )
                )
                logger.info(
                    "joined {} group={} port={} line={} feed={} role={} iface={}",
                    mode,
                    endpoint.group,
                    endpoint.port,
                    endpoint.line,
                    feed.id,
                    endpoint.role_name,
                    self._feeds.interface_ip,
                )
            sock.setblocking(False)
            self._sockets.append(sock)
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="lme-multicast", daemon=True)
        self._thread.start()

    def _match(self, dest: str, port: int, source: str) -> tuple[int, int, int] | None:
        dest_i = _ip_to_int(dest)
        src_i = _ip_to_int(source)
        for group, rport, rsrc, feed_id, role, line in self._routes:
            if group == dest_i and rport == port and (not rsrc or rsrc == src_i):
                return feed_id, role, line
        return None

    def _loop(self) -> None:
        try:
            self._recv_loop()
        except Exception:
            logger.exception("multicast receive loop crashed")

    def _recv_loop(self) -> None:
        packets = 0
        byte_count = 0
        unmatched = 0
        missing_pktinfo = 0
        by_dest: dict[str, int] = defaultdict(int)
        last_log = time.monotonic()
        logged_first = False
        logged_unmatched = False
        logger.debug(
            "multicast receive loop started sockets={} routes={}",
            len(self._sockets),
            len(self._routes),
        )
        while not self._stop.is_set():
            if not self._sockets:
                logger.warning("multicast receive loop exiting: no sockets")
                return
            readable, _, _ = select.select(self._sockets, [], [], 0.2)
            now = time.monotonic()
            if now - last_log >= 10:
                dests = ",".join(f"{ip}:{n}" for ip, n in sorted(by_dest.items())) or "-"
                logger.info(
                    "multicast received packets={} bytes={} unmatched={} missing_pktinfo={} dests={}",
                    packets,
                    byte_count,
                    unmatched,
                    missing_pktinfo,
                    dests,
                )
                packets = 0
                byte_count = 0
                unmatched = 0
                missing_pktinfo = 0
                by_dest.clear()
                last_log = now
            for sock in readable:
                while True:
                    try:
                        payload, ancdata, _flags, addr = sock.recvmsg(65536, 256)
                    except BlockingIOError:
                        break
                    except OSError:
                        break
                    dest = dst_ip_from_pktinfo(ancdata)
                    port = sock.getsockname()[1]
                    source_ip = addr[0]
                    header = payload_header(payload)
                    if not dest:
                        missing_pktinfo += 1
                        logger.warning(
                            "UDP packet from {}:{} on port {} missing IP_PKTINFO dest bytes={} {}",
                            source_ip,
                            addr[1],
                            port,
                            len(payload),
                            header,
                        )
                        continue
                    matched = self._match(dest, port, source_ip)
                    if matched is None:
                        unmatched += 1
                        by_dest[f"unmatched:{dest}"] += 1
                        if not logged_unmatched:
                            logger.warning(
                                "unmatched UDP dest={} from {}:{} port={} bytes={} {}",
                                dest,
                                source_ip,
                                addr[1],
                                port,
                                len(payload),
                                header,
                            )
                            logged_unmatched = True
                        else:
                            logger.debug(
                                "unmatched UDP dest={} from {}:{} port={} bytes={} {}",
                                dest,
                                source_ip,
                                addr[1],
                                port,
                                len(payload),
                                header,
                            )
                        continue
                    feed_id, role, line = matched
                    packets += 1
                    byte_count += len(payload)
                    by_dest[dest] += 1
                    if not logged_first:
                        logger.info(
                            "first multicast datagram dest={} src={} port={} feed={} role={} line={} bytes={} {}",
                            dest,
                            source_ip,
                            port,
                            feed_id,
                            _role_name(role),
                            line,
                            len(payload),
                            header,
                        )
                        logged_first = True
                    logger.debug(
                        "multicast datagram dest={} src={} port={} feed={} role={} line={} bytes={} {}",
                        dest,
                        source_ip,
                        port,
                        feed_id,
                        _role_name(role),
                        line,
                        len(payload),
                        header,
                    )
                    datagram = Datagram(
                        feed_id=feed_id,
                        role=role,
                        data=payload,
                        receive_time_ns=_now_ns(),
                        flags=0,
                        line=line,
                    )
                    try:
                        self._on_datagram(datagram)
                    except Exception:
                        logger.exception("failed to handle multicast datagram feed={}", feed_id)

    def stop(self) -> None:
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)
        for sock in self._sockets:
            try:
                sock.close()
            except OSError:
                pass
        self._sockets.clear()
        self._routes.clear()


def _now_ns() -> int:
    return time.time_ns()


def iter_pcap(path: str) -> Iterator[tuple[bytes, str, int, str, int, int]]:
    """Yield (payload, dst_ip, dst_port, src_ip, src_port, time_ns) from a classic PCAP."""
    with open(path, "rb") as handle:
        header = handle.read(24)
        if len(header) != 24:
            raise ParseError("truncated PCAP header")
        magic = int.from_bytes(header[0:4], "little")
        big = False
        nano = False
        if magic == 0xA1B2C3D4:
            pass
        elif magic == 0xA1B23C4D:
            nano = True
        elif magic == 0xD4C3B2A1:
            big = True
        elif magic == 0x4D3CB2A1:
            big = True
            nano = True
        else:
            raise ParseError("unsupported PCAP magic (pcapng is not supported)")
        endian = "big" if big else "little"

        def u16(data: bytes, offset: int) -> int:
            return int.from_bytes(data[offset : offset + 2], endian)

        def u32(data: bytes, offset: int) -> int:
            return int.from_bytes(data[offset : offset + 4], endian)

        if u16(header, 4) != 2 or u16(header, 6) != 4:
            raise ParseError("unsupported PCAP version")
        snaplen = u32(header, 16)
        link = u32(header, 20)
        if link not in (1, 101):
            raise ParseError("PCAP requires Ethernet (1) or raw IPv4 (101)")
        while True:
            rec = handle.read(16)
            if not rec:
                break
            if len(rec) != 16:
                raise ParseError("truncated PCAP record header")
            length = u32(rec, 8)
            original = u32(rec, 12)
            fraction = u32(rec, 4)
            if length > 1048576 or length > snaplen or length > original:
                raise ParseError("invalid PCAP record size")
            unit = 1_000_000_000 if nano else 1_000_000
            if fraction >= unit:
                raise ParseError("invalid PCAP timestamp fraction")
            frame = handle.read(length)
            if len(frame) != length:
                raise ParseError("truncated PCAP data")
            offset = 0
            if link == 1:
                if len(frame) < 14:
                    continue
                eth = int.from_bytes(frame[12:14], "big")
                offset = 14
                vlan = 0
                while eth in (0x8100, 0x88A8):
                    vlan += 1
                    if vlan > 2:
                        raise ParseError("more than two VLAN tags")
                    if len(frame) < offset + 4:
                        raise ParseError("truncated VLAN tag")
                    eth = int.from_bytes(frame[offset + 2 : offset + 4], "big")
                    offset += 4
                if eth != 0x0800:
                    continue
            if len(frame) < offset + 20:
                continue
            if (frame[offset] >> 4) != 4 or frame[offset + 9] != 17:
                continue
            if length != original:
                # Only error when the frame is a candidate; checked after IP/UDP match below.
                truncated = True
            else:
                truncated = False
            ihl = (frame[offset] & 15) * 4
            if ihl < 20:
                raise ParseError("invalid IPv4 IHL")
            total = int.from_bytes(frame[offset + 2 : offset + 4], "big")
            if total < ihl + 8:
                raise ParseError("invalid IPv4 total length")
            if len(frame) < offset + total:
                raise ParseError("invalid IPv4 total length")
            if int.from_bytes(frame[offset + 6 : offset + 8], "big") & 0x3FFF:
                raise ParseError("fragmented IPv4 target packet is unsupported")
            udp = offset + ihl
            udp_len = int.from_bytes(frame[udp + 4 : udp + 6], "big")
            if udp_len < 8 or udp_len != total - ihl:
                raise ParseError("invalid UDP length")
            src_ip = socket.inet_ntoa(frame[offset + 12 : offset + 16])
            dst_ip = socket.inet_ntoa(frame[offset + 16 : offset + 20])
            dst_port = int.from_bytes(frame[udp + 2 : udp + 4], "big")
            src_port = int.from_bytes(frame[udp : udp + 2], "big")
            payload = bytes(frame[udp + 8 : udp + udp_len])
            if truncated:
                raise ParseError("target PCAP frame truncated by snaplen")
            time_ns = u32(rec, 0) * 1_000_000_000 + fraction * (1 if nano else 1000)
            yield payload, dst_ip, dst_port, src_ip, src_port, time_ns


def replay_pcap(path: str, feeds: FeedFile, on_datagram: OnDatagram) -> int:
    """Replay matching UDP payloads as fast as possible. Returns datagram count."""
    routes: list[tuple[int, int, int, int, int, int]] = []
    for feed in feeds.feeds:
        for endpoint in feed.endpoints:
            routes.append(
                (
                    _ip_to_int(endpoint.group),
                    endpoint.port,
                    _ip_to_int(endpoint.source_ip) if endpoint.source_ip else 0,
                    feed.id,
                    endpoint.role,
                    endpoint.line,
                )
            )
    count = 0
    for payload, dst_ip, dst_port, src_ip, _src_port, time_ns in iter_pcap(path):
        dest = _ip_to_int(dst_ip)
        source = _ip_to_int(src_ip)
        for group, port, rsrc, feed_id, role, line in routes:
            if group == dest and port == dst_port and (not rsrc or rsrc == source):
                on_datagram(
                    Datagram(
                        feed_id=feed_id,
                        role=role,
                        data=payload,
                        receive_time_ns=time_ns,
                        flags=FLAG_REPLAYED,
                        line=line,
                    )
                )
                count += 1
                break
    return count


def peek_packet(data: bytes) -> LmePacketView:
    return LmePacketView(data)
