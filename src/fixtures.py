"""LMEsource v4.13 test fixtures, ported from futures_quote tests/wire_fixture.hpp.

These constructors are for tests and demo PCAPs only. They are not an official
encoder. Field offsets follow the C++ demo's v4.13 322/341/405 layouts.
"""

from __future__ import annotations

from typing import Iterable, Sequence

NULL_I64 = -(2**63)
NULL_U32 = 0xFFFFFFFF
NULL_U64 = 0xFFFFFFFFFFFFFFFF


def _put(buf: bytearray, offset: int, value: int, width: int) -> None:
    need = offset + width
    if len(buf) < need:
        buf.extend(b"\x00" * (need - len(buf)))
    raw = int(value).to_bytes(width, "little", signed=False)
    buf[offset : offset + width] = raw


def _text(buf: bytearray, offset: int, value: str | bytes, width: int) -> None:
    data = value.encode("ascii") if isinstance(value, str) else value
    if len(data) > width:
        raise ValueError("fixture text exceeds field width")
    need = offset + width
    if len(buf) < need:
        buf.extend(b"\x00" * (need - len(buf)))
    buf[offset : offset + len(data)] = data


def lme_packet(
    seq: int,
    messages: Sequence[bytes],
    send_time: int = 10_000,
) -> bytes:
    buf = bytearray(16)
    buf[2] = len(messages)
    _put(buf, 4, seq, 4)
    _put(buf, 8, send_time, 8)
    for message in messages:
        buf.extend(message)
    _put(buf, 0, len(buf), 2)
    return bytes(buf)


def control(msg_type: int, value: int) -> bytes:
    buf = bytearray(8)
    _put(buf, 0, 8, 2)
    _put(buf, 2, msg_type, 2)
    _put(buf, 4, value, 4)
    return bytes(buf)


def lme_book(
    instrument_id: int,
    side: int | str,
    position: int,
    action: int,
    price: int,
    explicit_qty: int,
    implied_qty: int,
) -> bytes:
    side_b = side.encode("ascii")[0] if isinstance(side, str) else int(side)
    buf = bytearray(66)
    _put(buf, 0, len(buf), 2)
    _put(buf, 2, 322, 2)
    _text(buf, 4, "EL", 2)
    _put(buf, 6, 123000, 8)
    _put(buf, 14, instrument_id, 8)
    buf[22] = 1
    _put(buf, 23, explicit_qty + implied_qty, 8)
    _put(buf, 31, price & 0xFFFFFFFFFFFFFFFF, 8)
    _put(buf, 39, 2 if explicit_qty else 0, 4)
    _put(buf, 43, explicit_qty, 8)
    _put(buf, 51, 3 if implied_qty else 0, 4)
    _put(buf, 55, implied_qty, 8)
    buf[63] = side_b
    buf[64] = position
    buf[65] = action
    return bytes(buf)


def lme_trade(
    instrument_id: int,
    price: int,
    qty: int,
    match_id: int,
    cancel: bool = False,
) -> bytes:
    buf = bytearray(63)
    _put(buf, 0, len(buf), 2)
    _put(buf, 2, 341, 2)
    _put(buf, 4, 123000, 8)
    _put(buf, 12, instrument_id, 8)
    _text(buf, 20, "EL", 2)
    _put(buf, 22, 123000, 8)
    _text(buf, 30, "  ", 2)
    buf[32] = 1 if cancel else 0
    _put(buf, 33, price & 0xFFFFFFFFFFFFFFFF, 8)
    _put(buf, 41, qty, 4)
    _put(buf, 45, match_id, 8)
    buf[53] = ord(" ")
    return bytes(buf)


def lme_clear(instrument_id: int) -> bytes:
    buf = bytearray(22)
    _put(buf, 0, len(buf), 2)
    _put(buf, 2, 327, 2)
    _text(buf, 4, "EL", 2)
    _put(buf, 6, 123000, 8)
    _put(buf, 14, instrument_id, 8)
    return bytes(buf)


def lme_indicative(
    instrument_id: int,
    price: int,
    mid: int,
    volume: int,
) -> bytes:
    buf = bytearray(44)
    _put(buf, 0, len(buf), 2)
    _put(buf, 2, 320, 2)
    _text(buf, 4, "EL", 2)
    _put(buf, 6, 123000, 8)
    _put(buf, 14, instrument_id, 8)
    _put(buf, 22, price & 0xFFFFFFFFFFFFFFFF, 8)
    _put(buf, 30, volume, 4)
    _put(buf, 34, mid & 0xFFFFFFFFFFFFFFFF, 8)
    return bytes(buf)


def lme_quote_request(
    instrument_id: int,
    kind: int,
    side: int | str,
    volume: int,
) -> bytes:
    side_b = side.encode("ascii")[0] if isinstance(side, str) else int(side)
    buf = bytearray(30)
    _put(buf, 0, len(buf), 2)
    _put(buf, 2, 329, 2)
    _text(buf, 4, "EL", 2)
    _put(buf, 6, 123000, 8)
    _put(buf, 14, instrument_id, 8)
    buf[22] = kind
    buf[23] = side_b
    _put(buf, 24, volume, 4)
    return bytes(buf)


def lme_instrument_state(instrument_id: int, state: int, condition: str | int = " ") -> bytes:
    cond = condition.encode("ascii")[0] if isinstance(condition, str) else int(condition)
    buf = bytearray(44)
    _put(buf, 0, len(buf), 2)
    _put(buf, 2, 312, 2)
    _text(buf, 4, "EL", 2)
    _put(buf, 6, 123000, 8)
    _put(buf, 14, instrument_id, 8)
    buf[22] = ord("A")
    buf[23] = state
    buf[40] = cond
    return bytes(buf)


def auction(price: int = 123456789) -> bytes:
    """Independent fixture from LME v4.13 section 5.4.5: two groups, three currencies."""
    buf = bytearray(27)
    _put(buf, 2, 405, 2)
    _put(buf, 4, 123000, 8)
    buf[12] = ord("B")
    _put(buf, 13, 20261015, 4)
    _text(buf, 17, "PT", 2)
    _text(buf, 19, "AM", 2)
    _put(buf, 21, 10, 2)
    _put(buf, 23, 11, 2)
    _put(buf, 25, 2, 2)
    buf.extend(b"D")
    buf.extend((2).to_bytes(2, "little"))
    start = len(buf)
    buf.extend(b"\x00" * 22)
    _text(buf, start, "USD", 3)
    _put(buf, start + 3, price, 8)
    _text(buf, start + 11, "EUR", 3)
    _put(buf, start + 14, price + 1, 8)
    buf.extend(b"M")
    buf.extend((1).to_bytes(2, "little"))
    start = len(buf)
    buf.extend(b"\x00" * 11)
    _text(buf, start, "GBP", 3)
    _put(buf, start + 3, price + 2, 8)
    _put(buf, 0, len(buf), 2)
    return bytes(buf)


def set_u16(buf: bytearray | bytes, offset: int, value: int) -> bytearray:
    out = bytearray(buf)
    _put(out, offset, value, 2)
    return out


def write_classic_pcap(
    path: str,
    packets: Iterable[tuple[bytes, str, int, str, int]],
    *,
    ts_sec: int = 1_700_000_000,
) -> None:
    """Write a little-endian classic Ethernet/IPv4/UDP PCAP.

    Each packet is ``(payload, dst_group, dst_port, src_ip, src_port)``.
    """
    import socket
    import struct

    records = bytearray()
    for index, (payload, dst_group, dst_port, src_ip, src_port) in enumerate(packets):
        udp_len = 8 + len(payload)
        ip_len = 20 + udp_len
        src = socket.inet_aton(src_ip)
        dst = socket.inet_aton(dst_group)
        # IPv4 header: version/ihl, tos, total, id, flags, ttl, proto, checksum, src, dst
        ip = bytearray(20)
        ip[0] = 0x45
        ip[2:4] = ip_len.to_bytes(2, "big")
        ip[8] = 64
        ip[9] = 17
        ip[12:16] = src
        ip[16:20] = dst
        udp = bytearray(8)
        udp[0:2] = src_port.to_bytes(2, "big")
        udp[2:4] = int(dst_port).to_bytes(2, "big")
        udp[4:6] = udp_len.to_bytes(2, "big")
        ethernet = bytes(12) + (0x0800).to_bytes(2, "big")
        frame = ethernet + bytes(ip) + bytes(udp) + payload
        rec = struct.pack("<IIII", ts_sec, index, len(frame), len(frame)) + frame
        records.extend(rec)
    header = struct.pack("<IHHiIII", 0xA1B2C3D4, 2, 4, 0, 0, 65535, 1)
    with open(path, "wb") as handle:
        handle.write(header)
        handle.write(records)
