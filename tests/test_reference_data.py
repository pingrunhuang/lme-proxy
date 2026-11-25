"""SYNTHETIC v4.18 fixtures from documented layouts, NOT exchange captures."""
from __future__ import annotations

import importlib.util
from pathlib import Path
import socket
import struct

import pytest

MODULE = Path(__file__).resolve().parents[1] / "scripts" / "lme_reference_data.py"
spec = importlib.util.spec_from_file_location("lme_reference_data", MODULE)
ref = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ref)


def msg(typ, body):
    return struct.pack("<HH", 4 + len(body), typ) + body


def marker(w):
    return msg(203, struct.pack("<I", w))


def packet(seq, *messages, send=100):
    body = b"".join(messages)
    return struct.pack("<HBBIQ", 16 + len(body), len(messages), 0, seq, send) + body


def outright(tid=42, merged=2**64-1, label=b"3M", expiry=20270225):
    # Literal offsets here, not generated from the production layout constants.
    b = bytearray(b" " * 125)
    struct.pack_into("<HH2sQQQ2s1s3s12sI1s", b, 0, 125, 301, b"EL", tid,
                     merged, 2**64-1, b"XX", b"F", b"USD", b"SYNTHETIC   ", expiry, b"R")
    struct.pack_into("<q1sb6s", b, 53, -(2**63), b" ", -128, label.ljust(6))
    struct.pack_into("<I", b, 105, 7)
    struct.pack_into("<Q", b, 109, 25)
    struct.pack_into("<I", b, 118, 20270224)
    return bytes(b)


def strategy(tid=99, legs=(42, 43)):
    b = bytearray(b" " * (87 + 29 * len(legs)))
    struct.pack_into("<HH2sQQ2s1s3sI12s", b, 0, len(b), 302, b"EL", tid,
                     2**64-1, b"XX", b"F", b"USD", 2, b"SYNTHETIC   ")
    struct.pack_into("<b", b, 44, -128)
    struct.pack_into("<I", b, 63, 7)
    struct.pack_into("<Q", b, 67, 25)
    struct.pack_into("<I", b, 76, 20270224)
    struct.pack_into("<I", b, 83, len(legs))
    for i, leg in enumerate(legs):
        struct.pack_into("<IcQqQ", b, 87+29*i, i+1, b"B" if i == 0 else b"S",
                         1000, -(2**63), leg)
    return bytes(b)


def synchronized(*records, tail=None):
    c = ref.Collector("4.18")
    c.feed(0, packet(10))  # heartbeat at snapshot watermark
    if tail:
        for seq, record in tail:
            c.feed(0, packet(seq, record))
    c.feed(1, packet(100, marker(9), *records, marker(10)))
    return c


def test_official_fields_and_strategy():
    typ, r = ref.decode(outright(), "4.18")
    assert typ == 301 and r["TradableInstrumentID"] == 42
    assert (r["ProductCode"], r["ContractCode"], r["TradingCurrency"]) == ("XX", "SYNTHETIC", "USD")
    assert (r["ExpiryDate"], r["PromptType"], r["PromptDateLabel"]) == (20270225, "R", "3M")
    assert r["StrikePrice"] == -(2**63) and r["LotSize"] == 25
    _, s = ref.decode(strategy(), "4.18")
    assert s["StrategyLegCount"] == 2 and s["Legs"][1]["LegTradableInstrumentID"] == 43
    assert s["Legs"][1]["LegBuySell"] == "S" and s["Legs"][0]["LegRatio"] == 1000


@pytest.mark.parametrize("data", [b"", b"123", msg(301, b"x"), msg(302, b"x"),
                                       msg(303, b"x"), msg(305, b"x"), msg(203, b"x")])
def test_invalid_lengths_or_unexpected_messages(data):
    with pytest.raises(ref.ParseError):
        ref.decode(data, "4.18")


def test_leg_count_and_control_injection():
    bad = bytearray(strategy())
    struct.pack_into("<I", bad, 83, 13)
    with pytest.raises(ref.ParseError):
        ref.decode(bytes(bad), "4.18")
    bad = bytearray(outright())
    bad[36] = 27
    with pytest.raises(ref.ParseError):
        ref.decode(bytes(bad), "4.18")


def test_no_implicit_v413_compatibility():
    with pytest.raises(ref.ParseError):
        ref.decode(outright(), "4.13")
    with pytest.raises(ValueError):
        ref.Collector("4.13")


def test_complete_atomic_snapshot_and_incremental_catchup():
    c = synchronized(outright(), tail=[(12, outright(43)), (11, outright(label=b"CASH"))])
    assert c.confirmed and c.watermark == 10
    assert c.records[42]["PromptDateLabel"] == "CASH" and set(c.records) == {42, 43}


def test_mid_cycle_is_discarded():
    c = ref.Collector("4.18")
    c.feed(0, packet(10))
    c.feed(1, packet(98, outright(999), marker(9), outright(), marker(10)))
    assert c.confirmed and set(c.records) == {42}


def test_refresh_reordering_and_ab_duplicates():
    c = ref.Collector("4.18")
    c.feed(0, packet(10))
    c.feed(1, packet(100, marker(9)))
    c.feed(1, packet(102, marker(10)))
    assert not c.confirmed and not c.records
    c.feed(1, packet(100, marker(9)))
    c.feed(1, packet(101, outright()))
    assert c.confirmed


def test_gap_on_realtime_and_late_fill():
    c = synchronized(outright(), tail=[(12, outright(44))])
    assert not c.confirmed and not c.records
    c.feed(0, packet(11, outright(43)))
    assert c.confirmed and set(c.records) == {42, 43, 44}


def test_gap_cannot_hide_behind_heartbeat():
    c = ref.Collector("4.18")
    c.feed(0, packet(12))
    c.feed(1, packet(100, marker(9), outright(), marker(10)))
    c.finish("timeout")
    assert not c.confirmed and "timeout" in c.reason


def test_partial_refresh_and_timeout(capsys):
    c = ref.Collector("4.18")
    c.feed(0, packet(10))
    c.feed(1, packet(100, marker(9), outright()))
    assert not c.records and c.staged
    c.finish("timeout")
    ref.display(c)
    output = capsys.readouterr().out
    assert "UNCONFIRMED" in output and "42\toutright" in output


def test_snapshot_gap_never_activates():
    c = ref.Collector("4.18")
    c.feed(0, packet(10))
    c.feed(1, packet(100, marker(9)))
    c.feed(1, packet(102, marker(10)))
    c.finish("capture ended")
    assert not c.confirmed and not c.records


def test_merged_and_missing_strategy_relationships():
    c = synchronized(outright(42, merged=43), outright(43, merged=43), strategy(99))
    assert c.confirmed and not ref.problems(c.records[42], c.records)
    c.records[43]["MergedTradableInstrumentID"] = 42
    assert "merged cycle" in ref.problems(c.records[42], c.records)
    del c.records[43]
    assert "missing leg=43" in ref.problems(c.records[99], c.records)


def test_conflicting_duplicate_and_bounds():
    c = ref.Collector("4.18")
    c.feed(0, packet(10, outright()))
    c.feed(0, packet(10, outright(label=b"CASH")))
    assert c.error and not c.confirmed
    c = ref.Collector("4.18", max_messages=1)
    c.feed(0, packet(10, outright(), outright(43)))
    assert "buffer limit" in c.error
    c = ref.Collector("4.18", max_bytes=5)
    c.feed(0, packet(10, outright()))
    assert "byte buffer" in c.error
    c = ref.Collector("4.18", max_records=1)
    c.feed(1, packet(1, marker(0), outright(), outright(43), marker(1)))
    assert "record capacity" in c.error


def test_reset_invalidates_staging_and_old_epoch_packets():
    c = ref.Collector("4.18")
    c.feed(0, packet(10))
    c.feed(1, packet(100, marker(9), outright()))
    c.feed(0, packet(1, msg(100, struct.pack("<I", 1)), send=200))
    assert not c.staged and not c.records and c.epoch == 1
    c.feed(0, packet(10, outright(999), send=100))
    assert not c.incremental
    c.feed(1, packet(100, marker(9), outright(999), marker(10), send=100))
    assert not c.staged and not c.confirmed  # Old OTHER-channel epoch must not leak.
    c.feed(0, packet(1, msg(100, struct.pack("<I", 1)), send=200))
    assert c.epoch == 1  # duplicate reset from B


def make_pcap(path, frames):
    # Synthetic classic PCAP, DLT_RAW IPv4. Destination groups are fixture-only.
    blob = struct.pack("<IHHIIII", 0xA1B2C3D4, 2, 4, 0, 0, 65535, 101)
    for group, payload in frames:
        udp = struct.pack("!HHHH", 1234, 9999, 8+len(payload), 0) + payload
        ip = struct.pack("!BBHHHBBH4s4s", 0x45, 0, 20+len(udp), 0, 0, 64, 17, 0,
                         socket.inet_aton("192.0.2.1"), socket.inet_aton(group)) + udp
        blob += struct.pack("<IIII", 1, 0, len(ip), len(ip)) + ip
    path.write_bytes(blob)


def arguments(path):
    return ["--spec-version", "4.18", "--pcap", str(path),
            "--incremental", "239.255.1.1:9999", "--snapshot", "239.255.1.2:9999"]


def test_real_cli_pcap_terminal_only_no_files(tmp_path, monkeypatch, capsys):
    pcap = tmp_path / "synthetic.pcap"
    make_pcap(pcap, [("239.255.1.1", packet(10)),
                     ("239.255.1.2", packet(100, marker(9), outright(), marker(10)))])
    monkeypatch.chdir(tmp_path)
    protected = ref.ROOT / "config" / "symbols.yaml"
    before = protected.read_bytes()
    paths = set(tmp_path.rglob("*"))
    assert ref.main(arguments(pcap)) == 0
    output = capsys.readouterr().out
    assert "SYNCED" in output and "42\toutright\tXX\tSYNTHETIC" in output
    assert "NOT verified for 4.13" in output and "No TAP symbol is generated" in output
    assert paths == set(tmp_path.rglob("*")) and protected.read_bytes() == before


def test_cli_partial_missing_input_and_version(tmp_path, capsys):
    pcap = tmp_path / "partial.pcap"
    make_pcap(pcap, [("239.255.1.2", packet(100, marker(9), outright()))])
    assert ref.main(arguments(pcap)) == 2
    assert "UNCONFIRMED" in capsys.readouterr().out
    assert ref.main(arguments(tmp_path / "missing.pcap")) == 2
    with pytest.raises(SystemExit):
        ref.main([x if x != "4.18" else "4.13" for x in arguments(pcap)])


def test_empty_confirmed_image_and_unknown_id_stay_empty():
    c = synchronized()
    assert c.confirmed and c.records == {}  # Nothing is inferred, including ID 122864.


def test_strategy_can_precede_legs_in_snapshot():
    c = synchronized(strategy(), outright(43), outright(42))
    assert c.confirmed and not ref.problems(c.records[99], c.records)


def test_decode_whole_packet_before_staging_and_conflicting_snapshot():
    c = ref.Collector("4.18")
    c.feed(1, packet(100, marker(9), outright(), msg(301, b"bad")))
    assert c.error and not c.staged
    c = ref.Collector("4.18")
    c.feed(1, packet(100, marker(9), outright(), outright(label=b"CASH")))
    assert "conflicting definitions" in c.error


def test_v418_ignored_messages_are_sequence_counted():
    c = synchronized(outright(), tail=[(11, msg(305, b"\0" * 32)), (12, outright(43))])
    assert c.confirmed and set(c.records) == {42, 43}


def test_cli_timeout_is_partial_and_does_not_create_outputs(tmp_path, monkeypatch, capsys):
    pcap = tmp_path / "synthetic.pcap"
    make_pcap(pcap, [("239.255.1.1", packet(10)), ("239.255.1.2", packet(100, marker(9), outright(), marker(10)))])
    ticks = iter([0, 100])
    monkeypatch.setattr(ref.time, "monotonic", lambda: next(ticks))
    assert ref.main(arguments(pcap) + ["--timeout", "1"]) == 2
    assert "timed out" in capsys.readouterr().out


def test_cli_live_requires_explicit_standalone_and_interface():
    with pytest.raises(SystemExit):
        ref.main(["--spec-version", "4.18", "--live", "--incremental", "239.255.1.1:9999", "--snapshot", "239.255.1.2:9999"])


def test_regressing_snapshot_watermark_fails_closed():
    c = ref.Collector("4.18")
    c.feed(0, packet(10))
    c.feed(1, packet(100, marker(10), outright(), marker(9)))
    assert "watermark regressed" in c.error and not c.confirmed
