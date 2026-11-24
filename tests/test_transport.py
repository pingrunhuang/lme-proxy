import socket
import struct

from transport import dst_ip_from_pktinfo, MulticastReceiver


def test_dst_ip_from_pktinfo_unpacks_multicast_group() -> None:
    ifindex = 4
    spec = socket.inet_aton("10.18.45.1")
    dest = socket.inet_aton("224.0.241.14")
    data = struct.pack("=I4s4s", ifindex, spec, dest)
    anc = [(socket.IPPROTO_IP, socket.IP_PKTINFO, data)]
    assert dst_ip_from_pktinfo(anc) == "224.0.241.14"


def test_dst_ip_from_pktinfo_ignores_other_cmsg() -> None:
    assert dst_ip_from_pktinfo([(socket.SOL_SOCKET, 0, b"xxxx")]) is None
    assert dst_ip_from_pktinfo([]) is None


def test_match_selects_route_by_destination_group() -> None:
    receiver = MulticastReceiver.__new__(MulticastReceiver)
    receiver._routes = [
        (int.from_bytes(socket.inet_aton("224.0.240.13"), "big"), 61000, 0, 112, 0, 0),
        (int.from_bytes(socket.inet_aton("224.0.241.13"), "big"), 61000, 0, 112, 0, 1),
        (int.from_bytes(socket.inet_aton("224.0.240.14"), "big"), 61000, 0, 112, 1, 0),
        (int.from_bytes(socket.inet_aton("224.0.241.14"), "big"), 61000, 0, 112, 1, 1),
        (int.from_bytes(socket.inet_aton("224.0.240.18"), "big"), 61000, 0, 614, 1, 0),
    ]
    assert receiver._match("224.0.241.14", 61000, "213.86.73.139") == (112, 1, 1)
    assert receiver._match("224.0.240.13", 61000, "213.86.73.131") == (112, 0, 0)
    assert receiver._match("224.0.241.15", 61000, "213.86.73.139") is None
