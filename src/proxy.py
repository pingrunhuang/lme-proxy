from __future__ import annotations

import json
import queue
import threading
import time
from typing import Any

import zmq
from loguru import logger

from config import FeedFile, Settings, is_placeholder_ip
from decoder import Datagram, Event, LmeEngine, ParseError
from mapper import SymbolResolver, map_market_data, map_status_or_error
from protocol import (
    TRADING_ACTIONS,
    Action,
    ErrorCode,
    Event as ProtoEvent,
    GATEWAY_NAME,
    SCHEMA_VERSION,
    ProtocolError,
    errors_topic,
    event_payload,
    normalize_symbols,
    response_error,
    response_ok,
    status_topic,
    validate_request,
)
from registry import SubscriptionRegistry
from transport import MulticastReceiver, replay_pcap


class LmeProxy:
    """ZeroMQ PUB/REP front for LMEsource market data.

    The REP socket is owned by the command thread. The PUB socket is owned by
    the publisher thread. Decode callbacks only enqueue publications.
    """

    def __init__(
        self,
        settings: Settings,
        feed_file: FeedFile,
        engine: LmeEngine | None = None,
    ) -> None:
        self.settings = settings
        self.feed_file = feed_file
        self.resolver = SymbolResolver(feed_file.symbol_map)
        self.engine = engine or LmeEngine(feed_file.feeds, self._on_event)
        self.subscriptions = SubscriptionRegistry()
        startup = normalize_symbols(settings.initial_symbols or [])
        if startup:
            self.subscriptions.subscribe("proxy", "startup", startup)
        self._publish_queue: queue.Queue[tuple[str, str, dict[str, Any]]] = queue.Queue(
            maxsize=settings.publish_queue_size
        )
        self.context: zmq.Context | None = None
        self.pub_socket: zmq.Socket | None = None
        self.rep_socket: zmq.Socket | None = None
        self.active = threading.Event()
        self.bound_pub_port: int | None = None
        self.bound_rep_port: int | None = None
        self._publisher_thread: threading.Thread | None = None
        self._command_thread: threading.Thread | None = None
        self._receiver: MulticastReceiver | None = None
        self.pcap_path = settings.pcap_path
        self.pcap_packets = 0
        self._lock = threading.Lock()

    def start(self) -> None:
        if self.active.is_set():
            return
        self.settings.validate()
        self.context = zmq.Context()
        self.pub_socket = self.context.socket(zmq.PUB)
        self.pub_socket.setsockopt(zmq.LINGER, 0)
        self.bound_pub_port = self._bind(self.pub_socket, self.settings.zmq_pub_port)
        self.rep_socket = self.context.socket(zmq.REP)
        self.rep_socket.setsockopt(zmq.LINGER, 0)
        self.bound_rep_port = self._bind(self.rep_socket, self.settings.zmq_rep_port)
        self.active.set()
        self._publisher_thread = threading.Thread(
            target=self._publisher_loop, name="lme-publisher", daemon=True
        )
        self._command_thread = threading.Thread(
            target=self._command_loop, name="lme-commands", daemon=True
        )
        self._publisher_thread.start()
        self._command_thread.start()
        logger.info(
            "LME proxy transport listening: PUB={}:{} REP={}:{}",
            self.settings.zmq_bind_host,
            self.bound_pub_port,
            self.settings.zmq_bind_host,
            self.bound_rep_port,
        )

    def start_feed(self) -> None:
        if self.pcap_path:
            logger.info("starting PCAP replay: {}", self.pcap_path)
            self.engine.transport_started = True
            self._publish_status(
                ready=False,
                reason=f"PCAP replay started: {self.pcap_path}",
                status_name="REPLAY_STARTED",
            )
            self.pcap_packets = replay_pcap(self.pcap_path, self.feed_file, self._handle_datagram)
            logger.info("PCAP replay finished, packets={}", self.pcap_packets)
            self._publish_status(
                ready=self.engine.is_ready(),
                reason=f"PCAP replay finished: {self.pcap_packets} datagrams",
                status_name="REPLAY_COMPLETE",
            )
            return
        if is_placeholder_ip(self.feed_file.interface_ip):
            raise RuntimeError(
                "live reception requires network.interface_ip or LME_INTERFACE_IP "
                "to be the IPv4 of the NIC that receives the LME multicast groups"
            )
        self._receiver = MulticastReceiver(self.feed_file, self._handle_datagram)
        self._receiver.start()
        self.engine.transport_started = True
        self._publish_status(
            ready=False,
            reason="multicast sockets joined; waiting for 612/807 refresh sync",
            status_name="MD_JOINED",
        )

    def _handle_datagram(self, datagram: Datagram) -> None:
        try:
            self.engine.feed(datagram)
        except ParseError as exc:
            logger.warning("parse error feed={}: {}", datagram.feed_id, exc)
            self.enqueue_publish(
                errors_topic(),
                ProtoEvent.ERROR.value,
                {
                    "code": "lme_parse_error",
                    "message": str(exc),
                    "retryable": True,
                    "occurred_at": int(time.time() * 1000),
                    "feed_id": datagram.feed_id,
                    "role": datagram.role,
                    "line": datagram.line,
                },
            )

    def _on_event(self, event: Event) -> None:
        status = map_status_or_error(event)
        if status is not None:
            topic, name, data = status
            logger.info(
                "lme {} topic={}: {}",
                name,
                topic,
                data.get("reason") or data.get("message") or data,
            )
            self.enqueue_publish(topic, name, data)
            return
        mapped = map_market_data(event, self.resolver, local_time_ms=int(time.time() * 1000))
        if mapped is None:
            return
        topic, data = mapped
        if self.subscriptions.has_any() and not self._subscribed(data["symbol"]):
            return
        self.enqueue_publish(topic, ProtoEvent.MARKET_DATA.value, data)

    def _subscribed(self, symbol: str) -> bool:
        active = self.subscriptions.active_symbols()
        if not active:
            return False
        return self.resolver.matches(symbol, active) or any(
            symbol == item or symbol.startswith(item + ":") for item in active
        )

    def _publish_status(self, ready: bool, reason: str, status_name: str | None = None) -> None:
        data: dict[str, Any] = {
            "ready": ready,
            "md_ready": ready,
            "td_ready": False,
            "reason": reason,
            "occurred_at": int(time.time() * 1000),
        }
        if status_name:
            data["status"] = status_name
        self.enqueue_publish(status_topic(), ProtoEvent.STATUS.value, data)

    def _bind(self, socket: zmq.Socket, port: int) -> int:
        base_endpoint = f"tcp://{self.settings.zmq_bind_host}"
        if port == 0:
            return socket.bind_to_random_port(base_endpoint)
        socket.bind(f"{base_endpoint}:{port}")
        return port

    def enqueue_publish(self, topic: str, event: str, data: dict[str, Any]) -> None:
        if not topic.strip():
            raise ValueError("topic must not be empty")
        try:
            self._publish_queue.put_nowait((topic, event, data))
        except queue.Full:
            logger.error("Publish queue is full; dropping topic={}", topic)

    def handle_command(self, request: Any) -> dict[str, Any]:
        request_id = (
            str(request.get("request_id", "")) or None if isinstance(request, dict) else None
        )
        try:
            action, normalized = validate_request(request)
            if action is Action.PING:
                return response_ok(self._health(), request_id)
            if action is Action.STATUS:
                return response_ok(self._status_data(), request_id)
            if action in TRADING_ACTIONS:
                raise NotImplementedError(
                    f"{action.value} is not available: lme-proxy is a market-data "
                    "service (no LME order entry)"
                )
            if action is Action.SUBSCRIBE_MARKET_DATA:
                return response_ok(self._subscribe(normalized), request_id)
            if action is Action.UNSUBSCRIBE_MARKET_DATA:
                return response_ok(self._unsubscribe(normalized), request_id)
            raise ProtocolError(
                f"Unsupported action: {action.value}",
                ErrorCode.UNSUPPORTED_ACTION,
            )
        except ProtocolError as exc:
            return response_error(str(exc), request_id, code=exc.code)
        except NotImplementedError as exc:
            return response_error(str(exc), request_id, code=ErrorCode.NOT_IMPLEMENTED)
        except RuntimeError as exc:
            return response_error(str(exc), request_id, code=ErrorCode.INTERNAL_ERROR)
        except Exception as exc:
            logger.exception("Unexpected command failure")
            return response_error(
                str(exc),
                request_id,
                code=ErrorCode.INTERNAL_ERROR,
                retryable=True,
            )

    def _health(self) -> dict[str, Any]:
        return {
            "service": "lme-proxy",
            "gateway": GATEWAY_NAME,
            "transport_ready": self.active.is_set() and self.engine.transport_started,
            "ready": bool(self.engine.is_ready()),
            "phase": "market_data",
            "protocol_version": SCHEMA_VERSION,
            "md_enabled": self.settings.enable_md,
            "td_enabled": False,
            "trading_enabled": False,
        }

    def _status_data(self) -> dict[str, Any]:
        return {
            **self._health(),
            "session": {
                "transport_started": self.engine.transport_started,
                "pcap": self.pcap_path or None,
                "pcap_packets": self.pcap_packets,
                "interface_ip": self.feed_file.interface_ip,
                "feeds": self.engine.feed_readiness(),
                "parse_errors": self.engine.parse_errors,
                "datagrams": self.engine.datagrams,
                "events": self.engine.events,
                "last_error": self.engine.last_error,
                "implementation": "lmesource_v4_13",
            },
            "active_symbols": self.subscriptions.active_symbols(),
            "published_queue_size": self._publish_queue.qsize(),
            "pub_port": self.bound_pub_port,
            "rep_port": self.bound_rep_port,
        }

    def _require_md(self) -> None:
        if not self.settings.enable_md:
            raise RuntimeError("LME market data is disabled by LME_ENABLE_MD=false")

    def _subscribe(self, request: dict[str, Any]) -> dict[str, Any]:
        self._require_md()
        symbols = request["symbols"]
        newly_active = self.subscriptions.subscribe(
            request["client_id"],
            request["strategy_id"],
            symbols,
        )
        # LME multicast is already joined for the whole channel. "Upstream subscribe"
        # only enables the local publish filter for this symbol.
        return {
            "symbols": symbols,
            "newly_active": newly_active,
            "active_symbols": self.subscriptions.active_symbols(),
            "gateway": None,
        }

    def _unsubscribe(self, request: dict[str, Any]) -> dict[str, Any]:
        self._require_md()
        symbols = request["symbols"]
        newly_inactive = self.subscriptions.unsubscribe(
            request["client_id"],
            request["strategy_id"],
            symbols,
        )
        return {
            "symbols": symbols,
            "newly_inactive": newly_inactive,
            "active_symbols": self.subscriptions.active_symbols(),
            "gateway": None,
        }

    def _publisher_loop(self) -> None:
        assert self.pub_socket is not None
        while self.active.is_set() or not self._publish_queue.empty():
            try:
                topic, event, data = self._publish_queue.get(timeout=0.2)
            except queue.Empty:
                continue
            try:
                payload = json.dumps(
                    event_payload(event, data),
                    ensure_ascii=False,
                    allow_nan=False,
                ).encode("utf-8")
                self.pub_socket.send_multipart([topic.encode("utf-8"), payload])
            except Exception:
                logger.exception("Failed to publish topic={}", topic)
            finally:
                self._publish_queue.task_done()

    def _command_loop(self) -> None:
        assert self.rep_socket is not None
        poller = zmq.Poller()
        poller.register(self.rep_socket, zmq.POLLIN)
        while self.active.is_set():
            request_id: str | None = None
            try:
                events = dict(poller.poll(250))
                if self.rep_socket not in events:
                    continue
                request = self.rep_socket.recv_json()
                if isinstance(request, dict):
                    request_id = str(request.get("request_id", "")) or None
                response = self.handle_command(request)
            except Exception as exc:
                logger.exception("Command loop error")
                response = response_error(
                    str(exc),
                    request_id,
                    code=ErrorCode.INTERNAL_ERROR,
                    retryable=True,
                )
            try:
                self.rep_socket.send_json(response)
            except zmq.ZMQError:
                if self.active.is_set():
                    logger.exception("Failed to send command response")

    def stop(self) -> None:
        if self._receiver is not None:
            self._receiver.stop()
            self._receiver = None
        if not self.active.is_set() and self.context is None:
            return
        self.active.clear()
        if self._command_thread and self._command_thread.is_alive():
            self._command_thread.join(timeout=2)
        if self._publisher_thread and self._publisher_thread.is_alive():
            self._publisher_thread.join(timeout=2)
        for socket in (self.rep_socket, self.pub_socket):
            if socket is not None:
                socket.close(0)
        if self.context is not None:
            self.context.term()
        self.context = None
        self.rep_socket = None
        self.pub_socket = None
