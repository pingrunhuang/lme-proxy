#!/usr/bin/env python3
"""Subscribe to lme-proxy and log ZeroMQ market-data events.

Connects to PUB first, then sends ``subscribe_market_data`` on REP using TAP-style
symbols (``LME:F:NI:3M``) or raw TradableInstrumentIDs (``42``). Topics are
``marketdata.LME.<symbol>``.

By default only book updates (bid/ask) are printed. Use --all-events to also
include trades and other event kinds.

Example:
  uv run python scripts/zmq_subscribe_market_data.py --symbol 'LME:F:NI:3M' --count 20 --pretty
  uv run python scripts/zmq_subscribe_market_data.py --symbol 'LME:F:NI:3M' --all-events --count 20
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Sequence

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import zmq  # noqa: E402
from loguru import logger  # noqa: E402

from protocol import (  # noqa: E402
    DEFAULT_PUB_PORT,
    DEFAULT_REP_PORT,
    GATEWAY_NAME,
    canonical_symbol,
)


def symbol_argument(value: str) -> str:
    try:
        return canonical_symbol(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def non_negative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must not be negative")
    return parsed


def non_negative_float(value: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must not be negative")
    return parsed


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--symbol",
        action="append",
        dest="symbols",
        type=symbol_argument,
        default=None,
        help="repeatable TAP symbol or TradableInstrumentID, e.g. LME:F:NI:3M or 42",
    )
    parser.add_argument("--host", default=os.getenv("ZMQ_HOST", "127.0.0.1"))
    parser.add_argument(
        "--pub-port",
        type=int,
        default=int(os.getenv("ZMQ_PUB_PORT", str(DEFAULT_PUB_PORT))),
    )
    parser.add_argument(
        "--rep-port",
        type=int,
        default=int(os.getenv("ZMQ_REP_PORT", str(DEFAULT_REP_PORT))),
    )
    parser.add_argument(
        "--client-id",
        default=os.getenv("MD_CLIENT_ID", "lme-market-data-example"),
    )
    parser.add_argument(
        "--strategy-id",
        default=os.getenv("MD_STRATEGY_ID", "manual"),
    )
    parser.add_argument(
        "--count",
        type=non_negative_int,
        default=0,
        help="exit after N printed events; 0 means run until Ctrl+C",
    )
    parser.add_argument(
        "--idle-timeout",
        type=non_negative_float,
        default=0.0,
        help="exit if no matching market data arrives for N seconds; 0 disables",
    )
    parser.add_argument("--command-timeout", type=non_negative_float, default=5.0)
    parser.add_argument(
        "--warmup",
        type=non_negative_float,
        default=0.2,
        help="seconds to wait after connecting PUB so the subscription is active",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="also print the full JSON payload after the bid/ask summary line",
    )
    quotes = parser.add_mutually_exclusive_group()
    quotes.add_argument(
        "--quotes-only",
        dest="quotes_only",
        action="store_true",
        default=True,
        help="only print book updates that carry bid/ask (default)",
    )
    quotes.add_argument(
        "--all-events",
        dest="quotes_only",
        action="store_false",
        help="include trades and other market-data events, not just bid/ask",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)
    if args.symbols is None:
        args.symbols = []
    return args


def configure_script_logger() -> Path:
    log_dir = Path(os.getenv("LOG_DIR", str(ROOT / "logs")))
    log_dir.mkdir(parents=True, exist_ok=True)
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    retention = int(os.getenv("LOG_BACKUP_COUNT", "30"))
    stamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S_%f")
    log_file = log_dir / f"zmq_subscribe_market_data.{stamp}.log"
    logger.remove()
    logger.add(sys.stderr, level=level)
    logger.add(
        log_file,
        level=level,
        retention=retention,
        enqueue=True,
        encoding="utf-8",
    )
    return log_file


def command_request(
    action: str,
    symbols: list[str],
    client_id: str,
    strategy_id: str,
) -> dict[str, Any]:
    return {
        "action": action,
        "request_id": f"lme-md-{uuid.uuid4().hex}",
        "client_id": client_id,
        "strategy_id": strategy_id,
        "symbols": list(symbols),
    }


def send_command(
    context: zmq.Context,
    endpoint: str,
    request: dict[str, Any],
    timeout_seconds: float,
) -> dict[str, Any]:
    timeout_ms = max(1, int(timeout_seconds * 1000))
    sock = context.socket(zmq.REQ)
    sock.setsockopt(zmq.LINGER, 0)
    sock.setsockopt(zmq.SNDTIMEO, timeout_ms)
    sock.setsockopt(zmq.RCVTIMEO, timeout_ms)
    sock.connect(endpoint)
    try:
        sock.send_json(request)
        response = sock.recv_json()
    except zmq.Again as exc:
        raise TimeoutError(
            f"lme-proxy command timed out after {timeout_seconds:g}s"
        ) from exc
    finally:
        sock.close(0)
    if not isinstance(response, dict):
        raise RuntimeError("lme-proxy returned a non-object response")
    if response.get("status") != "ok":
        error = response.get("error") or {}
        message = error.get("message") or "unknown lme-proxy error"
        raise RuntimeError(str(message))
    return response


def decode_pub_message(frames: list[bytes]) -> tuple[str, dict[str, Any]]:
    if len(frames) != 2:
        raise ValueError(f"expected 2 PUB frames, received {len(frames)}")
    topic = frames[0].decode("utf-8")
    payload = json.loads(frames[1].decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("PUB payload must be a JSON object")
    return topic, payload


def wire_symbol(topic: str, payload: dict[str, Any], prefix: str) -> str:
    data = payload.get("data")
    if isinstance(data, dict):
        symbol = data.get("symbol")
        if isinstance(symbol, str) and symbol.strip():
            return symbol.strip()
    if topic.startswith(prefix):
        return topic[len(prefix) :]
    return ""


def symbol_matches(wire: str, requested: Sequence[str]) -> bool:
    if not requested:
        return True
    for key in requested:
        if wire == key or wire.startswith(key + ":") or key.startswith(wire + ":"):
            return True
    return False


def event_data(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data")
    return data if isinstance(data, dict) else {}


def has_quote(payload: dict[str, Any]) -> bool:
    data = event_data(payload)
    if data.get("kind") == "book":
        return True
    return data.get("bid_price_1") is not None or data.get("ask_price_1") is not None


def format_quote_line(topic: str, payload: dict[str, Any]) -> str:
    data = event_data(payload)
    symbol = data.get("symbol") or topic
    kind = data.get("kind") or "-"
    last = data.get("last_price")
    bid = data.get("bid_price_1")
    bid_vol = data.get("bid_volume_1")
    ask = data.get("ask_price_1")
    ask_vol = data.get("ask_volume_1")
    return (
        f"{symbol}  {kind}  last={last}  "
        f"bid={bid} x {bid_vol}  ask={ask} x {ask_vol}"
    )


def log_event(topic: str, payload: dict[str, Any], *, pretty: bool) -> None:
    if topic.startswith("marketdata."):
        logger.info("{}", format_quote_line(topic, payload))
        if not pretty:
            return
    output = {"topic": topic, **payload}
    indent = 2 if pretty else None
    logger.info(
        "{}",
        json.dumps(output, ensure_ascii=False, indent=indent, allow_nan=False),
    )


def run(args: argparse.Namespace) -> int:
    if not args.symbols:
        log_file = configure_script_logger()
        logger.error(
            "need at least one --symbol (for example LME:F:NI:3M); log={}",
            log_file,
        )
        return 2
    log_file = configure_script_logger()
    logger.info("logging to {}", log_file.resolve())
    pub_endpoint = f"tcp://{args.host}:{args.pub_port}"
    rep_endpoint = f"tcp://{args.host}:{args.rep_port}"
    market_prefix = f"marketdata.{GATEWAY_NAME}."
    context = zmq.Context()
    subscriber = context.socket(zmq.SUB)
    subscriber.setsockopt(zmq.LINGER, 0)
    subscriber.setsockopt_string(zmq.SUBSCRIBE, market_prefix)
    subscriber.setsockopt_string(zmq.SUBSCRIBE, f"errors.{GATEWAY_NAME}")
    subscriber.setsockopt_string(zmq.SUBSCRIBE, f"status.{GATEWAY_NAME}")
    subscriber.connect(pub_endpoint)
    subscribed = False
    try:
        if args.warmup:
            time.sleep(args.warmup)
        ping = send_command(
            context,
            rep_endpoint,
            {"action": "ping", "request_id": f"lme-md-{uuid.uuid4().hex}"},
            args.command_timeout,
        )
        logger.info(
            "ping gateway={} ready={}",
            ping["data"].get("gateway"),
            ping["data"].get("ready"),
        )
        response = send_command(
            context,
            rep_endpoint,
            command_request(
                "subscribe_market_data",
                args.symbols,
                args.client_id,
                args.strategy_id,
            ),
            args.command_timeout,
        )
        subscribed = True
        logger.info(
            "subscribed symbols={} prefix={} quotes_only={} response={}",
            args.symbols,
            market_prefix,
            args.quotes_only,
            response["status"],
        )
        poller = zmq.Poller()
        poller.register(subscriber, zmq.POLLIN)
        received = 0
        last_market_data = time.monotonic()
        while args.count == 0 or received < args.count:
            events = dict(poller.poll(1000))
            if subscriber not in events:
                if (
                    args.idle_timeout > 0
                    and time.monotonic() - last_market_data >= args.idle_timeout
                ):
                    raise TimeoutError(
                        f"no matching market data received for {args.idle_timeout:g}s"
                    )
                continue
            topic, payload = decode_pub_message(subscriber.recv_multipart())
            if topic.startswith(market_prefix):
                wire = wire_symbol(topic, payload, market_prefix)
                if not symbol_matches(wire, args.symbols):
                    logger.debug("skip wire={} (does not match {})", wire, args.symbols)
                    continue
                if args.quotes_only and not has_quote(payload):
                    logger.debug("skip wire={} kind={} (no bid/ask)", wire, event_data(payload).get("kind"))
                    continue
                log_event(topic, payload, pretty=args.pretty)
                received += 1
                last_market_data = time.monotonic()
                continue
            if topic in {f"errors.{GATEWAY_NAME}", f"status.{GATEWAY_NAME}"}:
                log_event(topic, payload, pretty=args.pretty)
        return 0
    except KeyboardInterrupt:
        logger.info("stopped by user")
        return 0
    except (RuntimeError, TimeoutError, ValueError, zmq.ZMQError) as exc:
        logger.error("market-data example failed: {}", exc)
        return 1
    finally:
        if subscribed:
            try:
                send_command(
                    context,
                    rep_endpoint,
                    command_request(
                        "unsubscribe_market_data",
                        args.symbols,
                        args.client_id,
                        args.strategy_id,
                    ),
                    args.command_timeout,
                )
                logger.info("unsubscribed symbols={}", args.symbols)
            except Exception as exc:
                logger.warning("unsubscribe failed: {}", exc)
        subscriber.close(0)
        context.term()
        logger.complete()


def main(argv: Sequence[str] | None = None) -> int:
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
