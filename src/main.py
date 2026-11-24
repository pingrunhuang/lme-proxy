from __future__ import annotations

import argparse
import signal
import sys
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from config import Settings, check_config, is_placeholder_ip  # noqa: E402
from logger import configure_logger  # noqa: E402
from proxy import LmeProxy  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="LME LMEsource v4.13 market-data proxy (ZeroMQ PUB/REP)"
    )
    parser.add_argument("--config", help="path to lme.yaml (default: LME_CONFIG or config/lme.yaml)")
    parser.add_argument("--pcap", help="classic PCAP of LME UDP payloads (offline replay)")
    parser.add_argument(
        "--check-config",
        nargs="?",
        const="__default__",
        help="validate YAML and exit (optional path; default is --config)",
    )
    parser.add_argument("--symbol-map", help="YAML/JSON TradableInstrumentID -> TAP symbol map")
    parser.add_argument("--interface-ip", help="override network.interface_ip for live join")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    load_dotenv(ROOT / ".env")
    configure_logger()
    args = parse_args(argv)
    settings = Settings.from_env()
    if args.config:
        settings.config_path = args.config
    if args.pcap:
        settings.pcap_path = args.pcap
    if args.symbol_map:
        settings.symbol_map_path = args.symbol_map
    if args.interface_ip:
        settings.interface_ip = args.interface_ip

    check_path = settings.config_path
    if args.check_config and args.check_config != "__default__":
        check_path = args.check_config
    live = not bool(settings.pcap_path)
    try:
        feed_file = check_config(
            check_path,
            symbol_map_path=settings.symbol_map_path,
            live=False,
        )
        if settings.interface_ip:
            feed_file.interface_ip = settings.interface_ip
        if live and not settings.pcap_path:
            check_config(check_path, symbol_map_path=settings.symbol_map_path, live=False)
            if is_placeholder_ip(feed_file.interface_ip) and not args.check_config:
                # Validate structure now; live IP is required only when starting the feed.
                pass
    except Exception as exc:
        logger.error("config error: {}", exc)
        return 2

    if args.check_config:
        try:
            check_config(
                check_path,
                symbol_map_path=settings.symbol_map_path,
                live=live and is_placeholder_ip(feed_file.interface_ip) is False,
            )
        except Exception as exc:
            logger.error("check-config failed: {}", exc)
            return 2
        logger.info(
            "config ok: feeds={} interface_ip={} symbol_map={}",
            [feed.id for feed in feed_file.feeds],
            feed_file.interface_ip,
            len(feed_file.symbol_map),
        )
        return 0

    settings.validate()
    proxy = LmeProxy(settings, feed_file)
    stop = False

    def _handle(_signum, _frame) -> None:
        nonlocal stop
        stop = True
        logger.info("stopping lme-proxy")
        proxy.stop()

    signal.signal(signal.SIGINT, _handle)
    signal.signal(signal.SIGTERM, _handle)
    try:
        proxy.start()
        proxy.start_feed()
        if settings.pcap_path:
            logger.info("PCAP replay complete; ZeroMQ remains up until signal")
        while not stop and proxy.active.is_set():
            signal.pause()
    except Exception:
        logger.exception("lme-proxy failed")
        proxy.stop()
        return 1
    finally:
        proxy.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
