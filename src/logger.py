import logging
import os
import sys
from pathlib import Path

from loguru import logger

_KNOWN_LEVELS = {"TRACE", "DEBUG", "INFO", "SUCCESS", "WARNING", "ERROR", "CRITICAL"}


class InterceptHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno
        logger.opt(exception=record.exc_info).log(level, record.getMessage())


def configure_logger() -> None:
    log_dir = Path(os.getenv("LOG_DIR", "logs"))
    log_dir.mkdir(parents=True, exist_ok=True)
    raw = os.getenv("LOG_LEVEL", "INFO").strip().upper()
    level = raw if raw in _KNOWN_LEVELS else "INFO"
    retention = int(os.getenv("LOG_BACKUP_COUNT", "30"))
    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)
    logger.remove()
    logger.add(sys.stderr, level=level)
    logger.add(
        log_dir / "lme_proxy.{time:YYYY-MM-DD_HH-mm-ss_SSSSSS}.log",
        level=level,
        retention=retention,
        enqueue=True,
        encoding="utf-8",
    )
    if raw not in _KNOWN_LEVELS:
        logger.warning("unknown LOG_LEVEL={}, falling back to INFO", raw)
    logger.info("logging at level={} dir={}", level, log_dir)
