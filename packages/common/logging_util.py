"""
MetalLedger — Structured logging utility.

All services call `get_logger(__name__)` to obtain a pre-configured logger.
Output is JSON-formatted in production (LOG_FORMAT=json) or human-readable
in development (default).
"""

from __future__ import annotations

import logging
import os
import sys
from typing import Optional


LOG_LEVEL:  str = os.getenv("LOG_LEVEL",  "INFO").upper()
LOG_FORMAT: str = os.getenv("LOG_FORMAT", "text").lower()  # "text" | "json"


class _JsonFormatter(logging.Formatter):
    """Emit log records as single-line JSON objects."""

    import json as _json

    def format(self, record: logging.LogRecord) -> str:  # type: ignore[override]
        import json
        payload = {
            "ts":      self.formatTime(record, self.datefmt),
            "level":   record.levelname,
            "logger":  record.name,
            "msg":     record.getMessage(),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload)


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Return a configured logger.

    Usage::

        from common.logging_util import get_logger
        log = get_logger(__name__)
        log.info("Ingested price", extra={"metal": "XAU", "value": 2050.0})
    """
    logger = logging.getLogger(name or "metalledger")

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)

        if LOG_FORMAT == "json":
            handler.setFormatter(_JsonFormatter())
        else:
            handler.setFormatter(
                logging.Formatter(
                    fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                    datefmt="%Y-%m-%dT%H:%M:%S",
                )
            )

        logger.addHandler(handler)
        logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
        logger.propagate = False

    return logger
