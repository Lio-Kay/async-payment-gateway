import json
import logging
import sys
from datetime import UTC, datetime
from typing import Any

from src.core.config import get_settings

# Built-in LogRecord attributes that should not be duplicated as custom extra fields.
_BUILTIN_RECORD_ATTRS = frozenset(
    {
        "args",
        "asctime",
        "created",
        "exc_info",
        "exc_text",
        "filename",
        "funcName",
        "levelname",
        "levelno",
        "lineno",
        "message",
        "module",
        "msecs",
        "msg",
        "name",
        "pathname",
        "process",
        "processName",
        "relativeCreated",
        "stack_info",
        "taskName",
        "thread",
        "threadName",
    }
)


class JsonFormatter(logging.Formatter):
    """Serializes log records to single-line JSON for container stdout collection.

    Any keyword fields passed via `logger.info(..., extra={"payment_id": ...})`
    are merged into the top-level JSON object without allowing `extra` keys to
    overwrite core fields like `timestamp` or `level`.
    """

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        for key, value in record.__dict__.items():
            if key not in _BUILTIN_RECORD_ATTRS and key not in payload and not key.startswith("_"):
                payload[key] = value

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        if record.stack_info:
            payload["stack_info"] = self.formatStack(record.stack_info)

        return json.dumps(payload, default=str, ensure_ascii=False)


def setup_logging(log_level: str | None = None) -> None:
    """Replace root logger handlers with a single stdout JSON handler."""
    level_name = (log_level or get_settings().log_level).upper()

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(level_name)
