from __future__ import annotations

import json
import logging
import re
import sys
from typing import Any

from betterer_ratings.config.schema import AppConfig, ensure_app_config

_BASE_RECORD_KEYS = set(logging.makeLogRecord({}).__dict__) | {"message", "asctime"}
_COMPONENT_LABELS = {
    "Harvester": "harvester",
    "Submitter": "submitter",
    "TMDB": "tmdb",
    "MDBList": "mdblist",
    "IMDbArchive": "imdb_archive",
}
_PROVIDER_COMPONENTS = {"tmdb", "mdblist", "imdb_archive"}


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    return str(value)


def _event_from_message(message: str) -> str:
    detail = message
    if message.startswith("[") and "]" in message:
        detail = message.split("]", 1)[1].strip()
    detail = detail.split(":", 1)[0].strip().rstrip(".")
    detail = re.sub(r"[^a-zA-Z0-9]+", "_", detail).strip("_").lower()
    return detail or "log"


def _context_from_message(message: str) -> dict[str, Any]:
    if not message.startswith("[") or "]" not in message:
        return {}
    label = message[1:message.index("]")]
    component = _COMPONENT_LABELS.get(label)
    if component is None:
        return {}
    context: dict[str, Any] = {
        "component": component,
        "event": _event_from_message(message),
    }
    if component in _PROVIDER_COMPONENTS:
        context["provider"] = component
    return context


class JsonLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        message = record.getMessage()
        payload: dict[str, Any] = {
            "level": record.levelname,
            "logger": record.name,
            "message": message,
        }
        payload.update(_context_from_message(message))
        for key, value in record.__dict__.items():
            if key in _BASE_RECORD_KEYS or key.startswith("_"):
                continue
            payload[key] = _json_safe(value)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=True, separators=(",", ":"))


def configure_logging(config: AppConfig) -> None:
    app_config = ensure_app_config(config)
    level = getattr(logging, app_config.runtime.log_level, logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    handler.setFormatter(JsonLogFormatter())

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)

    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
