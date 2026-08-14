from __future__ import annotations

import json
import logging
from collections import deque
from typing import Any, Optional

from betterer_ratings.observability.logging_setup import JsonLogFormatter

LOG_BUFFER_MAX_ENTRIES = 500
LOG_BUFFER_IMPORTANT_MAX_ENTRIES = 500

_LEVEL_VALUES: dict[str, int] = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}


class LogBufferHandler(logging.Handler):
    def __init__(
        self,
        maxlen: int = LOG_BUFFER_MAX_ENTRIES,
        *,
        important_maxlen: int = LOG_BUFFER_IMPORTANT_MAX_ENTRIES,
    ) -> None:
        super().__init__()
        self._formatter = JsonLogFormatter()
        self._entries: deque[dict[str, Any]] = deque(maxlen=maxlen)
        # WARNING+ entries are retained here too, so they survive eviction
        # from the main rolling buffer once 500 newer DEBUG/INFO logs push
        # them out. Both deques hold the same payload objects (not copies),
        # so `snapshot` can dedupe entries present in both by identity.
        self._important_entries: deque[dict[str, Any]] = deque(maxlen=important_maxlen)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            payload: dict[str, Any] = json.loads(self._formatter.format(record))
            payload["timestamp"] = record.created
        except Exception:
            self.handleError(record)
            return
        self._entries.append(payload)
        if record.levelno >= logging.WARNING:
            self._important_entries.append(payload)

    def _combined_entries(self) -> list[dict[str, Any]]:
        merged: dict[int, dict[str, Any]] = {}
        for entry in self._entries:
            merged[id(entry)] = entry
        for entry in self._important_entries:
            merged.setdefault(id(entry), entry)
        return sorted(merged.values(), key=lambda entry: entry.get("timestamp", 0.0))

    def snapshot(self, level: Optional[str] = None) -> list[dict[str, Any]]:
        assert self.lock is not None
        with self.lock:
            entries = self._combined_entries()
        if not level:
            return entries
        min_value = _LEVEL_VALUES.get(level.upper())
        if min_value is None:
            return entries
        return [
            entry
            for entry in entries
            if _LEVEL_VALUES.get(str(entry.get("level", "")).upper(), 0) >= min_value
        ]

    def clear(self) -> None:
        self._entries.clear()
        self._important_entries.clear()


_buffer_handler: Optional[LogBufferHandler] = None


def attach_once() -> LogBufferHandler:
    global _buffer_handler
    root = logging.getLogger()
    if _buffer_handler is None or _buffer_handler not in root.handlers:
        _buffer_handler = LogBufferHandler()
        root.addHandler(_buffer_handler)
    return _buffer_handler
