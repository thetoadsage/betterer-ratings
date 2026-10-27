from __future__ import annotations

import re
from typing import Any

from betterer_ratings.core.urls import sanitize_url_for_logs

_SECRET_KEYS = {
    "apikey",
    "token",
    "accesstoken",
    "refreshtoken",
    "auth",
    "authorization",
    "key",
    "password",
    "passwd",
    "secret",
    "clientsecret",
    "xapikey",
    "xmalclientid",
    "cookie",
    "setcookie",
}
_URL = re.compile(r"https?://[^\s\"'<>]+")
_BEARER = re.compile(r"\b(Bearer|Basic)\s+[A-Za-z0-9._~+/%=-]+", re.IGNORECASE)
_ASSIGNMENT = re.compile(
    r"(\b(?:api[_-]?key|access[_-]?token|refresh[_-]?token|token|authorization|auth|"
    r"password|passwd|client[_-]?secret|secret|x-api-key|x-mal-client-id|key)[\"']?\s*[=:]\s*)"
    r"(?:\"[^\"]*\"|'[^']*'|[^\s&,;\)\]}]+)",
    re.IGNORECASE,
)


def redact_text(value: str) -> str:
    value = _URL.sub(lambda match: sanitize_url_for_logs(match.group()), value)
    value = _BEARER.sub(r"\1 ***", value)
    return _ASSIGNMENT.sub(r"\1***", value)


def redact_payload(value: Any) -> Any:
    """Mask common credential fields and text without mutating caller-owned data."""
    if isinstance(value, dict):
        return {
            str(key): "***"
            if re.sub(r"[^a-z0-9]", "", str(key).lower()) in _SECRET_KEYS
            else redact_payload(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [redact_payload(item) for item in value]
    if isinstance(value, str):
        return redact_text(value)
    return value
