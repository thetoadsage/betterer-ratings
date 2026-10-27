from __future__ import annotations

from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


def sanitize_url_for_logs(url: str) -> str:
    sensitive_keys = {
        "api_key",
        "apikey",
        "token",
        "access_token",
        "auth",
        "authorization",
        "key",
        "password",
        "client_secret",
        "refresh_token",
    }
    try:
        parts = urlsplit(url)
        netloc = parts.netloc
        if "@" in netloc:
            netloc = "***@" + netloc.rsplit("@", 1)[1]
        if not parts.query and netloc == parts.netloc:
            return url
        sanitized_query = []
        for key, value in parse_qsl(parts.query, keep_blank_values=True):
            if key.lower() in sensitive_keys:
                sanitized_query.append((key, "***"))
            else:
                sanitized_query.append((key, value))
        return urlunsplit(
            (
                parts.scheme,
                netloc,
                parts.path,
                urlencode(sanitized_query, doseq=True),
                parts.fragment,
            )
        )
    except Exception:
        return url
