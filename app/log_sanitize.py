"""Helpers to avoid persisting secrets in user-visible or exported logs."""

from __future__ import annotations

import logging
import os
import re
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

# Query keys that often carry credentials (Emby uses api_key on the wire).
_SENSITIVE_QUERY_KEYS = frozenset(
    {
        "api_key",
        "apikey",
        "sonarr_key",
        "radarr_key",
        "token",
        "access_token",
        "refresh_token",
        "key",
        "password",
        "secret",
    }
)

# HTTP(S) URLs in arbitrary text (logs, tracebacks, JSON snippets).
_URL_RE = re.compile(r"https?://[^\s\)\]\}\"\'<>]+", re.IGNORECASE)

# key=value / key: value forms (logs, query fragments in bodies).
_KV_SECRET_RE = re.compile(
    r"(?i)\b(api_key|apikey|x-api-key|sonarr_key|radarr_key|access_token|refresh_token|password|secret|token|set-cookie)\b\s*[:=]\s*"
    r'(?:\[[^\]]+\]|"[^"]*"|\'[^\']*\'|\S+)',
)

# JSON-style "key":"value" for sensitive keys.
_JSON_SECRET_RE = re.compile(
    r'(?i)("(?:api_key|apikey|x-api-key|sonarr_key|radarr_key|access_token|refresh_token|password|secret|token|set-cookie)"\s*:\s*)"[^"]*"',
)

_BEARER_RE = re.compile(r"(?i)Bearer\s+[\w\-.~+/=]+")
_AUTH_HEADER_RE = re.compile(r"(?im)^Authorization:\s*\S.*$")
# 32-char hex tokens (common API key material in logs / payloads).
_HEX_API_KEY_32_RE = re.compile(
    r"(?i)(?:api_key|apikey|x-api-key|sonarr_key|radarr_key|token|secret|key)\s*[:=]\s*['\"]?([A-Fa-f0-9]{32})\b"
)


def redact_url_for_logging(url: str | object) -> str:
    """Remove credential-like query params and userinfo from a URL for logging."""
    try:
        p = urlparse(str(url))
        netloc = p.netloc
        if "@" in netloc:
            userinfo, _sep, hostport = netloc.rpartition("@")
            if userinfo and hostport:
                netloc = "***:***@" + hostport
        if not p.query:
            return urlunparse((p.scheme, netloc, p.path, p.params, "", p.fragment))
        pairs = [
            (k, "***" if k.lower() in _SENSITIVE_QUERY_KEYS else v)
            for k, v in parse_qsl(p.query, keep_blank_values=True)
        ]
        new_query = urlencode(pairs)
        return urlunparse((p.scheme, netloc, p.path, p.params, new_query, p.fragment))
    except Exception:
        return "<url>"


def redact_sensitive_text(text: str | None) -> str:
    """Redact secrets from free-form log text (URLs, JSON snippets, headers, key=value, hex API keys)."""
    if text is None:
        return ""
    s = str(text)
    s = _URL_RE.sub(lambda m: redact_url_for_logging(m.group(0)), s)
    s = _KV_SECRET_RE.sub(lambda m: f"{m.group(1)}=[REDACTED]", s)
    s = _JSON_SECRET_RE.sub(r'\1"[REDACTED]"', s)
    s = _BEARER_RE.sub("Bearer [REDACTED]", s)
    s = _AUTH_HEADER_RE.sub("Authorization: [REDACTED]", s)
    s = _HEX_API_KEY_32_RE.sub(lambda m: m.group(0).replace(m.group(1), "[REDACTED]"), s)
    return s


class SensitiveLogFilter(logging.Filter):
    """Redacts secrets in the message template and %-format args before formatting."""

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: A003 — logging API
        try:
            if isinstance(record.msg, str):
                record.msg = redact_sensitive_text(record.msg)
            if record.args:
                record.args = tuple(
                    redact_sensitive_text(a) if isinstance(a, str) else a for a in record.args
                )
        except Exception:
            pass
        return True


class RedactingFormatter(logging.Formatter):
    """Wraps another formatter so the final line (including tracebacks) is redacted."""

    def __init__(self, base: logging.Formatter) -> None:
        self._base = base

    def format(self, record: logging.LogRecord) -> str:
        return redact_sensitive_text(self._base.format(record))


_CONFIGURED_HANDLER_IDS: set[int] = set()
_FETCHER_ROTATING_FILE_HANDLER: logging.Handler | None = None


def configure_fetcher_logging() -> None:
    """Set root log level to WARNING, attach redaction to handlers, and add ``fetcher.log`` file output."""
    root = logging.getLogger()
    level_name = (os.environ.get("FETCHER_LOG_LEVEL") or "WARNING").strip().upper()
    level = getattr(logging, level_name, logging.WARNING)
    root.setLevel(level)

    filt = SensitiveLogFilter()
    for h in root.handlers:
        hid = id(h)
        if hid in _CONFIGURED_HANDLER_IDS:
            continue
        _CONFIGURED_HANDLER_IDS.add(hid)
        h.addFilter(filt)
        if h.formatter is not None and not isinstance(h.formatter, RedactingFormatter):
            h.setFormatter(RedactingFormatter(h.formatter))

    global _FETCHER_ROTATING_FILE_HANDLER
    if _FETCHER_ROTATING_FILE_HANDLER is None:
        try:
            from logging.handlers import RotatingFileHandler

            from app.paths import resolved_logs_dir

            log_path = resolved_logs_dir() / "fetcher.log"
            fh = RotatingFileHandler(
                str(log_path),
                maxBytes=10 * 1024 * 1024,
                backupCount=5,
                encoding="utf-8",
            )
            fh.setLevel(level)
            base_fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s", "%Y-%m-%d %H:%M:%S")
            fh.setFormatter(RedactingFormatter(base_fmt))
            fh.addFilter(SensitiveLogFilter())
            root.addHandler(fh)
            _FETCHER_ROTATING_FILE_HANDLER = fh
        except OSError:
            logging.getLogger(__name__).warning(
                "Could not create rotating log file under %s",
                (os.environ.get("FETCHER_LOG_DIR") or "<data dir>/logs"),
                exc_info=True,
            )
