from __future__ import annotations

from urllib.parse import urlparse

_TZ_ALIASES = {
    "AEDT": "Australia/Sydney",
    "AEST": "Australia/Brisbane",
}

_PEOPLE_CREDIT_TYPE_FORM_MAP = {
    "actor": "Actor",
    "director": "Director",
    "writer": "Writer",
    "producer": "Producer",
    "gueststar": "GuestStar",
}


def _resolve_timezone_name(raw: str) -> str:
    v = (raw or "UTC").strip() or "UTC"
    return _TZ_ALIASES.get(v.upper(), v)


def _normalize_base_url(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        return ""
    # If user enters "10.0.0.5:8989", assume http://
    if "://" not in raw:
        raw = "http://" + raw
    p = urlparse(raw)
    if not p.scheme or not p.netloc:
        return raw
    # Common pitfall: Sonarr/Radarr default ports are HTTP, not HTTPS.
    # If user enters https://host:8989 (or :7878) it will fail with SSL WRONG_VERSION_NUMBER.
    if p.scheme == "https" and (p.port in (8989, 7878)) and (p.path in ("", "/")):
        base = f"http://{p.netloc}".rstrip("/")
        return base
    # Strip trailing slash, keep path if they run behind a reverse proxy subpath.
    base = f"{p.scheme}://{p.netloc}{p.path}".rstrip("/")
    return base


def _looks_like_url(raw: str) -> bool:
    v = (raw or "").strip().lower()
    return v.startswith("http://") or v.startswith("https://")


def _people_credit_types_csv_from_form(form_values: list[str] | None) -> str:
    credit_vals: list[str] = []
    for v in form_values or []:
        key = str(v).strip().lower().replace(" ", "")
        canon = _PEOPLE_CREDIT_TYPE_FORM_MAP.get(key)
        if canon:
            credit_vals.append(canon)
    credit_vals = sorted(set(credit_vals))
    return ",".join(credit_vals) if credit_vals else "Actor"
