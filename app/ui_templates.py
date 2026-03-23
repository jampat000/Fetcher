"""Shared Jinja2 environment for HTML routes."""

from __future__ import annotations

from fastapi.templating import Jinja2Templates

from app.paths import TEMPLATES_DIR
from app.version_info import get_app_version

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
templates.env.globals["app_version"] = get_app_version()
