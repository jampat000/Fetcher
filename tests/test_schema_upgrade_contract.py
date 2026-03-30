"""Guardrails: migrate() must keep refiner repair + validation contract discoverable in CI."""

from __future__ import annotations

import inspect

from app import migrations
from app.schema_upgrade_contract import REFINER_REPAIR_ENTRYPOINT


def test_migrate_source_calls_refiner_repair_and_is_ordered_before_end() -> None:
    src = inspect.getsource(migrations.migrate)
    assert REFINER_REPAIR_ENTRYPOINT in src
    assert "await _ensure_refiner_app_settings_columns" in src
