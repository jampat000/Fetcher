"""Refiner readiness accepts typical Linux/container path strings."""

from __future__ import annotations

from pathlib import Path

import pytest

from app.models import AppSettings
from app.refiner_readiness import refiner_readiness_issues


@pytest.fixture
def base_row(tmp_path: Path) -> AppSettings:
    w = tmp_path / "downloads"
    o = tmp_path / "output"
    w.mkdir()
    o.mkdir()
    row = AppSettings()
    row.refiner_enabled = True
    row.refiner_primary_audio_lang = "eng"
    row.refiner_watched_folder = str(w)
    row.refiner_output_folder = str(o)
    return row


def test_refiner_readiness_no_issues_for_posix_style_paths(base_row: AppSettings, tmp_path: Path) -> None:
    """Paths need not look like Windows drives."""
    base_row.refiner_watched_folder = str(tmp_path / "downloads")
    base_row.refiner_output_folder = str(tmp_path / "output")
    assert refiner_readiness_issues(base_row) == []


def test_refiner_validate_folders_section_accepts_posix_strings() -> None:
    from app.refiner_readiness import refiner_validate_settings_save_section

    err = refiner_validate_settings_save_section(
        "folders",
        enabled=True,
        primary_lang="eng",
        watched_folder="/downloads",
        output_folder="/output",
    )
    assert err == (None, None)
