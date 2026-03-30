"""Operator-facing Refiner failure formatting."""

from __future__ import annotations

import errno

from app.refiner_errors import (
    failure_hint_from_exception,
    format_refiner_failure_for_operator,
)


def test_format_oserror_exdev_style() -> None:
    exc = OSError(errno.EXDEV, "Invalid cross-device link")
    summary, detail = format_refiner_failure_for_operator(exc)
    assert "finalize" in summary.lower()
    assert detail is not None
    assert "EXDEV" in detail or "cross" in detail.lower()


def test_format_winerror_17_style() -> None:
    exc = OSError(0, "cannot move")
    setattr(exc, "winerror", 17)
    summary, detail = format_refiner_failure_for_operator(exc)
    assert "finalize" in summary.lower()
    assert detail is not None


def test_failure_hint_from_exception_joins_detail() -> None:
    exc = OSError(errno.EXDEV, "boom")
    h = failure_hint_from_exception(exc)
    assert "finalize" in h.lower()
    assert "Reason:" in h


def test_format_output_race_runtimeerror() -> None:
    exc = RuntimeError(
        "Output file appeared while Refiner was working — another writer may have created it."
    )
    summary, detail = format_refiner_failure_for_operator(exc)
    assert "finalize" in summary.lower() or "output path" in summary.lower()
