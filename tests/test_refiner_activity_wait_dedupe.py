"""RefinerActivity persistence: merge repeat blocked-waiting rows (same file + reason_code)."""

from __future__ import annotations

import asyncio
import json

from sqlalchemy import delete, func, select

from app.db import SessionLocal
from app.models import RefinerActivity
from app.refiner_activity_context import parse_activity_context
from app.refiner_activity_persistence import _persist_refiner_activity_safe


def _wait_meta(
    file_name: str,
    *,
    reason: str,
    message: str,
) -> dict:
    return {
        "file_name": file_name,
        "media_title": "T",
        "status": "failed",
        "size_before_bytes": 100,
        "size_after_bytes": 100,
        "audio_tracks_before": 1,
        "audio_tracks_after": 1,
        "subtitle_tracks_before": 0,
        "subtitle_tracks_after": 0,
        "processing_time_ms": None,
        "activity_context": json.dumps(
            {
                "v": 1,
                "reason_code": reason,
                "failure_reason": message,
            }
        ),
    }


def test_blocked_wait_same_file_reason_merges_despite_message_change() -> None:
    fn = "wait_dedupe_unique_file.mkv"

    async def main() -> None:
        async with SessionLocal() as session:
            await session.execute(delete(RefinerActivity).where(RefinerActivity.file_name == fn))
            await session.commit()

        await _persist_refiner_activity_safe(_wait_meta(fn, reason="radarr_queue_active_download", message="Radarr says A"))
        await _persist_refiner_activity_safe(_wait_meta(fn, reason="radarr_queue_active_download", message="Radarr says B"))

        async with SessionLocal() as session:
            n = (
                await session.execute(
                    select(func.count()).select_from(RefinerActivity).where(RefinerActivity.file_name == fn)
                )
            ).scalar_one()
            assert int(n) == 1
            row = (
                await session.execute(select(RefinerActivity).where(RefinerActivity.file_name == fn))
            ).scalars().first()
            assert row is not None
            ctx = parse_activity_context(row.activity_context)
            assert int(ctx.get("wait_repeat_count") or 1) == 2
            assert "B" in (ctx.get("failure_reason") or "")

    asyncio.run(main())


def test_blocked_wait_after_success_inserts_new_row() -> None:
    fn = "wait_dedupe_after_success.mkv"

    async def main() -> None:
        async with SessionLocal() as session:
            await session.execute(delete(RefinerActivity).where(RefinerActivity.file_name == fn))
            await session.commit()

        await _persist_refiner_activity_safe(_wait_meta(fn, reason="sonarr_queue_active_download", message="wait 1"))
        async with SessionLocal() as session:
            session.add(
                RefinerActivity(
                    file_name=fn,
                    media_title="T",
                    status="success",
                    size_before_bytes=10,
                    size_after_bytes=9,
                    audio_tracks_before=1,
                    audio_tracks_after=1,
                    subtitle_tracks_before=0,
                    subtitle_tracks_after=0,
                    processing_time_ms=100,
                    activity_context=json.dumps({"v": 1, "finalized": True}),
                )
            )
            await session.commit()

        await _persist_refiner_activity_safe(_wait_meta(fn, reason="sonarr_queue_active_download", message="wait 2"))

        async with SessionLocal() as session:
            n = (
                await session.execute(
                    select(func.count()).select_from(RefinerActivity).where(RefinerActivity.file_name == fn)
                )
            ).scalar_one()
            assert int(n) == 3

    asyncio.run(main())
