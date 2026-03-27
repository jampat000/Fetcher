from __future__ import annotations

import asyncio
from typing import Any

from sqlalchemy import delete, desc, select

from app.db import SessionLocal
from app.models import ActivityLog, JobRunLog
from app.sonarr_failed_import_cleanup import run_sonarr_failed_import_queue_cleanup


class _FakeSonarrClient:
    def __init__(self) -> None:
        self.delete_calls: list[dict[str, Any]] = []

    async def history_page(self, *, page: int, page_size: int) -> dict[str, Any]:
        return {
            "records": [
                {
                    "eventType": "importFailed",
                    "downloadId": "d1",
                    "sourceTitle": "Test Series",
                    "reason": "bad file",
                }
            ],
            "totalRecords": 1,
        }

    async def queue_page(self, *, page: int, page_size: int) -> dict[str, Any]:
        return {
            "records": [{"id": 99, "downloadId": "d1"}],
            "totalRecords": 1,
        }

    async def delete_queue_item(self, *, queue_id: int, blocklist: bool = False, **kwargs: Any) -> None:
        self.delete_calls.append({"queue_id": int(queue_id), "blocklist": bool(blocklist)})


def test_sonarr_cleanup_success_requests_blocklist_on_delete() -> None:
    asyncio.run(_run_sonarr_success())


async def _run_sonarr_success() -> None:
    client = _FakeSonarrClient()
    actions: list[str] = []
    async with SessionLocal() as session:
        log = JobRunLog(ok=True, message="")
        session.add(log)
        await session.commit()
        await session.refresh(log)
        await run_sonarr_failed_import_queue_cleanup(
            client,
            session=session,
            job_run_id=log.id,
            actions=actions,
        )
        await session.commit()

    assert client.delete_calls == [{"queue_id": 99, "blocklist": True}]
    assert any("blocklist requested" in a.lower() for a in actions)

    async with SessionLocal() as session:
        row = (
            (await session.execute(select(ActivityLog).order_by(desc(ActivityLog.id)).limit(1)))
            .scalars()
            .first()
        )
        assert row is not None
        assert row.app == "sonarr"
        assert "blocklist requested via sonarr api" in (row.detail or "").lower()
        await session.execute(delete(ActivityLog))
        await session.execute(delete(JobRunLog))
        await session.commit()


class _FakeAmbiguousClient(_FakeSonarrClient):
    async def queue_page(self, *, page: int, page_size: int) -> dict[str, Any]:
        return {
            "records": [
                {"id": 1, "downloadId": "d1"},
                {"id": 2, "downloadId": "d1"},
            ],
            "totalRecords": 2,
        }


def test_sonarr_cleanup_ambiguous_download_id_removes_all_matches() -> None:
    asyncio.run(_run_sonarr_ambiguous())


async def _run_sonarr_ambiguous() -> None:
    client = _FakeAmbiguousClient()
    actions: list[str] = []
    async with SessionLocal() as session:
        await run_sonarr_failed_import_queue_cleanup(
            client,
            session=session,
            job_run_id=None,
            actions=actions,
        )
        await session.commit()
    assert client.delete_calls == [
        {"queue_id": 1, "blocklist": True},
        {"queue_id": 2, "blocklist": True},
    ]
    assert any("multiple queue ids" in a.lower() for a in actions)


class _FakeSonarrDeleteFails(_FakeSonarrClient):
    async def delete_queue_item(self, *, queue_id: int, blocklist: bool = False, **kwargs: Any) -> None:
        self.delete_calls.append({"queue_id": int(queue_id), "blocklist": bool(blocklist)})
        if blocklist:
            raise RuntimeError("sonarr delete failed")


def test_sonarr_cleanup_delete_failure_retries_without_blocklist() -> None:
    asyncio.run(_run_sonarr_delete_fail())


async def _run_sonarr_delete_fail() -> None:
    client = _FakeSonarrDeleteFails()
    actions: list[str] = []
    async with SessionLocal() as session:
        await run_sonarr_failed_import_queue_cleanup(
            client,
            session=session,
            job_run_id=None,
            actions=actions,
        )
        await session.commit()

    assert client.delete_calls == [
        {"queue_id": 99, "blocklist": True},
        {"queue_id": 99, "blocklist": False},
    ]
    assert any("without blocklist" in a.lower() for a in actions)
