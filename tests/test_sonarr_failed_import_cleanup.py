from __future__ import annotations

import asyncio
from typing import Any

from sqlalchemy import delete, desc, select

from app.db import SessionLocal
from app.failed_import_activity import FAILED_IMPORT_ACTIVITY_V1
from app.models import ActivityLog, JobRunLog
from app.sonarr_failed_import_cleanup import (
    SONARR_CLEANUP_POLICY_ALL_ON,
    SonarrCleanupPolicy,
    run_sonarr_failed_import_queue_cleanup,
)
from app.web_common import activity_display_row

_SONARR_CLEANUP_WITHOUT_IMPORT_FAILED = SonarrCleanupPolicy(
    remove_corrupt=True,
    blocklist_corrupt=True,
    remove_download_failed=True,
    blocklist_download_failed=True,
    remove_unmatched=True,
    blocklist_unmatched=True,
    remove_quality=True,
    blocklist_quality=True,
)


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
                    "reason": "File is corrupt",
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
        self.delete_calls.append({"queue_id": int(queue_id), "blocklist": bool(blocklist), **kwargs})


class _FakeSonarrWaitingToImportNoEligible(_FakeSonarrClient):
    async def history_page(self, *, page: int, page_size: int) -> dict[str, Any]:
        msg = "Downloaded - Waiting to Import - No files found are eligible for import in F:\\"
        return {
            "records": [
                {
                    "eventType": "importFailed",
                    "downloadId": "d-wait",
                    "sourceTitle": "Pending Ep",
                    "reason": msg,
                }
            ],
            "totalRecords": 1,
        }

    async def queue_page(self, *, page: int, page_size: int) -> dict[str, Any]:
        return {
            "records": [{"id": 101, "downloadId": "d-wait"}],
            "totalRecords": 1,
        }


class _FakeSonarrQueueOnlyWaitingNoEligible(_FakeSonarrClient):
    async def history_page(self, *, page: int, page_size: int) -> dict[str, Any]:
        return {"records": [], "totalRecords": 0}

    async def queue_page(self, *, page: int, page_size: int) -> dict[str, Any]:
        msg = "Downloaded - Waiting to Import - No files found are eligible for import in F:\\"
        return {
            "records": [
                {
                    "id": 202,
                    "downloadId": "q-wait",
                    "title": "Queue Only Show",
                    "statusMessages": [{"messages": [msg]}],
                }
            ],
            "totalRecords": 1,
        }


def test_sonarr_cleanup_skips_waiting_to_import_no_eligible() -> None:
    asyncio.run(_run_sonarr_waiting_skip())


async def _run_sonarr_waiting_skip() -> None:
    client = _FakeSonarrWaitingToImportNoEligible()
    actions: list[str] = []
    async with SessionLocal() as session:
        await run_sonarr_failed_import_queue_cleanup(
            client,
            session=session,
            job_run_id=None,
            actions=actions,
            policy=SONARR_CLEANUP_POLICY_ALL_ON,
        )
        await session.commit()
    assert client.delete_calls == []


class _FakeSonarrUnknownHistoryClient(_FakeSonarrClient):
    async def history_page(self, *, page: int, page_size: int) -> dict[str, Any]:
        return {
            "records": [
                {
                    "eventType": "importFailed",
                    "downloadId": "su1",
                    "sourceTitle": "Mystery Ep",
                    "reason": "Deferred — manual operator review required",
                }
            ],
            "totalRecords": 1,
        }

    async def queue_page(self, *, page: int, page_size: int) -> dict[str, Any]:
        return {
            "records": [{"id": 77, "downloadId": "su1"}],
            "totalRecords": 1,
        }


def test_sonarr_cleanup_unknown_history_no_delete_without_import_failed_toggle() -> None:
    asyncio.run(_run_sonarr_unknown_history())


async def _run_sonarr_unknown_history() -> None:
    client = _FakeSonarrUnknownHistoryClient()
    actions: list[str] = []
    async with SessionLocal() as session:
        await run_sonarr_failed_import_queue_cleanup(
            client,
            session=session,
            job_run_id=None,
            actions=actions,
            policy=_SONARR_CLEANUP_WITHOUT_IMPORT_FAILED,
        )
        await session.commit()
    assert client.delete_calls == []


def test_sonarr_cleanup_unknown_history_removes_when_import_failed_toggle_only() -> None:
    asyncio.run(_run_sonarr_unknown_import_failed_only())


async def _run_sonarr_unknown_import_failed_only() -> None:
    client = _FakeSonarrUnknownHistoryClient()
    actions: list[str] = []
    policy = SonarrCleanupPolicy(
        remove_import_failed=True,
        blocklist_import_failed=True,
    )
    async with SessionLocal() as session:
        await run_sonarr_failed_import_queue_cleanup(
            client,
            session=session,
            job_run_id=None,
            actions=actions,
            policy=policy,
        )
        await session.commit()
    assert client.delete_calls == [{"queue_id": 77, "blocklist": True, "remove_from_client": False}]


class _FakeSonarrQueueGenericImportFailed(_FakeSonarrClient):
    async def history_page(self, *, page: int, page_size: int) -> dict[str, Any]:
        return {"records": [], "totalRecords": 0}

    async def queue_page(self, *, page: int, page_size: int) -> dict[str, Any]:
        return {
            "records": [
                {
                    "id": 305,
                    "downloadId": "s-gen",
                    "title": "Generic Ep",
                    "errorMessage": "Import failed",
                }
            ],
            "totalRecords": 1,
        }


def test_sonarr_cleanup_queue_generic_import_failed_skipped_without_toggle() -> None:
    asyncio.run(_run_sonarr_queue_generic_no_toggle())


async def _run_sonarr_queue_generic_no_toggle() -> None:
    client = _FakeSonarrQueueGenericImportFailed()
    actions: list[str] = []
    policy = SonarrCleanupPolicy(
        remove_download_failed=True,
        blocklist_download_failed=True,
    )
    async with SessionLocal() as session:
        await run_sonarr_failed_import_queue_cleanup(
            client,
            session=session,
            job_run_id=None,
            actions=actions,
            policy=policy,
        )
        await session.commit()
    assert client.delete_calls == []


def test_sonarr_cleanup_queue_generic_import_failed_removes_with_all_on() -> None:
    asyncio.run(_run_sonarr_queue_generic_all_on())


async def _run_sonarr_queue_generic_all_on() -> None:
    client = _FakeSonarrQueueGenericImportFailed()
    actions: list[str] = []
    async with SessionLocal() as session:
        await run_sonarr_failed_import_queue_cleanup(
            client,
            session=session,
            job_run_id=None,
            actions=actions,
            policy=SONARR_CLEANUP_POLICY_ALL_ON,
        )
        await session.commit()
    assert client.delete_calls == [{"queue_id": 305, "blocklist": True, "remove_from_client": False}]


class _FakeSonarrQueueDownloadFailedOnly(_FakeSonarrClient):
    async def history_page(self, *, page: int, page_size: int) -> dict[str, Any]:
        return {"records": [], "totalRecords": 0}

    async def queue_page(self, *, page: int, page_size: int) -> dict[str, Any]:
        return {
            "records": [
                {
                    "id": 303,
                    "downloadId": "s-dl-fail",
                    "title": "Failed Grab Show",
                    "errorMessage": "Download failed",
                }
            ],
            "totalRecords": 1,
        }


def test_sonarr_cleanup_queue_only_download_failed_removes_when_enabled() -> None:
    asyncio.run(_run_sonarr_queue_download_failed())


async def _run_sonarr_queue_download_failed() -> None:
    client = _FakeSonarrQueueDownloadFailedOnly()
    actions: list[str] = []
    policy = SonarrCleanupPolicy(
        remove_download_failed=True,
        blocklist_download_failed=True,
    )
    async with SessionLocal() as session:
        await run_sonarr_failed_import_queue_cleanup(
            client,
            session=session,
            job_run_id=None,
            actions=actions,
            policy=policy,
        )
        await session.commit()
    assert client.delete_calls == [{"queue_id": 303, "blocklist": True, "remove_from_client": False}]


class _FakeSonarrTrackedStateFailedOnly(_FakeSonarrClient):
    async def history_page(self, *, page: int, page_size: int) -> dict[str, Any]:
        return {"records": [], "totalRecords": 0}

    async def queue_page(self, *, page: int, page_size: int) -> dict[str, Any]:
        return {
            "records": [
                {
                    "id": 304,
                    "downloadId": "s-td",
                    "title": "Tracked Fail Show",
                    "trackedDownloadState": "failed",
                }
            ],
            "totalRecords": 1,
        }


def test_sonarr_cleanup_queue_tracked_state_failed_removes() -> None:
    asyncio.run(_run_sonarr_tracked_failed())


async def _run_sonarr_tracked_failed() -> None:
    client = _FakeSonarrTrackedStateFailedOnly()
    actions: list[str] = []
    policy = SonarrCleanupPolicy(
        remove_download_failed=True,
        blocklist_download_failed=True,
    )
    async with SessionLocal() as session:
        await run_sonarr_failed_import_queue_cleanup(
            client,
            session=session,
            job_run_id=None,
            actions=actions,
            policy=policy,
        )
        await session.commit()
    assert client.delete_calls == [{"queue_id": 304, "blocklist": True, "remove_from_client": False}]


def test_sonarr_cleanup_queue_only_waiting_no_eligible_no_delete() -> None:
    asyncio.run(_run_sonarr_queue_waiting_skip())


async def _run_sonarr_queue_waiting_skip() -> None:
    client = _FakeSonarrQueueOnlyWaitingNoEligible()
    actions: list[str] = []
    async with SessionLocal() as session:
        await run_sonarr_failed_import_queue_cleanup(
            client,
            session=session,
            job_run_id=None,
            actions=actions,
            policy=SONARR_CLEANUP_POLICY_ALL_ON,
        )
        await session.commit()
    assert client.delete_calls == []


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
            policy=SONARR_CLEANUP_POLICY_ALL_ON,
        )
        await session.commit()

    assert client.delete_calls == [{"queue_id": 99, "blocklist": True, "remove_from_client": False}]
    assert any("failed import cleaned up" in a.lower() for a in actions)

    async with SessionLocal() as session:
        row = (
            (await session.execute(select(ActivityLog).order_by(desc(ActivityLog.id)).limit(1)))
            .scalars()
            .first()
        )
        assert row is not None
        assert row.app == "sonarr"
        assert (row.detail or "").startswith(FAILED_IMPORT_ACTIVITY_V1)
        assert "Failed import cleaned up" in (row.detail or "")
        assert "Removed download and blocklisted release" in (row.detail or "")
        assert "Sonarr" in (row.detail or "")
        disp = activity_display_row(row, "UTC", now=row.created_at)
        assert disp["primary_label"] == "Failed import cleaned up"
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


def test_sonarr_cleanup_ambiguous_download_id_skips_history_driven_removal() -> None:
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
            policy=SONARR_CLEANUP_POLICY_ALL_ON,
        )
        await session.commit()
    assert client.delete_calls == []
    assert any("skipped failed-import cleanup" in a.lower() and "ambiguous" in a.lower() for a in actions)
    async with SessionLocal() as session:
        rows = (await session.execute(select(ActivityLog))).scalars().all()
        assert len(rows) == 0
        await session.execute(delete(ActivityLog))
        await session.commit()


class _FakeSonarrDeleteFails(_FakeSonarrClient):
    async def delete_queue_item(self, *, queue_id: int, blocklist: bool = False, **kwargs: Any) -> None:
        self.delete_calls.append({"queue_id": int(queue_id), "blocklist": bool(blocklist), **kwargs})
        if blocklist:
            raise RuntimeError("sonarr delete failed")


def test_sonarr_cleanup_delete_failure_writes_failed_activity() -> None:
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
            policy=SONARR_CLEANUP_POLICY_ALL_ON,
        )
        await session.commit()

    assert client.delete_calls == [{"queue_id": 99, "blocklist": True, "remove_from_client": False}]
    assert any("failed import removal failed" in a.lower() for a in actions)
    async with SessionLocal() as session:
        rows = (await session.execute(select(ActivityLog))).scalars().all()
        assert len(rows) == 1
        assert rows[0].status == "failed"
        assert "Failed import removal failed" in (rows[0].detail or "")
        await session.execute(delete(ActivityLog))
        await session.commit()
