from __future__ import annotations

import asyncio
from dataclasses import replace
from typing import Any

import pytest
from sqlalchemy import delete, desc, select
from app.db import SessionLocal, get_or_create_settings
from app.failed_import_activity import FAILED_IMPORT_ACTIVITY_V1
from app.models import ActivityLog, AppSnapshot, JobRunLog
from app.web_common import activity_display_row
from app.radarr_failed_import_cleanup import (
    RADARR_CLEANUP_POLICY_ALL_ON,
    RadarrCleanupPolicy,
    classify_queue_matches_by_download_id,
    import_failed_record_is_pending_waiting_no_eligible,
    is_radarr_import_failed_record,
    is_radarr_queue_non_quality_upgrade_rejection,
    parse_radarr_import_failed_reason,
    run_radarr_failed_import_queue_cleanup,
    user_visible_text_is_pending_waiting_no_eligible,
)

# Every scenario except unclassified import-failed (matches pre–import_failed-toggle behavior).
_RADARR_CLEANUP_WITHOUT_IMPORT_FAILED = RadarrCleanupPolicy(
    remove_corrupt=True,
    blocklist_corrupt=True,
    remove_download_failed=True,
    blocklist_download_failed=True,
    remove_unmatched=True,
    blocklist_unmatched=True,
    remove_quality=True,
    blocklist_quality=True,
)


def test_is_radarr_import_failed_record_accepts_string() -> None:
    assert is_radarr_import_failed_record({"eventType": "importFailed"}) is True
    assert is_radarr_import_failed_record({"eventType": "IMPORTFAILED"}) is True
    assert is_radarr_import_failed_record({"eventType": "downloadFailed"}) is False


def test_is_radarr_import_failed_record_accepts_int_nine() -> None:
    assert is_radarr_import_failed_record({"eventType": 9}) is True
    assert is_radarr_import_failed_record({"eventType": 8}) is False


def test_classify_queue_matches_none() -> None:
    kind, qid = classify_queue_matches_by_download_id(
        "abc",
        [{"id": 1, "downloadId": "other"}],
    )
    assert kind == "none" and qid is None


def test_classify_queue_matches_one() -> None:
    kind, qid = classify_queue_matches_by_download_id(
        "dl-1",
        [
            {"id": 10, "downloadId": "dl-1"},
            {"id": 11, "downloadId": "x"},
        ],
    )
    assert kind == "one" and qid == 10


def test_classify_queue_matches_many_distinct_ids() -> None:
    kind, qid = classify_queue_matches_by_download_id(
        "dup",
        [
            {"id": 1, "downloadId": "dup"},
            {"id": 2, "downloadId": "dup"},
        ],
    )
    assert kind == "many" and qid is None


def test_classify_queue_matches_duplicate_rows_same_id_is_one() -> None:
    """Duplicate API rows pointing at the same queue id → still exactly one target."""
    kind, qid = classify_queue_matches_by_download_id(
        "same",
        [
            {"id": 5, "downloadId": "same"},
            {"id": 5, "downloadId": "same"},
        ],
    )
    assert kind == "one" and qid == 5


def test_parse_radarr_import_failed_reason_top_level() -> None:
    assert parse_radarr_import_failed_reason({"reason": " bad "}) == "bad"


def test_parse_radarr_import_failed_reason_nested_data() -> None:
    rec: dict[str, Any] = {"data": {"message": "nested msg"}}
    assert parse_radarr_import_failed_reason(rec) == "nested msg"


def test_parse_radarr_import_failed_reason_missing_is_empty() -> None:
    assert parse_radarr_import_failed_reason({}) == ""
    assert parse_radarr_import_failed_reason({"data": None}) == ""
    assert parse_radarr_import_failed_reason({"data": "not-a-dict"}) == ""


def test_parse_radarr_import_failed_reason_non_string_values_ignored() -> None:
    assert parse_radarr_import_failed_reason({"reason": 123}) == ""


def test_is_radarr_queue_non_quality_upgrade_rejection_detects_not_an_upgrade_message() -> None:
    q: dict[str, Any] = {
        "id": 1,
        "statusMessages": [
            {
                "messages": [
                    "Not an upgrade for existing movie file. Existing quality: Bluray-1080p. New Quality WEBDL-480p"
                ]
            }
        ],
    }
    assert is_radarr_queue_non_quality_upgrade_rejection(q) is True


def test_is_radarr_queue_non_quality_upgrade_rejection_detects_preferred_word_variant() -> None:
    q: dict[str, Any] = {
        "errorMessage": "Not a preferred word upgrade for existing movie file.",
    }
    assert is_radarr_queue_non_quality_upgrade_rejection(q) is True


def test_user_visible_text_pending_waiting_no_eligible_known_case() -> None:
    msg = "Downloaded - Waiting to Import - No files found are eligible for import in F:\\"
    assert user_visible_text_is_pending_waiting_no_eligible(msg) is True


def test_user_visible_text_pending_requires_both_phrases() -> None:
    assert user_visible_text_is_pending_waiting_no_eligible("No files found are eligible for import") is False
    assert user_visible_text_is_pending_waiting_no_eligible("Waiting to import something") is False


def test_import_failed_record_pending_detects_nested_message() -> None:
    rec = {
        "eventType": "importFailed",
        "downloadId": "x",
        "data": {
            "message": "Downloaded - Waiting to Import - No files found are eligible for import in /data",
        },
    }
    assert import_failed_record_is_pending_waiting_no_eligible(rec) is True


def test_is_radarr_queue_non_quality_upgrade_rejection_ignores_parse_errors() -> None:
    q: dict[str, Any] = {
        "id": 2,
        "statusMessages": [{"messages": ["Unable to parse media info from file"]}],
    }
    assert is_radarr_queue_non_quality_upgrade_rejection(q) is False


class _FakeRadarrClient:
    def __init__(self) -> None:
        self.delete_calls: list[dict[str, Any]] = []

    async def history_page(self, *, page: int, page_size: int) -> dict[str, Any]:
        if page != 1:
            return {"records": [], "totalRecords": 1}
        return {
            "records": [
                {
                    "eventType": "importFailed",
                    "downloadId": "d1",
                    "sourceTitle": "Test Movie",
                    "reason": "corrupt",
                }
            ],
            "totalRecords": 1,
        }

    async def queue_page(self, *, page: int, page_size: int) -> dict[str, Any]:
        if page != 1:
            return {"records": [], "totalRecords": 1}
        return {
            "records": [{"id": 42, "downloadId": "d1"}],
            "totalRecords": 1,
        }

    async def delete_queue_item(self, *, queue_id: int, blocklist: bool = False, **kwargs: Any) -> None:
        self.delete_calls.append({"queue_id": int(queue_id), "blocklist": bool(blocklist), **kwargs})


def test_run_cleanup_success_removes_queue_and_writes_activity() -> None:
    asyncio.run(_run_cleanup_success())


async def _run_cleanup_success() -> None:
    client = _FakeRadarrClient()
    actions: list[str] = []
    async with SessionLocal() as session:
        log = JobRunLog(ok=True, message="")
        session.add(log)
        await session.commit()
        await session.refresh(log)
        await run_radarr_failed_import_queue_cleanup(
            client,
            session=session,
            job_run_id=log.id,
            actions=actions,
            policy=RADARR_CLEANUP_POLICY_ALL_ON,
        )
        await session.commit()

    assert client.delete_calls == [{"queue_id": 42, "blocklist": True, "remove_from_client": False}]
    assert any("failed import cleaned up" in a.lower() for a in actions)

    async with SessionLocal() as session:
        row = (
            (await session.execute(select(ActivityLog).order_by(desc(ActivityLog.id)).limit(1)))
            .scalars()
            .first()
        )
        assert row is not None
        assert row.app == "radarr"
        assert row.kind == "cleanup"
        assert row.count == 1
        assert (row.detail or "").startswith(FAILED_IMPORT_ACTIVITY_V1)
        assert "Failed import cleaned up" in (row.detail or "")
        assert "Removed download and blocklisted release" in (row.detail or "")
        assert "Radarr" in (row.detail or "")
        assert "Test Movie" in (row.detail or "")
        assert "Reason: corrupt" in (row.detail or "")
        disp = activity_display_row(row, "UTC", now=row.created_at)
        assert disp["primary_label"] == "Failed import cleaned up"
        await session.execute(delete(ActivityLog))
        await session.execute(delete(JobRunLog))
        await session.commit()


def test_run_cleanup_remove_from_client_true_when_policy_set() -> None:
    asyncio.run(_run_cleanup_success_remove_from_client())


async def _run_cleanup_success_remove_from_client() -> None:
    client = _FakeRadarrClient()
    actions: list[str] = []
    policy = replace(RADARR_CLEANUP_POLICY_ALL_ON, remove_from_client=True)
    async with SessionLocal() as session:
        log = JobRunLog(ok=True, message="")
        session.add(log)
        await session.commit()
        await session.refresh(log)
        await run_radarr_failed_import_queue_cleanup(
            client,
            session=session,
            job_run_id=log.id,
            actions=actions,
            policy=policy,
        )
        await session.commit()

    assert client.delete_calls == [{"queue_id": 42, "blocklist": True, "remove_from_client": True}]
    async with SessionLocal() as session:
        row = (
            (await session.execute(select(ActivityLog).order_by(desc(ActivityLog.id)).limit(1)))
            .scalars()
            .first()
        )
        assert row is not None
        assert "removeFromClient" in (row.detail or "")
        await session.execute(delete(ActivityLog))
        await session.execute(delete(JobRunLog))
        await session.commit()


class _FakeNoMatchClient(_FakeRadarrClient):
    async def queue_page(self, *, page: int, page_size: int) -> dict[str, Any]:
        return {"records": [{"id": 1, "downloadId": "other"}], "totalRecords": 1}


class _FakeWaitingToImportNoEligibleClient(_FakeRadarrClient):
    """History importFailed + known waiting-to-import wording — must not remove or blocklist."""

    async def history_page(self, *, page: int, page_size: int) -> dict[str, Any]:
        if page != 1:
            return {"records": [], "totalRecords": 1}
        msg = "Downloaded - Waiting to Import - No files found are eligible for import in F:\\"
        return {
            "records": [
                {
                    "eventType": "importFailed",
                    "downloadId": "d-wait",
                    "sourceTitle": "Pending Movie",
                    "reason": msg,
                }
            ],
            "totalRecords": 1,
        }

    async def queue_page(self, *, page: int, page_size: int) -> dict[str, Any]:
        if page != 1:
            return {"records": [], "totalRecords": 1}
        return {
            "records": [{"id": 100, "downloadId": "d-wait"}],
            "totalRecords": 1,
        }


class _FakeUnknownHistoryImportFailedClient(_FakeRadarrClient):
    """importFailed with message that does not match corrupt/quality/etc. → IMPORT_FAILED scenario."""

    async def history_page(self, *, page: int, page_size: int) -> dict[str, Any]:
        if page != 1:
            return {"records": [], "totalRecords": 1}
        return {
            "records": [
                {
                    "eventType": "importFailed",
                    "downloadId": "du-unknown",
                    "sourceTitle": "Obscure Title",
                    "reason": "Needs operator review — internal code 999",
                }
            ],
            "totalRecords": 1,
        }

    async def queue_page(self, *, page: int, page_size: int) -> dict[str, Any]:
        if page != 1:
            return {"records": [], "totalRecords": 1}
        return {
            "records": [{"id": 55, "downloadId": "du-unknown"}],
            "totalRecords": 1,
        }


def test_run_cleanup_unknown_history_no_delete_without_import_failed_toggle() -> None:
    asyncio.run(_run_unknown_history_no_delete())


async def _run_unknown_history_no_delete() -> None:
    client = _FakeUnknownHistoryImportFailedClient()
    actions: list[str] = []
    async with SessionLocal() as session:
        await run_radarr_failed_import_queue_cleanup(
            client,
            session=session,
            job_run_id=None,
            actions=actions,
            policy=_RADARR_CLEANUP_WITHOUT_IMPORT_FAILED,
        )
        await session.commit()
    assert client.delete_calls == []


def test_run_cleanup_unknown_history_removes_when_import_failed_toggle_only() -> None:
    asyncio.run(_run_unknown_history_import_failed_only())


async def _run_unknown_history_import_failed_only() -> None:
    client = _FakeUnknownHistoryImportFailedClient()
    actions: list[str] = []
    policy = RadarrCleanupPolicy(
        remove_import_failed=True,
        blocklist_import_failed=True,
    )
    async with SessionLocal() as session:
        await run_radarr_failed_import_queue_cleanup(
            client,
            session=session,
            job_run_id=None,
            actions=actions,
            policy=policy,
        )
        await session.commit()
    assert client.delete_calls == [{"queue_id": 55, "blocklist": True, "remove_from_client": False}]


class _FakeRadarrQueueGenericImportFailedMessage(_FakeRadarrClient):
    """Queue-only: generic 'Import failed' line → IMPORT_FAILED when that toggle is on."""

    async def history_page(self, *, page: int, page_size: int) -> dict[str, Any]:
        return {"records": [], "totalRecords": 0}

    async def queue_page(self, *, page: int, page_size: int) -> dict[str, Any]:
        if page != 1:
            return {"records": [], "totalRecords": 1}
        return {
            "records": [
                {
                    "id": 66,
                    "downloadId": "gen-fail",
                    "title": "Generic Fail Movie",
                    "errorMessage": "Import failed",
                }
            ],
            "totalRecords": 1,
        }


class _FakeRadarrQueueDownloadFailedOnly(_FakeRadarrClient):
    """Queue-only: explicit download-failed copy (no matching history row)."""

    async def history_page(self, *, page: int, page_size: int) -> dict[str, Any]:
        return {"records": [], "totalRecords": 0}

    async def queue_page(self, *, page: int, page_size: int) -> dict[str, Any]:
        if page != 1:
            return {"records": [], "totalRecords": 1}
        return {
            "records": [
                {
                    "id": 301,
                    "downloadId": "dl-fail",
                    "title": "Failed Grab Movie",
                    "errorMessage": "Download failed",
                }
            ],
            "totalRecords": 1,
        }


def test_run_cleanup_queue_only_download_failed_removes_when_enabled() -> None:
    asyncio.run(_run_queue_download_failed_only())


async def _run_queue_download_failed_only() -> None:
    client = _FakeRadarrQueueDownloadFailedOnly()
    actions: list[str] = []
    policy = RadarrCleanupPolicy(
        remove_download_failed=True,
        blocklist_download_failed=True,
    )
    async with SessionLocal() as session:
        await run_radarr_failed_import_queue_cleanup(
            client,
            session=session,
            job_run_id=None,
            actions=actions,
            policy=policy,
        )
        await session.commit()
    assert client.delete_calls == [{"queue_id": 301, "blocklist": True, "remove_from_client": False}]


def test_run_cleanup_queue_only_download_failed_skipped_when_only_corrupt_enabled() -> None:
    asyncio.run(_run_queue_download_failed_corrupt_only_policy())


async def _run_queue_download_failed_corrupt_only_policy() -> None:
    client = _FakeRadarrQueueDownloadFailedOnly()
    actions: list[str] = []
    policy = RadarrCleanupPolicy(remove_corrupt=True, blocklist_corrupt=True)
    async with SessionLocal() as session:
        await run_radarr_failed_import_queue_cleanup(
            client,
            session=session,
            job_run_id=None,
            actions=actions,
            policy=policy,
        )
        await session.commit()
    assert client.delete_calls == []


class _FakeRadarrTrackedStateFailedOnly(_FakeRadarrClient):
    """Empty user messages but API tracked state Failed — still terminal download failure."""

    async def history_page(self, *, page: int, page_size: int) -> dict[str, Any]:
        return {"records": [], "totalRecords": 0}

    async def queue_page(self, *, page: int, page_size: int) -> dict[str, Any]:
        if page != 1:
            return {"records": [], "totalRecords": 1}
        return {
            "records": [
                {
                    "id": 302,
                    "downloadId": "td-fail",
                    "title": "Tracked Fail Movie",
                    "trackedDownloadState": "failed",
                }
            ],
            "totalRecords": 1,
        }


def test_run_cleanup_queue_tracked_state_failed_removes() -> None:
    asyncio.run(_run_tracked_state_failed())


async def _run_tracked_state_failed() -> None:
    client = _FakeRadarrTrackedStateFailedOnly()
    actions: list[str] = []
    policy = RadarrCleanupPolicy(
        remove_download_failed=True,
        blocklist_download_failed=True,
    )
    async with SessionLocal() as session:
        await run_radarr_failed_import_queue_cleanup(
            client,
            session=session,
            job_run_id=None,
            actions=actions,
            policy=policy,
        )
        await session.commit()
    assert client.delete_calls == [{"queue_id": 302, "blocklist": True, "remove_from_client": False}]


def test_run_cleanup_queue_only_generic_import_failed_no_delete_without_toggle() -> None:
    asyncio.run(_run_queue_generic_no_delete())


async def _run_queue_generic_no_delete() -> None:
    client = _FakeRadarrQueueGenericImportFailedMessage()
    actions: list[str] = []
    policy = RadarrCleanupPolicy(
        remove_download_failed=True,
        blocklist_download_failed=True,
    )
    async with SessionLocal() as session:
        await run_radarr_failed_import_queue_cleanup(
            client,
            session=session,
            job_run_id=None,
            actions=actions,
            policy=policy,
        )
        await session.commit()
    assert client.delete_calls == []


def test_run_cleanup_queue_only_generic_import_failed_removes_with_all_on() -> None:
    asyncio.run(_run_queue_generic_all_on())


async def _run_queue_generic_all_on() -> None:
    client = _FakeRadarrQueueGenericImportFailedMessage()
    actions: list[str] = []
    async with SessionLocal() as session:
        await run_radarr_failed_import_queue_cleanup(
            client,
            session=session,
            job_run_id=None,
            actions=actions,
            policy=RADARR_CLEANUP_POLICY_ALL_ON,
        )
        await session.commit()
    assert client.delete_calls == [{"queue_id": 66, "blocklist": True, "remove_from_client": False}]


def test_run_cleanup_skips_waiting_to_import_no_eligible_no_delete() -> None:
    asyncio.run(_run_waiting_no_eligible_skip())


async def _run_waiting_no_eligible_skip() -> None:
    client = _FakeWaitingToImportNoEligibleClient()
    actions: list[str] = []
    async with SessionLocal() as session:
        await run_radarr_failed_import_queue_cleanup(
            client,
            session=session,
            job_run_id=None,
            actions=actions,
            policy=RADARR_CLEANUP_POLICY_ALL_ON,
        )
        await session.commit()
    assert client.delete_calls == []


class _FakeQueueOnlyWaitingNoEligibleClient(_FakeRadarrClient):
    async def history_page(self, *, page: int, page_size: int) -> dict[str, Any]:
        return {"records": [], "totalRecords": 0}

    async def queue_page(self, *, page: int, page_size: int) -> dict[str, Any]:
        if page != 1:
            return {"records": [], "totalRecords": 1}
        msg = "Downloaded - Waiting to Import - No files found are eligible for import in F:\\"
        return {
            "records": [
                {
                    "id": 200,
                    "downloadId": "q-wait",
                    "title": "Queue Only",
                    "statusMessages": [{"messages": [msg]}],
                }
            ],
            "totalRecords": 1,
        }


def test_run_cleanup_queue_only_waiting_no_eligible_no_delete() -> None:
    asyncio.run(_run_queue_only_waiting_skip())


async def _run_queue_only_waiting_skip() -> None:
    client = _FakeQueueOnlyWaitingNoEligibleClient()
    actions: list[str] = []
    async with SessionLocal() as session:
        await run_radarr_failed_import_queue_cleanup(
            client,
            session=session,
            job_run_id=None,
            actions=actions,
            policy=RADARR_CLEANUP_POLICY_ALL_ON,
        )
        await session.commit()
    assert client.delete_calls == []


def test_run_cleanup_no_match_no_delete() -> None:
    asyncio.run(_run_no_match())


async def _run_no_match() -> None:
    client = _FakeNoMatchClient()
    actions: list[str] = []
    async with SessionLocal() as session:
        await run_radarr_failed_import_queue_cleanup(
            client,
            session=session,
            job_run_id=None,
            actions=actions,
            policy=RADARR_CLEANUP_POLICY_ALL_ON,
        )
        await session.commit()
    assert client.delete_calls == []


class _FakeAmbiguousClient(_FakeRadarrClient):
    async def queue_page(self, *, page: int, page_size: int) -> dict[str, Any]:
        return {
            "records": [
                {"id": 1, "downloadId": "d1"},
                {"id": 2, "downloadId": "d1"},
            ],
            "totalRecords": 2,
        }


def test_run_cleanup_ambiguous_no_delete() -> None:
    asyncio.run(_run_ambiguous())


async def _run_ambiguous() -> None:
    client = _FakeAmbiguousClient()
    actions: list[str] = []
    async with SessionLocal() as session:
        await session.execute(delete(ActivityLog))
        await session.commit()
        await run_radarr_failed_import_queue_cleanup(
            client,
            session=session,
            job_run_id=None,
            actions=actions,
            policy=RADARR_CLEANUP_POLICY_ALL_ON,
        )
        await session.commit()
    assert client.delete_calls == []
    assert any("skipped failed-import cleanup" in a.lower() and "ambiguous" in a.lower() for a in actions)
    async with SessionLocal() as session:
        rows = (await session.execute(select(ActivityLog))).scalars().all()
        assert len(rows) == 0
        await session.execute(delete(ActivityLog))
        await session.commit()


class _FakeDeleteFailsClient(_FakeRadarrClient):
    async def delete_queue_item(self, *, queue_id: int, blocklist: bool = False, **kwargs: Any) -> None:
        self.delete_calls.append({"queue_id": int(queue_id), "blocklist": bool(blocklist), **kwargs})
        raise RuntimeError("radarr delete failed")


class _FakeRadarrBlocklistFailsThenRemoveOk(_FakeRadarrClient):
    async def delete_queue_item(self, *, queue_id: int, blocklist: bool = False, **kwargs: Any) -> None:
        self.delete_calls.append({"queue_id": int(queue_id), "blocklist": bool(blocklist), **kwargs})
        if blocklist:
            raise RuntimeError("blocklist path failed")


def test_run_cleanup_blocklist_delete_error_writes_failed_activity() -> None:
    asyncio.run(_run_blocklist_delete_error_radarr())


async def _run_blocklist_delete_error_radarr() -> None:
    client = _FakeRadarrBlocklistFailsThenRemoveOk()
    actions: list[str] = []
    async with SessionLocal() as session:
        await session.execute(delete(ActivityLog))
        await session.commit()
        await run_radarr_failed_import_queue_cleanup(
            client,
            session=session,
            job_run_id=None,
            actions=actions,
            policy=RADARR_CLEANUP_POLICY_ALL_ON,
        )
        await session.commit()
    assert client.delete_calls == [{"queue_id": 42, "blocklist": True, "remove_from_client": False}]
    async with SessionLocal() as session:
        rows = (await session.execute(select(ActivityLog))).scalars().all()
        assert len(rows) == 1
        assert rows[0].status == "failed"
        assert "Failed import removal failed" in (rows[0].detail or "")
        await session.execute(delete(ActivityLog))
        await session.commit()


def test_run_cleanup_delete_failure_appends_action_with_blocklist_attempt() -> None:
    asyncio.run(_run_delete_failure())


async def _run_delete_failure() -> None:
    client = _FakeDeleteFailsClient()
    actions: list[str] = []
    async with SessionLocal() as session:
        await session.execute(delete(ActivityLog))
        await session.commit()
        await run_radarr_failed_import_queue_cleanup(
            client,
            session=session,
            job_run_id=None,
            actions=actions,
            policy=RADARR_CLEANUP_POLICY_ALL_ON,
        )
        await session.commit()

    assert client.delete_calls == [{"queue_id": 42, "blocklist": True, "remove_from_client": False}]
    assert any("failed import removal failed" in a.lower() for a in actions)
    async with SessionLocal() as session:
        n = (await session.execute(select(ActivityLog))).scalars().all()
        assert len(n) == 1
        assert n[0].status == "failed"
        assert "Failed import removal failed" in (n[0].detail or "")


class _FakeNonUpgradeQueueOnlyClient(_FakeRadarrClient):
    async def history_page(self, *, page: int, page_size: int) -> dict[str, Any]:
        return {"records": [], "totalRecords": 0}

    async def queue_page(self, *, page: int, page_size: int) -> dict[str, Any]:
        if page != 1:
            return {"records": [], "totalRecords": 1}
        return {
            "records": [
                {
                    "id": 77,
                    "downloadId": "dl-x",
                    "title": "Queue Only Film",
                    "statusMessages": [
                        {
                            "messages": [
                                "Not an upgrade for existing movie file. Existing quality: Bluray-1080p. "
                                "New Quality WEBDL-480p"
                            ]
                        }
                    ],
                }
            ],
            "totalRecords": 1,
        }


def test_run_cleanup_non_upgrade_queue_without_history_removes_and_logs() -> None:
    asyncio.run(_run_non_upgrade_queue_only())


async def _run_non_upgrade_queue_only() -> None:
    client = _FakeNonUpgradeQueueOnlyClient()
    actions: list[str] = []
    async with SessionLocal() as session:
        log = JobRunLog(ok=True, message="")
        session.add(log)
        await session.commit()
        await session.refresh(log)
        await run_radarr_failed_import_queue_cleanup(
            client,
            session=session,
            job_run_id=log.id,
            actions=actions,
            policy=RADARR_CLEANUP_POLICY_ALL_ON,
        )
        await session.commit()

    assert client.delete_calls == [{"queue_id": 77, "blocklist": True, "remove_from_client": False}]
    assert any("failed import cleaned up" in a.lower() for a in actions)

    async with SessionLocal() as session:
        row = (
            (await session.execute(select(ActivityLog).order_by(desc(ActivityLog.id)).limit(1)))
            .scalars()
            .first()
        )
        assert row is not None
        assert row.app == "radarr"
        assert "Failed import cleaned up" in (row.detail or "")
        assert "Matched: not an upgrade vs existing file" in (row.detail or "")
        assert "Queue Only Film" in (row.detail or "")
        await session.execute(delete(ActivityLog))
        await session.execute(delete(JobRunLog))
        await session.commit()


class _FakeParseErrorOnlyQueueClient(_FakeNonUpgradeQueueOnlyClient):
    async def queue_page(self, *, page: int, page_size: int) -> dict[str, Any]:
        if page != 1:
            return {"records": [], "totalRecords": 1}
        return {
            "records": [
                {
                    "id": 88,
                    "downloadId": "dl-y",
                    "statusMessages": [{"messages": ["Unable to parse media info from file"]}],
                }
            ],
            "totalRecords": 1,
        }


def test_run_cleanup_parse_error_queue_only_no_non_upgrade_delete() -> None:
    asyncio.run(_run_parse_error_only_queue())


async def _run_parse_error_only_queue() -> None:
    client = _FakeParseErrorOnlyQueueClient()
    actions: list[str] = []
    async with SessionLocal() as session:
        await run_radarr_failed_import_queue_cleanup(
            client,
            session=session,
            job_run_id=None,
            actions=actions,
            policy=RADARR_CLEANUP_POLICY_ALL_ON,
        )
        await session.commit()

    assert client.delete_calls == []


def test_run_once_radarr_failed_import_cleanup_disabled_skips_api(monkeypatch: pytest.MonkeyPatch) -> None:
    from datetime import datetime

    from app.service_logic import run_once

    async def _prep() -> None:
        async with SessionLocal() as s:
            row = await get_or_create_settings(s)
            row.radarr_remove_failed_imports = False
            await s.commit()
            await s.execute(delete(ActivityLog))
            await s.execute(delete(AppSnapshot))
            await s.execute(delete(JobRunLog))
            await s.commit()

    asyncio.run(_prep())

    called = {"history": 0, "queue": 0, "delete": 0}

    class _FakeArrClient:
        def __init__(self, _cfg: object) -> None:
            pass

        async def health(self) -> bool:
            return True

        async def history_page(self, **kwargs: Any) -> dict[str, Any]:
            called["history"] += 1
            return {"records": [], "totalRecords": 0}

        async def queue_page(self, **kwargs: Any) -> dict[str, Any]:
            called["queue"] += 1
            return {"records": [], "totalRecords": 0}

        async def delete_queue_item(self, **kwargs: Any) -> None:
            called["delete"] += 1

        async def aclose(self) -> None:
            return None

    async def _paginate(*args: Any, **kwargs: Any) -> tuple[list[int], list[dict[str, Any]], int]:
        return [], [], 0

    async def _wanted_total(*args: Any, **kwargs: Any) -> int:
        return 0

    fixed_now = datetime(2026, 3, 23, 15, 0, 0)
    monkeypatch.setattr("app.service_logic.utc_now_naive", lambda: fixed_now)
    monkeypatch.setattr("app.service_logic.resolve_sonarr_api_key", lambda _s: "")
    monkeypatch.setattr("app.service_logic.resolve_radarr_api_key", lambda _s: "rk")
    monkeypatch.setattr("app.service_logic.resolve_emby_api_key", lambda _s: "")
    monkeypatch.setattr("app.service_logic.in_window", lambda **_kw: True)
    monkeypatch.setattr("app.service_logic.ArrClient", _FakeArrClient)
    monkeypatch.setattr("app.service_logic.paginate_wanted_for_search", _paginate)
    monkeypatch.setattr("app.service_logic._wanted_queue_total", _wanted_total)

    async def _set_radarr() -> None:
        async with SessionLocal() as s:
            row = await get_or_create_settings(s)
            row.radarr_enabled = True
            row.radarr_url = "http://localhost:7878"
            row.radarr_search_missing = False
            row.radarr_search_upgrades = False
            row.radarr_last_run_at = None
            row.radarr_remove_failed_imports = False
            await s.commit()

    asyncio.run(_set_radarr())

    async def _go() -> None:
        async with SessionLocal() as s:
            await run_once(s)

    asyncio.run(_go())
    assert called["history"] == 0
    assert called["queue"] == 0
    assert called["delete"] == 0
