"""Stream Manager settings and manual run — isolated from Trimmer service logic."""

from __future__ import annotations

import logging
import os

from fastapi import APIRouter, Depends, Form
from fastapi.responses import RedirectResponse
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import _get_or_create_settings, get_session
from app.display_helpers import _normalize_hhmm
from app.schedule import normalize_schedule_days_csv
from app.stream_manager_service import run_stream_manager_pass
from app.time_util import utc_now_naive
from app.web_common import (
    schedule_days_csv_from_named_day_checks,
    try_commit_and_reschedule,
    trimmer_settings_redirect_url,
)
from app.routers.deps import AUTH_DEPS, AUTH_FORM_DEPS

logger = logging.getLogger(__name__)

router = APIRouter(dependencies=AUTH_DEPS)


@router.post("/trimmer/settings/stream-manager/save", dependencies=AUTH_FORM_DEPS)
async def stream_manager_settings_save(
    stream_manager_enabled: bool = Form(False),
    stream_manager_dry_run: bool = Form(False),
    stream_manager_primary_audio_lang: str = Form(""),
    stream_manager_secondary_audio_lang: str = Form(""),
    stream_manager_tertiary_audio_lang: str = Form(""),
    stream_manager_default_audio_slot: str = Form("primary"),
    stream_manager_remove_commentary: bool = Form(False),
    stream_manager_subtitle_mode: str = Form("remove_all"),
    stream_manager_subtitle_langs: list[str] = Form(default=[]),
    stream_manager_preserve_forced_subs: bool = Form(False),
    stream_manager_preserve_default_subs: bool = Form(False),
    stream_manager_audio_preference_mode: str = Form("best_available"),
    stream_manager_watched_folder: str = Form(""),
    stream_manager_output_folder: str = Form(""),
    stream_manager_work_folder: str = Form(""),
    stream_manager_interval_minutes: int = Form(60),
    stream_manager_schedule_enabled: bool = Form(False),
    stream_manager_schedule_Mon: bool = Form(False),
    stream_manager_schedule_Tue: bool = Form(False),
    stream_manager_schedule_Wed: bool = Form(False),
    stream_manager_schedule_Thu: bool = Form(False),
    stream_manager_schedule_Fri: bool = Form(False),
    stream_manager_schedule_Sat: bool = Form(False),
    stream_manager_schedule_Sun: bool = Form(False),
    stream_manager_schedule_start: str = Form("00:00"),
    stream_manager_schedule_end: str = Form("23:59"),
    session: AsyncSession = Depends(get_session),
) -> RedirectResponse:
    try:
        row = await _get_or_create_settings(session)
        if stream_manager_enabled and not (stream_manager_primary_audio_lang or "").strip():
            return RedirectResponse(
                trimmer_settings_redirect_url(
                    saved=False, reason="primary_audio_required", section="stream_manager"
                ),
                status_code=303,
            )
        slot = (stream_manager_default_audio_slot or "primary").strip().lower()
        if slot not in ("primary", "secondary", "tertiary"):
            slot = "primary"
        mode = (stream_manager_subtitle_mode or "remove_all").strip().lower()
        if mode not in ("remove_all", "keep_selected"):
            mode = "remove_all"
        pref = (stream_manager_audio_preference_mode or "best_available").strip().lower()
        if pref not in ("best_available", "prefer_surround", "prefer_stereo", "prefer_lossless"):
            pref = "best_available"
        lang_set = sorted({str(v).strip() for v in stream_manager_subtitle_langs if str(v).strip()})
        sim = max(5, min(7 * 24 * 60, int(stream_manager_interval_minutes or 60)))
        watched_folder = (stream_manager_watched_folder or "").strip()
        output_folder = (stream_manager_output_folder or "").strip()
        if stream_manager_enabled and (not watched_folder or not output_folder):
            return RedirectResponse(
                trimmer_settings_redirect_url(
                    saved=False, reason="watched_output_required", section="stream_manager"
                ),
                status_code=303,
            )
        row.stream_manager_enabled = stream_manager_enabled
        row.stream_manager_dry_run = stream_manager_dry_run
        row.stream_manager_primary_audio_lang = (stream_manager_primary_audio_lang or "").strip()[:16]
        row.stream_manager_secondary_audio_lang = (stream_manager_secondary_audio_lang or "").strip()[:16]
        row.stream_manager_tertiary_audio_lang = (stream_manager_tertiary_audio_lang or "").strip()[:16]
        row.stream_manager_default_audio_slot = slot
        row.stream_manager_remove_commentary = stream_manager_remove_commentary
        row.stream_manager_subtitle_mode = mode
        row.stream_manager_subtitle_langs_csv = ",".join(lang_set)
        row.stream_manager_audio_preference_mode = pref
        row.stream_manager_preserve_forced_subs = stream_manager_preserve_forced_subs
        row.stream_manager_preserve_default_subs = stream_manager_preserve_default_subs
        row.stream_manager_watched_folder = watched_folder[:8000]
        row.stream_manager_output_folder = output_folder[:8000]
        row.stream_manager_work_folder = (stream_manager_work_folder or "").strip()[:8000]
        row.stream_manager_interval_minutes = sim
        row.stream_manager_schedule_enabled = stream_manager_schedule_enabled
        row.stream_manager_schedule_days = schedule_days_csv_from_named_day_checks(
            stream_manager_schedule_Mon,
            stream_manager_schedule_Tue,
            stream_manager_schedule_Wed,
            stream_manager_schedule_Thu,
            stream_manager_schedule_Fri,
            stream_manager_schedule_Sat,
            stream_manager_schedule_Sun,
        )
        row.stream_manager_schedule_start = _normalize_hhmm(stream_manager_schedule_start, "00:00")
        row.stream_manager_schedule_end = _normalize_hhmm(stream_manager_schedule_end, "23:59")
        # Canonicalize weekday CSV same as other schedules
        row.stream_manager_schedule_days = normalize_schedule_days_csv(row.stream_manager_schedule_days or "")
        row.updated_at = utc_now_naive()
        if not await try_commit_and_reschedule(session, targets={"stream_manager"}):
            return RedirectResponse(
                trimmer_settings_redirect_url(saved=False, reason="db_busy", section="stream_manager"),
                status_code=303,
            )
        return RedirectResponse(
            trimmer_settings_redirect_url(saved=True, section="stream_manager"), status_code=303
        )
    except SQLAlchemyError:
        logger.exception("POST stream-manager save SQLAlchemyError")
        try:
            await session.rollback()
        except Exception:
            pass
        return RedirectResponse(
            trimmer_settings_redirect_url(saved=False, reason="db_error", section="stream_manager"),
            status_code=303,
        )
    except Exception:
        logger.exception("POST stream-manager save failed")
        try:
            await session.rollback()
        except Exception:
            pass
        if (os.environ.get("FETCHER_LOG_LEVEL") or "").upper() == "DEBUG":
            raise
        return RedirectResponse(
            trimmer_settings_redirect_url(saved=False, reason="error", section="stream_manager"),
            status_code=303,
        )


@router.post("/trimmer/settings/stream-manager/run", dependencies=AUTH_FORM_DEPS)
async def stream_manager_manual_run(
    session: AsyncSession = Depends(get_session),
) -> RedirectResponse:
    row = await _get_or_create_settings(session)
    if not row.stream_manager_enabled:
        return RedirectResponse(
            trimmer_settings_redirect_url(
                saved=False, reason="stream_manager_disabled", section="stream_manager"
            ),
            status_code=303,
        )
    result = await run_stream_manager_pass(session, trigger="manual")
    if not result.get("ran"):
        reason = str(result.get("error") or result.get("reason") or "nothing_to_do")
        return RedirectResponse(
            trimmer_settings_redirect_url(saved=False, reason=reason, section="stream_manager"),
            status_code=303,
        )
    if not result.get("ok"):
        return RedirectResponse(
            trimmer_settings_redirect_url(saved=False, reason="stream_manager_errors", section="stream_manager"),
            status_code=303,
        )
    return RedirectResponse("/trimmer/settings?saved=1&stream_mgr_run=1#stream-manager", status_code=303)
