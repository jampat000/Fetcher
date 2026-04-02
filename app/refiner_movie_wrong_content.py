"""Aggressive wrong-content scoring for Radarr movies (post-ffprobe, pre-remux)."""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_REFINER_WC_LOG_MAX = 3500
_MOVIE_MIN_HARD_MINUTES = 30.0
_RUNTIME_RATIO_LOW = 0.55
_RUNTIME_RATIO_HIGH = 1.80
_SOFT_SCORE_THRESHOLD = 72
_YEAR_TOKEN_RE = re.compile(r"\b(19[5-9]\d|20[0-3]\d)\b")


def _clip(s: str, n: int = _REFINER_WC_LOG_MAX) -> str:
    t = (s or "").strip()
    return t if len(t) <= n else t[: n - 20] + "…(truncated)"


@dataclass(frozen=True)
class MovieWrongContentVerdict:
    """High-confidence wrong-content decision for a Radarr movie candidate."""

    wrong_content: bool
    triggered_reason: str
    score: int
    probed_runtime_minutes: float | None
    expected_runtime_minutes: float | None
    runtime_ratio: float | None
    token_overlap_summary: str
    hard_trigger: bool
    components: dict[str, Any]


def _probed_runtime_minutes(probe: dict[str, Any]) -> float | None:
    fmt = probe.get("format")
    if not isinstance(fmt, dict):
        return None
    raw = fmt.get("duration")
    try:
        sec = float(raw)
    except (TypeError, ValueError):
        return None
    if sec <= 0:
        return None
    return sec / 60.0


def _tokenize_phrase(s: str) -> set[str]:
    t = re.sub(r"[^a-z0-9]+", " ", (s or "").lower()).strip()
    out: set[str] = set()
    for w in t.split():
        if len(w) < 2:
            continue
        if w.isdigit() and len(w) == 4:
            out.add(w)
        elif not w.isdigit():
            out.add(w)
    return out


def _candidate_text(path: Path) -> str:
    return f"{path.stem} {path.parent.name}"


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return float(inter) / float(union) if union else 0.0


def _year_mismatch_score(target_year: int | None, candidate_text: str) -> tuple[int, str]:
    if target_year is None or target_year < 1900:
        return 0, ""
    found = {int(m.group(1)) for m in _YEAR_TOKEN_RE.finditer(candidate_text)}
    found.discard(target_year)
    bad = {y for y in found if 1950 <= y <= 2035}
    if not bad:
        return 0, ""
    return 42, f"years_in_candidate_excluding_target={sorted(bad)[:6]}"


def evaluate_movie_wrong_content(
    path: Path,
    probe: dict[str, Any],
    video_streams: list[dict[str, Any]],
    *,
    target_title: str,
    target_year: int | None,
    expected_runtime_minutes: float | None,
) -> MovieWrongContentVerdict:
    """
    Conservative wrong-content model: hard triggers OR soft score >= threshold.
    Does not use embedded tag-based media identification beyond stream layout + duration.
    """
    components: dict[str, Any] = {}
    hard = False
    reasons: list[str] = []
    soft = 0

    if not video_streams:
        hard = True
        reasons.append("no_video_stream")

    probed = _probed_runtime_minutes(probe)
    ratio: float | None = None
    if probed is not None and expected_runtime_minutes is not None and expected_runtime_minutes > 0:
        ratio = probed / expected_runtime_minutes
        components["runtime_ratio"] = round(ratio, 4)
    components["probed_runtime_minutes"] = round(probed, 2) if probed is not None else None
    components["expected_runtime_minutes"] = (
        round(expected_runtime_minutes, 2)
        if expected_runtime_minutes is not None and expected_runtime_minutes > 0
        else None
    )

    if not hard and probed is not None and probed < _MOVIE_MIN_HARD_MINUTES:
        hard = True
        reasons.append("runtime_under_30m")

    if (
        not hard
        and probed is not None
        and expected_runtime_minutes is not None
        and expected_runtime_minutes > 0
    ):
        if ratio is not None:
            if ratio < _RUNTIME_RATIO_LOW:
                hard = True
                reasons.append("runtime_ratio_below_0_55")
            elif ratio > _RUNTIME_RATIO_HIGH:
                hard = True
                reasons.append("runtime_ratio_above_1_80")

    target_toks = _tokenize_phrase(target_title)
    if target_year is not None and 1900 <= target_year <= 2035:
        target_toks.add(str(target_year))
    cand_text = _candidate_text(path)
    cand_toks = _tokenize_phrase(cand_text)
    jac = _jaccard(target_toks, cand_toks)
    overlap_summary = f"jaccard={jac:.3f};target_tokens={len(target_toks)};candidate_tokens={len(cand_toks)}"
    components["token_jaccard"] = round(jac, 4)

    if not hard:
        if jac < 0.06:
            soft += 38
            components["weak_tokens"] = "very_low"
        elif jac < 0.14:
            soft += 28
            components["weak_tokens"] = "low"
        elif jac < 0.22:
            soft += 15
            components["weak_tokens"] = "moderate_low"

        ys, ynote = _year_mismatch_score(target_year, cand_text.casefold())
        if ys:
            soft += ys
            components["year_mismatch"] = ynote

    triggered = bool(hard or soft >= _SOFT_SCORE_THRESHOLD)
    if hard:
        trig_reason = ",".join(reasons)
    elif triggered:
        trig_reason = f"soft_score>={_SOFT_SCORE_THRESHOLD}({soft})"
    else:
        trig_reason = ""

    verdict = MovieWrongContentVerdict(
        wrong_content=triggered,
        triggered_reason=_clip(trig_reason, 400),
        score=(1000 + soft) if hard else soft,
        probed_runtime_minutes=probed,
        expected_runtime_minutes=expected_runtime_minutes if expected_runtime_minutes and expected_runtime_minutes > 0 else None,
        runtime_ratio=ratio,
        token_overlap_summary=_clip(overlap_summary, 300),
        hard_trigger=hard,
        components=components,
    )

    payload = {
        "candidate": _clip(str(path), 512),
        "expected_runtime_minutes": verdict.expected_runtime_minutes,
        "probed_runtime_minutes": verdict.probed_runtime_minutes,
        "runtime_ratio": verdict.runtime_ratio,
        "token_overlap_summary": verdict.token_overlap_summary,
        "score": verdict.score,
        "soft_score": soft,
        "triggered_reason": verdict.triggered_reason,
        "wrong_content": verdict.wrong_content,
        "hard_trigger": hard,
        "components": dict(components),
    }
    line = json.dumps(payload, ensure_ascii=True)
    if verdict.wrong_content:
        logger.warning("REFINER_WRONG_CONTENT_SCORE: %s", _clip(line, _REFINER_WC_LOG_MAX))
    else:
        logger.debug("REFINER_WRONG_CONTENT_SCORE: %s", _clip(line, _REFINER_WC_LOG_MAX))

    return verdict
