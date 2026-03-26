import asyncio
from datetime import datetime
from types import SimpleNamespace

from app.scheduler import ServiceScheduler, compute_job_intervals_minutes


def _arr_settings(**kwargs: object) -> SimpleNamespace:
    base = dict(
        sonarr_enabled=False,
        sonarr_url="",
        sonarr_api_key="",
        radarr_enabled=False,
        radarr_url="",
        radarr_api_key="",
        emby_enabled=False,
        emby_url="",
        emby_api_key="",
        sonarr_interval_minutes=60,
        radarr_interval_minutes=60,
        emby_interval_minutes=60,
    )
    base.update(kwargs)
    return SimpleNamespace(**base)


def test_compute_job_intervals_minutes_uses_per_app_intervals() -> None:
    s = _arr_settings(
        sonarr_enabled=True,
        sonarr_url="http://127.0.0.1:8989",
        sonarr_api_key="k",
        radarr_enabled=True,
        radarr_url="http://127.0.0.1:7878",
        radarr_api_key="k",
        emby_enabled=True,
        emby_url="http://127.0.0.1:8096",
        emby_api_key="ek",
        sonarr_interval_minutes=30,
        radarr_interval_minutes=120,
        emby_interval_minutes=45,
    )
    assert compute_job_intervals_minutes(s) == {
        "sonarr": 30,
        "radarr": 120,
        "trimmer": 45,
    }


def test_compute_job_intervals_minutes_empty_when_no_apps_configured() -> None:
    s = _arr_settings()
    assert compute_job_intervals_minutes(s) == {}


def test_compute_job_intervals_minutes_uses_single_configured_app() -> None:
    s = _arr_settings(
        sonarr_enabled=True,
        sonarr_url="http://127.0.0.1:8989",
        sonarr_api_key="k",
        sonarr_interval_minutes=45,
    )
    assert compute_job_intervals_minutes(s) == {"sonarr": 45}


def test_compute_job_intervals_includes_stream_manager_when_configured() -> None:
    s = _arr_settings(
        stream_manager_enabled=True,
        stream_manager_paths="D:\\Media",
        stream_manager_interval_minutes=120,
    )
    assert compute_job_intervals_minutes(s) == {"stream_manager": 120}


def test_start_creates_independent_jobs_for_enabled_apps() -> None:
    s = ServiceScheduler()
    calls: list[tuple[str, int]] = []

    async def _fake_intervals():
        return {"sonarr": 30, "radarr": 120, "trimmer": 45}

    class _FakeSched:
        running = False

        def add_job(self, _fn, _trigger, *, minutes, id, replace_existing, next_run_time=None):  # noqa: ANN001
            calls.append((id, minutes))

        def start(self) -> None:
            self.running = True

    s._sched = _FakeSched()
    s._current_job_intervals_minutes = _fake_intervals
    asyncio.run(s.start())
    assert ("fetcher_sonarr", 30) in calls
    assert ("fetcher_radarr", 120) in calls
    assert ("fetcher_trimmer", 45) in calls


def test_reschedule_updates_only_requested_job() -> None:
    s = ServiceScheduler()
    calls: list[tuple[str, int]] = []

    async def _fake_intervals():
        return {"sonarr": 30, "radarr": 120, "trimmer": 45}

    class _FakeSched:
        running = True

        def add_job(self, _fn, _trigger, *, minutes, id, replace_existing, next_run_time=None):  # noqa: ANN001
            calls.append((id, minutes))

        def get_job(self, _id: str):  # noqa: ANN001
            return None

    s._sched = _FakeSched()
    s._current_job_intervals_minutes = _fake_intervals
    asyncio.run(s.reschedule(targets={"radarr"}))
    assert calls == [("fetcher_radarr", 120)]


def test_next_runs_by_job_returns_independent_values() -> None:
    s = ServiceScheduler()

    class _Job:
        def __init__(self, dt: datetime):
            self.next_run_time = dt

    class _FakeSched:
        running = True

        @staticmethod
        def get_job(job_id: str):
            if job_id == "fetcher_sonarr":
                return _Job(datetime(2026, 3, 24, 12, 0, 0))
            if job_id == "fetcher_radarr":
                return _Job(datetime(2026, 3, 24, 12, 30, 0))
            return None

    s._sched = _FakeSched()
    runs = s.next_runs_by_job()
    assert runs["sonarr"] == datetime(2026, 3, 24, 12, 0, 0)
    assert runs["radarr"] == datetime(2026, 3, 24, 12, 30, 0)
    assert runs["trimmer"] is None
    assert runs["stream_manager"] is None


def test_shutdown_ignores_runtime_error_when_loop_closed() -> None:
    scheduler = ServiceScheduler()

    class _FakeScheduler:
        running = True

        @staticmethod
        def shutdown(wait: bool = False) -> None:  # noqa: ARG001
            raise RuntimeError("Event loop is closed")

    scheduler._sched = _FakeScheduler()

    # Should not raise during teardown scenarios.
    scheduler.shutdown()
