import asyncio
from datetime import datetime
from types import SimpleNamespace

from app.scheduler import ServiceScheduler, compute_job_intervals_minutes, effective_stream_manager_interval_seconds


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


def test_compute_job_intervals_minutes_excludes_stream_manager() -> None:
    s = _arr_settings(
        stream_manager_enabled=True,
        stream_manager_watched_folder="D:\\Media\\incoming",
        stream_manager_output_folder="D:\\Media\\processed",
        stream_manager_interval_seconds=120,
    )
    assert compute_job_intervals_minutes(s) == {}


def test_effective_stream_manager_interval_seconds_when_configured() -> None:
    s = _arr_settings(
        stream_manager_enabled=True,
        stream_manager_watched_folder="D:\\Media\\incoming",
        stream_manager_output_folder="D:\\Media\\processed",
        stream_manager_interval_seconds=120,
    )
    assert effective_stream_manager_interval_seconds(s) == 120


def test_start_creates_independent_jobs_for_enabled_apps() -> None:
    s = ServiceScheduler()
    calls: list[tuple] = []

    async def _fake_payload():
        return ({"sonarr": 30, "radarr": 120, "trimmer": 45}, None)

    class _FakeSched:
        running = False

        def add_job(self, _fn, _trigger, *, id, replace_existing, next_run_time=None, minutes=None, seconds=None, **_kw):  # noqa: ANN001
            if seconds is not None:
                calls.append((id, "seconds", seconds))
            else:
                calls.append((id, "minutes", minutes))

        def start(self) -> None:
            self.running = True

    s._sched = _FakeSched()
    s._current_scheduler_intervals = _fake_payload
    asyncio.run(s.start())
    assert ("fetcher_sonarr", "minutes", 30) in calls
    assert ("fetcher_radarr", "minutes", 120) in calls
    assert ("fetcher_trimmer", "minutes", 45) in calls


def test_start_adds_stream_manager_job_in_seconds() -> None:
    s = ServiceScheduler()
    calls: list[tuple] = []

    async def _fake_payload():
        return ({"sonarr": 30}, 90)

    class _FakeSched:
        running = False

        def add_job(self, _fn, _trigger, *, id, replace_existing, next_run_time=None, minutes=None, seconds=None, **_kw):  # noqa: ANN001
            if seconds is not None:
                calls.append((id, "seconds", seconds))
            else:
                calls.append((id, "minutes", minutes))

        def start(self) -> None:
            self.running = True

    s._sched = _FakeSched()
    s._current_scheduler_intervals = _fake_payload
    asyncio.run(s.start())
    assert ("fetcher_sonarr", "minutes", 30) in calls
    assert ("fetcher_stream_manager", "seconds", 90) in calls


def test_reschedule_updates_only_requested_job() -> None:
    s = ServiceScheduler()
    calls: list[tuple] = []

    async def _fake_payload():
        return ({"sonarr": 30, "radarr": 120, "trimmer": 45}, None)

    class _FakeSched:
        running = True

        def add_job(self, _fn, _trigger, *, id, replace_existing, minutes=None, seconds=None, next_run_time=None, **_kw):  # noqa: ANN001
            if seconds is not None:
                calls.append((id, "seconds", seconds))
            else:
                calls.append((id, "minutes", minutes))

        def get_job(self, _id: str):  # noqa: ANN001
            return None

    s._sched = _FakeSched()
    s._current_scheduler_intervals = _fake_payload
    asyncio.run(s.reschedule(targets={"radarr"}))
    assert calls == [("fetcher_radarr", "minutes", 120)]


def test_reschedule_stream_manager_uses_seconds() -> None:
    s = ServiceScheduler()
    calls: list[tuple] = []

    async def _fake_payload():
        return ({}, 42)

    class _FakeSched:
        running = True

        def add_job(self, _fn, _trigger, *, id, replace_existing, minutes=None, seconds=None, next_run_time=None, **_kw):  # noqa: ANN001
            if seconds is not None:
                calls.append((id, "seconds", seconds))
            else:
                calls.append((id, "minutes", minutes))

        def get_job(self, _id: str):  # noqa: ANN001
            return None

    s._sched = _FakeSched()
    s._current_scheduler_intervals = _fake_payload
    asyncio.run(s.reschedule(targets={"stream_manager"}))
    assert calls == [("fetcher_stream_manager", "seconds", 42)]


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
