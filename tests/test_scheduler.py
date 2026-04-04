import asyncio
from datetime import datetime
from types import SimpleNamespace

from app.scheduler import ServiceScheduler, compute_job_intervals_minutes, effective_refiner_interval_seconds


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
        sonarr_search_interval_minutes=60,
        radarr_search_interval_minutes=60,
        trimmer_interval_minutes=60,
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
        sonarr_search_interval_minutes=30,
        radarr_search_interval_minutes=120,
        trimmer_interval_minutes=45,
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
        sonarr_search_interval_minutes=45,
    )
    assert compute_job_intervals_minutes(s) == {"sonarr": 45}


def test_compute_job_intervals_minutes_excludes_refiner() -> None:
    s = _arr_settings(
        refiner_enabled=True,
        refiner_watched_folder="D:\\Media\\incoming",
        refiner_output_folder="D:\\Media\\processed",
        movie_refiner_interval_seconds=120,
    )
    assert compute_job_intervals_minutes(s) == {}


def test_effective_refiner_interval_seconds_when_configured() -> None:
    s = _arr_settings(
        refiner_enabled=True,
        refiner_primary_audio_lang="eng",
        refiner_watched_folder="D:\\Media\\incoming",
        refiner_output_folder="D:\\Media\\processed",
        movie_refiner_interval_seconds=120,
    )
    assert effective_refiner_interval_seconds(s) == 120


def test_effective_refiner_interval_seconds_none_without_primary_lang() -> None:
    s = _arr_settings(
        refiner_enabled=True,
        refiner_primary_audio_lang="",
        refiner_watched_folder="D:\\Media\\incoming",
        refiner_output_folder="D:\\Media\\processed",
        movie_refiner_interval_seconds=120,
    )
    assert effective_refiner_interval_seconds(s) is None


def test_start_creates_independent_jobs_for_enabled_apps() -> None:
    s = ServiceScheduler()
    calls: list[tuple] = []

    async def _fake_payload():
        return ({"sonarr": 30, "radarr": 120, "trimmer": 45}, None, None, None, None)

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


def test_start_adds_refiner_job_in_seconds() -> None:
    s = ServiceScheduler()
    calls: list[tuple] = []

    async def _fake_payload():
        return ({"sonarr": 30}, 90, None, None, None)

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
    assert ("fetcher_refiner", "seconds", 90) in calls


def test_reschedule_updates_only_requested_job() -> None:
    s = ServiceScheduler()
    calls: list[tuple] = []

    async def _fake_payload():
        return ({"sonarr": 30, "radarr": 120, "trimmer": 45}, None, None, None, None)

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


def test_reschedule_refiner_uses_seconds() -> None:
    s = ServiceScheduler()
    calls: list[tuple] = []

    async def _fake_payload():
        return ({}, 42, None, None, None)

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
    asyncio.run(s.reschedule(targets={"refiner"}))
    assert calls == [("fetcher_refiner", "seconds", 42)]


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
    assert runs["refiner"] is None
    assert runs["sonarr_refiner"] is None
    assert runs["sonarr_failed_import_cleanup"] is None
    assert runs["radarr_failed_import_cleanup"] is None


def test_effective_sonarr_refiner_interval_seconds_when_configured() -> None:
    from app.scheduler import effective_sonarr_refiner_interval_seconds

    s = _arr_settings(
        sonarr_refiner_enabled=True,
        sonarr_refiner_primary_audio_lang="eng",
        sonarr_refiner_watched_folder="D:\\Media\\tv\\incoming",
        sonarr_refiner_output_folder="D:\\Media\\tv\\processed",
        tv_refiner_interval_seconds=120,
    )
    assert effective_sonarr_refiner_interval_seconds(s) == 120


def test_effective_sonarr_refiner_interval_seconds_none_when_disabled() -> None:
    from app.scheduler import effective_sonarr_refiner_interval_seconds

    s = _arr_settings(
        sonarr_refiner_enabled=False,
        tv_refiner_interval_seconds=120,
    )
    assert effective_sonarr_refiner_interval_seconds(s) is None


def test_start_adds_sonarr_refiner_job_in_seconds() -> None:
    s = ServiceScheduler()
    calls: list[tuple] = []

    async def _fake_payload():
        return ({}, None, 45, None, None)

    class _FakeSched:
        running = False

        def add_job(
            self, _fn, _trigger, *, id, replace_existing, next_run_time=None, minutes=None, seconds=None, **_kw
        ):  # noqa: ANN001
            if seconds is not None:
                calls.append((id, "seconds", seconds))
            else:
                calls.append((id, "minutes", minutes))

        def start(self) -> None:
            self.running = True

    s._sched = _FakeSched()
    s._current_scheduler_intervals = _fake_payload
    asyncio.run(s.start())
    assert ("fetcher_sonarr_refiner", "seconds", 45) in calls


def test_reschedule_sonarr_refiner_uses_seconds() -> None:
    s = ServiceScheduler()
    calls: list[tuple] = []

    async def _fake_payload():
        return ({}, None, 30, None, None)

    class _FakeSched:
        running = True

        def add_job(
            self, _fn, _trigger, *, id, replace_existing, minutes=None, seconds=None, next_run_time=None, **_kw
        ):  # noqa: ANN001
            if seconds is not None:
                calls.append((id, "seconds", seconds))
            else:
                calls.append((id, "minutes", minutes))

        def get_job(self, _id: str):  # noqa: ANN001
            return None

    s._sched = _FakeSched()
    s._current_scheduler_intervals = _fake_payload
    asyncio.run(s.reschedule(targets={"sonarr_refiner"}))
    assert calls == [("fetcher_sonarr_refiner", "seconds", 30)]


def test_next_runs_by_job_includes_sonarr_refiner() -> None:
    s = ServiceScheduler()

    class _FakeSched:
        running = True

        @staticmethod
        def get_job(job_id: str):
            return None

    s._sched = _FakeSched()
    runs = s.next_runs_by_job()
    assert "sonarr_refiner" in runs
    assert runs["sonarr_refiner"] is None
    assert "sonarr_failed_import_cleanup" in runs
    assert "radarr_failed_import_cleanup" in runs


def test_start_adds_failed_import_cleanup_jobs_when_intervals_present() -> None:
    s = ServiceScheduler()
    calls: list[tuple] = []

    async def _fake_payload():
        return ({}, None, None, 33, 44)

    class _FakeSched:
        running = False

        def add_job(
            self, _fn, _trigger, *, id, replace_existing, next_run_time=None, minutes=None, seconds=None, **_kw
        ):  # noqa: ANN001
            if seconds is not None:
                calls.append((id, "seconds", seconds))
            else:
                calls.append((id, "minutes", minutes))

        def start(self) -> None:
            self.running = True

    s._sched = _FakeSched()
    s._current_scheduler_intervals = _fake_payload
    asyncio.run(s.start())
    assert ("fetcher_sonarr_failed_import_cleanup", "minutes", 33) in calls
    assert ("fetcher_radarr_failed_import_cleanup", "minutes", 44) in calls


def test_reschedule_sonarr_failed_import_cleanup_updates_minutes() -> None:
    s = ServiceScheduler()
    calls: list[tuple] = []

    async def _fake_payload():
        return ({}, None, None, 15, None)

    class _FakeSched:
        running = True

        def add_job(
            self, _fn, _trigger, *, id, replace_existing, minutes=None, seconds=None, next_run_time=None, **_kw
        ):  # noqa: ANN001
            if seconds is not None:
                calls.append((id, "seconds", seconds))
            else:
                calls.append((id, "minutes", minutes))

        def get_job(self, _id: str):  # noqa: ANN001
            return None

    s._sched = _FakeSched()
    s._current_scheduler_intervals = _fake_payload
    asyncio.run(s.reschedule(targets={"sonarr_failed_import_cleanup"}))
    assert calls == [("fetcher_sonarr_failed_import_cleanup", "minutes", 15)]


def test_reschedule_radarr_failed_import_cleanup_removes_when_disabled() -> None:
    removed: list[str] = []

    async def _fake_payload():
        return ({}, None, None, None, None)

    class _FakeSched:
        running = True

        def add_job(self, *_a, **_k):
            raise AssertionError("should not add when cleanup disabled")

        def get_job(self, job_id: str):  # noqa: ANN001
            return object()

        def remove_job(self, job_id: str) -> None:
            removed.append(job_id)

    s = ServiceScheduler()
    s._sched = _FakeSched()
    s._current_scheduler_intervals = _fake_payload
    asyncio.run(s.reschedule(targets={"radarr_failed_import_cleanup"}))
    assert removed == ["fetcher_radarr_failed_import_cleanup"]


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
