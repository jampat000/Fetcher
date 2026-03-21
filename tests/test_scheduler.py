from types import SimpleNamespace

from app.scheduler import ServiceScheduler, compute_grabby_tick_minutes


def _arr_settings(**kwargs: object) -> SimpleNamespace:
    base = dict(
        sonarr_enabled=False,
        sonarr_url="",
        sonarr_api_key="",
        radarr_enabled=False,
        radarr_url="",
        radarr_api_key="",
        sonarr_interval_minutes=60,
        radarr_interval_minutes=60,
        interval_minutes=9999,
    )
    base.update(kwargs)
    return SimpleNamespace(**base)


def test_compute_grabby_tick_minutes_uses_min_of_enabled_arr_intervals() -> None:
    s = _arr_settings(
        sonarr_enabled=True,
        sonarr_url="http://127.0.0.1:8989",
        sonarr_api_key="k",
        radarr_enabled=True,
        radarr_url="http://127.0.0.1:7878",
        radarr_api_key="k",
        sonarr_interval_minutes=30,
        radarr_interval_minutes=120,
    )
    assert compute_grabby_tick_minutes(s) == 30


def test_compute_grabby_tick_minutes_fallback_when_no_arr_configured() -> None:
    s = _arr_settings()
    assert compute_grabby_tick_minutes(s) == 60


def test_compute_grabby_tick_minutes_ignores_legacy_interval_minutes_column() -> None:
    s = _arr_settings(
        sonarr_enabled=True,
        sonarr_url="http://127.0.0.1:8989",
        sonarr_api_key="k",
        sonarr_interval_minutes=45,
        interval_minutes=5,
    )
    assert compute_grabby_tick_minutes(s) == 45


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
