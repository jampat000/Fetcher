from app.refiner_watch_config import (
    STREAM_MANAGER_WATCH_INTERVAL_SEC_MAX,
    STREAM_MANAGER_WATCH_INTERVAL_SEC_MIN,
    clamp_stream_manager_interval_seconds,
)


def test_clamp_refiner_interval_coerces_and_bounds() -> None:
    assert clamp_stream_manager_interval_seconds(30) == 30
    assert clamp_stream_manager_interval_seconds(3) == STREAM_MANAGER_WATCH_INTERVAL_SEC_MIN
    assert clamp_stream_manager_interval_seconds(999999999) == STREAM_MANAGER_WATCH_INTERVAL_SEC_MAX
    assert clamp_stream_manager_interval_seconds("not-int") == 60
