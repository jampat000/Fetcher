from app.refiner_watch_config import (
    REFINER_WATCH_INTERVAL_SEC_MAX,
    REFINER_WATCH_INTERVAL_SEC_MIN,
    clamp_refiner_interval_seconds,
)


def test_clamp_refiner_interval_coerces_and_bounds() -> None:
    assert clamp_refiner_interval_seconds(30) == 30
    assert clamp_refiner_interval_seconds(3) == REFINER_WATCH_INTERVAL_SEC_MIN
    assert clamp_refiner_interval_seconds(999999999) == REFINER_WATCH_INTERVAL_SEC_MAX
    assert clamp_refiner_interval_seconds("not-int") == 60
