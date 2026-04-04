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


def test_clamp_minimum_age_default_on_invalid() -> None:
    from app.refiner_watch_config import (
        REFINER_MINIMUM_AGE_SEC_DEFAULT,
        clamp_refiner_minimum_age_seconds,
    )

    assert clamp_refiner_minimum_age_seconds(None) == REFINER_MINIMUM_AGE_SEC_DEFAULT
    assert clamp_refiner_minimum_age_seconds("bad") == REFINER_MINIMUM_AGE_SEC_DEFAULT


def test_clamp_minimum_age_clamps_to_min() -> None:
    from app.refiner_watch_config import (
        REFINER_MINIMUM_AGE_SEC_MIN,
        clamp_refiner_minimum_age_seconds,
    )

    assert clamp_refiner_minimum_age_seconds(0) == REFINER_MINIMUM_AGE_SEC_MIN
    assert clamp_refiner_minimum_age_seconds(-100) == REFINER_MINIMUM_AGE_SEC_MIN


def test_clamp_minimum_age_clamps_to_max() -> None:
    from app.refiner_watch_config import (
        REFINER_MINIMUM_AGE_SEC_MAX,
        clamp_refiner_minimum_age_seconds,
    )

    assert clamp_refiner_minimum_age_seconds(9999) == REFINER_MINIMUM_AGE_SEC_MAX


def test_clamp_minimum_age_accepts_valid_value() -> None:
    from app.refiner_watch_config import clamp_refiner_minimum_age_seconds

    assert clamp_refiner_minimum_age_seconds(120) == 120
    assert clamp_refiner_minimum_age_seconds("30") == 30
