from app.arr_intervals import ARR_INTERVAL_FALLBACK_MINUTES, effective_arr_interval_minutes


def test_effective_arr_interval_minutes_coerces_invalid_to_fallback() -> None:
    assert effective_arr_interval_minutes(0) == ARR_INTERVAL_FALLBACK_MINUTES
    assert effective_arr_interval_minutes(None) == ARR_INTERVAL_FALLBACK_MINUTES
    assert effective_arr_interval_minutes(-5) == ARR_INTERVAL_FALLBACK_MINUTES
    assert effective_arr_interval_minutes(90) == 90
