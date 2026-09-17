from datetime import date

from energy_platform.watermark import Watermark, dates_to_fetch

def test_first_run_from_default_start():
    """Test that the first run returns dates from the default start date to today."""
    default_start = date(2026, 9, 15)
    today = date(2026, 9, 17)
    watermark = None

    expected_dates = [date(2026, 9, 15), date(2026, 9, 16), date(2026, 9, 17)]
    actual_dates = dates_to_fetch(watermark, today, default_start)

    assert actual_dates == expected_dates

def test_watermark_date_is_refetched():
    wm = Watermark(settlement_date=date(2026, 9, 15), settlement_period=23)
    today = date(2026, 9, 17)
    default_start = date(2026, 9, 1)
    expected_dates = [date(2026, 9, 15), date(2026, 9, 16), date(2026, 9, 17)]
    actual_dates = dates_to_fetch(wm, today, default_start)
    assert actual_dates == expected_dates

def test_up_to_date_still_refetches_today():
    wm = Watermark(settlement_date=date(2026, 9, 17), settlement_period=23)
    today = date(2026, 9, 17)
    default_start = date(2026, 9, 1)
    expected_dates = [date(2026, 9, 17)]
    actual_dates = dates_to_fetch(wm, today, default_start)
    assert actual_dates == expected_dates

def test_future_watermark_returns_empty():
    wm = Watermark(settlement_date=date(2026, 9, 18), settlement_period=23)
    today = date(2026, 9, 17)
    default_start = date(2026, 9, 1)
    expected_dates = []
    actual_dates = dates_to_fetch(wm, today, default_start)
    assert actual_dates == expected_dates