from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

import energy_platform.settlements as st

london_tz = ZoneInfo("Europe/London")

def test_period_1_is_midnight():
    settlement_date = date(2026,9,15)
    period = 1
    expected_start_time = datetime(2026, 9, 15, 0, 0, tzinfo=london_tz)
    assert st.settlement_period_start(settlement_date, period) == expected_start_time

def test_period_10_is_4_30_am():
    settlement_date = date(2026,9,15)
    period = 10
    expected_start_time = datetime(2026, 9, 15, 4, 30, tzinfo=london_tz)
    assert st.settlement_period_start(settlement_date, period) == expected_start_time

def test_period_50_is_23_30_pm():
    settlement_date = date(2026,10,25)
    period = 50
    expected_start_time = datetime(2026, 10, 25, 23, 30, tzinfo=london_tz)
    assert st.settlement_period_start(settlement_date, period) == expected_start_time

def test_invalid_period_raises_value_error():
    settlement_date = date(2026,9,15)
    invalid_periods = [0, 51, -1, 100]
    for period in invalid_periods:
        with pytest.raises(ValueError):
            st.settlement_period_start(settlement_date, period)