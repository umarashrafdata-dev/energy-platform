from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

london_tz = ZoneInfo("Europe/London")

def settlement_period_start(settlement_date: date, period: int) -> datetime:
    """
    Start time of a GB settlement period, as an aware Europe/London datetime.

    Args:
        settlement_date (date): The date of the settlement.
        period (int): The settlement period number (1-50).

    Returns:
        datetime: The start time of the settlement period.
    """
    if not 1 <= period <= 50:
        raise ValueError("Settlement period must be between 1 and 50.")

    midnight_local = datetime.combine(settlement_date, datetime.min.time(), tzinfo=london_tz)
    start_time = midnight_local.astimezone(timezone.utc) + timedelta(minutes=(period - 1) * 30)
    return start_time.astimezone(london_tz)