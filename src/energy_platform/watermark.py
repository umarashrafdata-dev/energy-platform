"""Watermarking logic"""
from dataclasses import dataclass
from datetime import datetime, date, timedelta

@dataclass(frozen=True)
class Watermark:
    settlement_date: date
    settlement_period: int

def dates_to_fetch(
        watermark: Watermark | None,
        today: date,
        default_start: date,
) -> list[date]:
    """Determine which dates to fetch based on the watermark and today's date."""
    start = default_start if watermark is None else watermark.settlement_date
    if start > today:
        return []
    return [start + timedelta(days=i) for i in range((today - start).days + 1)]