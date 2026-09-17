from datetime import date

import pytest
import requests

from energy_platform.elexon import fetch_system_prices, ElexonServerError

class StubResponse:
    """A stub response object to simulate requests.Response."""
    def __init__(self, status_code: int, json_data: list | None=None):
        self.status_code = status_code
        self._json_data = json_data or []

    def json(self):
        return {"metadata": {}, "data": self._json_data}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(f"HTTP {self.status_code}")

class StubSession:
    """A stub session object to simulate requests.Session."""
    def __init__(self, response: list):
        self.response = response
        self.calls = 0

    def get(self, url: str, timeout=None):
        response = self.response[self.calls] if self.calls < len(self.response) else StubResponse(500)
        self.calls += 1
        return response


def test_happy_path_returns_rows():
    rows = [{"settlementDate": "2026-09-14", "settlementPeriod": 1}]
    session = StubSession([StubResponse(200, rows)])
    assert fetch_system_prices(date(2026, 9, 14), session=session) == rows


def test_empty_day_is_normal():
    session = StubSession([StubResponse(200, [])])
    assert fetch_system_prices(date(2026, 9, 30), session=session) == []


def test_400_fails_immediately_no_retry():
    session = StubSession([StubResponse(400)])
    with pytest.raises(requests.exceptions.HTTPError):
        fetch_system_prices(date(2026, 9, 14), session=session)
    assert session.calls == 1  # the contract clause: no retrying our bugs


def test_500_retries_then_succeeds():
    rows = [{"settlementPeriod": 1}]
    session = StubSession([StubResponse(500), StubResponse(500), StubResponse(200, rows)])
    assert fetch_system_prices(date(2026, 9, 14), session=session) == rows
    assert session.calls == 3