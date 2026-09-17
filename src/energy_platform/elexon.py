import logging
from datetime import date
import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

base_url = "https://data.elexon.co.uk/bmrs/api/v1"
_timeout = 30

class ElexonServerError(Exception):
    """Custom exception for Elexon server errors."""

@retry(
    retry=retry_if_exception_type((ElexonServerError, requests.exceptions.ConnectionError, requests.exceptions.Timeout)),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    reraise=True
)
def fetch_system_prices(settlement_date: date, session: requests.Session | None = None) -> list[dict]:
    """Fetch system prices from Elexon for a given settlement date."""
    url = f"{base_url}/balancing/settlement/system-prices/{settlement_date.isoformat()}"
    logger.info("Fetching system prices for %s", settlement_date)
    session = session or requests.Session()
    response = session.get(url, timeout=_timeout)
    
    if response.status_code >= 500:
        raise ElexonServerError(f"Server error {response.status_code} for URL: {url}")
    
    response.raise_for_status()
    payload = response.json().get("data", [])
    logger.info("Received %d rows for %s", len(payload), settlement_date)
    return payload

