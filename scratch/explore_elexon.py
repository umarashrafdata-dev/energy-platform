"""Not part of the main codebase, just a scratch file to explore Elexon data."""

import json
import logging
import requests

from energy_platform.logging_setup import configure_logging

configure_logging()

logger = logging.getLogger(__name__)

base_url = "https://data.elexon.co.uk/bmrs/api/v1"

response = requests.get(f"{base_url}/balancing/settlement/system-prices/2025-03-30", timeout=30)
logger.info(f"Response: {response.status_code}")
payload = response.json()

logger.info("top level keys: %s", list(payload.keys()))
rows= payload.get("data",payload)
logger.info("Number of rows: %d", len(rows))
logger.info("First row: %s", rows[0] if rows else "No data")

# type census

types = {k: type(v).__name__ for k, v in rows[0].items()} if rows else {}
logger.info("Types in first row: %s", types)
