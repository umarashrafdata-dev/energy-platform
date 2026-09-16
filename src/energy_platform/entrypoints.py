import logging
from datetime import date

from energy_platform.logging_setup import configure_logging

logger = logging.getLogger(__name__)

def hello_job():
    """Test Entrypoint: proves wheel is alive and kicking"""
    configure_logging(logging.INFO)

    from energy_platform.settlements import settlement_period_start

    start = settlement_period_start(date(2026,10,25),50)
    logger.info("energy-platform is alive and kicking")
    logger.info("Period 50 on the long day starts at %s", start)
    