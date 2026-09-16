import logging
import sys

def configure_logging(level:int = logging.INFO) -> None:
    """
    Configures the logging settings for the application.

    Args:
        level (int): The logging level (e.g., logging.INFO, logging.DEBUG).
        log_file (str): Optional. If provided, logs will be written to this file.
    """
    # Create a logger
    logger = logging.getLogger()
    logger.setLevel(level)
    logger.handlers.clear()  # Clear existing handlers 

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)