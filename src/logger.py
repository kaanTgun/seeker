import logging
import sys

def setup_logger(name, level=logging.INFO):
    """Sets up a custom logger."""
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    logger.propagate = False  # Prevent duplicate logs in parent loggers

    return logger

# Example of a default logger that can be imported
# To use in other modules: from .logger import log
# log.info("This is an info message")
log = setup_logger(__name__)
