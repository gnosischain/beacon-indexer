import logging
import sys

def setup_logger(name, log_level=logging.INFO):
    """Set up a logger with the given name and level."""
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    
    # Create a handler that outputs to stdout
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(log_level)
    
    # Format logs
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    handler.setFormatter(formatter)
    
    # Add the handler to the logger
    logger.addHandler(handler)
    
    return logger

# Create a default logger
logger = setup_logger('beacon_scraper')