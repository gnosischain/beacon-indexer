import logging
import sys

def setup_logger(name, log_level=logging.INFO):
    """Set up a logger with the given name and level.
    
    Args:
        name: Logger name
        log_level: Can be either:
            - A logging level constant (e.g., logging.INFO)
            - A string level name (e.g., "INFO", "DEBUG")
            - A numeric level (e.g., 20 for INFO)
            - A string containing a number (e.g., "20")
    """
    logger = logging.getLogger(name)
    
    # Clear any existing handlers to prevent duplicates
    if logger.handlers:
        logger.handlers = []
    
    # Handle different log level formats
    if isinstance(log_level, str):
        # Check if it's a numeric string like "20"
        try:
            numeric_level = int(log_level)
            log_level = numeric_level
        except ValueError:
            # It's a string level name like "INFO", convert to logging constant
            try:
                log_level = getattr(logging, log_level.upper())
            except AttributeError:
                # Default to INFO if unknown level
                log_level = logging.INFO
    
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
    
    # Prevent propagation to the root logger to avoid duplicate logs
    logger.propagate = False
    
    return logger

# Create a default logger
logger = setup_logger('beacon_scraper')