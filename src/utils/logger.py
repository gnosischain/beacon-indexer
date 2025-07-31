import structlog
import logging
import sys
import os
from src.config import config

def setup_logger():
    """Configure logging - force human readable format in containers too."""
    
    # Force human-readable format everywhere
    # You can change this by setting FORCE_JSON_LOGS=true in .env if needed
    force_json = os.getenv("FORCE_JSON_LOGS", "false").lower() == "true"
    
    # Configure standard library logging
    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL),
        format="%(message)s"
    )
    
    if force_json:
        # JSON format only if explicitly requested
        processors = [
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer()
        ]
    else:
        # Human readable format (default)
        processors = [
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
            simple_console_renderer
        ]
    
    structlog.configure(
        processors=processors,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

def simple_console_renderer(logger, method_name, event_dict):
    """Simple console renderer for development."""
    timestamp = event_dict.pop("timestamp", "")
    level = event_dict.pop("level", "INFO").upper()
    event = event_dict.pop("event", "")
    
    # Format main message  
    msg = f"{timestamp} [{level:<5}] {event}"
    
    # Add context if present (but keep it concise)
    if event_dict:
        # Skip logger field and prioritize important fields
        important_fields = ["worker", "loader", "start_slot", "end_slot", "chunk_number", "success_count"]
        context_parts = []
        
        # Add important fields first
        for field in important_fields:
            if field in event_dict:
                context_parts.append(f"{field}={event_dict.pop(field)}")
        
        # Add remaining fields (except logger, stack, exception)
        for k, v in event_dict.items():
            if k not in ["logger", "stack", "exception"]:
                context_parts.append(f"{k}={v}")
        
        if context_parts:
            msg += f" | {' '.join(context_parts)}"
    
    return msg

logger = structlog.get_logger()