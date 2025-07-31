import os
from typing import List, Optional
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Beacon Node
    BEACON_NODE_URL = os.getenv("BEACON_NODE_URL", "http://localhost:5052")
    
    # ClickHouse
    CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
    CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "9000"))
    CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
    CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
    CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "beacon_chain")
    CLICKHOUSE_SECURE = os.getenv("CLICKHOUSE_SECURE", "false").lower() == "true"
    
    # Loaders - ensure all loaders are enabled by default
    ENABLED_LOADERS = os.getenv("ENABLED_LOADERS", "blocks,validators,specs,genesis").split(",")
    
    # Validator Configuration
    VALIDATOR_MODE = os.getenv("VALIDATOR_MODE", "daily")  # daily or all_slots
    
    # Backfill Configuration
    START_SLOT = int(os.getenv("START_SLOT", "0"))
    END_SLOT = int(os.getenv("END_SLOT", "0")) if os.getenv("END_SLOT") else None
    BACKFILL_WORKERS = int(os.getenv("BACKFILL_WORKERS", "4"))
    CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "1000"))
    
    # Logging
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

config = Config()