import os
from pydantic import BaseModel, Field
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

class BeaconNodeConfig(BaseModel):
    url: str = Field(default=os.getenv("BEACON_NODE_URL", "http://localhost:5052"))

class ClickHouseConfig(BaseModel):
    host: str = Field(default=os.getenv("CLICKHOUSE_HOST", "localhost"))
    port: int = Field(default=int(os.getenv("CLICKHOUSE_PORT", "9000")))
    user: str = Field(default=os.getenv("CLICKHOUSE_USER", "default"))
    password: str = Field(default=os.getenv("CLICKHOUSE_PASSWORD", ""))
    database: str = Field(default=os.getenv("CLICKHOUSE_DATABASE", "beacon_chain"))
    
    # Add secure and verify attributes
    @property
    def secure(self) -> bool:
        """Determine if connection should be secure based on port or env var."""
        # First check explicit env var
        env_secure = os.getenv("CLICKHOUSE_SECURE")
        if env_secure is not None:
            return env_secure.lower() in ("true", "1", "yes")
        # Otherwise determine based on port
        return self.port == 443 or self.port == 8443
    
    @property
    def verify(self) -> bool:
        """Determine if SSL certificates should be verified."""
        # Default to False for self-signed certificates
        return os.getenv("CLICKHOUSE_VERIFY", "false").lower() in ("true", "1", "yes")

class ScraperConfig(BaseModel):
    mode: str = Field(default=os.getenv("SCRAPER_MODE", "realtime"))
    historical_start_slot: int = Field(default=int(os.getenv("HISTORICAL_START_SLOT", "0")))
    historical_end_slot: Optional[int] = Field(
        default=int(os.getenv("HISTORICAL_END_SLOT", "0")) if os.getenv("HISTORICAL_END_SLOT") else None
    )
    batch_size: int = Field(default=int(os.getenv("BATCH_SIZE", "1000")))
    max_concurrent_requests: int = Field(default=int(os.getenv("MAX_CONCURRENT_REQUESTS", "50")))
    log_level: int = Field(default=int(os.getenv("LOG_LEVEL", "20")))  # Default to INFO level (20)
    enabled_scrapers: str = Field(default=os.getenv("ENABLED_SCRAPERS", "core_block,operational_events,validator"))
    
    # Parallel mode settings
    parallel_workers: int = Field(default=int(os.getenv("PARALLEL_WORKERS", "4")))
    state_dir: str = Field(default="./state")
    
    # Performance settings
    request_timeout: int = Field(default=int(os.getenv("REQUEST_TIMEOUT", "30")))
    retry_delay: int = Field(default=int(os.getenv("RETRY_DELAY", "5")))
    max_retries: int = Field(default=int(os.getenv("MAX_RETRIES", "3")))
    
    # State management
    state_range_size: int = Field(default=int(os.getenv("STATE_RANGE_SIZE", "50000")))
    state_update_batch_size: int = Field(default=int(os.getenv("STATE_UPDATE_BATCH_SIZE", "1000")))
    max_retry_attempts: int = Field(default=int(os.getenv("MAX_RETRY_ATTEMPTS", "3")))
    stale_job_timeout_minutes: int = Field(default=int(os.getenv("STALE_JOB_TIMEOUT_MINUTES", "30")))
    enable_state_caching: bool = Field(default=os.getenv("ENABLE_STATE_CACHING", "true").lower() == "true")
    state_cache_size: int = Field(default=int(os.getenv("STATE_CACHE_SIZE", "10000")))
    gap_check_interval_seconds: int = Field(default=int(os.getenv("GAP_CHECK_INTERVAL_SECONDS", "300")))
    
    # Worker pool configuration (used in parallel mode)
    worker_pool_size: int = Field(default=int(os.getenv("WORKER_POOL_SIZE", "8")))
    worker_batch_size: int = Field(default=int(os.getenv("WORKER_BATCH_SIZE", "500")))
    worker_queue_size: int = Field(default=int(os.getenv("WORKER_QUEUE_SIZE", "24")))
    connection_pool_size: int = Field(default=int(os.getenv("CONNECTION_POOL_SIZE", "16")))
    worker_pool_monitoring_interval: int = Field(default=int(os.getenv("WORKER_POOL_MONITORING_INTERVAL", "10")))
    enable_worker_pool: bool = Field(default=os.getenv("ENABLE_WORKER_POOL", "true").lower() == "true")
    
    # Worker progress logging
    worker_progress_log_interval: int = Field(default=int(os.getenv("WORKER_PROGRESS_LOG_INTERVAL", "100")))
    
    # Bulk operations
    state_bulk_insert_batch_size: int = Field(default=int(os.getenv("STATE_BULK_INSERT_BATCH_SIZE", "5000")))
    state_bulk_check_batch_size: int = Field(default=int(os.getenv("STATE_BULK_CHECK_BATCH_SIZE", "5000")))
    state_bulk_log_interval: int = Field(default=int(os.getenv("STATE_BULK_LOG_INTERVAL", "10000")))
    
    # Bulk insertion service
    bulk_insert_queue_size: int = Field(default=int(os.getenv("BULK_INSERT_QUEUE_SIZE", "10000")))
    bulk_insert_max_wait_time: int = Field(default=int(os.getenv("BULK_INSERT_MAX_WAIT_TIME", "5")))
    bulk_insert_batch_size: int = Field(default=int(os.getenv("BULK_INSERT_BATCH_SIZE", "5000")))

class Config(BaseModel):
    beacon_node: BeaconNodeConfig = Field(default_factory=BeaconNodeConfig)
    clickhouse: ClickHouseConfig = Field(default_factory=ClickHouseConfig)
    scraper: ScraperConfig = Field(default_factory=ScraperConfig)

# Create the config object
config = Config()

# Default scraper combinations
DEFAULT_SCRAPER_SETS = {
    # Single-purpose sets
    "minimal": ["core_block"],
    "basic": ["core_block", "operational_events", "validator", "transaction"],
    "validators_only": ["validator"],
    "rewards_only": ["reward"],
    "attestations_only": ["attestation"],
    
    # Additional single-purpose sets
    "slashing_monitoring": ["slashing", "operational_events"],
    "blob_data": ["blob"],
    "specs_update": ["specs"],
    
    # Complete data collection
    "extended": ["core_block", "attestation", "operational_events", "validator", "reward", "slashing", "transaction"],
    "full": ["core_block", "attestation", "operational_events", "validator", "reward", "slashing", "transaction", "blob"],
    
    # Legacy compatibility
    "legacy": ["block"],
    "legacy_full": ["block", "validator", "reward", "blob", "specs"]
}

def get_enabled_scrapers(scraper_set: Optional[str] = None, custom_scrapers: Optional[str] = None) -> list:
    """
    Get the list of enabled scrapers based on scraper set or custom list.
    
    Args:
        scraper_set: Name of a predefined scraper set
        custom_scrapers: Comma-separated list of custom scrapers
    
    Returns:
        List of enabled scraper names
    """
    if scraper_set and scraper_set in DEFAULT_SCRAPER_SETS:
        return DEFAULT_SCRAPER_SETS[scraper_set]
    elif custom_scrapers:
        return [s.strip() for s in custom_scrapers.split(',') if s.strip()]
    else:
        # Default to the configured enabled scrapers
        return [s.strip() for s in config.scraper.enabled_scrapers.split(',') if s.strip()]